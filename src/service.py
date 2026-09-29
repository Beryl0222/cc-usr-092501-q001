"""返还后入藏责任链领域服务。

围绕同一件文物持续追加事实：运输封签、现场点交、材质检测、病害报告、
来源档案、权利限制、衍生申请、保管交接、入藏决定与公开目录。

关键规则：
- 封签回执按（回执编号、封签、状态摘要）判重；冲突回执隔离复核，重传回原结果。
- 检测发现风险时只冻结受影响文物及其衍生申请，不波及其它文物。
- 实体转库与责任人变更是单事务原子交接，任何一刻只有一个有效保管方。
- 修复人员、来源研究人员、入藏批准人各自只能完成职责内动作。
- 争议材料不得进入公开目录；已发布快照不被后续鉴定静默改写。
- 撤销错误入藏分配以新事件留痕，原决定仍可追溯。
"""

from __future__ import annotations

import functools
import hashlib
import json
import threading
import uuid
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Callable, Iterator

from .events import (
    ACCESSION_DECIDED,
    ACCESSION_REVOKED,
    APPLICATION_APPROVED,
    APPLICATION_HELD,
    APPLICATION_REJECTED,
    APPLICATION_RESUMED,
    APPLICATION_SUBMITTED,
    BATCH_REGISTERED,
    CATALOG_RELEASED,
    CUSTODY_TRANSFERRED,
    DISEASE_REPORTED,
    DISPUTE_CLEARED,
    DISPUTE_RECORDED,
    DomainError,
    Event,
    MATERIAL_TESTED,
    OBJECT_FROZEN,
    OBJECT_HANDED_OVER,
    OBJECT_UNFROZEN,
    PROVENANCE_RECORDED,
    RESTRICTION_LIFTED,
    RESTRICTION_RECORDED,
    SEAL_RECEIPT_QUARANTINED,
    SEAL_RECEIPT_RECORDED,
    SEAL_RECEIPT_RESOLVED,
)
from .store import EventStore

SCOPES = ("conservation", "research", "public_display")
RISK_SEVERITIES = {"high", "medium"}

# 职责矩阵：动作 -> 允许的角色。
PERMISSIONS: dict[str, frozenset[str]] = {
    "register_batch": frozenset({"registry"}),
    "resolve_seal": frozenset({"registry"}),
    "record_handover": frozenset({"handover_clerk"}),
    "record_material": frozenset({"conservator"}),
    "report_disease": frozenset({"conservator"}),
    "clear_risk": frozenset({"conservator"}),
    "record_provenance": frozenset({"provenance_researcher"}),
    "record_dispute": frozenset({"provenance_researcher"}),
    "clear_dispute": frozenset({"provenance_researcher"}),
    "record_restriction": frozenset({"approver"}),
    "lift_restriction": frozenset({"approver"}),
    "transfer_custody": frozenset({"approver"}),
    "decide_application": frozenset({"approver"}),
    "decide_accession": frozenset({"approver"}),
    "revoke_accession": frozenset({"approver"}),
    "release_catalog": frozenset({"approver"}),
}

# 衍生申请种类 -> 提交人角色与所需可用范围。
APPLICATION_KINDS: dict[str, tuple[str, str]] = {
    "conservation": ("conservator", "conservation"),
    "research": ("provenance_researcher", "research"),
    "public_display": ("curator", "public_display"),
}


def _now() -> str:
    return datetime.now().astimezone().isoformat()


def _parse_time(value: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as error:
        raise DomainError("occurred_at 必须是 ISO 8601 时间") from error
    if parsed.tzinfo is None:
        raise DomainError("occurred_at 必须包含时区")
    return parsed


@dataclass
class Receipt:
    receipt_id: str
    seal: str
    summary: str
    status: str  # accepted / quarantined / rejected / superseded
    recorded_event_id: str
    conflict_with: str | None = None
    resolution_note: str | None = None
    # 同编号重传内容不一致时，重传作为候选隔离，原封签保持有效待裁决。
    candidate: dict[str, Any] | None = None


@dataclass
class Application:
    app_id: str
    object_no: str
    kind: str
    applicant: str
    status: str  # submitted / held / approved / rejected
    note: str
    history: list[dict[str, Any]] = field(default_factory=list)
    held_from: str | None = None  # 冻结挂起前的状态，解冻时据此恢复


@dataclass
class ObjectState:
    object_no: str
    batch_id: str
    stage: str = "expected"  # expected / handed_over / accessioned
    receipts: dict[str, Receipt] = field(default_factory=dict)
    open_quarantine: set[str] = field(default_factory=set)
    accepted_seal_event_id: str | None = None
    materials: list[dict[str, Any]] = field(default_factory=list)
    diseases: list[dict[str, Any]] = field(default_factory=list)
    frozen: bool = False
    freeze_reasons: list[str] = field(default_factory=list)
    provenance: list[dict[str, Any]] = field(default_factory=list)
    disputed: bool = False
    dispute_reasons: list[str] = field(default_factory=list)
    restrictions: dict[str, str] = field(default_factory=dict)  # scope -> reason
    custodian: str | None = None
    location: str | None = None
    applications: dict[str, Application] = field(default_factory=dict)
    accession: dict[str, Any] | None = None
    accession_history: list[dict[str, Any]] = field(default_factory=list)
    catalog_versions: list[int] = field(default_factory=list)


@dataclass
class BatchState:
    batch_id: str
    foreign_authority: str
    expected: set[str] = field(default_factory=set)
    arrived: set[str] = field(default_factory=set)
    handover_event_ids: list[str] = field(default_factory=list)


class AccessionService:
    """命令与查询都在进程内串行化；存储只增，重启时回放恢复。"""

    def __init__(
        self,
        store: EventStore,
        clock: Callable[[], str] = _now,
    ) -> None:
        self.store = store
        self._clock = clock
        self._lock = threading.RLock()
        self.batches: dict[str, BatchState] = {}
        self.objects: dict[str, ObjectState] = {}
        self.applications: dict[str, Application] = {}
        self.catalog: dict[int, dict[str, Any]] = {}
        self._versions: dict[tuple[str, str], int] = {}
        self._catalog_seq = 0
        for event in self.store.all_events():
            self._apply(event, replay=True)

        # 所有公开命令/查询统一在同一把可重入锁内串行执行。
        for _name in (
            "register_batch", "record_seal_receipt", "resolve_seal_receipt",
            "record_handover", "record_material", "report_disease", "clear_risk",
            "record_provenance", "record_dispute", "clear_dispute",
            "record_restriction", "lift_restriction", "transfer_custody",
            "submit_application", "decide_application", "decide_accession",
            "revoke_accession", "release_catalog", "trace", "batch_view",
            "catalog_view", "usable_scopes",
        ):
            _fn = getattr(type(self), _name)

            @functools.wraps(_fn)
            def _guarded(*args: Any, _fn: Callable[..., Any] = _fn, **kwargs: Any) -> Any:
                with self._lock:
                    return _fn(self, *args, **kwargs)

            setattr(self, _name, _guarded)

    @contextmanager
    def exclusive(self) -> Iterator[None]:
        """命令与查询的串行化区间；并发转库在此决出唯一有效保管方。"""
        self._lock.acquire()
        try:
            yield
        finally:
            self._lock.release()

    # ------------------------------------------------------------------ 基础

    def _authorize(self, action: str, role: str) -> None:
        if role not in PERMISSIONS[action]:
            raise DomainError(
                f"角色 {role} 无权执行 {action}（职责隔离）", status=403
            )

    def _require_object(self, object_no: str) -> ObjectState:
        obj = self.objects.get(object_no)
        if obj is None:
            raise DomainError(f"文物 {object_no} 不存在", status=404)
        return obj

    def _version(self, event: Event) -> None:
        key = (event.aggregate_type, event.aggregate_id)
        self._versions[key] = self._versions.get(key, 0) + 1
        object.__setattr__(event, "version", self._versions[key])

    def _make_event(
        self,
        event_type: str,
        aggregate_type: str,
        aggregate_id: str,
        actor: str,
        summary: str,
        payload: dict[str, Any],
        occurred_at: str | None = None,
        causation_id: str | None = None,
    ) -> Event:
        event = Event(
            event_id=uuid.uuid4().hex,
            event_type=event_type,
            aggregate_type=aggregate_type,
            aggregate_id=aggregate_id,
            occurred_at=occurred_at or self._clock(),
            version=0,
            actor=actor,
            summary=summary,
            payload=payload,
            causation_id=causation_id,
        )
        self._version(event)
        return event

    def _commit(self, events: Event | list[Event]) -> list[Event]:
        events = [events] if isinstance(events, Event) else events
        # 先编号再落库，落库与状态变更在同一把锁内；失败则不改变内存状态。
        self.store.append_all(events)
        for event in events:
            self._apply(event)
        return events

    # ------------------------------------------------------------------ 命令

    def register_batch(
        self,
        actor: str,
        role: str,
        batch_id: str,
        foreign_authority: str,
        object_numbers: list[str],
        occurred_at: str | None = None,
    ) -> list[Event]:
        """登记返还批次（12 件可为其中部分先到）。"""
        self._authorize("register_batch", role)
        if not object_numbers:
            raise DomainError("批次至少包含一件文物")
        if batch_id in self.batches:
            raise DomainError(f"批次 {batch_id} 已登记", status=409)
        if occurred_at:
            _parse_time(occurred_at)
        event = self._make_event(
            BATCH_REGISTERED,
            "return_batch",
            batch_id,
            actor,
            f"登记 {foreign_authority} 返还批次，应到 {len(object_numbers)} 件",
            {"foreign_authority": foreign_authority, "objects": list(object_numbers)},
            occurred_at,
        )
        return self._commit(event)

    def record_seal_receipt(
        self,
        actor: str,
        role: str,
        object_no: str,
        receipt_id: str,
        seal: str,
        summary: str,
        occurred_at: str | None = None,
    ) -> dict[str, Any]:
        """登记运输封签回执。

        - 与既有回执完全相同（编号/封签/摘要一致）：重传，只返回原结果。
        - 编号相同但封签或摘要不同，或与该物已采纳封签冲突：隔离待复核。
        """
        self._authorize("record_handover", role)
        obj = self._require_object(object_no)
        if not seal or not summary:
            raise DomainError("封签与状态摘要均不能为空")
        if occurred_at:
            _parse_time(occurred_at)

        existing = obj.receipts.get(receipt_id)
        if existing is not None:
            if existing.seal == seal and existing.summary == summary:
                return {
                    "result": "duplicate",
                    "receipt_id": receipt_id,
                    "original_event_id": existing.recorded_event_id,
                    "events": [],
                }
            # 重传内容不一致：重传作为候选隔离，原封签仍有效，等待复核裁决。
            if existing.candidate is not None:
                raise DomainError(f"回执 {receipt_id} 已有待复核候选，请先裁决", status=409)
            event = self._make_event(
                SEAL_RECEIPT_QUARANTINED,
                "returned_object",
                object_no,
                actor,
                f"封签回执 {receipt_id} 重传内容不一致，隔离复核",
                {
                    "receipt_id": receipt_id,
                    "seal": seal,
                    "summary": summary,
                    "conflict_with": existing.recorded_event_id,
                },
                occurred_at,
            )
            self._commit(event)
            return {"result": "quarantined", "receipt_id": receipt_id, "events": [event]}

        accepted = self._accepted_receipt(obj)
        if accepted is not None and (
            accepted.seal != seal or accepted.summary != summary
        ):
            event = self._make_event(
                SEAL_RECEIPT_QUARANTINED,
                "returned_object",
                object_no,
                actor,
                f"封签回执 {receipt_id} 与已采纳封签 {accepted.receipt_id} 不一致，隔离复核",
                {
                    "receipt_id": receipt_id,
                    "seal": seal,
                    "summary": summary,
                    "conflict_with": accepted.recorded_event_id,
                },
                occurred_at,
            )
            self._commit(event)
            return {"result": "quarantined", "receipt_id": receipt_id, "events": [event]}

        event = self._make_event(
            SEAL_RECEIPT_RECORDED,
            "returned_object",
            object_no,
            actor,
            f"登记运输封签回执 {receipt_id}",
            {"receipt_id": receipt_id, "seal": seal, "summary": summary},
            occurred_at,
        )
        self._commit(event)
        return {"result": "recorded", "receipt_id": receipt_id, "events": [event]}

    def resolve_seal_receipt(
        self,
        actor: str,
        role: str,
        receipt_id: str,
        outcome: str,
        note: str = "",
        occurred_at: str | None = None,
    ) -> list[Event]:
        """复核裁决隔离回执：accepted 采纳为当前封签，rejected 不予采纳。"""
        self._authorize("resolve_seal", role)
        if outcome not in ("accepted", "rejected"):
            raise DomainError("outcome 只能是 accepted 或 rejected")
        target = None
        for obj in self.objects.values():
            if receipt_id in obj.receipts:
                target = obj
                break
        if target is None:
            raise DomainError(f"回执 {receipt_id} 不存在", status=404)
        receipt = target.receipts[receipt_id]
        if receipt_id not in target.open_quarantine:
            raise DomainError(f"回执 {receipt_id} 不处于待复核状态", status=409)
        if occurred_at:
            _parse_time(occurred_at)
        event = self._make_event(
            SEAL_RECEIPT_RESOLVED,
            "returned_object",
            target.object_no,
            actor,
            f"封签回执 {receipt_id} 复核{('采纳' if outcome == 'accepted' else '驳回')}",
            {"receipt_id": receipt_id, "outcome": outcome, "note": note},
            occurred_at,
        )
        return self._commit(event)

    def record_handover(
        self,
        actor: str,
        role: str,
        batch_id: str,
        object_numbers: list[str],
        location: str,
        custodian: str,
        occurred_at: str | None = None,
    ) -> list[Event]:
        """现场点交（支持部分到货），并确立首个有效保管方与临时库房。"""
        self._authorize("record_handover", role)
        batch = self.batches.get(batch_id)
        if batch is None:
            raise DomainError(f"批次 {batch_id} 不存在", status=404)
        if not object_numbers:
            raise DomainError("点交清单不能为空")
        if not location or not custodian:
            raise DomainError("点交需要位置与保管责任人")
        unknown = [n for n in object_numbers if n not in batch.expected]
        if unknown:
            raise DomainError(f"文物不属于该批次：{unknown}")
        already = [n for n in object_numbers if n in batch.arrived]
        if already:
            raise DomainError(f"文物已完成点交：{already}", status=409)
        blocked = []
        for no in object_numbers:
            obj = self.objects[no]
            if obj.open_quarantine:
                blocked.append((no, "存在待复核封签"))
            elif self._accepted_receipt(obj) is None:
                blocked.append((no, "缺少有效封签回执"))
        if blocked:
            raise DomainError(f"不具备点交条件：{blocked}", status=409)
        if occurred_at:
            _parse_time(occurred_at)
        events = []
        for no in object_numbers:
            events.append(
                self._make_event(
                    OBJECT_HANDED_OVER,
                    "returned_object",
                    no,
                    actor,
                    f"现场点交完成，入临时库房 {location}，保管人 {custodian}",
                    {"batch_id": batch_id, "location": location, "custodian": custodian},
                    occurred_at,
                )
            )
        return self._commit(events)

    def record_material(
        self,
        actor: str,
        role: str,
        object_no: str,
        material: str,
        findings: str = "",
        occurred_at: str | None = None,
    ) -> list[Event]:
        """材质检测结果。检测可先于点交到达，沿同一对象挂接。"""
        self._authorize("record_material", role)
        obj = self._require_object(object_no)
        if occurred_at:
            _parse_time(occurred_at)
        event = self._make_event(
            MATERIAL_TESTED,
            "returned_object",
            object_no,
            actor,
            f"材质检测：{material}",
            {"material": material, "findings": findings},
            occurred_at,
        )
        return self._commit(event)

    def report_disease(
        self,
        actor: str,
        role: str,
        object_no: str,
        disease: str,
        severity: str,
        occurred_at: str | None = None,
    ) -> list[Event]:
        """病害报告；高/中风险即冻结该文物及其在途衍生申请，迟到结果同样生效。"""
        self._authorize("report_disease", role)
        obj = self._require_object(object_no)
        if severity not in ("high", "medium", "low"):
            raise DomainError("severity 只能是 high / medium / low")
        if occurred_at:
            _parse_time(occurred_at)
        events = [
            self._make_event(
                DISEASE_REPORTED,
                "returned_object",
                object_no,
                actor,
                f"病害报告：{disease}（{severity}）",
                {"disease": disease, "severity": severity},
                occurred_at,
            )
        ]
        if severity in RISK_SEVERITIES and not obj.frozen:
            reason = f"病害风险：{disease}（{severity}）"
            events.append(
                self._make_event(
                    OBJECT_FROZEN,
                    "returned_object",
                    object_no,
                    actor,
                    f"检测发现风险，冻结文物：{reason}",
                    {"reason": reason, "disease": disease, "severity": severity},
                    occurred_at,
                    causation_id=events[0].event_id,
                )
            )
            for app in obj.applications.values():
                if app.status in ("submitted", "approved"):
                    events.append(
                        self._make_event(
                            APPLICATION_HELD,
                            "application",
                            app.app_id,
                            actor,
                            f"文物冻结，衍生申请 {app.app_id}（{app.kind}）挂起",
                            {"object_no": object_no, "reason": reason,
                             "from_status": app.status},
                            occurred_at,
                            causation_id=events[-1].event_id,
                        )
                    )
        return self._commit(events)

    def clear_risk(
        self,
        actor: str,
        role: str,
        object_no: str,
        note: str,
        occurred_at: str | None = None,
    ) -> list[Event]:
        """复检确认风险解除：解冻文物并恢复被连带挂起的申请。"""
        self._authorize("clear_risk", role)
        obj = self._require_object(object_no)
        if not obj.frozen:
            raise DomainError("文物当前未被冻结", status=409)
        if occurred_at:
            _parse_time(occurred_at)
        events = [
            self._make_event(
                OBJECT_UNFROZEN,
                "returned_object",
                object_no,
                actor,
                f"复检风险解除：{note}",
                {"note": note},
                occurred_at,
            )
        ]
        for app in obj.applications.values():
            if app.status == "held":
                events.append(
                    self._make_event(
                        APPLICATION_RESUMED,
                        "application",
                        app.app_id,
                        actor,
                        f"文物解冻，恢复申请 {app.app_id}",
                        {"object_no": object_no},
                        occurred_at,
                        causation_id=events[0].event_id,
                    )
                )
        return self._commit(events)

    def record_provenance(
        self,
        actor: str,
        role: str,
        object_no: str,
        record: str,
        occurred_at: str | None = None,
    ) -> list[Event]:
        self._authorize("record_provenance", role)
        obj = self._require_object(object_no)
        if not record:
            raise DomainError("来源记录不能为空")
        if occurred_at:
            _parse_time(occurred_at)
        event = self._make_event(
            PROVENANCE_RECORDED,
            "returned_object",
            object_no,
            actor,
            f"追加来源档案：{record[:40]}",
            {"record": record},
            occurred_at,
        )
        return self._commit(event)

    def record_dispute(
        self,
        actor: str,
        role: str,
        object_no: str,
        reason: str,
        occurred_at: str | None = None,
    ) -> list[Event]:
        """标记争议；争议材料不得进入公开目录（已发布快照不受影响）。"""
        self._authorize("record_dispute", role)
        obj = self._require_object(object_no)
        if not reason:
            raise DomainError("争议理由不能为空")
        if occurred_at:
            _parse_time(occurred_at)
        event = self._make_event(
            DISPUTE_RECORDED,
            "returned_object",
            object_no,
            actor,
            f"标记来源/权利争议：{reason[:40]}",
            {"reason": reason},
            occurred_at,
        )
        return self._commit(event)

    def clear_dispute(
        self,
        actor: str,
        role: str,
        object_no: str,
        note: str,
        occurred_at: str | None = None,
    ) -> list[Event]:
        self._authorize("clear_dispute", role)
        obj = self._require_object(object_no)
        if not obj.disputed:
            raise DomainError("文物当前无争议标记", status=409)
        if occurred_at:
            _parse_time(occurred_at)
        event = self._make_event(
            DISPUTE_CLEARED,
            "returned_object",
            object_no,
            actor,
            f"争议澄清：{note[:40]}",
            {"note": note},
            occurred_at,
        )
        return self._commit(event)

    def record_restriction(
        self,
        actor: str,
        role: str,
        object_no: str,
        scope: str,
        reason: str,
        occurred_at: str | None = None,
    ) -> list[Event]:
        """登记权利限制（修复/研究/公开展示范围之一）。"""
        self._authorize("record_restriction", role)
        obj = self._require_object(object_no)
        if scope not in SCOPES:
            raise DomainError(f"限制范围只能是 {SCOPES}")
        if occurred_at:
            _parse_time(occurred_at)
        event = self._make_event(
            RESTRICTION_RECORDED,
            "returned_object",
            object_no,
            actor,
            f"登记权利限制（{scope}）：{reason[:40]}",
            {"scope": scope, "reason": reason},
            occurred_at,
        )
        return self._commit(event)

    def lift_restriction(
        self,
        actor: str,
        role: str,
        object_no: str,
        scope: str,
        occurred_at: str | None = None,
    ) -> list[Event]:
        self._authorize("lift_restriction", role)
        obj = self._require_object(object_no)
        if scope not in obj.restrictions:
            raise DomainError(f"范围 {scope} 当前没有生效中的限制", status=409)
        if occurred_at:
            _parse_time(occurred_at)
        event = self._make_event(
            RESTRICTION_LIFTED,
            "returned_object",
            object_no,
            actor,
            f"解除权利限制（{scope}）",
            {"scope": scope},
            occurred_at,
        )
        return self._commit(event)

    def transfer_custody(
        self,
        actor: str,
        role: str,
        object_no: str,
        from_location: str,
        from_custodian: str,
        to_location: str,
        to_custodian: str,
        occurred_at: str | None = None,
    ) -> list[Event]:
        """实体转库与责任人变更：单事件原子交接。

        调用方必须指认当前有效保管方；并发转库时失配方收到 409，
        保证任何一刻只有一个有效保管方。
        """
        self._authorize("transfer_custody", role)
        obj = self._require_object(object_no)
        if obj.custodian is None:
            raise DomainError("文物尚未点交，无有效保管方", status=409)
        if obj.location != from_location or obj.custodian != from_custodian:
            raise DomainError(
                f"当前保管方为 {obj.custodian}@{obj.location}，"
                f"与指认的 {from_custodian}@{from_location} 不符",
                status=409,
            )
        if not to_location or not to_custodian:
            raise DomainError("转入位置与新保管责任人不能为空")
        if to_location == from_location and to_custodian == from_custodian:
            raise DomainError("交接双方不能完全相同")
        if occurred_at:
            _parse_time(occurred_at)
        event = self._make_event(
            CUSTODY_TRANSFERRED,
            "returned_object",
            object_no,
            actor,
            f"保管原子交接：{from_custodian}@{from_location}"
            f" → {to_custodian}@{to_location}",
            {
                "from_location": from_location,
                "from_custodian": from_custodian,
                "to_location": to_location,
                "to_custodian": to_custodian,
            },
            occurred_at,
        )
        return self._commit(event)

    def submit_application(
        self,
        actor: str,
        role: str,
        object_no: str,
        kind: str,
        note: str = "",
        app_id: str | None = None,
        occurred_at: str | None = None,
    ) -> list[Event]:
        if kind not in APPLICATION_KINDS:
            raise DomainError(f"申请种类只能是 {tuple(APPLICATION_KINDS)}")
        required_role, _ = APPLICATION_KINDS[kind]
        if role != required_role:
            raise DomainError(
                f"{kind} 申请只能由 {required_role} 发起（职责隔离）", status=403
            )
        obj = self._require_object(object_no)
        if obj.stage == "expected":
            raise DomainError("文物尚未点交，不能提交申请", status=409)
        if occurred_at:
            _parse_time(occurred_at)
        app_id = app_id or f"app-{uuid.uuid4().hex[:12]}"
        if app_id in self.applications:
            raise DomainError(f"申请 {app_id} 已存在", status=409)
        initial = "held" if obj.frozen else "submitted"
        event = self._make_event(
            APPLICATION_SUBMITTED,
            "application",
            app_id,
            actor,
            f"提交{kind}申请" + ("（文物冻结中，先行挂起）" if initial == "held" else ""),
            {"object_no": object_no, "kind": kind, "note": note, "initial": initial},
            occurred_at,
        )
        return self._commit(event)

    def decide_application(
        self,
        actor: str,
        role: str,
        app_id: str,
        decision: str,
        note: str = "",
        occurred_at: str | None = None,
    ) -> list[Event]:
        self._authorize("decide_application", role)
        app = self.applications.get(app_id)
        if app is None:
            raise DomainError(f"申请 {app_id} 不存在", status=404)
        if decision not in ("approved", "rejected"):
            raise DomainError("decision 只能是 approved 或 rejected")
        if app.status not in ("submitted",):
            raise DomainError(f"申请当前状态为 {app.status}，不能审批", status=409)
        obj = self.objects[app.object_no]
        _, scope = APPLICATION_KINDS[app.kind]
        if decision == "approved" and scope not in self.usable_scopes(obj):
            raise DomainError(
                f"{scope} 当前不在可用范围（冻结/争议/权利限制/未入藏），不能批准",
                status=409,
            )
        if occurred_at:
            _parse_time(occurred_at)
        event = self._make_event(
            APPLICATION_APPROVED if decision == "approved" else APPLICATION_REJECTED,
            "application",
            app_id,
            actor,
            f"申请 {app_id} 审批{('通过' if decision == 'approved' else '驳回')}：{note[:40]}",
            {"object_no": app.object_no, "kind": app.kind, "decision": decision, "note": note},
            occurred_at,
        )
        return self._commit(event)

    def decide_accession(
        self,
        actor: str,
        role: str,
        object_no: str,
        collection: str,
        accession_number: str,
        occurred_at: str | None = None,
    ) -> list[Event]:
        """入藏批准；冻结、未决争议或封签隔离未清不得入藏。"""
        self._authorize("decide_accession", role)
        obj = self._require_object(object_no)
        if obj.stage == "expected":
            raise DomainError("文物尚未点交，不能入藏", status=409)
        if obj.frozen:
            raise DomainError("文物处于冻结状态，不能入藏", status=409)
        if obj.disputed:
            raise DomainError("文物存在未决争议，不能入藏", status=409)
        if obj.open_quarantine:
            raise DomainError("存在待复核封签，不能入藏", status=409)
        if obj.accession and obj.accession["active"]:
            raise DomainError("文物已有生效入藏分配", status=409)
        if not collection or not accession_number:
            raise DomainError("馆藏单位与入藏编号不能为空")
        if occurred_at:
            _parse_time(occurred_at)
        event = self._make_event(
            ACCESSION_DECIDED,
            "returned_object",
            object_no,
            actor,
            f"批准入藏 {collection}，编号 {accession_number}",
            {
                "collection": collection,
                "accession_number": accession_number,
                "custodian": obj.custodian,
                "location": obj.location,
            },
            occurred_at,
        )
        return self._commit(event)

    def revoke_accession(
        self,
        actor: str,
        role: str,
        object_no: str,
        reason: str,
        occurred_at: str | None = None,
    ) -> list[Event]:
        """撤销错误分配：新事件记录前后责任，原决定保留可追溯。"""
        self._authorize("revoke_accession", role)
        obj = self._require_object(object_no)
        if not obj.accession or not obj.accession["active"]:
            raise DomainError("文物没有生效中的入藏分配", status=409)
        if not reason:
            raise DomainError("撤销必须说明理由")
        if occurred_at:
            _parse_time(occurred_at)
        before = dict(obj.accession)
        event = self._make_event(
            ACCESSION_REVOKED,
            "returned_object",
            object_no,
            actor,
            f"撤销入藏分配 {before['accession_number']}：{reason[:40]}",
            {
                "reason": reason,
                "before": {
                    "decision_event_id": before["decision_event_id"],
                    "decided_by": before["decided_by"],
                    "collection": before["collection"],
                    "accession_number": before["accession_number"],
                },
                "after": {"active": False, "revoked_by": actor},
            },
            occurred_at,
        )
        return self._commit(event)

    def release_catalog(
        self,
        actor: str,
        role: str,
        occurred_at: str | None = None,
    ) -> dict[str, Any]:
        """发布不可变公开目录快照；争议/冻结/未入藏/展示受限材料一律排除。"""
        self._authorize("release_catalog", role)
        if occurred_at:
            _parse_time(occurred_at)
        entries = []
        for no in sorted(self.objects):
            obj = self.objects[no]
            if not obj.accession or not obj.accession["active"]:
                continue
            if obj.frozen or obj.disputed:
                continue
            if "public_display" in obj.restrictions:
                continue
            entries.append(
                {
                    "object_no": no,
                    "batch_id": obj.batch_id,
                    "collection": obj.accession["collection"],
                    "accession_number": obj.accession["accession_number"],
                }
            )
        version = self._catalog_seq + 1
        canonical = json.dumps(entries, ensure_ascii=False, sort_keys=True)
        digest = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
        event = self._make_event(
            CATALOG_RELEASED,
            "catalog_release",
            f"catalog-v{version}",
            actor,
            f"发布公开目录快照 v{version}，含 {len(entries)} 件",
            {"version": version, "entries": entries, "content_sha256": digest},
            occurred_at,
        )
        self._commit(event)
        return {"version": version, "content_sha256": digest, "entries": entries, "event": event}

    # ------------------------------------------------------------------ 查询

    @staticmethod
    def _accepted_receipt(obj: ObjectState) -> Receipt | None:
        for receipt in obj.receipts.values():
            if receipt.status == "accepted":
                return receipt
        return None

    def usable_scopes(self, obj: ObjectState) -> list[str]:
        """当前可用范围：修复、研究、公开展示。"""
        if obj.stage == "expected":
            return []
        scopes = {"conservation", "research"}
        if obj.accession and obj.accession["active"]:
            scopes.add("public_display")
        if obj.frozen:
            scopes.clear()
        if obj.disputed:
            scopes.discard("public_display")
        scopes.difference_update(obj.restrictions)
        return sorted(scopes)

    def trace(self, object_no: str) -> dict[str, Any]:
        """从任一文物追溯批次、位置、可用范围、冻结原因与历次决定。"""
        obj = self._require_object(object_no)
        return {
            "object_no": object_no,
            "batch_id": obj.batch_id,
            "stage": obj.stage,
            "current_location": obj.location,
            "current_custodian": obj.custodian,
            "usable_scopes": self.usable_scopes(obj),
            "frozen": obj.frozen,
            "freeze_reasons": list(obj.freeze_reasons),
            "disputed": obj.disputed,
            "active_restrictions": dict(obj.restrictions),
            "seal_receipts": [
                {
                    "receipt_id": r.receipt_id,
                    "seal": r.seal,
                    "summary": r.summary,
                    "status": r.status,
                    "conflict_with": r.conflict_with,
                }
                for r in obj.receipts.values()
            ],
            "materials": list(obj.materials),
            "diseases": list(obj.diseases),
            "provenance": list(obj.provenance),
            "applications": [
                {
                    "app_id": a.app_id,
                    "kind": a.kind,
                    "applicant": a.applicant,
                    "status": a.status,
                    "history": a.history,
                }
                for a in obj.applications.values()
            ],
            "accession": obj.accession,
            "accession_history": obj.accession_history,
            "catalog_versions": list(obj.catalog_versions),
            "events": [e.as_dict() for e in self.store.events_for("returned_object", object_no)],
        }

    def batch_view(self, batch_id: str) -> dict[str, Any]:
        batch = self.batches.get(batch_id)
        if batch is None:
            raise DomainError(f"批次 {batch_id} 不存在", status=404)
        return {
            "batch_id": batch_id,
            "foreign_authority": batch.foreign_authority,
            "expected_count": len(batch.expected),
            "arrived_count": len(batch.arrived),
            "partial": len(batch.arrived) < len(batch.expected),
            "expected": sorted(batch.expected),
            "arrived": sorted(batch.arrived),
            "outstanding": sorted(batch.expected - batch.arrived),
        }

    def catalog_view(self, version: int) -> dict[str, Any]:
        release = self.catalog.get(version)
        if release is None:
            raise DomainError(f"目录版本 {version} 不存在", status=404)
        return release

    # ------------------------------------------------------------------ 回放

    def _apply(self, event: Event, replay: bool = False) -> None:
        et, p = event.event_type, event.payload
        if et == BATCH_REGISTERED:
            batch = BatchState(
                batch_id=event.aggregate_id,
                foreign_authority=p["foreign_authority"],
                expected=set(p["objects"]),
            )
            self.batches[batch.batch_id] = batch
            for no in p["objects"]:
                self.objects[no] = ObjectState(object_no=no, batch_id=batch.batch_id)
            if replay:
                self._catalog_seq = max(self._catalog_seq, 0)
        elif et == SEAL_RECEIPT_RECORDED:
            obj = self.objects[event.aggregate_id]
            obj.receipts[p["receipt_id"]] = Receipt(
                receipt_id=p["receipt_id"],
                seal=p["seal"],
                summary=p["summary"],
                status="accepted",
                recorded_event_id=event.event_id,
            )
            obj.accepted_seal_event_id = event.event_id
        elif et == SEAL_RECEIPT_QUARANTINED:
            obj = self.objects[event.aggregate_id]
            receipt = obj.receipts.get(p["receipt_id"])
            if receipt is None:
                # 不同回执编号但与已采纳封签冲突：整条回执隔离。
                receipt = Receipt(
                    receipt_id=p["receipt_id"],
                    seal=p["seal"],
                    summary=p["summary"],
                    status="quarantined",
                    recorded_event_id=event.event_id,
                    conflict_with=p.get("conflict_with"),
                )
                obj.receipts[p["receipt_id"]] = receipt
            else:
                # 同编号重传内容不一致：重传挂为候选，原封签保持 accepted。
                receipt.candidate = {"seal": p["seal"], "summary": p["summary"]}
                receipt.conflict_with = p.get("conflict_with")
            obj.open_quarantine.add(p["receipt_id"])
        elif et == SEAL_RECEIPT_RESOLVED:
            obj = self.objects[event.aggregate_id]
            receipt = obj.receipts[p["receipt_id"]]
            receipt.resolution_note = p.get("note", "")
            obj.open_quarantine.discard(p["receipt_id"])
            if p["outcome"] == "accepted":
                if receipt.candidate is not None:
                    # 同编号重传候选被采纳：原封签内容被候选替换，原事件仍可追溯。
                    receipt.seal = receipt.candidate["seal"]
                    receipt.summary = receipt.candidate["summary"]
                    receipt.candidate = None
                for other in obj.receipts.values():
                    if other is not receipt and other.status == "accepted":
                        other.status = "superseded"
                receipt.status = "accepted"
                obj.accepted_seal_event_id = receipt.recorded_event_id
            else:  # rejected
                if receipt.candidate is not None:
                    # 候选驳回：原封签继续有效。
                    receipt.candidate = None
                    receipt.status = "accepted"
                else:
                    receipt.status = "rejected"
        elif et == OBJECT_HANDED_OVER:
            obj = self.objects[event.aggregate_id]
            obj.stage = "handed_over"
            obj.location = p["location"]
            obj.custodian = p["custodian"]
            self.batches[p["batch_id"]].arrived.add(obj.object_no)
            self.batches[p["batch_id"]].handover_event_ids.append(event.event_id)
        elif et == MATERIAL_TESTED:
            self.objects[event.aggregate_id].materials.append(
                {"material": p["material"], "findings": p.get("findings", ""),
                 "event_id": event.event_id, "at": event.occurred_at}
            )
        elif et == DISEASE_REPORTED:
            self.objects[event.aggregate_id].diseases.append(
                {"disease": p["disease"], "severity": p["severity"],
                 "event_id": event.event_id, "at": event.occurred_at}
            )
        elif et == OBJECT_FROZEN:
            obj = self.objects[event.aggregate_id]
            obj.frozen = True
            obj.freeze_reasons.append(p["reason"])
        elif et == OBJECT_UNFROZEN:
            obj = self.objects[event.aggregate_id]
            obj.frozen = False
            obj.freeze_reasons.clear()
        elif et == PROVENANCE_RECORDED:
            self.objects[event.aggregate_id].provenance.append(
                {"record": p["record"], "event_id": event.event_id,
                 "by": event.actor, "at": event.occurred_at}
            )
        elif et == DISPUTE_RECORDED:
            obj = self.objects[event.aggregate_id]
            obj.disputed = True
            obj.dispute_reasons.append(p["reason"])
        elif et == DISPUTE_CLEARED:
            obj = self.objects[event.aggregate_id]
            obj.disputed = False
            obj.dispute_reasons.clear()
        elif et == RESTRICTION_RECORDED:
            self.objects[event.aggregate_id].restrictions[p["scope"]] = p["reason"]
        elif et == RESTRICTION_LIFTED:
            self.objects[event.aggregate_id].restrictions.pop(p["scope"], None)
        elif et == CUSTODY_TRANSFERRED:
            obj = self.objects[event.aggregate_id]
            obj.location = p["to_location"]
            obj.custodian = p["to_custodian"]
        elif et == APPLICATION_SUBMITTED:
            app = Application(
                app_id=event.aggregate_id,
                object_no=p["object_no"],
                kind=p["kind"],
                applicant=event.actor,
                status=p.get("initial", "submitted"),
                note=p.get("note", ""),
            )
            app.history.append({"status": app.status, "by": event.actor,
                                "event_id": event.event_id, "at": event.occurred_at})
            self.applications[app.app_id] = app
            self.objects[app.object_no].applications[app.app_id] = app
        elif et in (APPLICATION_HELD, APPLICATION_RESUMED,
                    APPLICATION_APPROVED, APPLICATION_REJECTED):
            app = self.applications[event.aggregate_id]
            new_status = {
                APPLICATION_HELD: "held",
                APPLICATION_RESUMED: app.held_from or "submitted",
                APPLICATION_APPROVED: "approved",
                APPLICATION_REJECTED: "rejected",
            }[et]
            if et == APPLICATION_HELD:
                app.held_from = p.get("from_status", "submitted")
            elif et == APPLICATION_RESUMED:
                app.held_from = None
            app.status = new_status
            app.history.append({"status": new_status, "by": event.actor,
                                "event_id": event.event_id, "at": event.occurred_at,
                                "note": p.get("note", "")})
        elif et == ACCESSION_DECIDED:
            obj = self.objects[event.aggregate_id]
            obj.stage = "accessioned"
            obj.accession = {
                "active": True,
                "collection": p["collection"],
                "accession_number": p["accession_number"],
                "decided_by": event.actor,
                "decision_event_id": event.event_id,
                "decided_at": event.occurred_at,
            }
            obj.accession_history.append({**dict(obj.accession), "event_id": event.event_id})
        elif et == ACCESSION_REVOKED:
            obj = self.objects[event.aggregate_id]
            if obj.accession:
                obj.accession["active"] = False
                obj.accession["revoked_by"] = event.actor
                obj.accession["revoke_event_id"] = event.event_id
                obj.accession["revoke_reason"] = p["reason"]
                obj.accession_history.append({
                    "active": False, "revoked_by": event.actor,
                    "event_id": event.event_id, "at": event.occurred_at,
                    "reason": p["reason"], "before": p["before"],
                })
        elif et == CATALOG_RELEASED:
            version = p["version"]
            self._catalog_seq = max(self._catalog_seq, version)
            snapshot = {
                "version": version,
                "released_at": event.occurred_at,
                "released_by": event.actor,
                "content_sha256": p["content_sha256"],
                "entries": p["entries"],
                "event_id": event.event_id,
            }
            self.catalog[version] = snapshot
            for entry in p["entries"]:
                self.objects[entry["object_no"]].catalog_versions.append(version)

        if replay:
            key = (event.aggregate_type, event.aggregate_id)
            self._versions[key] = max(self._versions.get(key, 0), event.version)
