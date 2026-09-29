"""返还文物入藏责任链领域服务。

一条文物从返还批次登记到公开目录，沿途的运输封签、现场点交、材质
检测、来源档案、权利限制、衍生申请、入藏决定与目录发布都沿同一对象
持续追加事件。服务只依赖：class:`EventStore` 中仅追加的事件日志，
进程重启后重放日志即可恢复未完成点交、待复核封签与全部责任视图。
"""

from __future__ import annotations

import functools
from typing import Any

from .eventstore import DomainError, EventStore, now_iso


def _command(method):
    """使一整条命令（读状态、校验、追加事件）相对其他命令原子执行。"""

    @functools.wraps(method)
    def wrapper(self, *args, **kwargs):
        with self._command_lock:
            return method(self, *args, **kwargs)

    return wrapper

# 角色 -> 可执行动作类别
ROLE_HANDOVER = "handover_officer"        # 现场点交人员
ROLE_CONSERVATOR = "conservator"          # 修复 / 材质检测人员
ROLE_RESEARCHER = "provenance_researcher"  # 来源研究人员
ROLE_ACCESSIONS = "accessions_officer"    # 入藏批准人
ROLE_CUSTODIAN = "custodian"              # 库房保管方
ROLE_REVIEWER = "review_officer"          # 封签隔离复核人

DERIVATIVE_KIND_ROLE = {
    "REPAIR": ROLE_CONSERVATOR,
    "RESEARCH": ROLE_RESEARCHER,
    "EXHIBITION": ROLE_ACCESSIONS,
}

_BASE_SCOPES = ("REPAIR", "RESEARCH")


class AccessionService:
    def __init__(self, store: EventStore) -> None:
        import threading

        self.store = store
        self._command_lock = threading.RLock()
        self.batches: dict[str, dict[str, Any]] = {}
        self.objects: dict[str, dict[str, Any]] = {}
        self.receipts: dict[tuple[str, str], dict[str, Any]] = {}
        self.derivatives: dict[str, dict[str, Any]] = {}
        self.releases: list[dict[str, Any]] = []
        self.events: list[dict[str, Any]] = []

    # ------------------------------------------------------------------
    # 重放
    # ------------------------------------------------------------------

    def replay(self) -> None:
        self.batches.clear()
        self.objects.clear()
        self.receipts.clear()
        self.derivatives.clear()
        self.releases.clear()
        self.events.clear()
        self.store.replay(self._fold)

    def _fold(self, event: dict[str, Any]) -> None:
        self.events.append(event)
        etype = event["event_type"]
        p = event.get("payload") or {}
        oid = p.get("object_id")
        if etype == "BATCH_REGISTERED":
            batch = {
                "batch_id": event["aggregate_id"],
                "foreign_authority": p.get("foreign_authority"),
                "object_ids": list(p.get("object_ids", [])),
                "registered_at": event["occurred_at"],
            }
            self.batches[batch["batch_id"]] = batch
            for obj_id in batch["object_ids"]:
                self.objects.setdefault(
                    obj_id,
                    self._new_object(obj_id, batch["batch_id"]),
                )
            return
        obj = self.objects.get(oid) if oid else None
        if obj is None and event["aggregate_type"] == "returned_object":
            obj = self.objects.get(event["aggregate_id"])
        if etype == "SEAL_RECEIPTED":
            self._fold_receipt(event, accepted=True)
            if obj is not None:
                obj["status"] = "ARRIVED"
                obj["custodian"] = p.get("custodian")
        elif etype == "SEAL_QUARANTINED":
            self._fold_receipt(event, accepted=False)
            if obj is not None:
                obj["receipt_pending_review"] = True
        elif etype == "SEAL_REVIEWED":
            key = (p["object_id"], p["receipt_no"])
            receipt = self.receipts[key]
            receipt["state"] = "CONFIRMED" if p["decision"] == "CONFIRM" else "REJECTED"
            receipt["review"] = {
                "decision": p["decision"],
                "reviewer": p.get("reviewer"),
                "at": event["occurred_at"],
                "note": p.get("note", ""),
                "chosen_hash": p.get("chosen_hash"),
                "event_seq": event["seq"],
            }
            if obj is not None:
                obj["receipt_pending_review"] = False
                if p["decision"] == "CONFIRM":
                    obj["status"] = "ARRIVED"
                    obj["custodian"] = p.get("custodian", obj.get("custodian"))
                else:
                    # 驳回意味着该编号回执未被采信，文物回到等待有效回执的状态。
                    obj["status"] = "RECEIPT_REJECTED"
        elif etype == "HANDOVER_RECORDED" and obj is not None:
            obj["status"] = "HANDED_OVER"
            obj["handed_over_at"] = event["occurred_at"]
            obj["handover_by"] = p.get("actor")
        elif etype == "CONDITION_ASSESSED_RISK" and obj is not None:
            obj["assessments"].append(
                {
                    "report_id": event["aggregate_id"],
                    "risk": bool(p.get("risk")),
                    "reasons": list(p.get("reasons", [])),
                    "by": p.get("actor"),
                    "at": event["occurred_at"],
                    "seq": event["seq"],
                }
            )
        elif etype == "OBJECT_FROZEN" and obj is not None:
            obj["freeze"] = {
                "reason": p.get("reason"),
                "reasons": list(p.get("reasons", [])),
                "by": p.get("actor"),
                "at": event["occurred_at"],
                "seq": event["seq"],
            }
        elif etype == "DERIVATIVE_FROZEN_RECORD":
            req = self.derivatives.get(event["aggregate_id"])
            if req is not None:
                req["frozen"] = True
                req["frozen_reason"] = p.get("reason")
        elif etype == "OBJECT_UNFROZEN" and obj is not None:
            obj["freeze"] = None
            for req_id in obj["derivative_ids"]:
                req = self.derivatives.get(req_id)
                if req is not None:
                    req["frozen"] = False
                    req["frozen_reason"] = None
        elif etype == "CUSTODY_TRANSFERRED" and obj is not None:
            obj["custodian"] = p["to_custodian"]
            obj["custodian_history"].append(
                {
                    "from_custodian": p.get("from_custodian"),
                    "to_custodian": p["to_custodian"],
                    "officer": p.get("officer"),
                    "at": event["occurred_at"],
                    "seq": event["seq"],
                }
            )
        elif etype == "PROVENANCE_RECORDED" and obj is not None:
            obj["provenance"].append(
                {
                    "record_id": event["aggregate_id"],
                    "title": p.get("title", ""),
                    "source_doc": p.get("source_doc", ""),
                    "by": p.get("actor"),
                    "at": event["occurred_at"],
                    "seq": event["seq"],
                }
            )
        elif etype == "DISPUTE_RAISED" and obj is not None:
            obj["dispute_open"] = True
            obj["disputes"].append(
                {
                    "open": True,
                    "reason": p.get("reason"),
                    "by": p.get("actor"),
                    "raised_at": event["occurred_at"],
                    "raised_seq": event["seq"],
                }
            )
        elif etype == "DISPUTE_CLEARED" and obj is not None:
            obj["dispute_open"] = False
            for dispute in reversed(obj["disputes"]):
                if dispute["open"]:
                    dispute["open"] = False
                    dispute["cleared_by"] = p.get("actor")
                    dispute["cleared_at"] = event["occurred_at"]
                    dispute["cleared_seq"] = event["seq"]
                    dispute["resolution"] = p.get("note", "")
                    break
        elif etype == "RIGHTS_ATTACHED" and obj is not None:
            obj["rights"].append(
                {
                    "restriction_id": event["aggregate_id"],
                    "scope": p.get("scope"),
                    "effect": p.get("effect", "DENY"),
                    "note": p.get("note", ""),
                    "by": p.get("actor"),
                    "at": event["occurred_at"],
                    "seq": event["seq"],
                }
            )
        elif etype == "DERIVATIVE_REQUESTED":
            freeze_info = obj.get("freeze") if obj else None
            req = {
                "request_id": event["aggregate_id"],
                "object_id": p["object_id"],
                "kind": p.get("kind"),
                "applicant": p.get("applicant"),
                "frozen": obj is not None and obj["freeze"] is not None,
                "frozen_reason": freeze_info.get("reason") if freeze_info else None,
                "created_at": event["occurred_at"],
                "seq": event["seq"],
            }
            self.derivatives[req["request_id"]] = req
            if obj is not None:
                obj["derivative_ids"].append(req["request_id"])
        elif etype == "ACCESSION_DECIDED" and obj is not None:
            obj["accession"] = {
                "decision": p.get("decision"),
                "note": p.get("note", ""),
                "approver": p.get("actor"),
                "at": event["occurred_at"],
                "seq": event["seq"],
                "version": event["version"],
            }
        elif etype == "ACCESSION_REVOKED" and obj is not None:
            prior = obj.get("accession") or {}
            obj["accession"] = {
                "decision": "REVOKED",
                "note": p.get("reason", ""),
                "approver": p.get("actor"),
                "at": event["occurred_at"],
                "seq": event["seq"],
                "previous": {
                    "decision": prior.get("decision"),
                    "approver": prior.get("approver"),
                    "at": prior.get("at"),
                    "seq": prior.get("seq"),
                },
            }
        elif etype == "CATALOG_RELEASED":
            release = {
                "release_id": event["aggregate_id"],
                "at": event["occurred_at"],
                "seq": event["seq"],
                "snapshot": p.get("snapshot", []),
                "immutable_notes": [],
            }
            self.releases.append(release)
            for entry in release["snapshot"]:
                target = self.objects.get(entry["object_id"])
                if target is not None:
                    target["release_ids"].append(release["release_id"])
        elif etype == "CATALOG_RELEASE_IMMUTABLE":
            for release in self.releases:
                if release["release_id"] == event["aggregate_id"]:
                    release["immutable_notes"].append(
                        {
                            "object_id": p.get("object_id"),
                            "reason": p.get("reason"),
                            "at": event["occurred_at"],
                            "seq": event["seq"],
                        }
                    )

    def _fold_receipt(self, event: dict[str, Any], *, accepted: bool) -> None:
        p = event.get("payload") or {}
        key = (p["object_id"], p["receipt_no"])
        candidate = {
            "seal": p.get("seal"),
            "condition_digest": p.get("condition_digest"),
            "content_hash": p.get("content_hash"),
            "custodian": p.get("custodian"),
            "actor": p.get("actor"),
            "at": event["occurred_at"],
        }
        receipt = self.receipts.get(key)
        if receipt is None:
            receipt = {
                "object_id": key[0],
                "receipt_no": key[1],
                "state": "ACCEPTED" if accepted else "QUARANTINED",
                "candidates": [candidate],
                "review": None,
            }
            self.receipts[key] = receipt
        else:
            receipt["candidates"].append(candidate)
            receipt["state"] = "ACCEPTED" if accepted else "QUARANTINED"

    @staticmethod
    def _new_object(object_id: str, batch_id: str) -> dict[str, Any]:
        return {
            "object_id": object_id,
            "batch_id": batch_id,
            "status": "EXPECTED",
            "custodian": None,
            "custodian_history": [],
            "receipt_pending_review": False,
            "handed_over_at": None,
            "freeze": None,
            "dispute_open": False,
            "disputes": [],
            "provenance": [],
            "rights": [],
            "assessments": [],
            "derivative_ids": [],
            "accession": None,
            "release_ids": [],
        }

    # ------------------------------------------------------------------
    # 工具
    # ------------------------------------------------------------------

    def _append(self, events, expected=None):
        written = self.store.append(events, expected_versions=expected)
        for event in written:
            self._fold(event)
        return written

    @staticmethod
    def _require_role(role: str | None, allowed: str, action: str) -> None:
        if role != allowed:
            raise DomainError(
                "FORBIDDEN",
                f"动作 {action} 只允许 {allowed} 执行（当前身份：{role or '未声明'}）",
            )

    def _require_object(self, object_id: str) -> dict[str, Any]:
        obj = self.objects.get(object_id)
        if obj is None:
            raise DomainError("UNKNOWN_OBJECT", f"文物 {object_id} 尚未登记返还批次")
        return obj

    def _version(self, object_id: str) -> int:
        return self.store.version_of("returned_object", object_id)

    @staticmethod
    def _content_hash(seal: str, condition_digest: str) -> str:
        import hashlib

        joined = f"{seal}|{condition_digest}".encode("utf-8")
        return hashlib.sha256(joined).hexdigest()[:16]

    # ------------------------------------------------------------------
    # 命令：返还批次与点交
    # ------------------------------------------------------------------

    @_command
    def register_batch(self, *, actor: str, batch_id: str, foreign_authority: str,
                       object_ids: list[str]) -> dict[str, Any]:
        if not batch_id:
            raise DomainError("INVALID_INPUT", "batch_id 不能为空")
        if not object_ids:
            raise DomainError("INVALID_INPUT", "批次至少包含一件文物")
        if len(object_ids) != len(set(object_ids)):
            raise DomainError("INVALID_INPUT", "批次内文物编号重复")
        if batch_id in self.batches:
            raise DomainError("BATCH_EXISTS", f"批次 {batch_id} 已登记")
        for object_id in object_ids:
            if object_id in self.objects:
                raise DomainError("OBJECT_EXISTS", f"文物 {object_id} 已属于其他批次")
        payload = {
            "foreign_authority": foreign_authority,
            "object_ids": list(object_ids),
            "actor": actor,
            "__summary__": f"登记返还批次 {batch_id}，共 {len(object_ids)} 件，来源 {foreign_authority}",
        }
        written = self._append([("BATCH_REGISTERED", "return_batch", batch_id, payload)])
        return {"batch_id": batch_id, "object_count": len(object_ids), "events": written}

    @_command
    def record_seal_receipt(self, *, actor: str, role: str | None, object_id: str,
                            receipt_no: str, seal: str, condition_digest: str,
                            custodian: str) -> dict[str, Any]:
        self._require_role(role, ROLE_HANDOVER, "运输封签回执登记")
        self._require_object(object_id)
        if not receipt_no or not seal or not custodian:
            raise DomainError("INVALID_INPUT", "receipt_no、seal、custodian 均不能为空")
        content_hash = self._content_hash(seal, condition_digest)
        key = (object_id, receipt_no)
        prior = self.receipts.get(key)

        # 完全相同的重传：不产生新事件，只返回原结果。
        if prior is not None:
            for candidate in prior["candidates"]:
                if candidate["content_hash"] == content_hash:
                    return {
                        "idempotent": True,
                        "object_id": object_id,
                        "receipt_no": receipt_no,
                        "state": prior["state"],
                        "content_hash": content_hash,
                        "original_at": candidate["at"],
                        "events": [],
                    }
            incoming = {
                "object_id": object_id,
                "receipt_no": receipt_no,
                "seal": seal,
                "condition_digest": condition_digest,
                "content_hash": content_hash,
                "prior_seal": prior["candidates"][-1]["seal"],
                "prior_digest": prior["candidates"][-1]["condition_digest"],
                "custodian": custodian,
                "actor": actor,
                "__summary__": (
                    f"文物 {object_id} 回执 {receipt_no} 编号相同但封签/状态摘要不一致，隔离复核"
                ),
            }
            written = self._append(
                [("SEAL_QUARANTINED", "seal_receipt", f"{object_id}/{receipt_no}", incoming)]
            )
            return {
                "idempotent": False,
                "quarantined": True,
                "object_id": object_id,
                "receipt_no": receipt_no,
                "state": "QUARANTINED",
                "events": written,
            }

        payload = {
            "object_id": object_id,
            "receipt_no": receipt_no,
            "seal": seal,
            "condition_digest": condition_digest,
            "content_hash": content_hash,
            "custodian": custodian,
            "actor": actor,
            "__summary__": f"文物 {object_id} 接收运输封签回执 {receipt_no}",
        }
        written = self._append(
            [("SEAL_RECEIPTED", "seal_receipt", f"{object_id}/{receipt_no}", payload)]
        )
        return {
            "idempotent": False,
            "quarantined": False,
            "object_id": object_id,
            "receipt_no": receipt_no,
            "state": "ACCEPTED",
            "events": written,
        }

    @_command
    def review_seal_receipt(self, *, actor: str, role: str | None, object_id: str,
                            receipt_no: str, decision: str, note: str = "") -> dict[str, Any]:
        self._require_role(role, ROLE_REVIEWER, "封签隔离复核")
        receipt = self.receipts.get((object_id, receipt_no))
        if receipt is None:
            raise DomainError("UNKNOWN_RECEIPT", f"回执 {receipt_no} 不存在")
        if receipt["state"] != "QUARANTINED":
            raise DomainError("RECEIPT_NOT_QUARANTINED", "该回执不处于隔离状态，无需复核")
        if decision not in ("CONFIRM", "REJECT"):
            raise DomainError("INVALID_INPUT", "decision 必须为 CONFIRM 或 REJECT")
        chosen = receipt["candidates"][-1]
        payload = {
            "object_id": object_id,
            "receipt_no": receipt_no,
            "decision": decision,
            "chosen_hash": chosen["content_hash"],
            "custodian": chosen.get("custodian"),
            "reviewer": actor,
            "note": note,
            "__summary__": (
                f"文物 {object_id} 回执 {receipt_no} 复核{('确认' if decision == 'CONFIRM' else '驳回')}"
            ),
        }
        written = self._append(
            [("SEAL_REVIEWED", "seal_receipt", f"{object_id}/{receipt_no}", payload)],
            expected={("seal_receipt", f"{object_id}/{receipt_no}"):
                      self.store.version_of("seal_receipt", f"{object_id}/{receipt_no}")},
        )
        return {"object_id": object_id, "receipt_no": receipt_no, "decision": decision,
                "events": written}

    @_command
    def record_handover(self, *, actor: str, role: str | None, object_id: str,
                        note: str = "") -> dict[str, Any]:
        self._require_role(role, ROLE_HANDOVER, "现场点交")
        obj = self._require_object(object_id)
        if obj["status"] not in ("ARRIVED",):
            raise DomainError("NOT_ARRIVED", f"文物 {object_id} 尚无已接收封签，无法点交")
        if obj["receipt_pending_review"]:
            raise DomainError("RECEIPT_QUARANTINED", "封签回执尚在隔离复核，不得完成点交")
        payload = {"object_id": object_id, "actor": actor, "note": note,
                   "__summary__": f"文物 {object_id} 现场点交完成"}
        written = self._append(
            [("HANDOVER_RECORDED", "returned_object", object_id, payload)],
            expected={("returned_object", object_id): self._version(object_id)},
        )
        return {"object_id": object_id, "status": "HANDED_OVER", "events": written}

    # ------------------------------------------------------------------
    # 命令：检测与冻结
    # ------------------------------------------------------------------

    @_command
    def record_assessment(self, *, actor: str, role: str | None, object_id: str,
                          report_id: str, risk: bool, reasons: list[str] | None = None
                          ) -> dict[str, Any]:
        self._require_role(role, ROLE_CONSERVATOR, "材质 / 病害检测")
        obj = self._require_object(object_id)
        reasons = reasons or []
        events = [(
            "CONDITION_ASSESSED_RISK", "condition_report", report_id,
            {
                "object_id": object_id,
                "risk": bool(risk),
                "reasons": reasons,
                "actor": actor,
                "__summary__": (
                    f"文物 {object_id} 检测报告 {report_id}："
                    + ("发现风险 " + "、".join(reasons) if risk else "未发现风险")
                ),
            },
        )]
        expected = {
            ("condition_report", report_id): self.store.version_of("condition_report", report_id),
            ("returned_object", object_id): self._version(object_id),
        }
        frozen_derivatives: list[str] = []
        immutable_notes: list[str] = []
        if risk:
            reason_text = "、".join(reasons) if reasons else "检测发现风险"
            events.append((
                "OBJECT_FROZEN", "returned_object", object_id,
                {
                    "object_id": object_id,
                    "reason": reason_text,
                    "reasons": reasons,
                    "actor": actor,
                    "report_id": report_id,
                    "__summary__": f"文物 {object_id} 因{reason_text}被冻结",
                },
            ))
            # 只冻结受影响文物自己的衍生申请。
            for req_id in obj["derivative_ids"]:
                req = self.derivatives[req_id]
                if not req["frozen"]:
                    frozen_derivatives.append(req_id)
                    events.append((
                        "DERIVATIVE_FROZEN_RECORD", "derivative_request", req_id,
                        {
                            "object_id": object_id,
                            "reason": reason_text,
                            "report_id": report_id,
                            "actor": actor,
                            "__summary__": f"衍生申请 {req_id} 随文物 {object_id} 冻结",
                        },
                    ))
            # 已发布的目录快照不得被后续鉴定静默改写：只追加不可变说明。
            for release_id in obj["release_ids"]:
                immutable_notes.append(release_id)
                events.append((
                    "CATALOG_RELEASE_IMMUTABLE", "catalog_release", release_id,
                    {
                        "object_id": object_id,
                        "reason": reason_text,
                        "report_id": report_id,
                        "actor": actor,
                        "__summary__": (
                            f"目录快照 {release_id} 已发布且不可改写；文物 {object_id} "
                            f"事后因{reason_text}冻结，仅在此留痕"
                        ),
                    },
                ))
        written = self._append(events, expected=expected)
        return {
            "object_id": object_id,
            "risk": bool(risk),
            "frozen": bool(risk),
            "frozen_derivatives": frozen_derivatives,
            "published_snapshots_kept": immutable_notes,
            "events": written,
        }

    @_command
    def clear_freeze(self, *, actor: str, role: str | None, object_id: str,
                     note: str = "") -> dict[str, Any]:
        self._require_role(role, ROLE_CONSERVATOR, "解除冻结")
        obj = self._require_object(object_id)
        if obj["freeze"] is None:
            raise DomainError("NOT_FROZEN", f"文物 {object_id} 当前未被冻结")
        payload = {
            "object_id": object_id,
            "actor": actor,
            "note": note,
            "__summary__": f"文物 {object_id} 风险排除，解除冻结",
        }
        written = self._append(
            [("OBJECT_UNFROZEN", "returned_object", object_id, payload)],
            expected={("returned_object", object_id): self._version(object_id)},
        )
        return {"object_id": object_id, "frozen": False, "events": written}

    # ------------------------------------------------------------------
    # 命令：保管原子交接
    # ------------------------------------------------------------------

    @_command
    def transfer_custody(self, *, actor: str, role: str | None, object_id: str,
                         to_custodian: str, officer: str, from_custodian: str,
                         expected_version: int | None = None) -> dict[str, Any]:
        self._require_role(role, ROLE_CUSTODIAN, "实体转库 / 保管责任人变更")
        obj = self._require_object(object_id)
        if not to_custodian or not officer or not from_custodian:
            raise DomainError("INVALID_INPUT", "from_custodian、to_custodian 与 officer 均不能为空")
        current = obj["custodian"]
        if current is None:
            raise DomainError("NO_CUSTODIAN", "文物尚无有效保管方，无法转库")
        # 原子交接：请求声明的移交方必须就是当前唯一有效保管方。
        # 两个基于同一快照发起的转库只有一个能匹配，另一个立即冲突。
        if from_custodian != current:
            raise DomainError(
                "CONCURRENT_MODIFICATION",
                f"移交方不匹配：当前有效保管方为 {current}，"
                f"请求声明的移交方为 {from_custodian}（可能已发生并发转库）",
            )
        if to_custodian == current:
            raise DomainError("SAME_CUSTODIAN", "新保管方与当前保管方相同")
        version = self._version(object_id)
        if expected_version is not None and expected_version != version:
            raise DomainError(
                "CONCURRENT_MODIFICATION",
                f"并发转库冲突（当前版本 {version}，客户端期望 {expected_version}）",
            )
        payload = {
            "object_id": object_id,
            "from_custodian": current,
            "to_custodian": to_custodian,
            "officer": officer,
            "actor": actor,
            "__summary__": f"文物 {object_id} 保管方由 {current} 原子交接给 {to_custodian}",
        }
        # 乐观并发：同一聚合版本只能有一个转库胜出，任何一刻只有一个有效保管方。
        written = self._append(
            [("CUSTODY_TRANSFERRED", "returned_object", object_id, payload)],
            expected={("returned_object", object_id): version},
        )
        return {
            "object_id": object_id,
            "custodian": to_custodian,
            "version": written[-1]["version"],
            "events": written,
        }

    # ------------------------------------------------------------------
    # 命令：来源、争议、权利
    # ------------------------------------------------------------------

    @_command
    def record_provenance(self, *, actor: str, role: str | None, object_id: str,
                          title: str, source_doc: str) -> dict[str, Any]:
        self._require_role(role, ROLE_RESEARCHER, "来源档案记录")
        self._require_object(object_id)
        record_id = f"{object_id}/prov-{len(self.objects[object_id]['provenance']) + 1}"
        payload = {
            "object_id": object_id,
            "title": title,
            "source_doc": source_doc,
            "actor": actor,
            "__summary__": f"文物 {object_id} 追加来源档案：{title}",
        }
        written = self._append(
            [("PROVENANCE_RECORDED", "provenance_record", record_id, payload)]
        )
        return {"object_id": object_id, "record_id": record_id, "events": written}

    @_command
    def raise_dispute(self, *, actor: str, role: str | None, object_id: str,
                      reason: str) -> dict[str, Any]:
        self._require_role(role, ROLE_RESEARCHER, "提出来源争议")
        obj = self._require_object(object_id)
        if obj["dispute_open"]:
            raise DomainError("DISPUTE_OPEN", "该文物已有未决争议")
        payload = {
            "object_id": object_id,
            "reason": reason,
            "actor": actor,
            "__summary__": f"文物 {object_id} 被标记来源争议：{reason}",
        }
        written = self._append(
            [("DISPUTE_RAISED", "returned_object", object_id, payload)],
            expected={("returned_object", object_id): self._version(object_id)},
        )
        return {"object_id": object_id, "dispute_open": True, "events": written}

    @_command
    def clear_dispute(self, *, actor: str, role: str | None, object_id: str,
                      note: str = "") -> dict[str, Any]:
        self._require_role(role, ROLE_RESEARCHER, "解除来源争议")
        obj = self._require_object(object_id)
        if not obj["dispute_open"]:
            raise DomainError("NO_DISPUTE", "该文物没有未决争议")
        payload = {
            "object_id": object_id,
            "note": note,
            "actor": actor,
            "__summary__": f"文物 {object_id} 来源争议解除：{note}",
        }
        written = self._append(
            [("DISPUTE_CLEARED", "returned_object", object_id, payload)],
            expected={("returned_object", object_id): self._version(object_id)},
        )
        return {"object_id": object_id, "dispute_open": False, "events": written}

    @_command
    def attach_rights(self, *, actor: str, role: str | None, object_id: str,
                      scope: str, effect: str = "DENY", note: str = "") -> dict[str, Any]:
        self._require_role(role, ROLE_ACCESSIONS, "登记权利限制")
        self._require_object(object_id)
        if scope not in ("REPAIR", "RESEARCH", "PUBLIC") or effect not in ("ALLOW", "DENY"):
            raise DomainError("INVALID_INPUT", "scope 必须为 REPAIR/RESEARCH/PUBLIC，effect 为 ALLOW/DENY")
        restriction_id = f"{object_id}/rights-{len(self.objects[object_id]['rights']) + 1}"
        payload = {
            "object_id": object_id,
            "scope": scope,
            "effect": effect,
            "note": note,
            "actor": actor,
            "__summary__": f"文物 {object_id} 登记权利限制：{scope} {effect}",
        }
        written = self._append(
            [("RIGHTS_ATTACHED", "rights_restriction", restriction_id, payload)]
        )
        return {"object_id": object_id, "restriction_id": restriction_id, "events": written}

    # ------------------------------------------------------------------
    # 命令：衍生申请
    # ------------------------------------------------------------------

    @_command
    def request_derivative(self, *, actor: str, role: str | None, object_id: str,
                           kind: str, applicant: str) -> dict[str, Any]:
        self._require_object(object_id)
        allowed_role = DERIVATIVE_KIND_ROLE.get(kind)
        if allowed_role is None:
            raise DomainError("INVALID_INPUT", "kind 必须为 REPAIR/RESEARCH/EXHIBITION")
        self._require_role(role, allowed_role, f"{kind} 类衍生申请")
        existing = sum(1 for req in self.derivatives.values()
                       if req["object_id"] == object_id and req["kind"] == kind)
        request_id = f"{object_id}/{kind.lower()}-{existing + 1}"
        if request_id in self.derivatives:
            raise DomainError("REQUEST_EXISTS", f"衍生申请 {request_id} 已存在")
        obj = self.objects[object_id]
        payload = {
            "object_id": object_id,
            "kind": kind,
            "applicant": applicant,
            "actor": actor,
            "__summary__": f"文物 {object_id} 的 {kind} 衍生申请（{applicant}）",
        }
        written = self._append(
            [("DERIVATIVE_REQUESTED", "derivative_request", request_id, payload)]
        )
        return {
            "request_id": request_id,
            "frozen": obj["freeze"] is not None,
            "events": written,
        }

    # ------------------------------------------------------------------
    # 命令：入藏决定、撤销、目录发布
    # ------------------------------------------------------------------

    @_command
    def decide_accession(self, *, actor: str, role: str | None, object_id: str,
                         decision: str, note: str = "") -> dict[str, Any]:
        self._require_role(role, ROLE_ACCESSIONS, "入藏决定")
        obj = self._require_object(object_id)
        if decision not in ("APPROVED", "REJECTED"):
            raise DomainError("INVALID_INPUT", "decision 必须为 APPROVED 或 REJECTED")
        if obj["status"] != "HANDED_OVER":
            raise DomainError("NOT_HANDED_OVER", "文物尚未完成现场点交，不能作出入藏决定")
        if obj["freeze"] is not None:
            raise DomainError("OBJECT_FROZEN", f"文物处于冻结状态：{obj['freeze']['reason']}")
        if obj["dispute_open"]:
            raise DomainError("DISPUTE_OPEN", "存在未决来源争议，不能作出入藏决定")
        if not obj["provenance"]:
            raise DomainError("NO_PROVENANCE", "缺少来源档案，不能作出入藏决定")
        payload = {
            "object_id": object_id,
            "decision": decision,
            "note": note,
            "actor": actor,
            "__summary__": f"文物 {object_id} 入藏决定：{decision}（批准人 {actor}）",
        }
        written = self._append(
            [("ACCESSION_DECIDED", "returned_object", object_id, payload)],
            expected={("returned_object", object_id): self._version(object_id)},
        )
        return {"object_id": object_id, "decision": decision, "events": written}

    @_command
    def revoke_accession(self, *, actor: str, role: str | None, object_id: str,
                         reason: str) -> dict[str, Any]:
        self._require_role(role, ROLE_ACCESSIONS, "撤销入藏分配")
        obj = self._require_object(object_id)
        if not reason:
            raise DomainError("INVALID_INPUT", "撤销必须留下原因")
        prior = obj.get("accession")
        if not prior or prior["decision"] != "APPROVED":
            raise DomainError("NOT_ACCESSIONED", "文物未获准入藏，无可撤销的分配")
        payload = {
            "object_id": object_id,
            "reason": reason,
            "actor": actor,
            "previous_approver": prior["approver"],
            "previous_at": prior["at"],
            "__summary__": (
                f"撤销文物 {object_id} 的入藏分配（原批准人 {prior['approver']}，"
                f"撤销人 {actor}）：{reason}"
            ),
        }
        written = self._append(
            [("ACCESSION_REVOKED", "returned_object", object_id, payload)],
            expected={("returned_object", object_id): self._version(object_id)},
        )
        return {"object_id": object_id, "decision": "REVOKED",
                "previous": {"approver": prior["approver"], "at": prior["at"]},
                "events": written}

    def _availability(self, obj: dict[str, Any]) -> dict[str, Any]:
        """计算可用范围：冻结 / 未决争议期间全部停用；公开目录另需入藏核准。"""

        scopes: list[str] = []
        blockers: list[str] = []
        if obj["freeze"] is not None:
            blockers.append(f"FROZEN:{obj['freeze']['reason']}")
        if obj["dispute_open"]:
            blockers.append("DISPUTE_OPEN")
        if not blockers and obj["status"] == "HANDED_OVER":
            scopes = list(_BASE_SCOPES)
        accession = obj.get("accession")
        if (not blockers and accession and accession["decision"] == "APPROVED"):
            scopes.append("PUBLIC")
        # 同一可用范围上的权利限制按记录顺序，以最后一条 ALLOW/DENY 为准。
        effective: dict[str, str] = {}
        for restriction in obj["rights"]:
            effective[restriction["scope"]] = restriction["effect"]
        scopes = [s for s in scopes if effective.get(s, "ALLOW") == "ALLOW"]
        return {
            "scopes": scopes,
            "blockers": blockers,
            "public_catalog_eligible": (
                not blockers
                and accession is not None
                and accession["decision"] == "APPROVED"
                and effective.get("PUBLIC", "ALLOW") == "ALLOW"
            ),
        }

    @_command
    def release_catalog(self, *, actor: str, role: str | None) -> dict[str, Any]:
        self._require_role(role, ROLE_ACCESSIONS, "发布公开目录")
        snapshot: list[dict[str, Any]] = []
        excluded: list[dict[str, str]] = []
        for object_id, obj in sorted(self.objects.items()):
            availability = self._availability(obj)
            if availability["public_catalog_eligible"]:
                snapshot.append({
                    "object_id": object_id,
                    "batch_id": obj["batch_id"],
                    "custodian": obj["custodian"],
                    "availability": availability["scopes"],
                    "rights": [
                        {"scope": r["scope"], "effect": r["effect"], "note": r["note"]}
                        for r in obj["rights"]
                    ],
                })
            else:
                reasons = list(availability["blockers"])
                if not reasons:
                    accession = obj.get("accession")
                    if accession is None or accession["decision"] != "APPROVED":
                        reasons.append(
                            f"NOT_APPROVED:{accession['decision'] if accession else 'NONE'}"
                        )
                    else:
                        reasons.append("RIGHTS:PUBLIC_DENY")
                excluded.append({"object_id": object_id, "reason": ";".join(reasons)})
        release_id = f"REL-{len(self.releases) + 1:04d}"
        payload = {
            "snapshot": snapshot,
            "excluded": excluded,
            "actor": actor,
            "__summary__": f"发布公开目录快照 {release_id}，收录 {len(snapshot)} 件，排除 {len(excluded)} 件",
        }
        written = self._append(
            [("CATALOG_RELEASED", "catalog_release", release_id, payload)]
        )
        # 快照内容在事件落盘后即为定值，后续任何鉴定只会产生新事件，
        # 不会改写这里返回给读者的内容。
        return {
            "release_id": release_id,
            "snapshot": snapshot,
            "excluded": excluded,
            "immutable": True,
            "events": written,
        }

    # ------------------------------------------------------------------
    # 查询
    # ------------------------------------------------------------------

    def trace_object(self, object_id: str) -> dict[str, Any]:
        obj = self._require_object(object_id)
        batch = self.batches.get(obj["batch_id"], {})
        decisions: list[dict[str, Any]] = []
        batch_event_seqs = {
            event["seq"] for event in self.events
            if event["event_type"] == "BATCH_REGISTERED"
            and event["aggregate_id"] == obj["batch_id"]
        }
        for event in self.events:
            p = event.get("payload") or {}
            touches = p.get("object_id") == object_id or (
                event["aggregate_type"] == "returned_object" and event["aggregate_id"] == object_id
            ) or event["seq"] in batch_event_seqs
            if event["event_type"] == "CATALOG_RELEASED":
                if any(e["object_id"] == object_id for e in p.get("snapshot", [])):
                    touches = True
            if not touches:
                continue
            decisions.append({
                "seq": event["seq"],
                "event_type": event["event_type"],
                "at": event["occurred_at"],
                "actor": p.get("actor"),
                "detail": {k: v for k, v in p.items() if k != "__summary__"},
            })
        return {
            "object_id": object_id,
            "return_batch": {
                "batch_id": obj["batch_id"],
                "foreign_authority": batch.get("foreign_authority"),
                "expected_objects": batch.get("object_ids"),
            },
            "status": obj["status"],
            "current_location": {
                "custodian": obj["custodian"],
                "custodian_history": obj["custodian_history"],
            },
            "availability": self._availability(obj),
            "freeze": obj["freeze"],
            "dispute_open": obj["dispute_open"],
            "receipts": [
                {"receipt_no": key[1], **receipt}
                for key, receipt in sorted(self.receipts.items())
                if key[0] == object_id
            ],
            "provenance_count": len(obj["provenance"]),
            "rights": obj["rights"],
            "derivatives": [self.derivatives[r] for r in obj["derivative_ids"]],
            "accession": obj["accession"],
            "published_in": obj["release_ids"],
            "aggregate_version": self._version(object_id),
            "decisions": decisions,
        }

    def recovery_view(self) -> dict[str, Any]:
        """服务重启后用于恢复工作队列：未完成点交、待复核封签、冻结中文物。"""

        pending_handovers = [
            {
                "object_id": oid,
                "batch_id": obj["batch_id"],
                "status": obj["status"],
                "custodian": obj["custodian"],
            }
            for oid, obj in sorted(self.objects.items())
            if obj["status"] in ("EXPECTED", "ARRIVED", "RECEIPT_REJECTED")
        ]
        pending_reviews = [
            {
                "object_id": key[0],
                "receipt_no": key[1],
                "candidate_count": len(receipt["candidates"]),
                "last_at": receipt["candidates"][-1]["at"],
            }
            for key, receipt in sorted(self.receipts.items())
            if receipt["state"] == "QUARANTINED"
        ]
        frozen = [
            {"object_id": oid, "reason": obj["freeze"]["reason"], "at": obj["freeze"]["at"]}
            for oid, obj in sorted(self.objects.items())
            if obj["freeze"] is not None
        ]
        return {
            "recovered_at": now_iso(),
            "total_events": len(self.events),
            "pending_handovers": pending_handovers,
            "pending_receipt_reviews": pending_reviews,
            "frozen_objects": frozen,
        }

    def get_release(self, release_id: str) -> dict[str, Any]:
        for release in self.releases:
            if release["release_id"] == release_id:
                return release
        raise DomainError("UNKNOWN_RELEASE", f"目录快照 {release_id} 不存在")

    def list_releases(self) -> list[dict[str, Any]]:
        return [
            {"release_id": r["release_id"], "at": r["at"], "seq": r["seq"],
             "object_count": len(r["snapshot"]), "immutable_notes": r["immutable_notes"]}
            for r in self.releases
        ]
