"""返还文物入藏责任链的领域事件定义。

所有状态变化都以事件追加表达：业务修订通过新事件完成，原始记录永不删除，
用于从任一文物追溯返还批次、当前位置、可用范围、冻结原因和历次决定。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

# 返还批次：境外查获文物完成返还交接后登记。
BATCH_REGISTERED = "BATCH_REGISTERED"

# 运输封签回执：按 (对象编号, 封签名, 状态摘要) 判重；
# 同编号但封签/摘要不同的回执隔离待复核，完全相同的重传返回原结果。
SEAL_RECEIPT_RECORDED = "SEAL_RECEIPT_RECORDED"
SEAL_RECEIPT_QUARANTINED = "SEAL_RECEIPT_QUARANTINED"
SEAL_RECEIPT_RESOLVED = "SEAL_RECEIPT_RESOLVED"

# 现场点交（支持部分到货；重启后未完成点交可继续）。
OBJECT_HANDED_OVER = "OBJECT_HANDED_OVER"

# 材质检测与病害报告（迟到检测在点交前到达先挂起，点交后生效）。
MATERIAL_TESTED = "MATERIAL_TESTED"
DISEASE_REPORTED = "DISEASE_REPORTED"

# 风险冻结/解除：只冻结受影响文物及其衍生申请。
OBJECT_FROZEN = "OBJECT_FROZEN"
OBJECT_UNFROZEN = "OBJECT_UNFROZEN"
APPLICATION_HELD = "APPLICATION_HELD"
APPLICATION_RESUMED = "APPLICATION_RESUMED"

# 来源档案、争议标记、权利限制沿同一对象持续追加。
PROVENANCE_RECORDED = "PROVENANCE_RECORDED"
DISPUTE_RECORDED = "DISPUTE_RECORDED"
DISPUTE_CLEARED = "DISPUTE_CLEARED"
RESTRICTION_RECORDED = "RESTRICTION_RECORDED"
RESTRICTION_LIFTED = "RESTRICTION_LIFTED"

# 实体转库与责任人变更原子交接，任何一刻只有一个有效保管方。
CUSTODY_TRANSFERRED = "CUSTODY_TRANSFERRED"

# 修复、研究、公开展示等衍生申请。
APPLICATION_SUBMITTED = "APPLICATION_SUBMITTED"
APPLICATION_APPROVED = "APPLICATION_APPROVED"
APPLICATION_REJECTED = "APPLICATION_REJECTED"

# 入藏决定；撤销错误分配必须留下前后责任。
ACCESSION_DECIDED = "ACCESSION_DECIDED"
ACCESSION_REVOKED = "ACCESSION_REVOKED"

# 不可变公开目录快照：发布后不被后续鉴定静默改写。
CATALOG_RELEASED = "CATALOG_RELEASED"

EVENT_TYPES = frozenset(
    {
        BATCH_REGISTERED,
        SEAL_RECEIPT_RECORDED,
        SEAL_RECEIPT_QUARANTINED,
        SEAL_RECEIPT_RESOLVED,
        OBJECT_HANDED_OVER,
        MATERIAL_TESTED,
        DISEASE_REPORTED,
        OBJECT_FROZEN,
        OBJECT_UNFROZEN,
        APPLICATION_HELD,
        APPLICATION_RESUMED,
        PROVENANCE_RECORDED,
        DISPUTE_RECORDED,
        DISPUTE_CLEARED,
        RESTRICTION_RECORDED,
        RESTRICTION_LIFTED,
        CUSTODY_TRANSFERRED,
        APPLICATION_SUBMITTED,
        APPLICATION_APPROVED,
        APPLICATION_REJECTED,
        ACCESSION_DECIDED,
        ACCESSION_REVOKED,
        CATALOG_RELEASED,
    }
)


class DomainError(Exception):
    """规则被违反时抛出；HTTP 层映射为 4xx。"""

    def __init__(self, message: str, status: int = 422) -> None:
        super().__init__(message)
        self.status = status


@dataclass(frozen=True)
class Event:
    event_id: str
    event_type: str
    aggregate_type: str
    aggregate_id: str
    occurred_at: str
    version: int
    actor: str
    summary: str
    payload: dict[str, Any] = field(default_factory=dict)
    causation_id: str | None = None

    def as_dict(self) -> dict[str, Any]:
        return {
            "event_id": self.event_id,
            "event_type": self.event_type,
            "aggregate_type": self.aggregate_type,
            "aggregate_id": self.aggregate_id,
            "occurred_at": self.occurred_at,
            "version": self.version,
            "actor": self.actor,
            "summary": self.summary,
            "payload": self.payload,
            "causation_id": self.causation_id,
        }
