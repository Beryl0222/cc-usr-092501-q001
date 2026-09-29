"""领域事件信封的基础字段校验。"""

from __future__ import annotations

from datetime import datetime

REQUIRED = (
    "event_id", "event_type", "aggregate_type", "aggregate_id",
    "occurred_at", "version", "actor", "summary", "payload",
)

EVENT_TYPES = frozenset({
    "BATCH_REGISTERED",
    "SEAL_RECEIPT_RECORDED",
    "SEAL_RECEIPT_QUARANTINED",
    "SEAL_RECEIPT_RESOLVED",
    "OBJECT_HANDED_OVER",
    "MATERIAL_TESTED",
    "DISEASE_REPORTED",
    "OBJECT_FROZEN",
    "OBJECT_UNFROZEN",
    "APPLICATION_HELD",
    "APPLICATION_RESUMED",
    "PROVENANCE_RECORDED",
    "DISPUTE_RECORDED",
    "DISPUTE_CLEARED",
    "RESTRICTION_RECORDED",
    "RESTRICTION_LIFTED",
    "CUSTODY_TRANSFERRED",
    "APPLICATION_SUBMITTED",
    "APPLICATION_APPROVED",
    "APPLICATION_REJECTED",
    "ACCESSION_DECIDED",
    "ACCESSION_REVOKED",
    "CATALOG_RELEASED",
})

AGGREGATE_TYPES = frozenset({"return_batch", "returned_object", "application", "catalog_release"})


def validate_event(record: object) -> list[str]:
    if not isinstance(record, dict):
        return ["事件必须是 JSON 对象"]
    errors = [f"缺少字段：{name}" for name in REQUIRED if name not in record]
    if "version" in record and (
        not isinstance(record["version"], int)
        or isinstance(record["version"], bool)
        or record["version"] < 1
    ):
        errors.append("version 必须是正整数")
    if record.get("event_type") is not None and record["event_type"] not in EVENT_TYPES:
        errors.append(f"未知事件类型：{record['event_type']}")
    if record.get("aggregate_type") is not None and record["aggregate_type"] not in AGGREGATE_TYPES:
        errors.append(f"未知聚合类型：{record['aggregate_type']}")
    if "payload" in record and not isinstance(record["payload"], dict):
        errors.append("payload 必须是 JSON 对象")
    if "occurred_at" in record:
        try:
            parsed = datetime.fromisoformat(str(record["occurred_at"]).replace("Z", "+00:00"))
            if parsed.tzinfo is None:
                errors.append("occurred_at 必须包含时区")
        except ValueError:
            errors.append("occurred_at 必须是 ISO 8601 时间")
    return errors
