"""责任链领域规则测试。

覆盖：部分到货与重启恢复、并发转库、迟到检测、重复回执隔离、
最小冻结、职责隔离、争议排除、撤销留痕、版本化公开目录。
"""

from __future__ import annotations

import tempfile
import threading
import unittest
from pathlib import Path

from src.events import DomainError
from src.service import AccessionService
from src.store import EventStore

OBJS = [f"OBJ-{i:03d}" for i in range(12)]


def new_service(path: str = ":memory:") -> AccessionService:
    return AccessionService(EventStore(path))


def seed_batch(svc: AccessionService, objs=OBJS, batch="B-12") -> None:
    svc.register_batch("登记员", "registry", batch, "X国执法机关", list(objs))


def seal_and_handover(svc: AccessionService, no: str, location="临时库房A", custodian="保管员甲",
                      receipt: str | None = None, seal="SEAL-A", summary="外观完好") -> None:
    svc.record_seal_receipt("点交员甲", "handover_clerk", no, receipt or f"R-{no}", seal, summary)
    svc.record_handover("点交员甲", "handover_clerk", "B-12", [no], location, custodian)


class PartialArrivalTest(unittest.TestCase):
    def test_partial_arrival_tracks_outstanding(self) -> None:
        svc = new_service()
        seed_batch(svc)
        for no in OBJS[:3]:
            svc.record_seal_receipt("点交员甲", "handover_clerk", no, f"R-{no}", "SEAL-A", "完好")
        svc.record_handover("点交员甲", "handover_clerk", "B-12", OBJS[:2], "临时库房A", "保管员甲")
        view = svc.batch_view("B-12")
        self.assertTrue(view["partial"])
        self.assertEqual(view["arrived_count"], 2)
        self.assertEqual(view["outstanding"], OBJS[2:])

    def test_handover_without_seal_is_rejected_atomically(self) -> None:
        svc = new_service()
        seed_batch(svc, OBJS[:2])
        svc.record_seal_receipt("点交员甲", "handover_clerk", OBJS[0], "R0", "S", "ok")
        with self.assertRaises(DomainError) as ctx:
            svc.record_handover("点交员甲", "handover_clerk", "B-12", OBJS[:2], "库", "甲")
        self.assertEqual(ctx.exception.status, 409)
        # 整批失败，第一件也不得入库。
        self.assertEqual(svc.batch_view("B-12")["arrived_count"], 0)

    def test_restart_recovers_unfinished_handover_and_quarantine(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db = str(Path(tmp) / "events.sqlite3")
            svc = new_service(db)
            seed_batch(svc, OBJS[:3])
            svc.record_seal_receipt("点交员甲", "handover_clerk", OBJS[0], "R0", "S", "完好")
            svc.record_handover("点交员甲", "handover_clerk", "B-12", [OBJS[0]], "库1", "甲")
            # OBJ-001 已有封签但尚未点交；OBJ-002 出现冲突回执待复核。
            svc.record_seal_receipt("点交员甲", "handover_clerk", OBJS[1], "R1", "S", "完好")
            svc.record_seal_receipt("点交员甲", "handover_clerk", OBJS[2], "R2", "S", "完好")
            svc.record_seal_receipt("点交员甲", "handover_clerk", OBJS[2], "R2", "S2", "破损")

            restored = new_service(db)  # 模拟服务重启
            view = restored.batch_view("B-12")
            self.assertTrue(view["partial"])
            self.assertEqual(view["arrived"], [OBJS[0]])
            self.assertEqual(view["outstanding"], [OBJS[1], OBJS[2]])
            trace2 = restored.trace(OBJS[2])
            self.assertEqual({r["status"] for r in trace2["seal_receipts"]}, {"accepted"})
            self.assertTrue(any(  # 待复核事件可追溯
                e["event_type"] == "SEAL_RECEIPT_QUARANTINED" for e in trace2["events"]
            ))
            # 待复核未清，恢复后点交仍被拦截；裁决驳回候选后可继续完成点交。
            with self.assertRaises(DomainError):
                restored.record_handover("点交员甲", "handover_clerk", "B-12", [OBJS[2]], "库1", "甲")
            restored.resolve_seal_receipt("登记员", "registry", "R2", "rejected", "维持原封签")
            restored.record_handover("点交员甲", "handover_clerk", "B-12", [OBJS[2]], "库1", "甲")
            self.assertEqual(restored.batch_view("B-12")["arrived_count"], 2)


class SealReceiptTest(unittest.TestCase):
    def setUp(self) -> None:
        self.svc = new_service()
        seed_batch(self.svc, OBJS[:2])

    def test_identical_retransmission_returns_original(self) -> None:
        first = self.svc.record_seal_receipt("点", "handover_clerk", OBJS[0], "R1", "S", "ok")
        self.assertEqual(first["result"], "recorded")
        original_id = first["events"][0].event_id
        again = self.svc.record_seal_receipt("点", "handover_clerk", OBJS[0], "R1", "S", "ok")
        self.assertEqual(again["result"], "duplicate")
        self.assertEqual(again["original_event_id"], original_id)
        self.assertEqual(again["events"], [])  # 不产生新事件

    def test_same_id_different_content_is_quarantined_original_stays_valid(self) -> None:
        self.svc.record_seal_receipt("点", "handover_clerk", OBJS[0], "R1", "S", "ok")
        q = self.svc.record_seal_receipt("点", "handover_clerk", OBJS[0], "R1", "S2", "破损")
        self.assertEqual(q["result"], "quarantined")
        # 隔离期间原封签仍有效，不能直接点交（隔离必须复核）。
        with self.assertRaises(DomainError):
            self.svc.record_handover("点", "handover_clerk", "B-12", [OBJS[0]], "库", "甲")
        # 驳回候选：原封签恢复可点交。
        self.svc.resolve_seal_receipt("登记员", "registry", "R1", "rejected", "维持原封签")
        self.svc.record_handover("点", "handover_clerk", "B-12", [OBJS[0]], "库", "甲")
        receipts = self.svc.trace(OBJS[0])["seal_receipts"]
        self.assertEqual(len(receipts), 1)
        self.assertEqual(receipts[0]["seal"], "S")

    def test_accept_candidate_replaces_seal_with_history(self) -> None:
        self.svc.record_seal_receipt("点", "handover_clerk", OBJS[0], "R1", "S", "ok")
        self.svc.record_seal_receipt("点", "handover_clerk", OBJS[0], "R1", "S2", "箱损")
        self.svc.resolve_seal_receipt("登记员", "registry", "R1", "accepted", "复核确认新封签")
        receipt = self.svc.trace(OBJS[0])["seal_receipts"][0]
        self.assertEqual(receipt["seal"], "S2")
        self.assertEqual(receipt["status"], "accepted")

    def test_distinct_receipt_conflicting_with_accepted_is_quarantined(self) -> None:
        self.svc.record_seal_receipt("点", "handover_clerk", OBJS[0], "R1", "S", "ok")
        q = self.svc.record_seal_receipt("点", "handover_clerk", OBJS[0], "R9", "S2", "冲突摘要")
        self.assertEqual(q["result"], "quarantined")
        with self.assertRaises(DomainError):
            self.svc.record_handover("点", "handover_clerk", "B-12", [OBJS[0]], "库", "甲")
        self.svc.resolve_seal_receipt("登记员", "registry", "R9", "rejected", "回执无效")
        self.svc.record_handover("点", "handover_clerk", "B-12", [OBJS[0]], "库", "甲")


class LateDetectionTest(unittest.TestCase):
    def test_detection_before_handover_takes_effect(self) -> None:
        svc = new_service()
        seed_batch(svc, OBJS[:2])
        # 检测早于点交到达（迟到/乱序），沿同一对象挂接。
        svc.record_material("修复师", "conservator", OBJS[0], "青铜", "表面锈迹")
        svc.report_disease("修复师", "conservator", OBJS[0], "粉状锈", "high")
        svc.record_seal_receipt("点", "handover_clerk", OBJS[0], "R0", "S", "ok")
        svc.record_handover("点", "handover_clerk", "B-12", [OBJS[0]], "库", "甲")
        trace = svc.trace(OBJS[0])
        self.assertTrue(trace["frozen"])
        self.assertEqual(trace["usable_scopes"], [])
        self.assertTrue(trace["freeze_reasons"])
        # 冻结件不得入藏。
        with self.assertRaises(DomainError):
            svc.decide_accession("批准人", "approver", OBJS[0], "器物部", "G-1")

    def test_late_disease_after_approvals_freezes_only_target(self) -> None:
        svc = new_service()
        seed_batch(svc, OBJS[:2])
        seal_and_handover(svc, OBJS[0])
        seal_and_handover(svc, OBJS[1])
        ev_a = svc.submit_application("修复师", "conservator", OBJS[0], "conservation")
        ev_b = svc.submit_application("修复师", "conservator", OBJS[1], "conservation")
        app_a, app_b = ev_a[0].aggregate_id, ev_b[0].aggregate_id
        svc.report_disease("修复师", "conservator", OBJS[0], "霉菌", "medium")
        self.assertEqual(svc.applications[app_a].status, "held")
        self.assertEqual(svc.applications[app_b].status, "submitted")  # 邻件不受影响
        self.assertFalse(svc.trace(OBJS[1])["frozen"])
        # 低风险不冻结。
        svc.report_disease("修复师", "conservator", OBJS[1], "轻微划痕", "low")
        self.assertFalse(svc.trace(OBJS[1])["frozen"])
        # 复检解除后连带恢复。
        svc.clear_risk("修复师", "conservator", OBJS[0], "复检合格")
        self.assertEqual(svc.applications[app_a].status, "submitted")
        self.assertFalse(svc.trace(OBJS[0])["frozen"])

    def test_application_submitted_while_frozen_is_held(self) -> None:
        svc = new_service()
        seed_batch(svc, OBJS[:1])
        seal_and_handover(svc, OBJS[0])
        svc.report_disease("修复师", "conservator", OBJS[0], "病害", "high")
        ev = svc.submit_application("修复师", "conservator", OBJS[0], "conservation")
        self.assertEqual(svc.applications[ev[0].aggregate_id].status, "held")


class CustodyTransferTest(unittest.TestCase):
    def setUp(self) -> None:
        self.svc = new_service()
        seed_batch(self.svc, OBJS[:2])
        seal_and_handover(self.svc, OBJS[0])

    def test_concurrent_transfers_single_valid_custodian(self) -> None:
        results: list[str] = []

        def transfer(to: str, custodian: str) -> None:
            try:
                self.svc.transfer_custody(
                    "批准人", "approver", OBJS[0],
                    "临时库房A", "保管员甲", to, custodian,
                )
                results.append(f"ok:{custodian}")
            except DomainError:
                results.append("conflict")

        t1 = threading.Thread(target=transfer, args=("修复室", "保管员乙"))
        t2 = threading.Thread(target=transfer, args=("展厅", "保管员丙"))
        t1.start(); t2.start(); t1.join(); t2.join()
        self.assertEqual(sorted(results).count("conflict"), 1)
        self.assertEqual(len(results), 2)
        trace = self.svc.trace(OBJS[0])
        winners = [r.split(":")[1] for r in results if r.startswith("ok:")]
        self.assertEqual(winners, [trace["current_custodian"]])
        self.assertEqual(len([e for e in trace["events"] if e["event_type"] == "CUSTODY_TRANSFERRED"]), 1)

    def test_transfer_requires_current_custodian_attestation(self) -> None:
        with self.assertRaises(DomainError) as ctx:
            self.svc.transfer_custody(
                "批准人", "approver", OBJS[0], "临时库房A", "伪造保管员", "修复室", "保管员乙"
            )
        self.assertEqual(ctx.exception.status, 409)

    def test_transfer_is_atomic_single_event(self) -> None:
        events = self.svc.transfer_custody(
            "批准人", "approver", OBJS[0], "临时库房A", "保管员甲", "修复室", "保管员乙"
        )
        self.assertEqual(len(events), 1)
        p = events[0].payload
        self.assertEqual((p["from_custodian"], p["to_custodian"]), ("保管员甲", "保管员乙"))
        trace = self.svc.trace(OBJS[0])
        self.assertEqual((trace["current_location"], trace["current_custodian"]), ("修复室", "保管员乙"))


class AccessControlTest(unittest.TestCase):
    def setUp(self) -> None:
        self.svc = new_service()
        seed_batch(self.svc, OBJS[:2])
        seal_and_handover(self.svc, OBJS[0])

    def test_role_matrix(self) -> None:
        with self.assertRaises(DomainError) as c:
            self.svc.record_material("研究员", "provenance_researcher", OBJS[0], "陶")
        self.assertEqual(c.exception.status, 403)
        with self.assertRaises(DomainError) as c:
            self.svc.record_provenance("修复师", "conservator", OBJS[0], "档案")
        self.assertEqual(c.exception.status, 403)
        with self.assertRaises(DomainError) as c:
            self.svc.decide_accession("研究员", "provenance_researcher", OBJS[0], "部", "G1")
        self.assertEqual(c.exception.status, 403)
        with self.assertRaises(DomainError) as c:
            self.svc.submit_application("研究员", "provenance_researcher", OBJS[0], "conservation")
        self.assertEqual(c.exception.status, 403)

    def test_application_kind_bound_to_role(self) -> None:
        with self.assertRaises(DomainError):
            self.svc.submit_application("修复师", "conservator", OBJS[0], "research")


class AccessionAndCatalogTest(unittest.TestCase):
    def setUp(self) -> None:
        self.svc = new_service()
        seed_batch(self.svc, OBJS[:3])
        seal_and_handover(self.svc, OBJS[0])
        seal_and_handover(self.svc, OBJS[1])
        seal_and_handover(self.svc, OBJS[2])
        self.svc.record_provenance("研究员", "provenance_researcher", OBJS[0], "档案一")
        self.svc.record_provenance("研究员", "provenance_researcher", OBJS[1], "档案二")
        self.svc.decide_accession("批准人", "approver", OBJS[0], "器物部", "G-001")
        self.svc.decide_accession("批准人", "approver", OBJS[1], "书画部", "G-002")

    def test_usable_scopes_expand_after_accession(self) -> None:
        self.assertEqual(set(self.svc.trace(OBJS[0])["usable_scopes"]),
                         {"conservation", "research", "public_display"})

    def test_revoke_keeps_before_and_after_responsibility(self) -> None:
        self.svc.revoke_accession("批准人", "approver", OBJS[0], "编号录入错误")
        trace = self.svc.trace(OBJS[0])
        self.assertFalse(trace["accession"]["active"])
        history = trace["accession_history"]
        self.assertEqual(len(history), 2)
        self.assertEqual(history[0]["accession_number"], "G-001")
        self.assertEqual(history[0]["decided_by"], "批准人")
        self.assertEqual(history[1]["before"]["collection"], "器物部")
        self.assertEqual(history[1]["reason"], "编号录入错误")
        self.assertIn("public_display", self.svc.usable_scopes(self.svc.objects[OBJS[0]]) ) if False else None
        self.assertNotIn("public_display", self.svc.trace(OBJS[0])["usable_scopes"])
        # 撤销后允许重新正确分配。
        self.svc.decide_accession("批准人", "approver", OBJS[0], "器物部", "G-100")
        self.assertTrue(self.svc.trace(OBJS[0])["accession"]["active"])

    def test_released_snapshot_not_rewritten_by_later_findings(self) -> None:
        v1 = self.svc.release_catalog("批准人", "approver")
        self.assertEqual(v1["version"], 1)
        self.assertEqual([e["object_no"] for e in v1["entries"]], [OBJS[0], OBJS[1]])
        digest_v1 = v1["content_sha256"]
        # 后续鉴定引发争议 + 撤销：v1 快照内容与摘要保持不变。
        self.svc.record_dispute("研究员", "provenance_researcher", OBJS[0], "出现新权利主张")
        self.svc.revoke_accession("批准人", "approver", OBJS[1], "分配错误")
        snap1 = self.svc.catalog_view(1)
        self.assertEqual(snap1["content_sha256"], digest_v1)
        self.assertEqual([e["object_no"] for e in snap1["entries"]], [OBJS[0], OBJS[1]])
        # 新版本反映当前状态：争议件、撤销件均排除；OBJ-002 未入藏也排除。
        v2 = self.svc.release_catalog("批准人", "approver")
        self.assertEqual(v2["version"], 2)
        self.assertNotEqual(v2["content_sha256"], digest_v1)
        self.assertEqual(v2["entries"], [])

    def test_disputed_or_restricted_object_excluded(self) -> None:
        self.svc.record_restriction("批准人", "approver", OBJS[1], "public_display", "展出协议限制")
        release = self.svc.release_catalog("批准人", "approver")
        self.assertEqual([e["object_no"] for e in release["entries"]], [OBJS[0]])
        # 解除限制后进入下一版。
        self.svc.lift_restriction("批准人", "approver", OBJS[1], "public_display")
        release2 = self.svc.release_catalog("批准人", "approver")
        self.assertEqual([e["object_no"] for e in release2["entries"]], [OBJS[0], OBJS[1]])

    def test_disputed_object_cannot_accession(self) -> None:
        self.svc.record_dispute("研究员", "provenance_researcher", OBJS[2], "来源争议")
        with self.assertRaises(DomainError):
            self.svc.decide_accession("批准人", "approver", OBJS[2], "部", "G-003")
        self.svc.clear_dispute("研究员", "provenance_researcher", OBJS[2], "档案补齐")
        self.svc.decide_accession("批准人", "approver", OBJS[2], "部", "G-003")

    def test_application_approval_respects_scopes(self) -> None:
        ev = self.svc.submit_application("策展人", "curator", OBJS[2], "public_display")
        # OBJ-002 尚未入藏，不能批准公开展示。
        with self.assertRaises(DomainError):
            self.svc.decide_application("批准人", "approver", ev[0].aggregate_id, "approved")
        self.svc.decide_accession("批准人", "approver", OBJS[2], "部", "G-003")
        self.svc.decide_application("批准人", "approver", ev[0].aggregate_id, "approved", "同意")
        self.assertEqual(self.svc.applications[ev[0].aggregate_id].status, "approved")


class VersionMonotonicTest(unittest.TestCase):
    def test_event_versions_increase_per_aggregate(self) -> None:
        svc = new_service()
        seed_batch(svc, OBJS[:1])
        seal_and_handover(svc, OBJS[0])
        svc.record_material("修复师", "conservator", OBJS[0], "青铜")
        versions = [e.version for e in svc.store.events_for("returned_object", OBJS[0])]
        self.assertEqual(versions, list(range(1, len(versions) + 1)))


if __name__ == "__main__":
    unittest.main()
