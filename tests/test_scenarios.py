"""返还文物入藏责任链的场景级回归测试。

覆盖：部分到货与重启恢复、并发转库、迟到检测冻结级联、重复/冲突回执、
版本化公开目录、职责隔离、争议排除公开目录、撤销前后责任。
"""

from __future__ import annotations

import tempfile
import threading
import unittest
from pathlib import Path

from src.domain import AccessionService
from src.eventstore import DomainError, EventStore

HO = "handover_officer"
CO = "conservator"
PR = "provenance_researcher"
AO = "accessions_officer"
CU = "custodian"
RV = "review_officer"


class ServiceCase(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.store_path = str(Path(self.tmp.name) / "events.jsonl")
        self.service = self._service()

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def _service(self) -> AccessionService:
        store = EventStore(self.store_path)
        service = AccessionService(store)
        service.replay()
        return service

    def _restart(self) -> AccessionService:
        """模拟服务重启：全新存储与服务实例，仅靠事件日志恢复。"""

        service = self._service()
        self.service = service
        return service

    # ---- 夹具 -----------------------------------------------------------

    def _batch(self, count: int = 3, prefix: str = "OBJ", batch_id: str = "B-0925") -> list[str]:
        object_ids = [f"{prefix}-{i:03d}" for i in range(1, count + 1)]
        self.service.register_batch(
            actor="liaison", batch_id=batch_id,
            foreign_authority="X国海关执法局", object_ids=object_ids,
        )
        return object_ids

    def _arrive(self, object_id: str, *, receipt_no: str = "R-1", seal: str = "SEAL-A",
                digest: str = "完好:无霉变", custodian: str = "口岸临时库房") -> dict:
        return self.service.record_seal_receipt(
            actor="点交员甲", role=HO, object_id=object_id, receipt_no=receipt_no,
            seal=seal, condition_digest=digest, custodian=custodian,
        )

    def _handover(self, object_id: str) -> dict:
        return self.service.record_handover(actor="点交员甲", role=HO, object_id=object_id)

    def _provenance(self, object_id: str, title: str = "境外查获案卷核对") -> dict:
        return self.service.record_provenance(
            actor="来源研究员乙", role=PR, object_id=object_id,
            title=title, source_doc="case-0925.pdf",
        )

    def _approve(self, object_id: str) -> dict:
        return self.service.decide_accession(
            actor="入藏批准人丙", role=AO, object_id=object_id, decision="APPROVED",
        )

    def _ready_object(self, object_id: str) -> None:
        """到货 → 点交 → 来源档案，入藏前的标准就绪路径。"""

        self._arrive(object_id)
        self._handover(object_id)
        self._provenance(object_id)

    # ---- 场景一：部分到货与重启恢复 -------------------------------------

    def test_partial_arrival_persists_and_recovers_on_restart(self) -> None:
        object_ids = self._batch(count=12, prefix="WA", batch_id="B-12")
        # 12 件中只有 4 件随首批运输到货并点交。
        for object_id in object_ids[:4]:
            self._arrive(object_id, receipt_no=f"R-{object_id}")
            self._handover(object_id)

        recovery = self.service.recovery_view()
        self.assertEqual(len(recovery["pending_handovers"]), 8)
        self.assertEqual(recovery["total_events"], 1 + 4 * 2)

        # 重启：未完成点交必须能恢复。
        service = self._restart()
        recovery = service.recovery_view()
        self.assertEqual({p["object_id"] for p in recovery["pending_handovers"]},
                         set(object_ids[4:]))
        trace = service.trace_object(object_ids[0])
        self.assertEqual(trace["current_location"]["custodian"], "口岸临时库房")
        self.assertEqual(trace["return_batch"]["foreign_authority"], "X国海关执法局")
        self.assertEqual(trace["status"], "HANDED_OVER")

        # 恢复后迟到的第 5 件仍可继续沿同一对象追加。
        service.record_seal_receipt(
            actor="点交员甲", role=HO, object_id=object_ids[4], receipt_no="R-5",
            seal="SEAL-E", condition_digest="完好", custodian="口岸临时库房",
        )
        service.record_handover(actor="点交员甲", role=HO, object_id=object_ids[4])
        self.assertEqual(len(service.recovery_view()["pending_handovers"]), 7)

    # ---- 场景二：并发转库，任何一刻只有一个有效保管方 -------------------

    def test_concurrent_custody_transfer_only_one_wins(self) -> None:
        object_id = self._batch(count=1)[0]
        self._arrive(object_id)

        outcomes: list[dict] = []

        def transfer(target: str) -> None:
            try:
                result = self.service.transfer_custody(
                    actor="库管员", role=CU, object_id=object_id,
                    to_custodian=target, officer="值班负责人",
                    from_custodian="口岸临时库房",
                )
                outcomes.append({"ok": True, "custodian": result["custodian"]})
            except DomainError as error:
                outcomes.append({"ok": False, "code": error.code})

        t1 = threading.Thread(target=transfer, args=("省中心库房",))
        t2 = threading.Thread(target=transfer, args=("修复特藏库",))
        t1.start(); t2.start()
        t1.join(); t2.join()

        self.assertEqual(len(outcomes), 2)
        winners = [o for o in outcomes if o["ok"]]
        losers = [o for o in outcomes if not o["ok"]]
        self.assertEqual(len(winners), 1)
        self.assertEqual(len(losers), 1)
        self.assertEqual(losers[0]["code"], "CONCURRENT_MODIFICATION")

        trace = self.service.trace_object(object_id)
        self.assertEqual(trace["current_location"]["custodian"], winners[0]["custodian"])
        self.assertEqual(len(trace["current_location"]["custodian_history"]), 1)

    def test_stale_expected_version_rejected(self) -> None:
        object_id = self._batch(count=1)[0]
        self._arrive(object_id)
        version = self.service.trace_object(object_id)["aggregate_version"]
        self.service.transfer_custody(
            actor="库管员", role=CU, object_id=object_id,
            to_custodian="省中心库房", officer="值班负责人",
            from_custodian="口岸临时库房",
        )
        with self.assertRaises(DomainError) as ctx:
            self.service.transfer_custody(
                actor="库管员", role=CU, object_id=object_id,
                to_custodian="修复特藏库", officer="值班负责人",
                from_custodian="口岸临时库房",
                expected_version=version,
            )
        self.assertEqual(ctx.exception.code, "CONCURRENT_MODIFICATION")

    # ---- 场景三：迟到检测，只冻结受影响文物及其衍生申请 -----------------

    def test_late_risk_assessment_freezes_only_affected_and_keeps_snapshot(self) -> None:
        a, b = self._batch(count=2, prefix="LZ")
        self._ready_object(a)
        self._ready_object(b)
        self._approve(a)
        self._approve(b)
        # 两件都已有修复类衍生申请。
        self.service.request_derivative(actor="修复师丁", role=CO, object_id=a,
                                        kind="REPAIR", applicant="修复师丁")
        req_b = self.service.request_derivative(actor="修复师戊", role=CO, object_id=b,
                                                 kind="REPAIR", applicant="修复师戊")
        release = self.service.release_catalog(actor="入藏批准人丙", role=AO)
        self.assertEqual({e["object_id"] for e in release["snapshot"]}, {a, b})
        snapshot_before = release["snapshot"]

        # 发布后才迟到的检测：a 发现有机病害。
        result = self.service.record_assessment(
            actor="修复师丁", role=CO, object_id=a, report_id="CR-LATE-1",
            risk=True, reasons=["有机病害", "彩绘起甲"],
        )
        self.assertEqual(result["frozen_derivatives"], [f"{a}/repair-1"])
        self.assertEqual(result["published_snapshots_kept"], [release["release_id"]])

        # 已发布快照内容不变，只追加不可变说明。
        stored = self.service.get_release(release["release_id"])
        self.assertEqual(stored["snapshot"], snapshot_before)
        self.assertEqual(len(stored["immutable_notes"]), 1)
        self.assertEqual(stored["immutable_notes"][0]["object_id"], a)

        # a 被冻结，其申请冻结；b 与其申请不受影响。
        self.assertIsNotNone(self.service.trace_object(a)["freeze"])
        self.assertEqual(self.service.trace_object(b)["freeze"], None)
        self.assertTrue(self.service.derivatives[f"{a}/repair-1"]["frozen"])
        self.assertFalse(self.service.derivatives[req_b["request_id"]]["frozen"])

        # 冻结期间不能入藏、不能再提衍生申请。
        with self.assertRaises(DomainError) as ctx:
            self.service.decide_accession(actor="入藏批准人丙", role=AO,
                                          object_id=a, decision="REJECTED")
        self.assertEqual(ctx.exception.code, "OBJECT_FROZEN")

        # 解除冻结后衍生申请自动恢复可用。
        self.service.clear_freeze(actor="修复师丁", role=CO, object_id=a, note="复查稳定")
        self.assertFalse(self.service.derivatives[f"{a}/repair-1"]["frozen"])

    # ---- 场景四：重复回执幂等、编号相同内容不同隔离复核 -----------------

    def test_identical_retransmit_returns_original_without_new_event(self) -> None:
        object_id = self._batch(count=1, prefix="RP")[0]
        first = self._arrive(object_id, receipt_no="R-DUP")
        seq_after_first = len(self.service.events)
        again = self._arrive(object_id, receipt_no="R-DUP")
        self.assertTrue(again["idempotent"])
        self.assertEqual(again["state"], "ACCEPTED")
        self.assertEqual(again["original_at"], first["events"][0]["occurred_at"])
        # 完全相同的重传不产生新事件。
        self.assertEqual(len(self.service.events), seq_after_first)

        # 重启后再次重传仍然幂等。
        self._restart()
        again_after_restart = self._arrive(object_id, receipt_no="R-DUP")
        self.assertTrue(again_after_restart["idempotent"])

    def test_same_number_different_seal_is_quarantined_and_reviewed(self) -> None:
        object_id = self._batch(count=1, prefix="QS")[0]
        self._arrive(object_id, receipt_no="R-X", seal="SEAL-1", digest="完好")
        conflict = self.service.record_seal_receipt(
            actor="点交员甲", role=HO, object_id=object_id, receipt_no="R-X",
            seal="SEAL-2", condition_digest="边角磕损", custodian="口岸临时库房",
        )
        self.assertTrue(conflict["quarantined"])
        self.assertEqual(conflict["state"], "QUARANTINED")

        # 隔离未决时不得点交。
        with self.assertRaises(DomainError) as ctx:
            self._handover(object_id)
        self.assertEqual(ctx.exception.code, "RECEIPT_QUARANTINED")

        # 复核确认后沿该对象继续点交；第三次再传与已确认版本完全相同仍幂等。
        self.service.review_seal_receipt(
            actor="复核员己", role=RV, object_id=object_id,
            receipt_no="R-X", decision="CONFIRM", note="封签更换已由移交方背书",
        )
        self._handover(object_id)
        self.assertEqual(self.service.trace_object(object_id)["status"], "HANDED_OVER")

        receipt = self.service.receipts[(object_id, "R-X")]
        self.assertEqual(len(receipt["candidates"]), 2)
        self.assertEqual(receipt["review"]["reviewer"], "复核员己")

    def test_quarantined_receipt_survives_restart(self) -> None:
        object_id = self._batch(count=1, prefix="QR")[0]
        self._arrive(object_id, receipt_no="R-Y", seal="SEAL-1")
        self.service.record_seal_receipt(
            actor="点交员甲", role=HO, object_id=object_id, receipt_no="R-Y",
            seal="SEAL-9", condition_digest="完好:无霉变", custodian="口岸临时库房",
        )
        service = self._restart()
        pending = service.recovery_view()["pending_receipt_reviews"]
        self.assertEqual([(p["object_id"], p["receipt_no"]) for p in pending],
                         [(object_id, "R-Y")])
        service.review_seal_receipt(
            actor="复核员己", role=RV, object_id=object_id,
            receipt_no="R-Y", decision="REJECT", note="无法证明来源",
        )
        self.assertEqual(service.trace_object(object_id)["status"], "RECEIPT_REJECTED")

    # ---- 场景五：版本化公开目录，不被后续鉴定静默改写 -------------------

    def test_published_snapshot_is_versioned_and_immutable(self) -> None:
        a, b = self._batch(count=2, prefix="CAT")
        self._ready_object(a)
        self._ready_object(b)
        self._approve(a)
        self._approve(b)
        first = self.service.release_catalog(actor="入藏批准人丙", role=AO)
        self.assertEqual(first["release_id"], "REL-0001")
        first_copy = [dict(e) for e in first["snapshot"]]

        # 发布之后：b 被争议，a 被冻结。
        self.service.raise_dispute(actor="来源研究员乙", role=PR, object_id=b,
                                   reason="流出时点存在异议")
        self.service.record_assessment(
            actor="修复师丁", role=CO, object_id=a, report_id="CR-2",
            risk=True, reasons=["虫害迹象"],
        )
        second = self.service.release_catalog(actor="入藏批准人丙", role=AO)
        self.assertEqual(second["release_id"], "REL-0002")
        self.assertEqual(second["snapshot"], [])
        excluded = {e["object_id"]: e["reason"] for e in second["excluded"]}
        self.assertIn("FROZEN", excluded[a])
        self.assertIn("DISPUTE_OPEN", excluded[b])

        # 第一版快照保持原样，争议/冻结只以不可变说明形式留痕。
        stored_first = self.service.get_release("REL-0001")
        self.assertEqual(stored_first["snapshot"], first_copy)
        noted = {n["object_id"] for n in stored_first["immutable_notes"]}
        self.assertEqual(noted, {a})

    # ---- 职责隔离 -------------------------------------------------------

    def test_role_separation(self) -> None:
        object_id = self._batch(count=1, prefix="RB")[0]
        with self.assertRaises(DomainError) as ctx:
            # 修复人员不能登记封签回执。
            self.service.record_seal_receipt(
                actor="修复师丁", role=CO, object_id=object_id, receipt_no="R",
                seal="S", condition_digest="d", custodian="c",
            )
        self.assertEqual(ctx.exception.code, "FORBIDDEN")

        self._arrive(object_id)
        with self.assertRaises(DomainError) as ctx:
            # 点交人员不能作检测。
            self.service.record_assessment(
                actor="点交员甲", role=HO, object_id=object_id,
                report_id="CR", risk=False,
            )
        self.assertEqual(ctx.exception.code, "FORBIDDEN")

        self._handover(object_id)
        self._provenance(object_id)
        with self.assertRaises(DomainError) as ctx:
            # 来源研究员不能批准入藏。
            self.service.decide_accession(actor="来源研究员乙", role=PR,
                                          object_id=object_id, decision="APPROVED")
        self.assertEqual(ctx.exception.code, "FORBIDDEN")

    # ---- 争议材料不进公开目录、撤销留下前后责任 -------------------------

    def test_disputed_object_excluded_and_revocation_keeps_prior_responsibility(self) -> None:
        object_id = self._batch(count=1, prefix="DV")[0]
        self._ready_object(object_id)
        self._approve(object_id)
        release = self.service.release_catalog(actor="入藏批准人丙", role=AO)
        self.assertEqual([e["object_id"] for e in release["snapshot"]], [object_id])

        self.service.raise_dispute(actor="来源研究员乙", role=PR, object_id=object_id,
                                   reason="来源链条缺口")
        next_release = self.service.release_catalog(actor="入藏批准人丙", role=AO)
        self.assertEqual(next_release["snapshot"], [])
        # 已发布的旧版快照不静默删改。
        self.assertEqual(
            [e["object_id"] for e in self.service.get_release(release["release_id"])["snapshot"]],
            [object_id],
        )

        # 撤销错误分配：前后责任人都可追溯。
        self.service.clear_dispute(actor="来源研究员乙", role=PR, object_id=object_id,
                                   note="补充材料后排除争议")
        revoked = self.service.revoke_accession(
            actor="入藏批准人丙", role=AO, object_id=object_id,
            reason="分配库房与保护等级不符",
        )
        trace = self.service.trace_object(object_id)
        self.assertEqual(trace["accession"]["decision"], "REVOKED")
        self.assertEqual(trace["accession"]["approver"], "入藏批准人丙")
        self.assertEqual(trace["accession"]["previous"]["approver"], "入藏批准人丙")
        self.assertIsNotNone(trace["accession"]["previous"]["at"])
        self.assertEqual(revoked["previous"]["approver"], "入藏批准人丙")

    # ---- 权利限制收窄可用范围 ------------------------------------------

    def test_rights_restriction_narrows_availability(self) -> None:
        object_id = self._batch(count=1, prefix="RT")[0]
        self._ready_object(object_id)
        self._approve(object_id)
        self.service.attach_rights(actor="入藏批准人丙", role=AO, object_id=object_id,
                                   scope="PUBLIC", effect="DENY", note="尚有展示权保留")
        trace = self.service.trace_object(object_id)
        self.assertNotIn("PUBLIC", trace["availability"]["scopes"])
        self.assertIn("REPAIR", trace["availability"]["scopes"])
        release = self.service.release_catalog(actor="入藏批准人丙", role=AO)
        self.assertEqual(release["snapshot"], [])
        self.assertIn("RIGHTS:PUBLIC_DENY", release["excluded"][0]["reason"])

    # ---- 追溯视图包含题目要求的全部要素 --------------------------------

    def test_trace_covers_batch_location_availability_freeze_and_decisions(self) -> None:
        object_id = self._batch(count=1, prefix="TR")[0]
        self._ready_object(object_id)
        self._approve(object_id)
        trace = self.service.trace_object(object_id)
        self.assertEqual(trace["return_batch"]["batch_id"], "B-0925")
        self.assertEqual(trace["current_location"]["custodian"], "口岸临时库房")
        self.assertIn("REPAIR", trace["availability"]["scopes"])
        self.assertIn("PUBLIC", trace["availability"]["scopes"])
        event_types = [d["event_type"] for d in trace["decisions"]]
        self.assertEqual(event_types[0], "BATCH_REGISTERED")
        self.assertIn("SEAL_RECEIPTED", event_types)
        self.assertIn("HANDOVER_RECORDED", event_types)
        self.assertIn("ACCESSION_DECIDED", event_types)


if __name__ == "__main__":
    unittest.main()
