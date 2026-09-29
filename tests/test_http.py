"""HTTP 接口端到端测试。

覆盖：部分到货、封签重复/隔离、迟到检测冻结、并发转库、
职责隔离 403、版本化目录、以及落盘后重启恢复。
"""

from __future__ import annotations

import json
import tempfile
import threading
import unittest
import urllib.error
import urllib.request
from pathlib import Path

from src.http_app import serve
from src.service import AccessionService
from src.store import EventStore

OBJS = [f"OBJ-{i:03d}" for i in range(12)]


class HttpTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.db = str(Path(self.tmp.name) / "events.sqlite3")
        self.httpd = serve("127.0.0.1", 0, self.db)
        self.port = self.httpd.server_address[1]
        self.thread = threading.Thread(target=self.httpd.serve_forever, daemon=True)
        self.thread.start()

    def tearDown(self) -> None:
        self.httpd.shutdown()
        self.httpd.server_close()
        self.httpd.store.close()
        self.tmp.cleanup()

    def req(self, method: str, path: str, body=None):
        data = json.dumps(body).encode() if body is not None else None
        request = urllib.request.Request(
            f"http://127.0.0.1:{self.port}{path}", data=data,
            headers={"Content-Type": "application/json"}, method=method,
        )
        try:
            with urllib.request.urlopen(request) as resp:
                return resp.status, json.loads(resp.read())
        except urllib.error.HTTPError as error:
            return error.code, json.loads(error.read())

    def register(self, objs=OBJS) -> None:
        st, body = self.req("POST", "/batches", {
            "actor": "登记员", "role": "registry", "batch_id": "B-12",
            "foreign_authority": "X国执法机关", "object_numbers": list(objs),
        })
        self.assertEqual(st, 201, body)

    def seal(self, no: str, receipt: str | None = None, seal="SEAL-A", summary="完好"):
        return self.req("POST", f"/objects/{no}/seal-receipts", {
            "actor": "点交员甲", "role": "handover_clerk",
            "receipt_id": receipt or f"R-{no}", "seal": seal, "summary": summary,
        })

    def handover(self, nos, location="临时库房A", custodian="保管员甲"):
        return self.req("POST", "/batches/B-12/handover", {
            "actor": "点交员甲", "role": "handover_clerk",
            "object_numbers": list(nos), "location": location, "custodian": custodian,
        })

    # ---------------------------------------------------------------- 场景

    def test_health(self) -> None:
        st, body = self.req("GET", "/healthz")
        self.assertEqual((st, body["status"]), (200, "ok"))

    def test_partial_arrival_and_trace(self) -> None:
        self.register(OBJS[:3])
        for no in OBJS[:3]:
            self.seal(no)
        st, body = self.handover(OBJS[:2])
        self.assertEqual(st, 200, body)
        st, view = self.req("GET", "/batches/B-12")
        self.assertTrue(view["partial"])
        self.assertEqual(view["outstanding"], [OBJS[2]])
        st, trace = self.req("GET", f"/objects/{OBJS[0]}/trace")
        self.assertEqual(trace["batch_id"], "B-12")
        self.assertEqual(trace["current_location"], "临时库房A")
        self.assertIn("conservation", trace["usable_scopes"])

    def test_duplicate_receipt_returns_original(self) -> None:
        self.register(OBJS[:1])
        st, first = self.seal(OBJS[0])
        self.assertEqual(first["result"], "recorded")
        st, again = self.seal(OBJS[0])
        self.assertEqual(st, 200)
        self.assertEqual(again["result"], "duplicate")
        self.assertEqual(again["original_event_id"], first["events"][0]["event_id"])
        self.assertEqual(again["count"] if "count" in again else 0, 0)

    def test_conflicting_receipt_blocks_handover_until_resolved(self) -> None:
        self.register(OBJS[:1])
        self.seal(OBJS[0])
        st, body = self.seal(OBJS[0], seal="SEAL-B", summary="箱体破损")
        self.assertEqual(body["result"], "quarantined")
        st, body = self.handover([OBJS[0]])
        self.assertEqual(st, 409)
        st, body = self.req("POST", "/seal-receipts/R-OBJ-000/resolve", {
            "actor": "登记员", "role": "registry", "outcome": "rejected", "note": "维持原封签",
        })
        self.assertEqual(st, 200, body)
        st, body = self.handover([OBJS[0]])
        self.assertEqual(st, 200, body)

    def test_late_detection_freezes_target_only(self) -> None:
        self.register(OBJS[:2])
        # OBJ-000 点交前就出现高风险病害。
        st, _ = self.req("POST", f"/objects/{OBJS[0]}/diseases", {
            "actor": "修复师", "role": "conservator", "disease": "粉状锈", "severity": "high",
        })
        self.assertEqual(st, 200)
        self.seal(OBJS[0]); self.seal(OBJS[1])
        self.handover([OBJS[0], OBJS[1]])
        st, trace0 = self.req("GET", f"/objects/{OBJS[0]}/trace")
        self.assertTrue(trace0["frozen"])
        self.assertEqual(trace0["usable_scopes"], [])
        st, trace1 = self.req("GET", f"/objects/{OBJS[1]}/trace")
        self.assertFalse(trace1["frozen"])
        # 冻结件入藏被拒。
        st, body = self.req("POST", f"/objects/{OBJS[0]}/accession", {
            "actor": "批准人", "role": "approver", "collection": "部", "accession_number": "G1",
        })
        self.assertEqual(st, 409, body)

    def test_concurrent_transfer_only_one_wins(self) -> None:
        import http.client

        self.register(OBJS[:1])
        self.seal(OBJS[0])
        self.handover([OBJS[0]])
        outcomes = []

        def transfer(to_location: str, to_custodian: str) -> None:
            conn = http.client.HTTPConnection("127.0.0.1", self.port, timeout=10)
            payload = json.dumps({
                "actor": "批准人", "role": "approver",
                "from_location": "临时库房A", "from_custodian": "保管员甲",
                "to_location": to_location, "to_custodian": to_custodian,
            })
            conn.request("POST", f"/objects/{OBJS[0]}/transfers", payload,
                         {"Content-Type": "application/json"})
            resp = conn.getresponse()
            resp.read()
            outcomes.append((resp.status, to_custodian))
            conn.close()

        threads = [
            threading.Thread(target=transfer, args=("修复室", "保管员乙")),
            threading.Thread(target=transfer, args=("展厅", "保管员丙")),
            threading.Thread(target=transfer, args=("库房C", "保管员丁")),
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        statuses = sorted(s for s, _ in outcomes)
        self.assertEqual(statuses.count(200), 1)
        self.assertEqual(statuses.count(409), 2)
        winner = [c for s, c in outcomes if s == 200][0]
        _, trace = self.req("GET", f"/objects/{OBJS[0]}/trace")
        self.assertEqual(trace["current_custodian"], winner)

    def test_rbac_forbidden(self) -> None:
        self.register(OBJS[:1])
        st, body = self.req("POST", f"/objects/{OBJS[0]}/material", {
            "actor": "研究员", "role": "provenance_researcher", "material": "陶",
        })
        self.assertEqual(st, 403, body)

    def test_versioned_catalog_immutable(self) -> None:
        self.register(OBJS[:2])
        for no in OBJS[:2]:
            self.seal(no)
        self.handover(OBJS[:2])
        for no in OBJS[:2]:
            self.req("POST", f"/objects/{no}/provenance", {
                "actor": "研究员", "role": "provenance_researcher", "record": f"{no} 来源档案",
            })
            self.req("POST", f"/objects/{no}/accession", {
                "actor": "批准人", "role": "approver",
                "collection": "器物部", "accession_number": f"G-{no}",
            })
        st, v1 = self.req("POST", "/catalog/releases", {"actor": "批准人", "role": "approver"})
        self.assertEqual(st, 201)
        self.assertEqual(len(v1["entries"]), 2)
        # 发布后再标记争议：旧快照原样可取。
        self.req("POST", f"/objects/{OBJS[0]}/disputes", {
            "actor": "研究员", "role": "provenance_researcher", "reason": "新主张",
        })
        st, snap1 = self.req("GET", "/catalog/releases/1")
        self.assertEqual(snap1["content_sha256"], v1["content_sha256"])
        self.assertEqual(len(snap1["entries"]), 2)
        st, v2 = self.req("POST", "/catalog/releases", {"actor": "批准人", "role": "approver"})
        self.assertEqual(v2["version"], 2)
        self.assertEqual([e["object_no"] for e in v2["entries"]], [OBJS[1]])

    def test_revoke_accession_keeps_history(self) -> None:
        self.register(OBJS[:1])
        self.seal(OBJS[0])
        self.handover([OBJS[0]])
        self.req("POST", f"/objects/{OBJS[0]}/accession", {
            "actor": "批准人", "role": "approver", "collection": "器物部", "accession_number": "G-WRONG",
        })
        st, _ = self.req("POST", f"/objects/{OBJS[0]}/accession/revoke", {
            "actor": "批准人", "role": "approver", "reason": "编号错误",
        })
        self.assertEqual(st, 200)
        _, trace = self.req("GET", f"/objects/{OBJS[0]}/trace")
        self.assertFalse(trace["accession"]["active"])
        self.assertEqual(len(trace["accession_history"]), 2)
        self.assertEqual(trace["accession_history"][-1]["before"]["accession_number"], "G-WRONG")

    def test_restart_restores_state(self) -> None:
        self.register(OBJS[:2])
        self.seal(OBJS[0])
        self.handover([OBJS[0]])
        self.req("POST", f"/objects/{OBJS[0]}/diseases", {
            "actor": "修复师", "role": "conservator", "disease": "锈", "severity": "high",
        })
        # 关闭 HTTP 服务后，用同一事件库新建服务实例（模拟重启）。
        self.httpd.shutdown()
        self.thread.join()
        self.httpd.server_close()
        self.httpd.store.close()

        httpd2 = serve("127.0.0.1", 0, self.db)
        port2 = httpd2.server_address[1]
        threading.Thread(target=httpd2.serve_forever, daemon=True).start()
        self.httpd = httpd2
        self.port = port2

        st, view = self.req("GET", "/batches/B-12")
        self.assertEqual(view["arrived"], [OBJS[0]])
        self.assertTrue(view["partial"])
        st, trace = self.req("GET", f"/objects/{OBJS[0]}/trace")
        self.assertTrue(trace["frozen"])
        self.assertEqual(trace["current_custodian"], "保管员甲")
        # 重启后未完成的工作可以继续：解冻并完成第二件点交。
        st, _ = self.req("POST", f"/objects/{OBJS[0]}/risk/clear", {
            "actor": "修复师", "role": "conservator", "note": "复检合格",
        })
        self.assertEqual(st, 200)
        self.seal(OBJS[1])
        st, _ = self.handover([OBJS[1]])
        self.assertEqual(st, 200)

    def test_direct_service_replay_matches_http(self) -> None:
        # 独立校验：事件落盘后，领域服务回放得到一致状态。
        self.register(OBJS[:1])
        self.seal(OBJS[0])
        self.handover([OBJS[0]])
        svc = AccessionService(EventStore(self.db))
        trace = svc.trace(OBJS[0])
        self.assertEqual(trace["stage"], "handed_over")
        self.assertEqual(trace["current_custodian"], "保管员甲")


if __name__ == "__main__":
    unittest.main()
