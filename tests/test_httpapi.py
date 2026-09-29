"""HTTP 接口端到端测试：真实线程服务器 + urllib 客户端。"""

from __future__ import annotations

import json
import tempfile
import threading
import unittest
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from src.httpapi import build_server

HO = "handover_officer"
CO = "conservator"
PR = "provenance_researcher"
AO = "accessions_officer"
CU = "custodian"
RV = "review_officer"


class HttpCase(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        store = str(Path(self.tmp.name) / "events.jsonl")
        self.httpd, self.state = build_server("127.0.0.1", 0, store)
        self.port = self.httpd.server_address[1]
        self.thread = threading.Thread(target=self.httpd.serve_forever, daemon=True)
        self.thread.start()

    def tearDown(self) -> None:
        self.httpd.shutdown()
        self.httpd.server_close()
        self.thread.join()
        self.tmp.cleanup()

    def request(self, method: str, path: str, body: dict | None = None,
                role: str | None = None, staff: str = "staff-01"):
        url = f"http://127.0.0.1:{self.port}{path}"
        data = json.dumps(body).encode("utf-8") if body is not None else None
        headers = {"Content-Type": "application/json", "X-Staff-Id": staff}
        if role:
            headers["X-Staff-Role"] = role
        req = urllib.request.Request(url, data=data, headers=headers, method=method)
        try:
            with urllib.request.urlopen(req) as resp:
                return resp.status, json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as error:
            return error.code, json.loads(error.read().decode("utf-8"))

    def _arrive_handover(self, object_id: str, *, receipt_no: str = "R-1") -> None:
        self.request("POST", f"/objects/{object_id}/seal-receipts", {
            "receipt_no": receipt_no, "seal": "SEAL-A",
            "condition_digest": "完好", "custodian": "口岸临时库房",
        }, role=HO)
        self.request("POST", f"/objects/{object_id}/handovers", {}, role=HO)

    def _ready(self, object_id: str) -> None:
        self._arrive_handover(object_id)
        self.request("POST", f"/objects/{object_id}/provenance",
                     {"title": "案卷", "source_doc": "case.pdf"}, role=PR)

    def test_full_flow_and_trace(self) -> None:
        status, body = self.request("POST", "/batches", {
            "batch_id": "B-1", "foreign_authority": "Y国执法局",
            "object_ids": ["A-1", "A-2"],
        })
        self.assertEqual(status, 201, body)
        self._ready("A-1")
        status, body = self.request("POST", "/objects/A-1/accession",
                                    {"decision": "APPROVED"}, role=AO)
        self.assertEqual(status, 200, body)

        status, body = self.request("GET", "/objects/A-1/trace")
        self.assertEqual(status, 200)
        self.assertEqual(body["return_batch"]["batch_id"], "B-1")
        self.assertEqual(body["current_location"]["custodian"], "口岸临时库房")
        self.assertIn("PUBLIC", body["availability"]["scopes"])
        self.assertIsNone(body["freeze"])
        self.assertTrue(any(d["event_type"] == "ACCESSION_DECIDED"
                            for d in body["decisions"]))

        status, release = self.request("POST", "/catalog/releases", {}, role=AO)
        self.assertEqual(status, 201)
        self.assertEqual([e["object_id"] for e in release["snapshot"]], ["A-1"])
        self.assertEqual([e["object_id"] for e in release["excluded"]], ["A-2"])

        status, listing = self.request("GET", "/catalog/releases")
        self.assertEqual(status, 200)
        self.assertEqual(listing["releases"][0]["release_id"], "REL-0001")

    def test_duplicate_and_quarantined_receipts_over_http(self) -> None:
        self.request("POST", "/batches", {
            "batch_id": "B-2", "foreign_authority": "Z局", "object_ids": ["Q-1"],
        })
        payload = {"receipt_no": "R-9", "seal": "S-1",
                   "condition_digest": "d1", "custodian": "口岸临时库房"}
        status, first = self.request("POST", "/objects/Q-1/seal-receipts", payload, role=HO)
        self.assertEqual(status, 201)
        status, again = self.request("POST", "/objects/Q-1/seal-receipts", payload, role=HO)
        self.assertEqual(status, 201)
        self.assertTrue(again["idempotent"])

        status, conflict = self.request("POST", "/objects/Q-1/seal-receipts",
                                        {**payload, "seal": "S-2"}, role=HO)
        self.assertEqual(status, 201)
        self.assertTrue(conflict["quarantined"])

        # 点交人员不能自己复核。
        status, denied = self.request("POST", "/objects/Q-1/receipt-review", {
            "receipt_no": "R-9", "decision": "CONFIRM",
        }, role=HO)
        self.assertEqual(status, 403)
        self.assertEqual(denied["error"], "FORBIDDEN")

        status, reviewed = self.request("POST", "/objects/Q-1/receipt-review", {
            "receipt_no": "R-9", "decision": "CONFIRM",
        }, role=RV)
        self.assertEqual(status, 200, reviewed)

    def test_concurrent_transfers_over_http(self) -> None:
        self.request("POST", "/batches", {
            "batch_id": "B-3", "foreign_authority": "W局", "object_ids": ["C-1"],
        })
        self._arrive_handover("C-1")

        def transfer(target: str):
            return self.request("POST", "/objects/C-1/custody-transfers",
                                {"to_custodian": target, "from_custodian": "口岸临时库房"},
                                role=CU)

        with ThreadPoolExecutor(max_workers=2) as pool:
            r1, r2 = list(pool.map(transfer, ["省中心库房", "修复特藏库"]))
        statuses = sorted(r[0] for r in (r1, r2))
        self.assertEqual(statuses, [200, 409])
        _, trace = self.request("GET", "/objects/C-1/trace")
        self.assertEqual(len(trace["current_location"]["custodian_history"]), 1)

    def test_late_assessment_and_recovery(self) -> None:
        self.request("POST", "/batches", {
            "batch_id": "B-4", "foreign_authority": "V局", "object_ids": ["L-1", "L-2"],
        })
        self._ready("L-1")
        self.request("POST", "/objects/L-1/accession", {"decision": "APPROVED"}, role=AO)
        status, release = self.request("POST", "/catalog/releases", {}, role=AO)
        self.assertEqual(status, 201)

        status, result = self.request("POST", "/objects/L-1/assessments", {
            "report_id": "CR-1", "risk": True, "reasons": ["霉变"],
        }, role=CO)
        self.assertEqual(status, 201)
        self.assertEqual(result["published_snapshots_kept"], ["REL-0001"])

        status, snapshot = self.request("GET", "/catalog/releases/REL-0001")
        self.assertEqual(status, 200)
        self.assertEqual(len(snapshot["snapshot"]), 1)
        self.assertEqual(snapshot["immutable_notes"][0]["reason"], "霉变")

        status, recovery = self.request("GET", "/recovery")
        self.assertEqual(status, 200)
        self.assertEqual([f["object_id"] for f in recovery["frozen_objects"]], ["L-1"])
        self.assertIn("L-2", [p["object_id"] for p in recovery["pending_handovers"]])

    def test_revocation_records_before_and_after(self) -> None:
        self.request("POST", "/batches", {
            "batch_id": "B-5", "foreign_authority": "U局", "object_ids": ["V-1"],
        })
        self._ready("V-1")
        self.request("POST", "/objects/V-1/accession",
                     {"decision": "APPROVED", "note": "初核"}, role=AO, staff="approver-jia")
        status, body = self.request("POST", "/objects/V-1/accession/revoke",
                                    {"reason": "分配错误"}, role=AO, staff="approver-yi")
        self.assertEqual(status, 200, body)
        self.assertEqual(body["previous"]["approver"], "approver-jia")
        _, trace = self.request("GET", "/objects/V-1/trace")
        self.assertEqual(trace["accession"]["decision"], "REVOKED")
        self.assertEqual(trace["accession"]["approver"], "approver-yi")
        self.assertEqual(trace["accession"]["previous"]["approver"], "approver-jia")

    def test_requires_identity(self) -> None:
        # 不带身份头也不带 actor：400。
        url = f"http://127.0.0.1:{self.port}/batches"
        req = urllib.request.Request(
            url, data=json.dumps({"batch_id": "B", "foreign_authority": "x",
                                  "object_ids": ["o"]}).encode(),
            headers={"Content-Type": "application/json"}, method="POST")
        try:
            urllib.request.urlopen(req)
            self.fail("应当拒绝无身份请求")
        except urllib.error.HTTPError as error:
            self.assertEqual(error.code, 400)
            self.assertEqual(json.loads(error.read())["error"], "UNAUTHENTICATED")


if __name__ == "__main__":
    unittest.main()
