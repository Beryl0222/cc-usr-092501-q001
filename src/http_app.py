"""返还后入藏服务的 HTTP 接口（标准库 http.server）。

命令接口写入领域事件，查询接口从事件流回放后的状态返回：
- GET  /objects/{编号}/trace 返还批次、当前位置、可用范围、冻结原因、历次决定
- GET  /batches/{批次}        到货情况（含部分到货与未到清单）
- GET  /catalog/releases/{v}  不可变目录快照
"""

from __future__ import annotations

import json
import re
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Callable

from .events import DomainError
from .service import AccessionService
from .store import DuplicateEvent, EventStore


def _build(service: AccessionService) -> type[BaseHTTPRequestHandler]:
    class Handler(BaseHTTPRequestHandler):
        server_version = "ReturnedArtifacts/1.0"

        def log_message(self, fmt: str, *args: Any) -> None:  # 静默访问日志
            return

        def _send(self, status: int, body: Any) -> None:
            data = json.dumps(body, ensure_ascii=False, indent=2).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def _read_body(self) -> dict[str, Any]:
            length = int(self.headers.get("Content-Length") or 0)
            if length == 0:
                return {}
            raw = self.rfile.read(length)
            try:
                body = json.loads(raw.decode("utf-8"))
            except (json.JSONDecodeError, UnicodeDecodeError) as error:
                raise DomainError(f"请求体不是合法 JSON：{error}", status=400)
            if not isinstance(body, dict):
                raise DomainError("请求体必须是 JSON 对象", status=400)
            return body

        def _require(self, body: dict[str, Any], *names: str) -> None:
            missing = [n for n in names if body.get(n) in (None, "")]
            if missing:
                raise DomainError(f"缺少参数：{missing}", status=400)

        def _call(self, fn: Callable[..., Any], body: dict[str, Any], *names: str, **fixed: Any) -> None:
            self._require(body, "actor", "role", *names)
            kwargs = {k: body[k] for k in names}
            at = body.get("occurred_at")
            if at:
                kwargs["occurred_at"] = at
            result = fn(body["actor"], body["role"], **kwargs, **fixed)
            self._send(200, self._result_body(result))

        @staticmethod
        def _result_body(result: Any) -> Any:
            if isinstance(result, list):  # 事件列表
                return {"events": [e.as_dict() for e in result], "count": len(result)}
            if isinstance(result, dict):
                out = dict(result)
                if "event" in out:
                    out["event"] = out["event"].as_dict()
                if "events" in out:
                    out["events"] = [e.as_dict() for e in out["events"]]
                return out
            return {"result": result}

        # -------------------------------------------------------------- GET

        def do_GET(self) -> None:  # noqa: N802
            try:
                path = self.path.rstrip("/") or "/"
                if path == "/healthz":
                    self._send(200, {"status": "ok"})
                    return
                m = re.fullmatch(r"/objects/([^/]+)/trace", path)
                if m:
                    with service.exclusive():
                        self._send(200, service.trace(m.group(1)))
                    return
                m = re.fullmatch(r"/batches/([^/]+)", path)
                if m:
                    with service.exclusive():
                        self._send(200, service.batch_view(m.group(1)))
                    return
                m = re.fullmatch(r"/catalog/releases/(\d+)", path)
                if m:
                    with service.exclusive():
                        self._send(200, service.catalog_view(int(m.group(1))))
                    return
                self._send(404, {"error": "未知路径", "path": path})
            except DomainError as error:
                self._send(error.status, {"error": str(error)})

        # -------------------------------------------------------------- POST

        def do_POST(self) -> None:  # noqa: N802
            try:
                body = self._read_body()
                path = self.path.rstrip("/")
                with service.exclusive():
                    self._route(path, body)
            except DomainError as error:
                self._send(error.status, {"error": str(error)})
            except DuplicateEvent as error:
                self._send(409, {"error": f"事件标识冲突：{error}"})

        def _route(self, path: str, body: dict[str, Any]) -> None:
            # 批次登记
            if path == "/batches":
                self._require(body, "actor", "role", "batch_id",
                              "foreign_authority", "object_numbers")
                result = service.register_batch(
                    body["actor"], body["role"], body["batch_id"],
                    body["foreign_authority"], body["object_numbers"],
                    body.get("occurred_at"),
                )
                self._send(201, self._result_body(result))
                return

            # 封签
            m = re.fullmatch(r"/objects/([^/]+)/seal-receipts", path)
            if m:
                self._require(body, "actor", "role", "receipt_id", "seal", "summary")
                result = service.record_seal_receipt(
                    body["actor"], body["role"], m.group(1),
                    body["receipt_id"], body["seal"], body["summary"],
                    body.get("occurred_at"),
                )
                self._send(200, self._result_body(result))
                return
            m = re.fullmatch(r"/seal-receipts/([^/]+)/resolve", path)
            if m:
                self._require(body, "actor", "role", "outcome")
                result = service.resolve_seal_receipt(
                    body["actor"], body["role"], m.group(1),
                    body["outcome"], body.get("note", ""), body.get("occurred_at"),
                )
                self._send(200, self._result_body(result))
                return

            # 点交
            m = re.fullmatch(r"/batches/([^/]+)/handover", path)
            if m:
                self._require(body, "actor", "role", "object_numbers", "location", "custodian")
                result = service.record_handover(
                    body["actor"], body["role"], m.group(1), body["object_numbers"],
                    body["location"], body["custodian"], body.get("occurred_at"),
                )
                self._send(200, self._result_body(result))
                return

            # 检测 / 病害 / 解除
            m = re.fullmatch(r"/objects/([^/]+)/material", path)
            if m:
                self._require(body, "actor", "role", "material")
                result = service.record_material(
                    body["actor"], body["role"], m.group(1), body["material"],
                    body.get("findings", ""), body.get("occurred_at"),
                )
                self._send(200, self._result_body(result))
                return
            m = re.fullmatch(r"/objects/([^/]+)/diseases", path)
            if m:
                self._require(body, "actor", "role", "disease", "severity")
                result = service.report_disease(
                    body["actor"], body["role"], m.group(1), body["disease"],
                    body["severity"], body.get("occurred_at"),
                )
                self._send(200, self._result_body(result))
                return
            m = re.fullmatch(r"/objects/([^/]+)/risk/clear", path)
            if m:
                self._call(service.clear_risk, body, "note", object_no=m.group(1))
                return

            # 来源 / 争议
            m = re.fullmatch(r"/objects/([^/]+)/provenance", path)
            if m:
                self._call(service.record_provenance, body, "record", object_no=m.group(1))
                return
            m = re.fullmatch(r"/objects/([^/]+)/disputes", path)
            if m:
                self._call(service.record_dispute, body, "reason", object_no=m.group(1))
                return
            m = re.fullmatch(r"/objects/([^/]+)/disputes/clear", path)
            if m:
                self._call(service.clear_dispute, body, "note", object_no=m.group(1))
                return

            # 权利限制
            m = re.fullmatch(r"/objects/([^/]+)/restrictions", path)
            if m:
                self._call(service.record_restriction, body, "scope", "reason", object_no=m.group(1))
                return
            m = re.fullmatch(r"/objects/([^/]+)/restrictions/([^/]+)/lift", path)
            if m:
                self._require(body, "actor", "role")
                result = service.lift_restriction(
                    body["actor"], body["role"], m.group(1), m.group(2),
                    body.get("occurred_at"),
                )
                self._send(200, self._result_body(result))
                return

            # 转库
            m = re.fullmatch(r"/objects/([^/]+)/transfers", path)
            if m:
                self._require(body, "actor", "role", "from_location", "from_custodian",
                              "to_location", "to_custodian")
                result = service.transfer_custody(
                    body["actor"], body["role"], m.group(1),
                    body["from_location"], body["from_custodian"],
                    body["to_location"], body["to_custodian"], body.get("occurred_at"),
                )
                self._send(200, self._result_body(result))
                return

            # 衍生申请
            m = re.fullmatch(r"/objects/([^/]+)/applications", path)
            if m:
                self._require(body, "actor", "role", "kind")
                result = service.submit_application(
                    body["actor"], body["role"], m.group(1), body["kind"],
                    body.get("note", ""), body.get("app_id"), body.get("occurred_at"),
                )
                self._send(201, self._result_body(result))
                return
            m = re.fullmatch(r"/applications/([^/]+)/decision", path)
            if m:
                self._call(service.decide_application, body, "decision", app_id=m.group(1))
                return

            # 入藏 / 撤销
            m = re.fullmatch(r"/objects/([^/]+)/accession", path)
            if m:
                self._call(service.decide_accession, body, "collection", "accession_number",
                           object_no=m.group(1))
                return
            m = re.fullmatch(r"/objects/([^/]+)/accession/revoke", path)
            if m:
                self._call(service.revoke_accession, body, "reason", object_no=m.group(1))
                return

            # 目录发布
            if path == "/catalog/releases":
                self._require(body, "actor", "role")
                result = service.release_catalog(body["actor"], body["role"],
                                                 body.get("occurred_at"))
                self._send(201, self._result_body(result))
                return

            self._send(404, {"error": "未知路径", "path": path})

    return Handler


def serve(host: str, port: int, db_path: str) -> ThreadingHTTPServer:
    store = EventStore(db_path)
    service = AccessionService(store)
    httpd = ThreadingHTTPServer((host, port), _build(service))
    httpd.service = service  # type: ignore[attr-defined]
    httpd.store = store  # type: ignore[attr-defined]
    return httpd


def main() -> int:
    import argparse

    parser = argparse.ArgumentParser(description="返还文物入藏责任链服务")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8080)
    parser.add_argument("--db", default="data/accession.sqlite3")
    args = parser.parse_args()
    httpd = serve(args.host, args.port, args.db)
    print(f"服务监听 http://{args.host}:{args.port}（事件库 {args.db}）")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        httpd.server_close()
        httpd.store.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
