"""返还后入藏服务的 HTTP 接口（仅依赖标准库）。

身份通过请求头传递：``X-Staff-Id`` 为经办人，``X-Staff-Role`` 为角色，
服务端据此执行职责隔离。写接口全部为 POST/JSON，查询接口为 GET。

接口一览：

* ``POST /batches``                         登记返还批次
* ``POST /objects/{id}/seal-receipts``      运输封签回执（幂等/隔离）
* ``POST /objects/{id}/receipt-review``     隔离回执复核
* ``POST /objects/{id}/handovers``          现场点交
* ``POST /objects/{id}/assessments``        材质/病害检测
* ``POST /objects/{id}/unfreeze``           解除冻结
* ``POST /objects/{id}/custody-transfers``  实体转库（支持并发版本号）
* ``POST /objects/{id}/provenance``         来源档案
* ``POST /objects/{id}/disputes``           提出/解除争议（``action`` 字段）
* ``POST /objects/{id}/rights``             权利限制
* ``POST /objects/{id}/derivative-requests`` 修复/研究/展示申请
* ``POST /objects/{id}/accession``          入藏决定
* ``POST /objects/{id}/accession/revoke``   撤销错误分配
* ``POST /catalog/releases``                发布版本化公开目录
* ``GET  /catalog/releases[/{id}]``         查看快照
* ``GET  /objects/{id}/trace``              追溯批次、位置、可用范围、决定
* ``GET  /recovery``                        重启后的恢复工作队列
"""

from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import urlsplit

from .domain import AccessionService
from .eventstore import DomainError, EventStore

_STATUS_BY_CODE = {
    "FORBIDDEN": 403,
    "UNKNOWN_OBJECT": 404,
    "UNKNOWN_RECEIPT": 404,
    "UNKNOWN_RELEASE": 404,
    "CONCURRENT_MODIFICATION": 409,
    "BATCH_EXISTS": 409,
    "OBJECT_EXISTS": 409,
    "REQUEST_EXISTS": 409,
}


class ServiceState:
    """持有存储、领域服务与串行化所有命令的锁。"""

    def __init__(self, store_path: str) -> None:
        self.store = EventStore(store_path)
        self.service = AccessionService(self.store)
        self.service.replay()
        self.lock = threading.RLock()


def _build_handler(state: ServiceState) -> type[BaseHTTPRequestHandler]:
    class Handler(BaseHTTPRequestHandler):
        server_version = "ReturnedArtifacts/1.0"

        def log_message(self, fmt: str, *args: Any) -> None:  # 安静：测试不刷日志
            return

        # ---- 响应工具 -------------------------------------------------

        def _send(self, status: int, body: Any) -> None:
            data = json.dumps(body, ensure_ascii=False, indent=2).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def _error(self, error: DomainError) -> None:
            self._send(_STATUS_BY_CODE.get(error.code, 400),
                       {"error": error.code, "message": str(error)})

        def _read_json(self) -> dict[str, Any]:
            length = int(self.headers.get("Content-Length") or 0)
            if length <= 0:
                return {}
            try:
                body = json.loads(self.rfile.read(length).decode("utf-8"))
            except json.JSONDecodeError as error:
                raise DomainError("INVALID_JSON", f"请求体不是合法 JSON：{error}")
            if not isinstance(body, dict):
                raise DomainError("INVALID_JSON", "请求体必须是 JSON 对象")
            return body

        def _identity(self, body: dict[str, Any]) -> tuple[str, str | None]:
            actor = self.headers.get("X-Staff-Id") or body.pop("actor", "")
            role = self.headers.get("X-Staff-Role") or body.pop("role", None)
            if not actor:
                raise DomainError("UNAUTHENTICATED", "缺少经办人身份（X-Staff-Id 头或 actor 字段）")
            return actor, role

        # ---- 路由 -----------------------------------------------------

        def do_GET(self) -> None:  # noqa: N802
            try:
                with state.lock:
                    self._route_read(urlsplit(self.path).path)
            except DomainError as error:
                self._error(error)

        def do_POST(self) -> None:  # noqa: N802
            try:
                body = self._read_json()
                with state.lock:
                    self._route_write(urlsplit(self.path).path, body)
            except DomainError as error:
                self._error(error)
            except KeyError as error:
                self._send(400, {"error": "INVALID_INPUT",
                                 "message": f"请求缺少必填字段：{error.args[0]}"})

        # ---- 查询 -----------------------------------------------------

        def _route_read(self, path: str) -> None:
            if path == "/recovery":
                self._send(200, state.service.recovery_view())
                return
            if path == "/catalog/releases":
                self._send(200, {"releases": state.service.list_releases()})
                return
            prefix = "/catalog/releases/"
            if path.startswith(prefix):
                self._send(200, state.service.get_release(path[len(prefix):]))
                return
            if path.endswith("/trace"):
                object_id = path[len("/objects/"):-len("/trace")]
                self._send(200, state.service.trace_object(object_id))
                return
            self._send(404, {"error": "NOT_FOUND", "message": f"未知路径：{path}"})

        # ---- 命令 -----------------------------------------------------

        def _route_write(self, path: str, body: dict[str, Any]) -> None:
            svc = state.service

            if path == "/batches":
                actor, _ = self._identity(body)
                self._send(201, svc.register_batch(
                    actor=actor,
                    batch_id=body.get("batch_id", ""),
                    foreign_authority=body.get("foreign_authority", ""),
                    object_ids=body.get("object_ids", []),
                ))
                return

            if path == "/catalog/releases":
                actor, role = self._identity(body)
                self._send(201, svc.release_catalog(actor=actor, role=role))
                return

            if not path.startswith("/objects/"):
                self._send(404, {"error": "NOT_FOUND", "message": f"未知路径：{path}"})
                return

            rest = path[len("/objects/"):]
            object_id, _, action = rest.partition("/")
            actor, role = self._identity(body)

            if action == "seal-receipts":
                self._send(201, svc.record_seal_receipt(
                    actor=actor, role=role, object_id=object_id,
                    receipt_no=body["receipt_no"], seal=body.get("seal", ""),
                    condition_digest=body.get("condition_digest", ""),
                    custodian=body.get("custodian", ""),
                ))
            elif action == "receipt-review":
                self._send(200, svc.review_seal_receipt(
                    actor=actor, role=role, object_id=object_id,
                    receipt_no=body["receipt_no"], decision=body["decision"],
                    note=body.get("note", ""),
                ))
            elif action == "handovers":
                self._send(201, svc.record_handover(
                    actor=actor, role=role, object_id=object_id,
                    note=body.get("note", ""),
                ))
            elif action == "assessments":
                self._send(201, svc.record_assessment(
                    actor=actor, role=role, object_id=object_id,
                    report_id=body["report_id"], risk=bool(body.get("risk")),
                    reasons=body.get("reasons", []),
                ))
            elif action == "unfreeze":
                self._send(200, svc.clear_freeze(
                    actor=actor, role=role, object_id=object_id,
                    note=body.get("note", ""),
                ))
            elif action == "custody-transfers":
                self._send(200, svc.transfer_custody(
                    actor=actor, role=role, object_id=object_id,
                    to_custodian=body["to_custodian"],
                    officer=body.get("officer", actor),
                    from_custodian=body.get("from_custodian", ""),
                    expected_version=body.get("expected_version"),
                ))
            elif action == "provenance":
                self._send(201, svc.record_provenance(
                    actor=actor, role=role, object_id=object_id,
                    title=body.get("title", ""), source_doc=body.get("source_doc", ""),
                ))
            elif action == "disputes":
                if body.get("action", "RAISE") == "CLEAR":
                    self._send(200, svc.clear_dispute(
                        actor=actor, role=role, object_id=object_id,
                        note=body.get("note", ""),
                    ))
                else:
                    self._send(201, svc.raise_dispute(
                        actor=actor, role=role, object_id=object_id,
                        reason=body.get("reason", ""),
                    ))
            elif action == "rights":
                self._send(201, svc.attach_rights(
                    actor=actor, role=role, object_id=object_id,
                    scope=body["scope"], effect=body.get("effect", "DENY"),
                    note=body.get("note", ""),
                ))
            elif action == "derivative-requests":
                self._send(201, svc.request_derivative(
                    actor=actor, role=role, object_id=object_id,
                    kind=body["kind"], applicant=body.get("applicant", actor),
                ))
            elif action == "accession":
                self._send(200, svc.decide_accession(
                    actor=actor, role=role, object_id=object_id,
                    decision=body["decision"], note=body.get("note", ""),
                ))
            elif action == "accession/revoke":
                self._send(200, svc.revoke_accession(
                    actor=actor, role=role, object_id=object_id,
                    reason=body.get("reason", ""),
                ))
            else:
                self._send(404, {"error": "NOT_FOUND", "message": f"未知动作：{action}"})

    return Handler


def build_server(host: str, port: int, store_path: str) -> tuple[ThreadingHTTPServer, ServiceState]:
    state = ServiceState(store_path)
    httpd = ThreadingHTTPServer((host, port), _build_handler(state))
    return httpd, state


def serve(host: str = "127.0.0.1", port: int = 8080, store_path: str = "data/events.jsonl") -> None:
    httpd, _ = build_server(host, port, store_path)
    print(f"返还文物入藏服务监听 http://{host}:{port}，事件日志：{store_path}", flush=True)
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        httpd.server_close()
