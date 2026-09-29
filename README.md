# 返还文物入藏责任链

境外执法机关查获并返还的文物，其证据封存、入境检查、临时库房保管与最终入藏分散在不同机构。本服务把运输封签、现场点交、材质检测、病害报告、来源档案、权利限制、衍生申请、入藏决定与公开目录发布沿**同一对象**持续追加为事件，提供追溯、冻结、原子交接与版本化公开能力。

## 目录

- `contracts/domain.schema.json`：领域对象与事件名称约定（事件枚举的权威列表）。
- `data/sample.json`：一条可用于本地联调的示例事件。
- `src/envelope.py`：事件信封的基础字段校验。
- `src/eventstore.py`：仅追加 JSONL 事件存储、乐观并发控制与重启重放。
- `src/domain.py`：入藏责任链领域服务（全部业务规则）。
- `src/httpapi.py`：HTTP 接口（仅依赖标准库）。
- `src/cli.py`：事件文件校验与 `serve` 启动入口。
- `tests/`：信封回归、领域场景与 HTTP 端到端测试。

## 领域规则

事件一经接收不得原地改写，业务修订一律追加新事件，原始记录继续用于追溯。在此之上：

- **部分到货**：批次可登记 12 件而只到货部分件；未到货件在恢复视图中保持 `EXPECTED`。
- **回执三态**：
  - 编号 + 封签 + 状态摘要**完全相同**的重传：幂等返回原结果，不产生新事件；
  - 编号相同但封签或摘要不同：`SEAL_QUARANTINED` 隔离，复核（确认/驳回）前禁止点交；
  - 复核确认后才能继续点交。
- **迟到检测与冻结级联**：检测发现风险只冻结**该文物**及其衍生申请（修复/研究/展示），不波及同批其他文物；解冻后申请恢复。冻结期间不得入藏。
- **原子保管交接**：转库必须声明移交方 `from_custodian`，与当前唯一有效保管方一致才允许提交，配合聚合版本号构成乐观并发控制；并发转库恰好一个成功，任何一刻只有一个有效保管方，历史全程留痕。
- **职责隔离（RBAC）**：
  - `handover_officer` 封签回执、现场点交；
  - `conservator` 材质/病害检测、冻结解除、修复申请；
  - `provenance_researcher` 来源档案、争议提出/解除、研究申请；
  - `accessions_officer` 入藏批准/撤销、权利限制、展示申请、目录发布；
  - `custodian` 转库交接；`review_officer` 隔离复核。
- **争议与权利限制**：存在未决来源争议或被拒绝公开的文物不得进入公开目录。
- **入藏前置**：已点交、未冻结、无未决争议、具备来源档案方可批准/驳回。
- **撤销留痕**：撤销错误入藏分配记录 `previous` 原批准人与时间，前后责任都可追溯。
- **版本化不可变目录**：每次发布生成 `REL-0001…` 快照；发布之后的鉴定/争议**不会静默改写**旧快照，只向该版本追加不可变说明（`CATALOG_RELEASE_IMMUTABLE`）。
- **重启恢复**：状态全部由事件日志推导，重启后 `/recovery` 给出未完成点交、待复核封签、冻结中文物。

## 本地运行

校验示例事件：

```bash
python3 -m src.cli data/sample.json
```

启动服务：

```bash
python3 -m src.cli serve --host 127.0.0.1 --port 8080 --store data/events.jsonl
```

运行测试：

```bash
python3 -m unittest discover -s tests
```

编译检查：

```bash
python3 -m compileall -q src tests
```

以上命令只使用 Python 标准库。

## HTTP 接口

写请求为 POST + JSON，身份用请求头 `X-Staff-Id`（经办人）与 `X-Staff-Role`（角色）传递；查询为 GET。冲突返回 `409 CONCURRENT_MODIFICATION`，越权返回 `403 FORBIDDEN`。

| 方法 & 路径 | 作用 |
| --- | --- |
| `POST /batches` | 登记返还批次与预期文物清单 |
| `POST /objects/{id}/seal-receipts` | 运输封签回执（幂等 / 隔离） |
| `POST /objects/{id}/receipt-review` | 隔离复核 `CONFIRM`/`REJECT` |
| `POST /objects/{id}/handovers` | 现场点交 |
| `POST /objects/{id}/assessments` | 材质/病害检测（`risk:true` 触发冻结级联） |
| `POST /objects/{id}/unfreeze` | 风险排除后解冻 |
| `POST /objects/{id}/custody-transfers` | 原子转库（`from_custodian`、可选 `expected_version`） |
| `POST /objects/{id}/provenance` | 追加来源档案 |
| `POST /objects/{id}/disputes` | 提出争议（`action:"CLEAR"` 解除） |
| `POST /objects/{id}/rights` | 权利限制 `REPAIR`/`RESEARCH`/`PUBLIC` |
| `POST /objects/{id}/derivative-requests` | 修复/研究/展示申请 |
| `POST /objects/{id}/accession` | 入藏 `APPROVED`/`REJECTED` |
| `POST /objects/{id}/accession/revoke` | 撤销错误分配（前后责任留痕） |
| `POST /catalog/releases` | 发布版本化公开目录 |
| `GET /catalog/releases[/{id}]` | 快照列表 / 指定快照（含不可变说明） |
| `GET /objects/{id}/trace` | 追溯返还批次、当前位置、可用范围、冻结原因、历次决定 |
| `GET /recovery` | 重启后的恢复工作队列 |

### 调用示例

```bash
curl -X POST http://127.0.0.1:8080/batches \
  -H "Content-Type: application/json" -H "X-Staff-Id: liaison-01" \
  -d '{"batch_id":"B-0925","foreign_authority":"X国海关执法局","object_ids":["W-1","W-2"]}'

curl -X POST http://127.0.0.1:8080/objects/W-1/custody-transfers \
  -H "Content-Type: application/json" -H "X-Staff-Id: custodian-01" -H "X-Staff-Role: custodian" \
  -d '{"from_custodian":"口岸临时库房","to_custodian":"省中心库房","officer":"值班负责人"}'
```

## 测试覆盖

`tests/test_scenarios.py` 与 `tests/test_httpapi.py` 覆盖：部分到货与重启恢复、并发转库（恰好一个成功）、迟到检测的冻结级联与快照不可变、重复回执幂等与冲突隔离、版本化公开目录，以及职责隔离、争议排除、撤销前后责任、权利限制收窄可用范围。
