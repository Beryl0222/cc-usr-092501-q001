# 返还文物入藏责任链

境外执法机关查获并返还的文物，交接完成不等于具备修复、研究或公开展示条件。
本服务让运输封签、现场点交、材质检测、病害报告、来源档案、权利限制、保管交接、
入藏决定与公开目录沿**同一对象**持续追加，所有事实只增不改，可从任一文物追溯
返还批次、当前位置、可用范围、冻结原因和历次决定。

## 核心规则

- **封签回执判重与隔离**：按（回执编号、封签、状态摘要）判重；完全相同的重传只返回
  原结果（不新增事件）；编号相同但封签/摘要不同，或与已采纳封签冲突的回执隔离待复核，
  隔离未清不得点交或入藏。冲突重传作为候选挂起，原封签继续有效，复核采纳才替换。
- **部分到货与恢复**：点交按批次逐件进行，未到文物保持 outstanding；服务重启时从
  SQLite 事件流完整回放，未完成点交、待复核封签、冻结与保管状态全部恢复。
- **风险最小冻结**：材质检测/病害报告（允许迟到，先于点交到达也可挂接）发现高/中风险时，
  只冻结受影响文物本身及其在途衍生申请；复检解除后连带恢复，不波及其它文物。
- **原子保管交接**：实体转库与责任人变更是单事件原子交接，调用方须指认当前保管方，
  陈旧指认在并发下收到 409，任何一刻只有一个有效保管方。
- **职责隔离（RBAC）**：登记/复核（registry）、点交（handover_clerk）、修复
  （conservator）、来源研究（provenance_researcher）、入藏批准（approver）、
  公开展陈申请（curator）各自只能完成职责内动作。
- **争议与权利限制**：争议材料不得进入公开目录；权利限制可针对修复/研究/公开展示
  分别登记与解除，直接收窄可用范围。
- **入藏决定可撤销且留痕**：撤销错误分配产生 `ACCESSION_REVOKED`，携带前后责任快照，
  原决定事件永久保留。
- **目录快照不可变**：`CATALOG_RELEASED` 带版本号与内容 SHA-256；发布后不被后续鉴定、
  争议或撤销静默改写，新版本另行发布。

## 目录

- `contracts/domain.schema.json`：领域对象与事件名称约定（23 种事件、4 类聚合）。
- `data/sample.json`：一条可用于本地联调的示例事件。
- `src/events.py`：事件类型、`Event` 与领域错误。
- `src/envelope.py`：事件信封的基础字段校验。
- `src/store.py`：只增 SQLite 事件存储（事务批量追加、重启回放）。
- `src/service.py`：责任链领域服务（规则、RBAC、状态回放、追溯查询）。
- `src/http_app.py`：HTTP 接口与服务入口。
- `src/cli.py`：检查 JSON 事件文件的命令入口。
- `tests/`：信封、领域规则、HTTP、并发与恢复的回归测试。

## 本地运行

启动服务（默认 `data/accession.sqlite3`，仅需 Python 3.11+ 标准库）：

```bash
python3 -m src.http_app --host 127.0.0.1 --port 8080 --db data/accession.sqlite3
```

检查示例事件 / 运行测试 / 编译检查：

```bash
python3 -m src.cli data/sample.json
python3 -m unittest discover -s tests
python3 -m compileall -q src tests
```

## HTTP 接口摘要

所有命令需在 JSON 体内提供 `actor` 与 `role`；可选 `occurred_at`（带时区 ISO 8601）。

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/batches` | 登记返还批次（应到清单） |
| POST | `/objects/{no}/seal-receipts` | 封签回执（重传回原结果/冲突隔离） |
| POST | `/seal-receipts/{id}/resolve` | 隔离回执复核（accepted/rejected） |
| POST | `/batches/{id}/handover` | 现场点交（支持部分到货） |
| POST | `/objects/{no}/material` | 材质检测（可迟到） |
| POST | `/objects/{no}/diseases` | 病害报告（高/中风险触发冻结） |
| POST | `/objects/{no}/risk/clear` | 复检解除风险 |
| POST | `/objects/{no}/provenance` | 追加来源档案 |
| POST | `/objects/{no}/disputes`、`/disputes/clear` | 争议标记/澄清 |
| POST | `/objects/{no}/restrictions`、`/restrictions/{scope}/lift` | 权利限制/解除 |
| POST | `/objects/{no}/transfers` | 原子转库与责任人变更 |
| POST | `/objects/{no}/applications`、`/applications/{id}/decision` | 衍生申请与审批 |
| POST | `/objects/{no}/accession`、`/accession/revoke` | 入藏批准/撤销留痕 |
| POST | `/catalog/releases` | 发布不可变公开目录快照 |
| GET | `/objects/{no}/trace` | 批次、位置、可用范围、冻结原因、历次决定 |
| GET | `/batches/{id}` | 到货/未到清单（部分到货） |
| GET | `/catalog/releases/{version}` | 取指定版本目录快照 |

快速联调：

```bash
curl -s localhost:8080/healthz
curl -s -X POST localhost:8080/batches -H 'Content-Type: application/json' \
  -d '{"actor":"登记员","role":"registry","batch_id":"B-12","foreign_authority":"X国海关","object_numbers":["OBJ-001"]}'
```
