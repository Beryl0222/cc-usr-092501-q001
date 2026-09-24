# 返还文物入藏责任链

本仓库维护该服务跨模块交换时使用的领域事件信封、中文样例和基础校验入口，使不同业务组件能够用一致的对象标识、事件版本和发生时间传递事实。

## 目录

- `contracts/domain.schema.json`：领域对象与事件名称约定。
- `data/sample.json`：一条可用于本地联调的示例事件。
- `src/envelope.py`：事件信封的基础字段校验。
- `src/cli.py`：检查 JSON 事件文件的命令入口。
- `tests/`：信封和命令入口的回归测试。

## 领域约定

当前交换协议覆盖返还点交、保管交接和状态冻结。事件标识一旦接收不得原地复用为另一份内容，版本必须为正整数，时间采用带时区的 ISO 8601 格式。业务修订通过新的事件表达，原始记录继续用于追溯。

## 本地运行

检查示例事件：

```bash
python3 -m src.cli data/sample.json
```

运行测试：

```bash
python3 -m unittest discover -s tests
```

编译检查：

```bash
python3 -m compileall -q src tests
```

这些命令只使用 Python 标准库，不需要单独运行数据库或其他服务。
