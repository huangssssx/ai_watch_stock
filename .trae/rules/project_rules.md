# 项目规则（ai_watch_stock）

## DB 安全

- 默认只使用已有的 `backend/stock_watch.db`，禁止隐式新建任何 `.db` 文件
- 任何对 SQLite 的连接/写入前，必须先检查目标 DB 文件已存在；不存在则中止并提示
- 禁止在真实 DB 上运行会执行 `drop_all/create_all` 的测试或脚本
- 如需测试数据库，必须使用独立测试库文件（例如 `stock_watch_test.db`）并与真实库隔离
- 如确需新建/切换 DB 文件，必须提前告知用户原因、路径与影响范围

