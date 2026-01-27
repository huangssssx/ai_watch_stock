# 项目规则（ai_watch_stock）
## 最重要的规则 
- 必须使用中文和我对话
## DB 安全

- 默认只使用已有的 `backend/stock_watch.db`，禁止隐式新建任何 `.db` 文件
- 任何对 SQLite 的连接/写入前，必须先检查目标 DB 文件已存在；不存在则中止并提示
- 禁止在真实 DB 上运行会执行 `drop_all/create_all` 的测试或脚本
- 如需测试数据库，必须使用独立测试库文件（例如 `stock_watch_test.db`）并与真实库隔离
- 如确需新建/切换 DB 文件，必须提前告知用户原因、路径与影响范围

## 数据来源

- 尽量使用 [tushare](https://tushare.pro/document/2?doc_id=14) 获取股票数据
- 如需新增数据来源，必须先与项目负责人确认并获得授权
- 所有数据获取均需在遵守相关法律法规的前提下进行，禁止进行任何形式的爬虫或数据采集

## AKShare 数据单位规范 (Hand vs. Share)

- **警惕单位陷阱**：AKShare 不同接口返回的成交量单位可能不一致（部分为“股”，部分为“手”）。
- **必须校验**：在使用 `成交量` 或 `成交额` 进行计算（如 VWAP、换手率）前，**必须**进行数量级校验。
- **自适应逻辑**：
  - 推荐使用 `Ratio Check`：计算 `Raw_VWAP = Amount / Volume`。
  - 如果 `Raw_VWAP` 约为当前股价的 100 倍（80-120倍），则说明 Volume 单位为“手”，计算时需除以 100。
  - 如果 `Raw_VWAP` 与当前股价接近（0.8-1.2倍），则说明 Volume 单位为“股”。
- **禁止假设**：永远不要假设 API 返回的单位是固定的，必须在代码中实现自适应防御逻辑。
- 常用接口对照表：`docs/akshare_units.md`

## tushare client 使用方式
- backend/utils/tushare_client.py 中包含 tushare client 的初始化代码，以及常用的接口调用示例
- 接口的作用和参数请参考 docs/tushare 文档
- 目前的 tushare 是 5000 积分权限
- 所有 tushare 接口调用均需在代码中显式使用 `ts` 对象，禁止直接调用 `tushare` 模块
- 所有 tushare 接口调用均需在代码中显式使用 `pro` 对象，禁止直接调用 `tushare` 模块


## 选股脚本编写规范
- 编写选股脚本时请参考：docs/选股脚本编写规则.md

## 指标脚本编写规范
- 编写指标脚本时请参考：docs/指标脚本编写规则.md
- 验证通过的 Tushare 脚本同步到数据库的 indicator_definitions 表中

## 硬规则脚本编写规范
- 编写规则脚本时请参考：docs/硬规则脚本编写规则.md
- 验证通过的规则脚本同步到数据库的 hard_rules 表中

# 项目引导
- 可以参考/ClAUDE.md文件 中的项目引导部分理解项目的设计与实现
