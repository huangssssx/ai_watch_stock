# AI 智能股票监测系统开发计划

根据您的需求，我们将构建一个基于 **React (前端)** + **FastAPI (后端)** + **SQLite (数据库)** 的全栈应用。系统将集成 **AkShare** 获取数据，并调用 **LLM (大模型)** 进行实时分析与预警。

## 1. 技术架构

* **前端**: React, Ant Design (UI组件库), Axios

* **后端**: Python, FastAPI, APScheduler (定时任务), SQLAlchemy (ORM)

* **数据源**: AkShare

* **AI**: OpenAI SDK (兼容 SiliconFlow, DeepSeek 等)

* **数据库**: SQLite (便于本地部署，可无缝迁移至 MySQL)

## 2. 核心功能模块设计

### 2.1 数据模型 (Database)

我们将设计以下核心数据表：

* **Stocks (股票表)**: 存储股票代码、名称、监测开关、监测频率、关联的 Prompt 模版 ID。

* **Indicators (指标配置表)**: 存储 AkShare 接口名、参数模版 (支持 JSON 及占位符)。

* **AIConfigs (AI 配置表)**: 存储不同厂商的 API Key, Base URL, Model Name。

* **Logs (日志/预警表)**: 存储每次分析的原始数据、AI 返回结果、是否触发报警。

### 2.2 后端核心服务

1. **数据获取服务 (DataFetcher)**:

   * 动态调用 `akshare` 接口。

   * 实现参数解析器：支持 `{symbol}`, `{today}`, `{today-N}` 等占位符的自动替换。

   * 数据格式化：将获取的 DataFrame 转换为 LLM 易读的文本格式 (CSV/Markdown)。
2. **AI 分析服务 (AIService)**:

   * 支持多 Provider 切换 (SiliconFlow, OpenAI, DeepSeek)。

   * 构建 Prompt：`System Prompt` + `User Prompt` (包含指标数据)。

   * 解析 AI 返回的 JSON 格式结果。
3. **调度服务 (Scheduler)**:

   * 使用 `APScheduler` 根据每只股票设定的间隔 (10s - 1h) 运行分析任务。
4. **报警服务 (AlertService)**:

   * 当 AI 返回 `{type: "warning"}` 时，触发邮件发送。
5. 注意构建知识库

   1. 用户可以维护知识库将遇到的一些经验同步到数据库中
   2. 在列表中应该有一个选择知识库条文的功能，给 ai 投喂数据时选择带上那些经验给 ai

### 2.3 前端界面

1. **仪表盘**: 展示股票列表，快速开关监测，查看最新状态。
2. **配置中心**:

   * **指标管理**: 添加/编辑 AkShare 接口调用规则。

   * **AI 设置**: 配置不同的模型参数。

   * **策略模板**: 编辑发送给 AI 的 Prompt 预设。
3. **日志视图**: 查看历史分析记录和报警详情。

## 3. 实施步骤

### 第一阶段：项目初始化与基础架构

1. 创建项目目录结构，初始化 `backend` 和 `frontend`。
2. 配置后端 Python 环境及依赖 (`fastapi`, `akshare`, `apscheduler`, `openai`, `sqlalchemy` 等)。
3. 配置前端 React 环境 (`vite`, `antd`).

### 第二阶段：后端核心逻辑实现

1. 实现 `DataFetcher`，完成 AkShare 接口的动态调用与参数替换逻辑测试。
2. 实现 `AIService`，打通与大模型的对话接口。
3. 实现数据库模型与 CRUD 接口。
4. 实现 `Scheduler`，支持动态添加/移除监测任务。

### 第三阶段：前端开发与联调

1. 开发股票列表页：支持添加股票、设置频率、开关监测。
2. 开发配置页：AI 配置、指标配置。
3. 联调前后端，验证“添加股票 -> 开启监测 -> 获取数据 -> AI 分析 -> 报警”全流程。

### 第四阶段：优化与交付

1. 完善邮件发送功能。
2. 增加日志展示。
3. 系统测试与稳定性优化。

***

请确认是否开始执行第一阶段？
