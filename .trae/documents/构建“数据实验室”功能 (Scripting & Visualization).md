# 开发自定义选股功能计划

本计划旨在实现一个基于 Python 脚本和 Akshare 数据的自定义选股器。用户可以编写脚本获取市场数据，过滤股票，并定时运行生成选股结果，最后支持一键加入自选监控。

## 1. 数据库设计 (Backend)

在 `backend/models.py` 中新增两个模型：

### `StockScreener` (选股策略)

用于存储用户的选股脚本和调度配置。

* `id`: Integer, PK

* `name`: String (策略名称)

* `description`: String (描述)

* `script_content`: Text (Python 脚本内容)

* `cron_expression`: String (Cron 表达式，如 "0 15 \* \* \*" 每天下午3点运行)

* `is_active`: Boolean (是否启用定时任务)

* `last_run_at`: DateTime

* `created_at` / `updated_at`

### `ScreenerResult` (选股结果)

用于存储每次运行后的选股结果，供前端展示。

* `id`: Integer, PK

* `screener_id`: Integer, FK

* `run_at`: DateTime

* `result_json`: Text/JSON (存储选出的股票列表，包含代码、名称、相关指标等)

## 2. 后端开发 (Backend)

### 新增服务 `backend/services/screener_service.py`

* **执行引擎**: 实现 `execute_screener_script(script_content)` 方法。

  * 注入 `akshare` 库作为 `ak`。

  * 规定脚本必须定义一个 `df` 变量或返回一个 DataFrame/List 作为结果。

  * 捕获执行日志和错误。

* **调度管理**:

  * 利用现有的 `APScheduler` 实例。

  * 实现 `update_screener_job`，根据 Cron 表达式添加/移除定时任务。

### 新增路由 `backend/routers/screeners.py`

* `POST /screeners`: 创建新策略

* `PUT /screeners/{id}`: 更新策略（同步更新定时任务）

* `GET /screeners`: 获取策略列表

* `POST /screeners/{id}/run`: 立即运行策略（测试用）

* `GET /screeners/{id}/results`: 获取历史结果

### 修改入口 `backend/main.py`

* 注册新的 router。

* 在启动时加载活跃的 Screener 任务到调度器。

## 3. 前端开发 (Frontend)

### 新增依赖

* 添加 `@monaco-editor/react` (如果需要更好的代码编辑体验) 或使用现有组件。

### 新增页面 `ScreenerPage`

包含三个主要区域：

1. **策略列表**: 左侧侧边栏，显示已保存的策略。
2. **代码编辑器**:

   * 提供 Python 代码编辑区域。

   * 预置一个 Akshare 选股模板（例如获取实时行情并按市盈率过滤）。

   * "保存" 和 "立即运行" 按钮。
3. **结果展示区**:

   * 表格展示运行结果 (`ScreenerResult`)。

   * **一键看盘**: 在表格每一行添加 "+" 按钮，点击后调用现有的 `POST /stocks` 接口，将该股票加入 `Stock` 表进行 AI 监控。

### 菜单更新

* 在 `App.tsx` 的侧边栏添加 "自定义选股" 入口。

## 4. 安全说明

* 该功能允许执行任意 Python 代码 (`exec`)。在本地部署环境（个人工具）下是可接受的，但请勿在不可信的网络环境中暴露此端口。

## 5. 开发步骤

1. 创建数据库模型并迁移。
2. 实现后端 Service 和 API。
3. 实现前端界面和交互。
4. 联调测试：编写一个简单的 Akshare 脚本并验证结果入库及一键添加监控功能。

