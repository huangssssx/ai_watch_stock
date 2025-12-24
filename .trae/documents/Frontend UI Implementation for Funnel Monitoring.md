# 双层监控架构（硬规则 + AI）前后端开发与贯通方案

为了在前端界面上体现后端已经实现的“双层监控架构”（硬规则 + AI 分析），我们需要开发相应的 UI 组件，并打通前后端的数据流。

## 1. 核心目标
*   **规则管理**：用户可以在前端编写、保存、测试硬规则脚本。
*   **策略配置**：用户在配置股票监控时，可以选择监控模式（AI Only / Script Only / Hybrid）并绑定相应的规则脚本。

## 2. 前端开发计划

### 2.1 新增：`RuleScript` 相关的类型定义与 API
*   **位置**：`frontend/src/types.ts`, `frontend/src/api.ts`
*   **内容**：
    *   定义 `RuleScript` 接口。
    *   定义 `RuleTestPayload`, `RuleTestResponse` 接口。
    *   添加 CRUD API：`getRules`, `createRule`, `updateRule`, `deleteRule`, `testRule`。

### 2.2 新增页面：规则脚本库 (`RuleLibrary`)
*   **位置**：`frontend/src/components/RuleLibrary.tsx`
*   **功能**：
    *   **列表展示**：显示已有的规则脚本。
    *   **编辑器**：集成代码编辑器（如 `monaco-editor` 或简单的 `TextArea`）编写 Python 脚本。
    *   **测试功能**：输入股票代码，实时运行脚本查看 `triggered` 状态和 `message` 输出。
    *   **保存/删除**：管理脚本。
*   **导航**：在 `App.tsx` 的侧边栏添加“规则库”入口。

### 2.3 升级：股票配置弹窗 (`StockConfigModal`)
*   **位置**：`frontend/src/components/StockConfigModal.tsx`
*   **变更**：
    *   新增 **“监控模式”** 下拉选择框 (`monitoring_mode`)：
        *   `ai_only` (仅 AI，默认)
        *   `script_only` (仅脚本)
        *   `hybrid` (混合/漏斗模式)
    *   新增 **“关联规则”** 下拉选择框 (`rule_script_id`)：
        *   当模式选择 `script_only` 或 `hybrid` 时显示并必填。
        *   从后端加载可选的 `RuleScript` 列表。
    *   **交互优化**：根据选择的模式，动态显示/隐藏“AI 配置”或“关联规则”。例如选 `script_only` 时隐藏 AI 配置。

## 3. 后端适配与验证 (已就绪)
*   后端已完成 Schema 变更、API 开发 (`/rules`) 和监控逻辑重构。
*   前端只需对接现有 API。

## 4. 实施步骤
1.  **定义类型与 API**：更新 `types.ts` 和 `api.ts`。
2.  **开发规则库页面**：创建 `RuleLibrary.tsx` 并注册到路由。
3.  **改造配置弹窗**：修改 `StockConfigModal.tsx` 支持模式选择与规则绑定。
4.  **联调测试**：
    *   创建一个规则（如“价格 > 0”）。
    *   配置一只股票使用“混合模式”绑定该规则。
    *   观察日志或邮件，确认流程贯通。

## 5. 预期效果
用户进入系统后，可以先在“规则库”写好监控脚本，然后在“看盘”页面对特定股票开启“混合监控”。系统将仅在脚本触发时调用 AI，既节省成本又精准捕捉机会。