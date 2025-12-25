# 重构监控服务以实现基于信号变化的告警流程

根据您的要求，我将修改 `backend/services/monitor_service.py` 中的 `process_stock` 函数，核心改变是**引入“上次信号”对比机制**，并移除基于内存的重复内容去重逻辑。

## 修改计划细节

### 1. 新增辅助函数 `_get_last_signal_from_db`
- **功能**: 从 `logs` 表中查询指定 `stock_id` 的最新一条记录。
- **逻辑**: 
    - 按 `timestamp` 倒序取第一条。
    - 解析 `ai_analysis` 字段中的 `signal`。
    - 如果没有记录或解析失败，默认返回 `WAIT`。
- **目的**: 为后续的“信号变化”判断提供基准。

### 2. 重构 `process_stock` 核心流程

#### A. 准备阶段
- 在任务开始时，调用 `_get_last_signal_from_db` 获取 `last_signal`。

#### B. 模式分支处理

**场景 1: Script Only (仅硬规则)**
- 执行规则脚本。
- **推断信号**: 根据 `script_signal` (如果脚本显式定义) 或 `script_triggered` 状态推断出 `current_signal` (BUY/WAIT)。
- **决策**:
    - 对比 `current_signal` 与 `last_signal`。
    - **不一致**: 标记 `is_signal_changed = True`，准备发送告警。
    - **一致**: 仅记录日志。

**场景 2: Hybrid (混合模式)**
- 执行规则脚本。
- **推断规则信号**: 获取规则层面的 `rule_derived_signal`。
- **第一层闸门 (Rule vs Last)**:
    - 对比 `rule_derived_signal` 与 `last_signal`。
    - **一致**: 
        - **跳过 AI**。
        - 直接记录日志 (Type: Info, Message: "Rule signal consistent with last DB signal (Wait/Hold), AI skipped")。
        - 结束本次任务。
    - **不一致**:
        - **调用 AI** (传入规则触发信息)。
        - 获取 AI 返回的 `ai_signal`。
        - **第二层闸门 (AI vs Last)**:
            - 对比 `ai_signal` 与 `last_signal`。
            - **不一致**: 标记 `is_signal_changed = True`，准备发送告警。
            - **一致**: 仅记录日志。

**场景 3: AI Only (仅 AI)**
- 直接调用 AI。
- 获取 `ai_signal`。
- **决策**:
    - 对比 `ai_signal` 与 `last_signal`。
    - **不一致**: 标记 `is_signal_changed = True`，准备发送告警。
    - **一致**: 仅记录日志。

### 3. 告警逻辑调整 (Email Trigger)
- **移除**: `alert_config.get("suppress_duplicates")` 相关的内存去重逻辑 (`_last_alert_content_by_stock_id`)。
- **修改触发条件**:
    - 必须满足 `is_signal_changed == True`。
    - 必须满足 `current_signal` 在 `allowed_signals` 列表中 (保留现有配置)。
    - 必须满足 `urgency` 在 `allowed_urgencies` 列表中 (保留现有配置)。
    - 必须满足 每小时限流 (保留现有配置)。

### 4. 日志记录
- 无论是否发邮件，每次运行结果都会写入 DB `logs` 表，确保下一次运行能查到本次的信号。

## 验证与安全
- 这种修改主要涉及逻辑流控制，不涉及外部 API 变更。
- 默认 `last_signal` 为 `WAIT` 确保了首次触发（如 `BUY`）能正常工作。
- “脉冲式”硬规则（触发一次后消失）在 Hybrid 模式下也能正常工作（Rule 变回 WAIT -> 触发 AI -> AI 确认趋势仍为 BUY -> AI(BUY) == Last(BUY) -> 保持静默），符合预期。
