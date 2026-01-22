我计划创建一个自动化脚本 `scripts/inject_kdj_chain.py`，用于生成并注入一套完整的 KDJ 策略链条。该脚本将包含四个核心模块的逻辑代码，并在注入数据库前自动执行“接口可用性验证”。

### 1. 策略链条设计 (KDJ 黄金交叉策略)
我们将实现一套基于 KDJ 指标的完整工作流：
*   **指标库 (Indicator)**: `KDJ_Custom` - 负责从 Tushare (优先) 或 Akshare 获取日线数据，并计算 K, D, J 值。
*   **数据实验室 (Lab)**: `KDJ_Backtest_Experiment` - 在 `000001.SZ` (平安银行) 上回测该指标，寻找最佳买入点，验证算法有效性。
*   **选股 (Selection)**: `KDJ_Oversold_Screener` - 扫描市场，寻找 K 值 < 20 (超卖区域) 的潜力股。
*   **规则库 (Rule)**: `KDJ_Golden_Cross_Monitor` - 实时监控指定股票，当 K 线向上穿过 D 线时触发“金叉”报警。

### 2. 技术实现细节
*   **数据源**: 严格按照您的要求，脚本将内置 Tushare Token (`4501928450004005131`) 和专用接口地址，使用 `daily` 接口（适配 5000 积分权限）。若 Tushare 不可用，自动降级至 Akshare 以保证系统稳定性。
*   **注入脚本 (`scripts/inject_kdj_chain.py`)**:
    1.  **定义**: 将上述 4 个模块的 Python 代码定义为字符串常量。
    2.  **验证**: 在脚本运行初期，直接调用这些代码块（模拟运行），确保 API 连接正常且计算逻辑无误。
    3.  **注入**: 验证通过后，通过 SQL/ORM 将脚本分别插入 `indicator_definitions`, `research_scripts`, `stock_screeners`, `rule_scripts` 表。
    4.  **配置**: 向 `stocks` 表插入/更新 `000001.SZ`，并将其 `rule_script_id` 关联到新创建的监控规则，实现“看盘验证”。

### 3. 执行步骤
1.  创建 `scripts/inject_kdj_chain.py` 文件。
2.  运行该脚本进行验证与注入。
3.  检查数据库确认数据已生效。

此方案无需修改现有系统代码，仅以数据形式注入，安全且符合“插件化”扩展思想。