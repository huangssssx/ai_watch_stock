# AI股票监控系统 - 回测系统开发计划

> 文档版本: v1.0
> 创建日期: 2026-01-18
> 状态: 设计阶段

---

## 一、项目概述

### 1.1 目标
构建一个完整的策略回测系统，支持：
- 基于历史数据验证交易策略
- 生成详细的回测报告和性能指标
- 可视化展示回测结果
- 参数优化功能

### 1.2 设计原则
- **原子化开发**: 每个功能点独立可测，支持增量开发
- **复用现有能力**: 复用现有的数据获取、脚本执行、AI分析能力
- **扩展性**: 支持后续添加更多回测类型和指标
- **用户友好**: 提供清晰的配置界面和结果展示

---

## 二、系统架构设计

### 2.1 技术架构图

```
┌─────────────────────────────────────────────────────────────────┐
│                         前端 (Frontend)                          │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐         │
│  │回测配置页│  │回测结果页│  │报告对比页│  │可视化图表│         │
│  └────┬─────┘  └────┬─────┘  └────┬─────┘  └────┬─────┘         │
└───────┼────────────┼────────────┼─────────────┼────────────────┘
        │            │            │              │
        └────────────┴────────────┴──────────────┼────────────────┘
                                                     │
┌────────────────────────────────────────────────────┼───────────┐
│                      后端API层                      │           │
│  ┌───────────────────────────────────────────────┐│           │
│  │  backtest_router.py - 回测API接口             ││           │
│  └───────────────────┬───────────────────────────┘│           │
└──────────────────────┼─────────────────────────────┼───────────┘
                       │                             │
┌──────────────────────┼─────────────────────────────┼───────────┐
│                    服务层                           │           │
│  ┌───────────────┐  ┌───────────────┐             │           │
│  │backtest_      │  │backtest_      │             │           │
│  │executor.py    │  │analyzer.py    │             │           │
│  │回测执行引擎   │  │性能分析器     │             │           │
│  └───────┬───────┘  └───────┬───────┘             │           │
│          │                  │                       │           │
│  ┌───────┴───────┐  ┌───────┴───────┐             │           │
│  │backtest_      │  │backtest_      │             │           │
│  │reporter.py    │  │optimizer.py   │             │           │
│  │报告生成器     │  │参数优化器     │             │           │
│  └───────────────┘  └───────────────┘             │           │
└──────────────────────┬─────────────────────────────┼───────────┘
                       │                             │
┌──────────────────────┼─────────────────────────────┼───────────┐
│                    数据层                           │           │
│  ┌──────────────────────┐  ┌────────────────────┐  │           │
│  │ backtest_data.py     │  │ 现有:             │  │           │
│  │ 历史数据获取与管理   │  │ - data_fetcher.py  │  │           │
│  └──────────────────────┘  │ - ai_service.py    │  │           │
│  ┌──────────────────────┐  │ - monitor_service │  │           │
│  │ backtest_models.py   │  │                    │  │           │
│  │ 回测数据模型         │  │                    │  │           │
│  └──────────────────────┘  └────────────────────┘  │           │
└──────────────────────┬─────────────────────────────┼───────────┘
                       │                             │
┌──────────────────────┼─────────────────────────────┼───────────┐
│                    持久化层                         │           │
│  ┌──────────────────────────────────────────────┐ │           │
│  │  SQLite Database - stock_watch.db            │ │           │
│  │  ┌─────────────────┐  ┌──────────────────┐  │ │           │
│  │  │BacktestConfig   │  │BacktestRun       │  │ │           │
│  │  │回测配置         │  │回测执行记录     │  │ │           │
│  │  └─────────────────┘  └──────────────────┘  │ │           │
│  │  ┌─────────────────┐  ┌──────────────────┐  │ │           │
│  │  │BacktestTrade    │  │BacktestReport    │  │ │           │
│  │  │交易记录         │  │回测报告         │  │           │
│  │  └─────────────────┘  └──────────────────┘  │ │           │
│  └──────────────────────────────────────────────┘ │           │
└────────────────────────────────────────────────────┴───────────┘
```

### 2.2 核心组件说明

| 组件名称 | 文件路径 | 职责描述 |
|---------|---------|---------|
| 回测执行引擎 | `backend/services/backtest_executor.py` | 执行回测逻辑，管理回测生命周期 |
| 性能分析器 | `backend/services/backtest_analyzer.py` | 计算回测性能指标 |
| 报告生成器 | `backend/services/backtest_reporter.py` | 生成结构化回测报告 |
| 参数优化器 | `backend/services/backtest_optimizer.py` | 参数网格搜索和优化 |
| 数据管理器 | `backend/services/backtest_data.py` | 历史数据获取和缓存 |
| 回测模型 | `backend/models.py` (新增) | 数据库模型定义 |
| 回测API | `backend/routers/backtest.py` | HTTP API接口 |

---

## 三、数据库设计

### 3.1 数据表模型

#### 3.1.1 BacktestConfig - 回测配置表
```python
class BacktestConfig(Base):
    __tablename__ = "backtest_configs"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, unique=True, index=True)                    # 配置名称
    description = Column(String, nullable=True)                        # 描述

    # 策略配置
    strategy_type = Column(String, default="script")                   # script, ai, hybrid
    rule_script_id = Column(Integer, ForeignKey("rule_scripts.id"), nullable=True)
    ai_provider_id = Column(Integer, ForeignKey("ai_configs.id"), nullable=True)

    # 回测参数
    initial_capital = Column(Float, default=100000.0)                  # 初始资金
    commission_rate = Column(Float, default=0.0003)                    # 手续费率
    slippage_rate = Column(Float, default=0.0)                         # 滑点率
    position_size = Column(Float, default=1.0)                         # 单次仓位比例(0-1)

    # 股票池配置
    stock_symbols = Column(Text, default="[]")                         # JSON: ["600519", "000001"]

    # 时间范围
    start_date = Column(String)                                        # YYYY-MM-DD
    end_date = Column(String)                                          # YYYY-MM-DD

    # 高级配置
    indicators_json = Column(Text, default="[]")                       # 关联指标ID列表
    custom_params = Column(Text, default="{}")                         # 自定义参数JSON

    is_pinned = Column(Boolean, default=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())
```

#### 3.1.2 BacktestRun - 回测执行记录表
```python
class BacktestRun(Base):
    __tablename__ = "backtest_runs"

    id = Column(Integer, primary_key=True, index=True)
    config_id = Column(Integer, ForeignKey("backtest_configs.id"), index=True)
    name = Column(String, index=True)                                  # 运行名称
    status = Column(String, default="pending")                         # pending, running, completed, failed

    # 执行参数快照
    params_snapshot = Column(Text, default="{}")                       # 执行时的参数JSON

    # 执行信息
    started_at = Column(DateTime(timezone=True), nullable=True)
    completed_at = Column(DateTime(timezone=True), nullable=True)
    error_message = Column(Text, nullable=True)

    # 关联结果
    trades = relationship("BacktestTrade", back_populates="run", cascade="all, delete-orphan")
    report = relationship("BacktestReport", back_populates="run", uselist=False, cascade="all, delete-orphan")
```

#### 3.1.3 BacktestTrade - 交易记录表
```python
class BacktestTrade(Base):
    __tablename__ = "backtest_trades"

    id = Column(Integer, primary_key=True, index=True)
    run_id = Column(Integer, ForeignKey("backtest_runs.id"), index=True)

    symbol = Column(String, index=True)                                # 股票代码
    trade_type = Column(String)                                        # buy, sell

    # 价格信息
    price = Column(Float)                                              # 成交价格
    shares = Column(Float)                                             # 成交数量
    amount = Column(Float)                                             # 成交金额

    # 成本计算
    commission = Column(Float, default=0.0)                            # 手续费
    slippage = Column(Float, default=0.0)                              # 滑点损失

    # 时间信息
    trade_date = Column(String)                                        # YYYY-MM-DD
    trade_time = Column(String, nullable=True)                         # HH:MM:SS (如果有)

    # 信号来源
    signal_source = Column(String, default="strategy")                 # strategy, ai, manual
    signal_metadata = Column(Text, nullable=True)                      # 信号元数据JSON

    created_at = Column(DateTime(timezone=True), server_default=func.now())

    run = relationship("BacktestRun", back_populates="trades")
```

#### 3.1.4 BacktestReport - 回测报告表
```python
class BacktestReport(Base):
    __tablename__ = "backtest_reports"

    id = Column(Integer, primary_key=True, index=True)
    run_id = Column(Integer, ForeignKey("backtest_runs.id"), unique=True, index=True)

    # 基础指标
    total_return = Column(Float, default=0.0)                          # 总收益率
    annual_return = Column(Float, default=0.0)                         # 年化收益率
    total_trades = Column(Integer, default=0)                          # 总交易次数
    winning_trades = Column(Integer, default=0)                        # 盈利交易数
    losing_trades = Column(Integer, default=0)                         # 亏损交易数
    win_rate = Column(Float, default=0.0)                              # 胜率

    # 风险指标
    max_drawdown = Column(Float, default=0.0)                          # 最大回撤
    max_drawdown_duration = Column(Integer, default=0)                 # 最大回撤持续天数
    volatility = Column(Float, default=0.0)                            # 波动率

    # 盈亏指标
    avg_profit = Column(Float, default=0.0)                            # 平均盈利
    avg_loss = Column(Float, default=0.0)                              # 平均亏损
    profit_factor = Column(Float, default=0.0)                         # 盈亏比
    expectancy = Column(Float, default=0.0)                            # 期望收益

    # 资金曲线
    equity_curve = Column(Text, default="[]")                          # JSON: [{date, equity}]

    # 详细统计
    sharpe_ratio = Column(Float, nullable=True)                        # 夏普比率
    sortino_ratio = Column(Float, nullable=True)                       # 索提诺比率
    calmar_ratio = Column(Float, nullable=True)                        # 卡玛比率

    # 交易统计
    avg_hold_days = Column(Float, default=0.0)                         # 平均持仓天数
    best_trade = Column(Float, default=0.0)                            # 最佳交易收益
    worst_trade = Column(Float, default=0.0)                           # 最差交易收益

    created_at = Column(DateTime(timezone=True), server_default=func.now())

    run = relationship("BacktestRun", back_populates="report")
```

#### 3.1.5 BacktestOptimization - 参数优化记录表
```python
class BacktestOptimization(Base):
    __tablename__ = "backtest_optimizations"

    id = Column(Integer, primary_key=True, index=True)
    config_id = Column(Integer, ForeignKey("backtest_configs.id"), index=True)
    name = Column(String, index=True)
    status = Column(String, default="pending")                         # pending, running, completed, failed

    # 优化配置
    optimization_method = Column(String, default="grid")               # grid, random, bayesian
    optimization_metric = Column(String, default="sharpe_ratio")       # 优化目标指标
    param_space_json = Column(Text, default="{}")                      # 参数空间定义

    # 执行信息
    started_at = Column(DateTime(timezone=True), nullable=True)
    completed_at = Column(DateTime(timezone=True), nullable=True)

    # 优化结果
    best_params = Column(Text, default="{}")                           # 最优参数JSON
    best_metric_value = Column(Float, nullable=True)                   # 最优指标值
    total_iterations = Column(Integer, default=0)                      # 总迭代次数

    # 所有运行结果关联
    run_ids = Column(Text, default="[]")                               # 关联的run_id列表
```

### 3.2 数据库迁移
- 在 `main.py` 的 `ensure_db_schema()` 函数中添加新表创建逻辑
- 支持在线添加新字段，不破坏现有数据

---

## 四、后端功能模块

### 4.1 数据管理器 (backtest_data.py)

**职责**: 历史数据获取、缓存和管理

| 功能点 | 描述 | 依赖 |
|-------|------|-----|
| F1. 获取股票历史日线 | 使用AkShare获取指定日期范围的日线数据 | data_fetcher |
| F2. 获取历史分钟线 | 获取分钟级别历史数据（可选） | data_fetcher |
| F3. 数据缓存 | 本地缓存历史数据，减少API调用 | filesystem/sqlite |
| F4. 数据清洗 | 复权、缺失值处理、异常值处理 | pandas |
| F5.交易日历 | 获取交易日列表，处理非交易日 | akshare |
| F6. 数据预加载 | 批量预加载多只股票数据 | data_fetcher |

### 4.2 回测执行引擎 (backtest_executor.py)

**职责**: 核心回测逻辑实现

| 功能点 | 描述 | 优先级 |
|-------|------|-------|
| E1. 初始化回测环境 | 创建回测上下文，初始化资金、持仓 | P0 |
| E2. 逐日循环引擎 | 按交易日顺序执行回测逻辑 | P0 |
| E3. 信号生成 | 调用策略脚本/AI生成交易信号 | P0 |
| E4. 订单执行 | 模拟订单成交，计算手续费和滑点 | P0 |
| E5. 持仓管理 | 维护当前持仓状态和可用资金 | P0 |
| E6. 交易记录 | 记录每笔交易的详细信息 | P0 |
| E7. 资金曲线更新 | 计算每日净值 | P0 |
| E8. 止损止盈 | 支持止损止盈逻辑 | P1 |
| E9. 分批建仓 | 支持分批买入策略 | P2 |
| E10. 多策略组合 | 同时运行多个策略 | P2 |

### 4.3 性能分析器 (backtest_analyzer.py)

**职责**: 计算回测性能指标

| 功能点 | 描述 | 优先级 |
|-------|------|-------|
| A1. 基础收益指标 | 总收益率、年化收益率 | P0 |
| A2. 交易统计 | 总交易次数、胜率、盈亏比 | P0 |
| A3. 最大回撤计算 | 计算最大回撤和持续时间 | P0 |
| A4. 资金曲线生成 | 生成每日净值序列 | P0 |
| A5. 夏普比率 | 计算风险调整后收益 | P1 |
| A6. 索提诺比率 | 下行风险调整收益 | P1 |
| A7. 卡玛比率 | 回撤调整收益 | P1 |
| A8. 波动率计算 | 收益率标准差 | P1 |
| A9. 月度收益分析 | 按月统计收益分布 | P2 |
| A10. 交易分布分析 | 分析盈利/亏损分布特征 | P2 |

### 4.4 报告生成器 (backtest_reporter.py)

**职责**: 生成结构化回测报告

| 功能点 | 描述 | 优先级 |
|-------|------|-------|
| R1. 生成JSON报告 | 输出标准JSON格式报告 | P0 |
| R2. 生成HTML报告 | 生成可视化HTML报告 | P1 |
| R3. 交易明细导出 | 导出CSV格式交易记录 | P1 |
| R4. 图表数据生成 | 生成图表所需的数据格式 | P1 |
| R5. 报告对比 | 对比多次回测结果 | P2 |
| R6. PDF报告生成 | 生成PDF格式报告 | P2 |

### 4.5 参数优化器 (backtest_optimizer.py)

**职责**: 策略参数优化

| 功能点 | 描述 | 优先级 |
|-------|------|-------|
| O1. 网格搜索 | 遍历参数空间所有组合 | P1 |
| O2. 随机搜索 | 随机采样参数组合 | P2 |
| O3. 贝叶斯优化 | 智能参数搜索 | P2 |
| O4. 并行执行 | 多进程并发回测 | P1 |
| O5. 优化进度跟踪 | 实时反馈优化进度 | P1 |
| O6. 最优参数推荐 | 根据指标推荐最优参数 | P1 |

### 4.6 回测API路由 (routers/backtest.py)

**职责**: HTTP API接口定义

| 端点 | 方法 | 描述 |
|------|------|------|
| `/api/backtest/configs` | GET | 获取所有回测配置 |
| `/api/backtest/configs` | POST | 创建回测配置 |
| `/api/backtest/configs/{id}` | GET | 获取单个配置 |
| `/api/backtest/configs/{id}` | PUT | 更新配置 |
| `/api/backtest/configs/{id}` | DELETE | 删除配置 |
| `/api/backtest/runs` | GET | 获取回测运行记录 |
| `/api/backtest/runs` | POST | 启动新回测 |
| `/api/backtest/runs/{id}` | GET | 获取运行详情 |
| `/api/backtest/runs/{id}/stop` | POST | 停止运行中回测 |
| `/api/backtest/runs/{id}/trades` | GET | 获取交易记录 |
| `/api/backtest/runs/{id}/report` | GET | 获取回测报告 |
| `/api/backtest/optimize` | POST | 启动参数优化 |
| `/api/backtest/optimize/{id}` | GET | 获取优化进度 |
| `/api/backtest/compare` | POST | 对比多次回测 |

---

## 五、前端功能模块

### 5.1 页面组件结构

```
frontend/src/pages/backtest/
├── BacktestConfigPage.tsx          # 回测配置管理页面
├── BacktestRunPage.tsx              # 回测执行页面
├── BacktestResultPage.tsx           # 回测结果详情页
├── BacktestComparePage.tsx          # 回测对比页面
└── BacktestOptimizePage.tsx         # 参数优化页面

frontend/src/components/backtest/
├── BacktestConfigModal.tsx          # 配置编辑弹窗
├── BacktestProgressCard.tsx         # 回测进度卡片
├── BacktestReportCard.tsx           # 回测报告卡片
├── BacktestMetricsChart.tsx         # 性能指标图表
├── EquityCurveChart.tsx             # 资金曲线图
├── TradeHistoryTable.tsx            # 交易历史表格
├── DrawdownChart.tsx                # 回撤图
└── OptimizationProgress.tsx         # 优化进度组件
```

### 5.2 前端功能点

#### 5.2.1 回测配置页面 (BacktestConfigPage)

| 功能点 | 描述 | 优先级 |
|-------|------|-------|
| UI1. 配置列表 | 展示所有回测配置 | P0 |
| UI2. 新建配置 | 创建新的回测配置 | P0 |
| UI3. 编辑配置 | 修改现有配置 | P0 |
| UI4. 删除配置 | 删除配置（含确认） | P0 |
| UI5. 复制配置 | 快速复制配置 | P1 |
| UI6. 配置置顶 | 置顶常用配置 | P2 |

#### 5.2.2 回测配置表单 (BacktestConfigModal)

| 功能点 | 描述 | 优先级 |
|-------|------|-------|
| UF1. 基本信息 | 名称、描述输入 | P0 |
| UF2. 策略选择 | 选择策略脚本/AI配置 | P0 |
| UF3. 股票池配置 | 添加/删除股票 | P0 |
| UF4. 日期范围 | 选择起止日期 | P0 |
| UF5. 资金参数 | 初始资金、仓位比例 | P0 |
| UF6. 成本参数 | 手续费、滑点设置 | P1 |
| UF7. 指标关联 | 选择技术指标 | P1 |
| UF8. 高级参数 | 自定义参数编辑 | P2 |
| UF9. 配置验证 | 提交前参数校验 | P0 |

#### 5.2.3 回测执行页面 (BacktestRunPage)

| 功能点 | 描述 | 优先级 |
|-------|------|-------|
| UR1. 启动回测 | 从配置启动回测 | P0 |
| UR2. 进度显示 | 实时显示回测进度 | P0 |
| UR3. 日志输出 | 显示回测执行日志 | P0 |
| UR4. 停止回测 | 中止运行中回测 | P1 |
| UR5. 批量回测 | 批量启动多个回测 | P2 |
| UR6. 快速回测 | 使用默认参数快速回测 | P1 |

#### 5.2.4 回测结果页面 (BacktestResultPage)

| 功能点 | 描述 | 优先级 |
|-------|------|-------|
| UU1. 报告概览 | 显示核心指标卡片 | P0 |
| UU2. 资金曲线 | 绘制净值曲线图 | P0 |
| UU3. 回撤分析 | 绘制回撤曲线 | P0 |
| UU4. 交易列表 | 显示所有交易记录 | P0 |
| UU5. 详细指标 | 显示所有性能指标 | P0 |
| UU6. 交易导出 | 导出交易记录CSV | P1 |
| UU7. 报告导出 | 导出HTML/PDF报告 | P1 |
| UU8. 图表交互 | 图表缩放、十字线 | P1 |
| UU9. 交易详情 | 点击查看交易详情 | P2 |

#### 5.2.5 回测对比页面 (BacktestComparePage)

| 功能点 | 描述 | 优先级 |
|-------|------|-------|
| UC1. 选择回测 | 选择要对比的回测结果 | P0 |
| UC2. 指标对比表 | 并排显示关键指标 | P0 |
| UC3. 曲线对比 | 多条资金曲线对比 | P0 |
| UC4. 对比导出 | 导出对比报告 | P1 |

#### 5.2.6 参数优化页面 (BacktestOptimizePage)

| 功能点 | 描述 | 优先级 |
|-------|------|-------|
| UO1. 参数空间定义 | 设置优化参数范围 | P0 |
| UO2. 优化方法选择 | 选择优化算法 | P1 |
| UO3. 目标指标选择 | 选择优化目标 | P0 |
| UO4. 启动优化 | 执行参数优化 | P0 |
| UO5. 进度显示 | 显示优化进度 | P0 |
| UO6. 结果热力图 | 参数-指标热力图 | P1 |
| UO7. 最优参数展示 | 显示最优参数组合 | P0 |
| UO8. 参数应用 | 一键应用最优参数 | P0 |

### 5.3 API客户端 (api/backtest.ts)

```typescript
// 回测配置API
export const getBacktestConfigs: () => Promise<BacktestConfig[]>
export const createBacktestConfig: (data: BacktestConfigCreate) => Promise<BacktestConfig>
export const updateBacktestConfig: (id: number, data: BacktestConfigUpdate) => Promise<BacktestConfig>
export const deleteBacktestConfig: (id: number) => Promise<void>

// 回测执行API
export const runBacktest: (configId: number, params?: BacktestParams) => Promise<BacktestRun>
export const getBacktestRun: (id: number) => Promise<BacktestRunDetail>
export const getBacktestRuns: () => Promise<BacktestRun[]>
export const stopBacktest: (id: number) => Promise<void>

// 回测结果API
export const getBacktestReport: (runId: number) => Promise<BacktestReport>
export const getBacktestTrades: (runId: number) => Promise<BacktestTrade[]>
export const exportBacktestTrades: (runId: number) => Promise<Blob>

// 回测对比API
export const compareBacktests: (runIds: number[]) => Promise<BacktestComparison>

// 参数优化API
export const runOptimization: (configId: number, params: OptimizationParams) => Promise<OptimizationJob>
export const getOptimizationProgress: (id: number) => Promise<OptimizationStatus>
```

---

## 六、开发计划 - 原子化任务分解

### 阶段一：基础框架搭建 (Week 1-2)

#### Phase 1.1: 数据层 (3天)
- [ ] **DB-01**: 创建 BacktestConfig 数据模型
- [ ] **DB-02**: 创建 BacktestRun 数据模型
- [ ] **DB-03**: 创建 BacktestTrade 数据模型
- [ ] **DB-04**: 创建 BacktestReport 数据模型
- [ ] **DB-05**: 创建 BacktestOptimization 数据模型
- [ ] **DB-06**: 添加数据库迁移逻辑到 main.py
- [ ] **DB-07**: 创建数据模型CRUD测试

#### Phase 1.2: 数据管理器 (2天)
- [ ] **DATA-01**: 实现获取股票历史日线数据
- [ ] **DATA-02**: 实现数据缓存机制
- [ ] **DATA-03**: 实现交易日历获取
- [ ] **DATA-04**: 实现数据清洗和复权处理
- [ ] **DATA-05**: 编写数据管理器单元测试

#### Phase 1.3: 回测执行引擎基础 (5天)
- [ ] **EXEC-01**: 实现回测环境初始化
- [ ] **EXEC-02**: 实现逐日循环引擎框架
- [ ] **EXEC-03**: 实现持仓管理
- [ ] **EXEC-04**: 实现订单执行和成本计算
- [ ] **EXEC-05**: 实现交易记录存储
- [ ] **EXEC-06**: 实现资金曲线更新
- [ ] **EXEC-07**: 实现脚本策略信号生成
- [ ] **EXEC-08**: 编写回测引擎单元测试

### 阶段二：核心回测功能 (Week 3-4)

#### Phase 2.1: 性能分析器 (3天)
- [ ] **ANALYZE-01**: 实现基础收益指标计算
- [ ] **ANALYZE-02**: 实现交易统计指标计算
- [ ] **ANALYZE-03**: 实现最大回撤计算
- [ ] **ANALYZE-04**: 实现资金曲线生成
- [ ] **ANALYZE-05**: 实现夏普比率计算
- [ ] **ANALYZE-06**: 实现其他高级指标
- [ ] **ANALYZE-07**: 编写分析器测试

#### Phase 2.2: 报告生成器 (2天)
- [ ] **REPORT-01**: 实现JSON格式报告生成
- [ ] **REPORT-02**: 实现交易明细导出
- [ ] **REPORT-03**: 实现图表数据格式生成
- [ ] **REPORT-04**: 编写报告生成器测试

#### Phase 2.3: API路由 (3天)
- [ ] **API-01**: 实现回测配置CRUD接口
- [ ] **API-02**: 实现回测启动接口
- [ ] **API-03**: 实现回测查询接口
- [ ] **API-04**: 实现交易记录查询接口
- [ ] **API-05**: 实现报告查询接口
- [ ] **API-06**: 编写API集成测试

### 阶段三：前端开发 (Week 5-6)

#### Phase 3.1: 回测配置页面 (3天)
- [ ] **FE-01**: 创建回测配置列表页面
- [ ] **FE-02**: 创建配置编辑弹窗组件
- [ ] **FE-03**: 实现股票池选择器
- [ ] **FE-04**: 实现日期范围选择器
- [ ] **FE-05**: 实现策略选择器
- [ ] **FE-06**: 实现配置CRUD功能

#### Phase 3.2: 回测执行页面 (2天)
- [ ] **FE-07**: 创建回测执行页面
- [ ] **FE-08**: 实现回测进度组件
- [ ] **FE-09**: 实现实时日志显示
- [ ] **FE-10**: 实现停止回测功能

#### Phase 3.3: 回测结果页面 (4天)
- [ ] **FE-11**: 创建回测结果页面
- [ ] **FE-12**: 实现报告指标卡片
- [ ] **FE-13**: 实现资金曲线图表
- [ ] **FE-14**: 实现回撤图表
- [ ] **FE-15**: 实现交易历史表格
- [ ] **FE-16**: 实现数据导出功能

#### Phase 3.4: TypeScript类型定义 (1天)
- [ ] **FE-17**: 定义所有回测相关TypeScript类型
- [ ] **FE-18**: 创建API客户端函数
- [ ] **FE-19**: 创建React Hooks封装

### 阶段四：高级功能 (Week 7-8)

#### Phase 4.1: 参数优化 (5天)
- [ ] **OPT-01**: 实现网格搜索算法
- [ ] **OPT-02**: 实现并行执行机制
- [ ] **OPT-03**: 实现优化进度跟踪
- [ ] **OPT-04**: 创建参数优化前端页面
- [ ] **OPT-05**: 实现结果热力图可视化
- [ ] **OPT-06**: 实现参数应用功能

#### Phase 4.2: 回测对比 (3天)
- [ ] **COMP-01**: 实现回测对比API
- [ ] **COMP-02**: 创建对比前端页面
- [ ] **COMP-03**: 实现多条曲线对比图表
- [ ] **COMP-04**: 实现对比指标表格

#### Phase 4.3: AI策略回测 (2天)
- [ ] **AI-01**: 实现AI信号生成集成
- [ ] **AI-02**: 实现AI回测成本控制（调用次数限制）
- [ ] **AI-03**: 实现AI回测结果缓存

### 阶段五：完善与测试 (Week 9)

#### Phase 5.1: 测试与优化 (5天)
- [ ] **TEST-01**: 编写端到端测试
- [ ] **TEST-02**: 性能压力测试
- [ ] **TEST-03**: 边界条件测试
- [ ] **TEST-04**: Bug修复和优化
- [ ] **TEST-05**: 代码重构和文档完善

---

## 七、技术实现要点

### 7.1 回测执行关键逻辑

#### 7.1.1 核心循环结构
```python
async def run_backtest(config: BacktestConfig, run: BacktestRun):
    # 1. 初始化
    context = BacktestContext(
        initial_capital=config.initial_capital,
        commission_rate=config.commission_rate,
        slippage_rate=config.slippage_rate,
        position_size=config.position_size
    )

    # 2. 获取数据
    data = await load_historical_data(config.stock_symbols, config.start_date, config.end_date)
    trade_dates = get_trade_dates(config.start_date, config.end_date)

    # 3. 逐日循环
    for date in trade_dates:
        # 获取当日数据
        daily_data = get_data_for_date(data, date)

        # 更新持仓价值
        context.update_positions_value(daily_data)

        # 生成信号
        signal = await generate_signal(config, context, daily_data)

        # 执行交易
        if signal:
            await execute_trade(context, signal, date)

        # 记录当日净值
        context.record_equity(date)

    # 4. 生成报告
    report = analyze_performance(context)
    save_report(run.id, report)
```

#### 7.1.2 信号生成集成
```python
async def generate_signal(config: BacktestConfig, context: BacktestContext, data: dict):
    if config.strategy_type == "script":
        # 复用现有脚本执行能力
        return await execute_script_strategy(config, data)

    elif config.strategy_type == "ai":
        # 复用现有AI分析能力
        return await execute_ai_strategy(config, data)

    elif config.strategy_type == "hybrid":
        # 先脚本过滤，再AI分析
        script_signal = await execute_script_strategy(config, data)
        if script_signal.passed:
            return await execute_ai_strategy(config, data)
```

### 7.2 性能考虑

| 问题 | 解决方案 |
|-----|---------|
| 大量历史数据加载 | 分批加载、懒加载 |
| AI调用耗时 | 结果缓存、异步并发 |
| 参数优化耗时 | 多进程并行、进度反馈 |
| 数据库写入频繁 | 批量写入、延迟写入 |

### 7.3 复用现有能力

| 现有模块 | 复用方式 |
|---------|---------|
| `data_fetcher.py` | 直接调用获取历史数据 |
| `ai_service.py` | 复用AI信号生成逻辑 |
| `monitor_service.py` | 参考脚本执行框架 |
| `RuleScript` 模型 | 直接作为回测策略源 |

---

## 八、风险与挑战

### 8.1 技术风险

| 风险 | 影响 | 缓解措施 |
|-----|------|---------|
| 历史数据不完整 | 回测结果失真 | 数据验证、填充缺失值 |
| AI成本过高 | 回测不经济 | 结果缓存、限制调用次数 |
| 回测速度慢 | 用户体验差 | 多进程优化、增量回测 |
| 现实偏差 | 实盘效果差 | 加入滑点、容量限制 |

### 8.2 业务风险

| 风险 | 影响 | 缓解措施 |
|-----|------|---------|
| 过度拟合 | 策略失效 | 样本外验证、参数惩罚 |
| 数据窥探 | 虚假高收益 | 严格的时间切分 |
| 交易成本低估 | 收益虚高 | 真实成本参数 |

---

## 九、验收标准

### 9.1 功能验收

- [ ] 能够创建并保存回测配置
- [ ] 能够执行完整的策略回测
- [ ] 能够生成完整的性能报告
- [ ] 能够可视化展示回测结果
- [ ] 能够进行参数优化

### 9.2 性能验收

- [ ] 单次回测（1年数据，1只股票） < 30秒
- [ ] 参数优化（100组参数） < 10分钟
- [ ] 前端页面响应 < 500ms

### 9.3 质量验收

- [ ] 代码覆盖率 > 70%
- [ ] 无严重Bug
- [ ] 通过安全扫描

---

## 十、后续扩展方向

1. **实盘对接**: 将验证好的策略接入实盘交易
2. **多策略组合**: 多个策略的组合回测
3. **因子研究**: 因子有效性检验工具
4. **机器学习**: 集成ML模型的回测框架
5. **高频回测**: 支持分钟级、tick级数据回测

---

## 附录

### A. 术语表

| 术语 | 说明 |
|-----|------|
| 回测 | 基于历史数据验证交易策略 |
| 夏普比率 | 风险调整后收益指标 |
| 最大回撤 | 从峰值到谷底的最大跌幅 |
| 资金曲线 | 每日净值变化曲线 |
| 滑点 | 实际成交价与预期价的差异 |
| 夏普比率 | (收益率 - 无风险利率) / 波动率 |

### B. 参考资料

- [Backtrader文档](https://www.backtrader.com/docu/)
- [Zipline文档](https://zipline.io/)
- [AkShare文档](https://akshare.akfamily.xyz/)

---

**文档变更记录**

| 版本 | 日期 | 作者 | 变更说明 |
|-----|------|------|---------|
| v1.0 | 2026-01-18 | Claude | 初始版本 |
