# 实施计划：突破后站稳3日选股脚本（模块化+测试验证版）

## 目标
创建 `backend/scripts/breakout_hold_3days.py` 选股脚本，实现"A股突破后站稳3日"策略。

---

## 第一阶段：模块拆分与设计

### 模块1：数据获取模块
**功能**：获取股票列表和K线数据
- `get_active_stocks()`: 从tushare获取活跃股票列表
- `get_daily_bars()`: 从pytdx获取日K线数据
- `filter_stocks()`: 过滤ST、退市、新股

### 模块2：技术指标计算模块
**功能**：计算均线和关键价位
- `calc_ma()`: 计算移动平均线
- `calc_rolling_high()`: 计算滚动最高价
- `calc_volume_ma()`: 计算成交量均线

### 模块3：突破检测模块
**功能**：检测突破信号
- `detect_breakout()`: 检测单日是否突破关键位
- `get_key_levels()`: 获取当日关键价位(High60, MA60, MA120)

### 模块4：站稳验证模块
**功能**：验证突破后是否站稳
- `check_stand_firm()`: 检查三日站稳条件

### 模块5：主流程模块
**功能**：整合各模块，执行选股
- `screen_one()`: 单只股票筛选
- `main()`: 主流程入口

---

## 第二阶段：数据接口检查

### 2.1 pytdx接口检查
**检查项**：
- [ ] K线数据日期是否为最新交易日
- [ ] 数据格式：datetime, open, close, high, low, vol, amount
- [ ] 成交量单位：pytdx返回的是"手"还是"股"
- [ ] 数据完整性：是否有缺失值

**测试方法**：
```python
# 测试代码：检查pytdx数据
from backend.utils.pytdx_client import tdx
data = tdx.get_security_bars(9, 0, "000001", 0, 10)
# 检查返回格式、日期、单位
```

### 2.2 tushare接口检查
**检查项**：
- [ ] stock_basic 返回字段：ts_code, name, list_status, list_date
- [ ] 是否能正确过滤ST股票
- [ ] 上市日期字段格式

**测试方法**：
```python
# 测试代码：检查tushare数据
from backend.utils.tushare_client import pro
df = pro.stock_basic(exchange="", list_status="L", fields="ts_code,name,list_status,list_date")
# 检查返回格式
```

---

## 第三阶段：测试用例设计

### 3.1 寻找测试标的
**方法**：
1. 使用同花顺/东方财富等工具手动寻找满足条件的股票
2. 或使用现有选股工具筛选候选股
3. 确认该股票满足：
   - T-3日突破High60/MA60/MA120
   - T-2、T-1、T三日站稳

**测试标的候选**：
- 待手动查找后填入

### 3.2 单元测试设计
```python
# 测试用例结构
def test_data_fetch():
    """测试数据获取"""
    
def test_indicator_calc():
    """测试指标计算"""
    
def test_breakout_detect():
    """测试突破检测"""
    
def test_stand_firm():
    """测试站稳验证"""
    
def test_full_pipeline():
    """测试完整流程"""
```

---

## 第四阶段：实施步骤

### 步骤1：创建测试脚本
创建 `backend/scripts/test_breakout_modules.py`，逐模块测试

### 步骤2：数据接口验证
- 运行pytdx数据检查
- 运行tushare数据检查
- 记录数据格式、单位等信息

### 步骤3：实现模块1-数据获取
- 实现函数
- 单元测试
- 验证数据正确性

### 步骤4：实现模块2-技术指标
- 实现函数
- 单元测试
- 与手动计算结果对比

### 步骤5：实现模块3-突破检测
- 实现函数
- 单元测试
- 用测试标的验证

### 步骤6：实现模块4-站稳验证
- 实现函数
- 单元测试
- 用测试标的验证

### 步骤7：实现模块5-主流程
- 整合各模块
- 全量测试

### 步骤8：生成测试报告
- 记录各模块测试结果
- 记录测试标的筛选结果
- 记录发现的问题和解决方案

---

## 第五阶段：输出规范

### 输出文件
1. `backend/scripts/breakout_hold_3days.py` - 主脚本
2. `backend/scripts/test_breakout_modules.py` - 测试脚本（可选，可删除）

### CSV输出列
| 列名 | 类型 | 说明 |
|-----|------|------|
| symbol | str | 股票代码 |
| name | str | 股票名称 |
| breakout_date | str | 突破日期 YYYY-MM-DD |
| key_level_type | str | 关键位类型: High60/MA60/MA120 |
| key_level | float | 关键位价格 |
| breakout_price | float | 突破日收盘价 |
| current_price | float | 当前价格 |
| breakout_vol_ratio | float | 突破日量比 |
| stand_days_above | int | 站上关键位天数(0-3) |
| min_close_ratio | float | 三日最低收盘比例 |
| trade_date | str | 交易日期 |

### 测试报告内容
1. 数据接口检查结果
2. 各模块测试结果
3. 测试标的筛选结果
4. 问题记录与解决方案

---

## 策略参数配置

| 参数名 | 默认值 | 说明 |
|-------|-------|------|
| breakout_buffer_high60 | 1.005 | High60突破缓冲 |
| breakout_buffer_ma | 1.015 | MA突破缓冲 |
| vol_ratio_min | 1.5 | 最小量比 |
| vol_ratio_max | 4.0 | 最大量比(防脉冲) |
| stand_buffer | 0.99 | 站稳缓冲(-1%) |
| stand_days_min | 2 | 最少站上天数 |
| min_listing_days | 140 | 最少上市天数 |

---

## 预计工作量
- 数据接口检查：30分钟
- 模块实现：2小时
- 测试验证：1小时
- 文档报告：30分钟
