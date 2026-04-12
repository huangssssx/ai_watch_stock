# Tushare A股接口说明文档

> 本文档基于实际代码验证生成，涵盖A股相关的核心接口
> 验证时间：2026-04-12

## 目录

1. [基础数据接口](#1-基础数据接口)
2. [行情数据接口](#2-行情数据接口)
3. [财务数据接口](#3-财务数据接口)
4. [参考数据接口](#4-参考数据接口)
5. [两融数据接口](#5-两融数据接口)
6. [资金流向接口](#6-资金流向接口)
7. [打板专题接口](#7-打板专题接口)
8. [特色数据接口](#8-特色数据接口)

---

## 1. 基础数据接口

### 1.1 股票列表 `stock_basic`

**接口**：`pro.stock_basic()`

**说明**：获取基础信息数据，包括股票代码、名称、上市日期、退市日期等。数据是一次性的，建议保存到本地。

**积分要求**：2000积分

**输入参数**：

| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| ts_code | str | N | TS股票代码，如 `000001.SZ` |
| name | str | N | 股票名称 |
| market | str | N | 市场类别：主板/创业板/科创板/CDR/北交所 |
| list_status | str | N | 上市状态：L上市 D退市 P暂停上市 G过会未交易，默认L |
| exchange | str | N | 交易所：SSE上交所 SZSE深交所 BSE北交所 |
| is_hs | str | N | 是否沪深港通标的：N否 H沪股通 S深股通 |

**输出参数**：

| 字段 | 类型 | 说明 |
|------|------|------|
| ts_code | str | TS代码 |
| symbol | str | 股票代码 |
| name | str | 股票名称 |
| area | str | 地域 |
| industry | str | 所属行业 |
| market | str | 市场类型 |
| list_date | str | 上市日期 |
| delist_date | str | 退市日期 |
| is_hs | str | 是否沪深港通标的 |
| act_name | str | 实控人名称 |

**验证结果**：✅ 通过

```python
df = pro.stock_basic(list_status='L', fields='ts_code,name,industry,market,list_date')
```

---

### 1.2 交易日历 `trade_cal`

**接口**：`pro.trade_cal()`

**说明**：获取各大交易所交易日历数据

**积分要求**：2000积分

**输入参数**：

| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| exchange | str | N | 交易所代码 |
| start_date | str | N | 开始日期(YYYYMMDD) |
| end_date | str | N | 结束日期(YYYYMMDD) |
| is_open | str | N | 是否交易：0休市 1交易 |

**输出参数**：

| 字段 | 类型 | 说明 |
|------|------|------|
| exchange | str | 交易所 |
| cal_date | str | 日期 |
| is_open | str | 是否交易 |
| pretrade_date | str | 上一个交易日 |

**验证结果**：✅ 通过

```python
df = pro.trade_cal(start_date='20240101', end_date='20240110', exchange='SSE')
```

---

### 1.3 股票曾用名 `namechange`

**接口**：`pro.namechange()`

**说明**：获取股票曾用名

**输入参数**：

| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| ts_code | str | N | 股票代码 |
| name | str | N | 股票名称 |
| start_date | str | N | 开始日期 |
| end_date | str | N | 结束日期 |

**输出参数**：

| 字段 | 类型 | 说明 |
|------|------|------|
| ts_code | str | TS代码 |
| name | str | 名称 |
| change_reason | str | 变更原因 |
| ann_date | str | 公告日期 |

**验证结果**：✅ 通过

```python
df = pro.namechange(ts_code='000001.SZ', fields='ts_code,name,change_reason,ann_date')
```

---

### 1.4 上市公司基本信息 `stock_company`

**接口**：`pro.stock_company()`

**说明**：获取上市公司基本信息

**输入参数**：

| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| ts_code | str | Y | 股票代码 |
| exchange | str | N | 交易所 |

**输出参数**：

| 字段 | 类型 | 说明 |
|------|------|------|
| ts_code | str | TS代码 |
| province | str | 省份 |
| employees | int | 员工数量 |
| main_business | str | 主营业务 |

**验证结果**：✅ 通过

```python
df = pro.stock_company(ts_code='000001.SZ', fields='ts_code,province,employees,main_business')
```

---

### 1.5 高管薪酬和持股 `stk_rewards`

**接口**：`pro.stk_rewards()`

**说明**：获取管理层薪酬和持股数据

**积分要求**：2000积分

**输入参数**：

| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| ts_code | str | Y | 股票代码 |
| start_date | str | N | 开始日期 |
| end_date | str | N | 结束日期 |

**输出参数**：

| 字段 | 类型 | 说明 |
|------|------|------|
| ts_code | str | TS代码 |
| ann_date | str | 公告日期 |
| name | str | 姓名 |
| title | str | 职务 |
| reward | float | 薪酬(万元) |
| hold_vol | float | 持股数量 |

**验证结果**：⚠️ 返回空数据（可能需要特定条件）

```python
df = pro.stk_rewards(ts_code='000001.SZ', start_date='20230101', end_date='20240308')
```

---

### 1.6 IPO新股上市 `new_share`

**接口**：`pro.new_share()`

**说明**：获取IPO新股上市数据

**输入参数**：

| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| start_date | str | N | 开始日期 |
| end_date | str | N | 结束日期 |

**输出参数**：

| 字段 | 类型 | 说明 |
|------|------|------|
| ts_code | str | 股票代码 |
| name | str | 名称 |
| ipo_date | str | 上市日期 |
| issue_date | str | 申购日期 |
| limit_price | float | 发行价格 |
| pe_ratio | float | 市盈率 |
| share_amount | float | 发行数量 |
| float_share | float | 流通股本 |

**验证结果**：✅ 通过

```python
df = pro.new_share(start_date='20240101', end_date='20240308')
```

---

## 2. 行情数据接口

### 2.1 历史日线行情 `daily`

**接口**：`pro.daily()`

**说明**：获取股票行情数据。交易日每天15点～16点之间入库。未复权行情，停牌期间不提供数据。

**积分要求**：基础积分每分钟500次，每次6000条

**输入参数**：

| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| ts_code | str | N | 股票代码（支持多股票逗号分隔） |
| trade_date | str | N | 交易日期(YYYYMMDD) |
| start_date | str | N | 开始日期 |
| end_date | str | N | 结束日期 |

**输出参数**：

| 字段 | 类型 | 说明 |
|------|------|------|
| ts_code | str | 股票代码 |
| trade_date | str | 交易日期 |
| open | float | 开盘价 |
| high | float | 最高价 |
| low | float | 最低价 |
| close | float | 收盘价 |
| pre_close | float | 昨收价【除权价】 |
| change | float | 涨跌额 |
| pct_chg | float | 涨跌幅(%) |
| vol | float | 成交量（手） |
| amount | float | 成交额（千元） |

**验证结果**：✅ 通过

```python
df = pro.daily(ts_code='000001.SZ', start_date='20240301', end_date='20240310')
```

---

### 2.2 每日指标 `daily_basic`

**接口**：`pro.daily_basic()`

**说明**：获取全部股票每日重要的基本面指标。交易日每日15点～17点之间更新。

**积分要求**：2000积分起，5000积分无总量限制

**输入参数**：

| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| ts_code | str | Y | 股票代码 |
| trade_date | str | N | 交易日期 |
| start_date | str | N | 开始日期 |
| end_date | str | N | 结束日期 |

**输出参数**：

| 字段 | 类型 | 说明 |
|------|------|------|
| ts_code | str | TS股票代码 |
| trade_date | str | 交易日期 |
| close | float | 当日收盘价 |
| turnover_rate | float | 换手率(%) |
| turnover_rate_f | float | 换手率(自由流通股) |
| volume_ratio | float | 量比 |
| pe | float | 市盈率(总市值/净利润) |
| pe_ttm | float | 市盈率(TTM) |
| pb | float | 市净率 |
| ps | float | 市销率 |
| ps_ttm | float | 市销率(TTM) |
| dv_ratio | float | 股息率(%) |
| dv_ttm | float | 股息率(TTM)(%) |
| total_share | float | 总股本(万股) |
| float_share | float | 流通股本(万股) |
| free_share | float | 自由流通股本(万) |
| total_mv | float | 总市值(万元) |
| circ_mv | float | 流通市值(万元) |

**验证结果**：✅ 通过

```python
df = pro.daily_basic(ts_code='000001.SZ', trade_date='20240308')
```

---

### 2.3 复权因子 `adj_factor`

**接口**：`pro.adj_factor()`

**说明**：获取股票复权因子。盘前9点15~20分完成当日复权因子入库。

**积分要求**：2000积分起，5000以上可高频调取

**输入参数**：

| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| ts_code | str | N | 股票代码 |
| trade_date | str | N | 交易日期 |
| start_date | str | N | 开始日期 |
| end_date | str | N | 结束日期 |

**输出参数**：

| 字段 | 类型 | 说明 |
|------|------|------|
| ts_code | str | 股票代码 |
| trade_date | str | 交易日期 |
| adj_factor | float | 复权因子 |

**验证结果**：✅ 通过

```python
df = pro.adj_factor(ts_code='000001.SZ', start_date='20240101', end_date='20240308')
```

---

### 2.4 通用行情接口 `pro_bar`

**接口**：`ts.pro_bar()` 或 `pro.pro_bar()`

**说明**：整合了股票（未复权、前复权、后复权）、指数、基金、期货、期权的行情数据。

**重要**：此接口是集成开发接口，目前暂时没法用http的方式调取，只能用Python SDK调用。

**输入参数**：

| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| ts_code | str | Y | 证券代码（不支持多值） |
| start_date | str | N | 开始日期(YYYYMMDD) |
| end_date | str | N | 结束日期(YYYYMMDD) |
| asset | str | Y | 资产类别：E股票 I指数 C数字货币 FT期货 FD基金 O期权 CB可转债 |
| adj | str | N | 复权类型：None未复权 qfq前复权 hfq后复权 |
| freq | str | Y | 数据频度：1min/5min/15min/30min/60min/D/W/M |
| ma | list | N | 均线，如[5,20,50] |
| factors | list | N | 因子：tor换手率 vr量比 |

**输出参数**：同股票日线行情指标

**验证结果**：✅ 通过

```python
df = pro.pro_bar(ts_code='000001.SZ', start_date='20240301', end_date='20240308', adj='qfq')
```

---

### 2.5 每日停复牌信息 `suspend_d`

**接口**：`pro.suspend_d()`

**说明**：获取每日停复牌信息

**输入参数**：

| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| ts_code | str | N | 股票代码 |
| suspend_timing | str | N | 停复牌时间：S上市 S暂停 D退市 R恢复 |
| trade_date | str | N | 交易日期 |
| start_date | str | N | 开始日期 |
| end_date | str | N | 结束日期 |

**输出参数**：

| 字段 | 类型 | 说明 |
|------|------|------|
| ts_code | str | 股票代码 |
| suspend_timing | str | 停复牌时间 |
| resume_date | str | 复牌日期 |

**验证结果**：✅ 通过（返回数据较少为正常现象）

```python
df = pro.suspend_d(trade_date='20240308', fields='ts_code,suspend_timing,resume_date')
```

---

## 3. 财务数据接口

### 3.1 利润表 `income`

**接口**：`pro.income()`

**说明**：获取上市公司财务利润表数据

**积分要求**：2000积分（单股票），5000积分可用income_vip获取全量

**输入参数**：

| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| ts_code | str | Y | 股票代码 |
| ann_date | str | N | 公告日期 |
| start_date | str | N | 公告日开始日期 |
| end_date | str | N | 公告日结束日期 |
| period | str | N | 报告期(如20171231表示年报) |
| report_type | str | N | 报告类型 |

**输出参数**：

| 字段 | 类型 | 说明 |
|------|------|------|
| ts_code | str | TS代码 |
| ann_date | str | 公告日期 |
| end_date | str | 报告期 |
| basic_eps | float | 基本每股收益 |
| diluted_eps | float | 稀释每股收益 |
| total_revenue | float | 营业总收入 |
| revenue | float | 营业收入 |
| operate_profit | float | 营业利润 |
| total_profit | float | 利润总额 |
| n_income | float | 净利润(含少数股东损益) |
| n_income_attr_p | float | 净利润(不含少数股东损益) |

**验证结果**：✅ 通过

```python
df = pro.income(ts_code='000001.SZ', start_date='20230101', end_date='20240101',
                fields='ts_code,ann_date,end_date,revenue,total_profit,n_income')
```

---

### 3.2 资产负债表 `balancesheet`

**接口**：`pro.balancesheet()`

**说明**：获取上市公司资产负债表数据

**积分要求**：2000积分

**输入参数**：同income接口

**输出参数**：

| 字段 | 类型 | 说明 |
|------|------|------|
| ts_code | str | TS代码 |
| ann_date | str | 公告日期 |
| end_date | str | 报告期 |
| total_assets | float | 总资产 |
| total_liab | float | 总负债 |
| total_equity | float | 股东权益 |
| current_assets | float | 流动资产 |
| non_current_assets | float | 非流动资产 |

**验证结果**：✅ 通过

```python
df = pro.balancesheet(ts_code='000001.SZ', start_date='20230101', end_date='20240101',
                      fields='ts_code,ann_date,end_date,total_assets,total_liab')
```

---

### 3.3 现金流量表 `cashflow`

**接口**：`pro.cashflow()`

**说明**：获取上市公司现金流量表数据

**积分要求**：2000积分

**输入参数**：同income接口

**输出参数**：

| 字段 | 类型 | 说明 |
|------|------|------|
| ts_code | str | TS代码 |
| ann_date | str | 公告日期 |
| end_date | str | 报告期 |
| net_operate_cashflow | float | 经营活动现金流量净额 |
| net_invest_cashflow | float | 投资活动现金流量净额 |
| net_finance_cashflow | float | 筹资活动现金流量净额 |

**验证结果**：✅ 通过

```python
df = pro.cashflow(ts_code='000001.SZ', start_date='20230101', end_date='20240101',
                  fields='ts_code,ann_date,end_date,net_operate_cashflow,net_invest_cashflow')
```

---

### 3.4 财务指标 `fina_indicator`

**接口**：`pro.fina_indicator()`

**说明**：获取上市公司财务指标数据。每次最多返回100条记录。

**积分要求**：2000积分（单股票），5000积分可用fina_indicator_vip获取全量

**输入参数**：

| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| ts_code | str | Y | 股票代码 |
| ann_date | str | N | 公告日期 |
| start_date | str | N | 报告期开始日期 |
| end_date | str | N | 报告期结束日期 |
| period | str | N | 报告期 |

**输出参数**（部分常用字段）：

| 字段 | 类型 | 说明 |
|------|------|------|
| ts_code | str | TS代码 |
| ann_date | str | 公告日期 |
| end_date | str | 报告期 |
| roe | float | 净资产收益率 |
| roe_waa | float | 加权平均净资产收益率 |
| roa | float | 总资产报酬率 |
| grossprofit_margin | float | 销售毛利率 |
| netprofit_margin | float | 销售净利率 |
| ebit | float | 息税前利润 |
| debt_to_assets | float | 资产负债率 |
| eps | float | 基本每股收益 |
| bps | float | 每股净资产 |

**验证结果**：✅ 通过

```python
df = pro.fina_indicator(ts_code='000001.SZ', start_date='20230101', end_date='20240101',
                        fields='ts_code,ann_date,end_date,roe,net_profit_ratio,gross_profit_rate')
```

---

### 3.5 业绩预告 `forecast`

**接口**：`pro.forecast()`

**说明**：获取上市公司业绩预告数据

**输入参数**：

| 参数 | Type | Required | Description |
|------|------|----------|-------------|
| ts_code | str | Y | 股票代码 |
| ann_date | str | N | 公告日期 |
| start_date | str | N | 开始日期 |
| end_date | str | N | 结束日期 |
| period | str | N | 报告期 |

**输出参数**：

| 字段 | 类型 | 说明 |
|------|------|------|
| ts_code | str | TS代码 |
| ann_date | str | 公告日期 |
| end_date | str | 报告期 |
| type | str | 业绩预告类型 |
| p_change | float | 业绩变动幅度 |
| profit_min | float | 预计最小净利润 |
| profit_max | float | 预计最大净利润 |

**验证结果**：✅ 通过

```python
df = pro.forecast(ts_code='000001.SZ', start_date='20230101', end_date='20240101')
```

---

### 3.6 业绩快报 `express`

**接口**：`pro.express()`

**说明**：获取上市公司业绩快报数据

**输入参数**：同forecast

**输出参数**：

| 字段 | 类型 | 说明 |
|------|------|------|
| ts_code | str | TS代码 |
| ann_date | str | 公告日期 |
| end_date | str | 报告期 |
| revenue | float | 营业收入 |
| profit | float | 净利润 |
| total_assets | float | 总资产 |
| total_equity | float | 股东权益 |
| eps | float | 每股收益 |

**验证结果**：✅ 通过

```python
df = pro.express(ts_code='000001.SZ', start_date='20230101', end_date='20240101')
```

---

### 3.7 分红送股 `dividend`

**接口**：`pro.dividend()`

**说明**：获取上市公司分红送股数据

**输入参数**：

| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| ts_code | str | N | 股票代码 |
| ann_date | str | N | 公告日期 |
| record_date | str | N | 股权登记日期 |
| ex_date | str | N | 除权除息日期 |

**输出参数**：

| 字段 | 类型 | 说明 |
|------|------|------|
| ts_code | str | TS代码 |
| ann_date | str | 公告日期 |
| div_proc | str | 分红方案 |
| stk_div | float | 送股 |
| cash_div | float | 现金分红 |
| record_date | str | 登记日期 |
| ex_date | str | 除权除息日期 |

**验证结果**：✅ 通过

```python
df = pro.dividend(ts_code='000001.SZ', start_date='20200101')
```

---

## 4. 参考数据接口

### 4.1 前十大股东 `top10_holders`

**接口**：`pro.top10_holders()`

**说明**：获取上市公司前十大股东数据

**积分要求**：2000积分，5000积分以上频次更高

**输入参数**：

| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| ts_code | str | Y | 股票代码 |
| period | str | N | 报告期 |
| ann_date | str | N | 公告日期 |
| start_date | str | N | 开始日期 |
| end_date | str | N | 结束日期 |

**输出参数**：

| 字段 | 类型 | 说明 |
|------|------|------|
| ts_code | str | TS股票代码 |
| ann_date | str | 公告日期 |
| end_date | str | 报告期 |
| holder_name | str | 股东名称 |
| hold_amount | float | 持有数量（股） |
| hold_ratio | float | 占总股本比例(%) |
| hold_float_ratio | float | 占流通股本比例(%) |
| holder_type | str | 股东类型 |

**验证结果**：✅ 通过

```python
df = pro.top10_holders(ts_code='000001.SZ', start_date='20230101', end_date='20240308')
```

---

### 4.2 股东人数 `stk_holdernumber`

**接口**：`pro.stk_holdernumber()`

**说明**：获取上市公司股东户数数据，数据不定期公布

**积分要求**：600积分

**输入参数**：

| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| ts_code | str | N | 股票代码 |
| ann_date | str | N | 公告日期 |
| enddate | str | N | 截止日期 |
| start_date | str | N | 公告开始日期 |
| end_date | str | N | 公告结束日期 |

**输出参数**：

| 字段 | 类型 | 说明 |
|------|------|------|
| ts_code | str | TS股票代码 |
| ann_date | str | 公告日期 |
| end_date | str | 截止日期 |
| holder_num | int | 股东户数 |

**验证结果**：✅ 通过

```python
df = pro.stk_holdernumber(ts_code='000001.SZ', start_date='20230101', end_date='20240308')
```

---

### 4.3 大宗交易 `block_trade`

**接口**：`pro.block_trade()`

**说明**：获取大宗交易数据

**输入参数**：

| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| ts_code | str | N | 股票代码 |
| start_date | str | N | 开始日期 |
| end_date | str | N | 结束日期 |

**输出参数**：

| 字段 | 类型 | 说明 |
|------|------|------|
| ts_code | str | 股票代码 |
| trade_date | str | 交易日期 |
| price | float | 成交价格 |
| vol | float | 成交量(万手) |
| amount | float | 成交金额(万元) |
| buyer | str | 买方营业部 |
| seller | str | 卖方营业部 |

**验证结果**：✅ 通过

```python
df = pro.block_trade(ts_code='000001.SZ', start_date='20230101', end_date='20240308')
```

---

### 4.4 股权质押统计 `pledge_stat`

**接口**：`pro.pledge_stat()`

**说明**：获取股权质押统计数据

**输入参数**：

| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| ts_code | str | N | 股票代码 |
| start_date | str | N | 开始日期 |
| end_date | str | N | 结束日期 |

**输出参数**：

| 字段 | 类型 | 说明 |
|------|------|------|
| ts_code | str | 股票代码 |
| end_date | str | 报告期 |
| pledge_count | int | 质押次数 |
| unrest_pledge | float | 未解押质押(万股) |
| rest_pledge | float | 已解押质押(万股) |
| total_share | float | 总股本(万股) |
| pledge_ratio | float | 质押比例(%) |

**验证结果**：✅ 通过

```python
df = pro.pledge_stat(ts_code='000001.SZ', start_date='20230101', end_date='20240308')
```

---

### 4.5 股东增减持 `stk_holdertrade`

**接口**：`pro.stk_holdertrade()`

**说明**：获取股东增减持数据

**积分要求**：1000积分

**输入参数**：

| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| ts_code | str | N | 股票代码 |
| start_date | str | N | 开始日期 |
| end_date | str | N | 结束日期 |
| ann_date | str | N | 公告日期 |

**输出参数**：

| 字段 | 类型 | 说明 |
|------|------|------|
| ts_code | str | 股票代码 |
| ann_date | str | 公告日期 |
| holder_name | str | 股东名称 |
| holder_type | str | 股东类型 |
| change_type | str | 变动类型 |
| change_vol | float | 变动数量 |
| change_ratio | float | 变动比例 |

**验证结果**：⚠️ 返回空数据（可能需要特定条件或日期范围）

```python
df = pro.stk_holdertrade(ts_code='000001.SZ', start_date='20230101', end_date='20240308')
```

---

## 5. 两融数据接口

### 5.1 融资融券交易汇总 `margin`

**接口**：`pro.margin()`

**说明**：获取融资融券每日交易汇总数据

**积分要求**：2000积分

**输入参数**：

| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| trade_date | str | N | 交易日期 |
| start_date | str | N | 开始日期 |
| end_date | str | N | 结束日期 |
| exchange_id | str | N | 交易所：SSE上交所 SZSE深交所 BSE北交所 |

**输出参数**：

| 字段 | 类型 | 说明 |
|------|------|------|
| trade_date | str | 交易日期 |
| exchange_id | str | 交易所代码 |
| rzye | float | 融资余额(元) |
| rzmre | float | 融资买入额(元) |
| rzche | float | 融资偿还额(元) |
| rqye | float | 融券余额(元) |
| rqmcl | float | 融券卖出量 |
| rzrqye | float | 融资融券余额(元) |
| rqyl | float | 融券余量 |

**计算公式**：
- 本日融资余额 = 前日融资余额 + 本日融资买入 - 本日融资偿还额
- 本日融券余量 = 前日融券余量 + 本日融券卖出量 - 本日融券买入量 - 本日现券偿还量
- 本日融券余额 = 本日融券余量 × 本日收盘价
- 本日融资融券余额 = 本日融资余额 + 本日融券余额

**验证结果**：✅ 通过

```python
df = pro.margin(trade_date='20240308')
```

---

### 5.2 融资融券交易明细 `margin_detail`

**接口**：`pro.margin_detail()`

**说明**：获取融资融券每日交易明细数据

**积分要求**：2000积分

**输入参数**：

| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| ts_code | str | N | 股票代码 |
| trade_date | str | N | 交易日期 |
| start_date | str | N | 开始日期 |
| end_date | str | N | 结束日期 |

**输出参数**：

| 字段 | 类型 | 说明 |
|------|------|------|
| ts_code | str | 股票代码 |
| trade_date | str | 交易日期 |
| rzye | float | 融资余额(元) |
| rzmre | float | 融资买入额(元) |
| rzche | float | 融资偿还额(元) |
| rqye | float | 融券余额(元) |
| rqmcl | float | 融券卖出量 |
| rqyl | float | 融券余量 |

**验证结果**：✅ 通过

```python
df = pro.margin_detail(ts_code='000001.SZ', trade_date='20240308')
```

---

## 6. 资金流向接口

### 6.1 个股资金流向 `moneyflow`

**接口**：`pro.moneyflow()`

**说明**：获取沪深A股票资金流向数据，分析大单小单成交情况。数据开始于2010年。

**积分要求**：2000积分

**输入参数**：

| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| ts_code | str | N | 股票代码 |
| trade_date | str | N | 交易日期 |
| start_date | str | N | 开始日期 |
| end_date | str | N | 结束日期 |

**输出参数**：

| 字段 | 类型 | 说明 |
|------|------|------|
| ts_code | str | TS代码 |
| trade_date | str | 交易日期 |
| buy_sm_vol | int | 小单买入量（手） |
| buy_sm_amount | float | 小单买入金额（万元） |
| sell_sm_vol | int | 小单卖出量（手） |
| sell_sm_amount | float | 小单卖出金额（万元） |
| buy_md_vol | int | 中单买入量（手） |
| buy_md_amount | float | 中单买入金额（万元） |
| sell_md_vol | int | 中单卖出量（手） |
| sell_md_amount | float | 中单卖出金额（万元） |
| buy_lg_vol | int | 大单买入量（手） |
| buy_lg_amount | float | 大单买入金额（万元） |
| sell_lg_vol | int | 大单卖出量（手） |
| sell_lg_amount | float | 大单卖出金额（万元） |
| buy_elg_vol | int | 特大单买入量（手） |
| buy_elg_amount | float | 特大单买入金额（万元） |
| sell_elg_vol | int | 特大单卖出量（手） |
| sell_elg_amount | float | 特大单卖出金额（万元） |
| net_mf_vol | int | 净流入量（手） |
| net_mf_amount | float | 净流入额（万元） |

**分类规则**：
- 小单：成交额 < 5万
- 中单：5万 ≤ 成交额 < 20万
- 大单：20万 ≤ 成交额 < 100万
- 特大单：成交额 ≥ 100万

**验证结果**：✅ 通过

```python
df = pro.moneyflow(ts_code='000001.SZ', start_date='20240301', end_date='20240308')
```

---

## 7. 打板专题接口

### 7.1 涨跌停列表 `limit_list_d`

**接口**：`pro.limit_list_d()`

**说明**：获取A股每日涨跌停、炸板数据情况。数据从2020年开始，不提供ST股票的统计。

**积分要求**：5000积分每分钟200次，8000积分以上每分钟500次

**输入参数**：

| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| trade_date | str | N | 交易日期 |
| ts_code | str | N | 股票代码 |
| limit_type | str | N | 涨跌停类型：U涨停 D跌停 Z炸板 |
| exchange | str | N | 交易所：SH上交所 SZ深交所 BJ北交所 |
| start_date | str | N | 开始日期 |
| end_date | str | N | 结束日期 |

**输出参数**：

| 字段 | 类型 | 说明 |
|------|------|------|
| trade_date | str | 交易日期 |
| ts_code | str | 股票代码 |
| industry | str | 所属行业 |
| name | str | 股票名称 |
| close | float | 收盘价 |
| pct_chg | float | 涨跌幅 |
| amount | float | 成交额 |
| limit_amount | float | 板上成交金额 |
| float_mv | float | 流通市值 |
| total_mv | float | 总市值 |
| turnover_ratio | float | 换手率 |
| fd_amount | float | 封单金额 |
| first_time | str | 首次封板时间 |
| last_time | str | 最后封板时间 |
| open_times | int | 炸板次数 |
| up_stat | str | 涨停统计 |
| limit_times | int | 连板数 |
| limit | str | D跌停 U涨停 Z炸板 |

**验证结果**：✅ 通过

```python
df = pro.limit_list_d(trade_date='20240308', limit_type='U')
```

---

### 7.2 龙虎榜每日明细 `top_list`

**接口**：`pro.top_list()`

**说明**：龙虎榜每日交易明细，数据历史从2005年至今

**积分要求**：2000积分

**输入参数**：

| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| trade_date | str | Y | 交易日期 |
| ts_code | str | N | 股票代码 |

**输出参数**：

| 字段 | 类型 | 说明 |
|------|------|------|
| trade_date | str | 交易日期 |
| ts_code | str | TS代码 |
| name | str | 名称 |
| close | float | 收盘价 |
| pct_change | float | 涨跌幅 |
| turnover_rate | float | 换手率 |
| amount | float | 总成交额 |
| l_sell | float | 龙虎榜卖出额 |
| l_buy | float | 龙虎榜买入额 |
| l_amount | float | 龙虎榜成交额 |
| net_amount | float | 龙虎榜净买入额 |
| net_rate | float | 龙虎榜净买额占比 |
| amount_rate | float | 龙虎榜成交额占比 |
| float_values | float | 当日流通市值 |
| reason | str | 上榜理由 |

**验证结果**：✅ 通过

```python
df = pro.top_list(trade_date='20240308')
```

---

## 8. 特色数据接口

### 8.1 每日筹码及胜率 `cyq_perf`

**接口**：`pro.cyq_perf()`

**说明**：获取A股每日筹码平均成本和胜率情况，每天18~19点左右更新，数据从2018年开始。

**积分要求**：5000积分每天20000次，10000积分每天200000次，15000积分每天不限总量

**输入参数**：

| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| ts_code | str | Y | 股票代码 |
| trade_date | str | N | 交易日期 |
| start_date | str | N | 开始日期 |
| end_date | str | N | 结束日期 |

**输出参数**：

| 字段 | 类型 | 说明 |
|------|------|------|
| ts_code | str | 股票代码 |
| trade_date | str | 交易日期 |
| his_low | float | 历史最低价 |
| his_high | float | 历史最高价 |
| cost_5pct | float | 5分位成本 |
| cost_15pct | float | 15分位成本 |
| cost_50pct | float | 50分位成本 |
| cost_85pct | float | 85分位成本 |
| cost_95pct | float | 95分位成本 |
| weight_avg | float | 加权平均成本 |
| winner_rate | float | 胜率 |

**验证结果**：✅ 通过

```python
df = pro.cyq_perf(ts_code='600000.SH', start_date='20220101', end_date='20220429')
```

---

### 8.2 神奇九转指标 `stk_nineturn`

**接口**：`pro.stk_nineturn()`

**说明**：神奇九转是一种基于技术分析的趋势反转指标，帮助识别潜在的抄底和逃顶点。日线级别配合60min的九转效果更好。

**数据时间**：数据从20230101开始

**积分要求**：6000积分

**输入参数**：

| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| ts_code | str | N | 股票代码 |
| trade_date | str | N | 交易日期 |
| freq | str | N | 频率：daily |
| start_date | str | N | 开始时间 |
| end_date | str | N | 结束时间 |

**输出参数**：

| 字段 | 类型 | 说明 |
|------|------|------|
| ts_code | str | 股票代码 |
| trade_date | datetime | 交易日期 |
| freq | str | 频率 |
| open | float | 开盘价 |
| high | float | 最高价 |
| low | float | 最低价 |
| close | float | 收盘价 |
| vol | float | 成交量 |
| amount | float | 成交额 |
| up_count | float | 上九转计数 |
| down_count | float | 下九转计数 |
| nine_up_turn | str | 是否上九转 |
| nine_down_turn | str | 是否下九转 |

**验证结果**：✅ 通过

```python
df = pro.stk_nineturn(ts_code='000001.SZ', freq='daily', 
                      fields='ts_code,trade_date,freq,up_count,down_count,nine_up_turn,nine_down_turn')
```

---

### 8.3 沪深股通十大成交股 `hsgt_top10`

**接口**：`pro.hsgt_top10()`

**说明**：获取沪深股通十大成交股数据

**积分要求**：1000积分

**输入参数**：

| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| ts_code | str | N | 股票代码 |
| trade_date | str | N | 交易日期 |
| start_date | str | N | 开始日期 |
| end_date | str | N | 结束日期 |
| market_type | str | N | 市场类型：SH沪股通 SZ深股通 |

**输出参数**：

| 字段 | 类型 | 说明 |
|------|------|------|
| ts_code | str | 股票代码 |
| trade_date | str | 交易日期 |
| name | str | 股票名称 |
| close | float | 收盘价 |
| pct_chg | float | 涨跌幅 |
| vol | float | 成交量 |
| amount | float | 成交额 |
| rank | int | 排名 |
| market_type | str | 市场类型 |

**验证结果**：✅ 通过

```python
df = pro.hsgt_top10(ts_code='000001.SZ', trade_date='20240308')
```

---

## 附录：接口调用规范

### 调用方式

根据项目规范，所有tushare接口调用必须：

1. 使用 `pro` 对象，禁止直接调用 `tushare` 模块
2. 在 `backend/scripts` 目录下的脚本，先添加 `sys.path.insert(0, 'backend')`
3. 然后 `from backend.utils.tushare_client import pro`

```python
import sys
sys.path.insert(0, '/Users/huangchuanjian/workspace/我的项目/ai_watch_stock/backend')
from backend.utils.tushare_client import pro
```

### 代码示例

```python
import sys
sys.path.insert(0, '/Users/huangchuanjian/workspace/我的项目/ai_watch_stock/backend')
from backend.utils.tushare_client import pro

df = pro.daily(ts_code='000001.SZ', start_date='20240301', end_date='20240308')
print(df)
```

---

## 验证结果汇总

| 接口名称 | 验证状态 | 说明 |
|---------|---------|------|
| stock_basic | ✅ 通过 | 股票列表 |
| trade_cal | ✅ 通过 | 交易日历 |
| namechange | ✅ 通过 | 股票曾用名 |
| stock_company | ✅ 通过 | 公司基本信息 |
| stk_rewards | ⚠️ 空数据 | 高管薪酬(可能需要特定条件) |
| new_share | ✅ 通过 | IPO新股 |
| daily | ✅ 通过 | 日线行情 |
| daily_basic | ✅ 通过 | 每日指标 |
| adj_factor | ✅ 通过 | 复权因子 |
| pro_bar | ✅ 通过 | 通用行情 |
| suspend_d | ✅ 通过 | 停复牌信息 |
| income | ✅ 通过 | 利润表 |
| balancesheet | ✅ 通过 | 资产负债表 |
| cashflow | ✅ 通过 | 现金流量表 |
| fina_indicator | ✅ 通过 | 财务指标 |
| forecast | ✅ 通过 | 业绩预告 |
| express | ✅ 通过 | 业绩快报 |
| dividend | ✅ 通过 | 分红送股 |
| top10_holders | ✅ 通过 | 前十大股东 |
| stk_holdernumber | ✅ 通过 | 股东人数 |
| block_trade | ✅ 通过 | 大宗交易 |
| pledge_stat | ✅ 通过 | 股权质押统计 |
| stk_holdertrade | ⚠️ 空数据 | 股东增减持(可能需要特定条件) |
| margin | ✅ 通过 | 融资融券汇总 |
| margin_detail | ✅ 通过 | 融资融券明细 |
| moneyflow | ✅ 通过 | 个股资金流向 |
| limit_list_d | ✅ 通过 | 涨跌停列表 |
| top_list | ✅ 通过 | 龙虎榜明细 |
| cyq_perf | ✅ 通过 | 每日筹码及胜率 |
| stk_nineturn | ✅ 通过 | 神奇九转指标 |
| hsgt_top10 | ✅ 通过 | 沪深股通十大成交股 |

---

*文档更新时间：2026-04-12*