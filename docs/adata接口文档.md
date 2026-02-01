# adata 接口文档（本地库反查版）

本文档基于本机已安装的 `adata==2.9.3` 进行源码反查与 `inspect.signature` 汇总，适用于本项目在本地直接调用 `adata` 的场景。

## 安装与入口

- Python 包路径：`/Users/huangchuanjian/Library/Python/3.9/lib/python/site-packages/adata/`
- 顶层入口：
  - `import adata`
  - `adata.version()`：返回版本号字符串
  - `adata.proxy(is_proxy=False, ip=None, proxy_url=None)`：设置请求代理
  - `adata.set_logger()`：设置日志 handler（库导入时已默认调用一次）

## 总体调用结构

`adata` 将接口按领域分组为 4 个对象：

- `adata.stock`：股票
  - `adata.stock.info`：基础信息/概念/交易日历/指数信息
  - `adata.stock.market`：行情/资金流/概念行情/指数行情/分红
  - `adata.stock.finance`：财务/核心指标
  - `adata.stock.index`：本版本未暴露可直接调用的方法
- `adata.fund`：基金（场内 ETF）
  - `adata.fund.info`：ETF 基础信息
  - `adata.fund.market`：ETF 行情
- `adata.bond`：可转债
  - `adata.bond.info`：可转债代码
  - `adata.bond.market`：可转债行情
- `adata.sentiment`：舆情/资金面/热度
  - `adata.sentiment.hot`：龙虎榜/热股/热门概念
  - `adata.sentiment.north`：北向资金
  - `adata.sentiment.mine`：通达信扫雷

绝大多数接口返回 `pandas.DataFrame`；无数据时通常返回空 DataFrame。

## 本项目现有用法提醒

项目内当前仅发现一处 `adata` 引用：[一夜持股法.py](file:///Users/huangchuanjian/workspace/my_projects/ai_watch_stock/backend/scripts/%E4%B8%80%E5%A4%9C%E6%8C%81%E8%82%A1%E6%B3%95.py#L41-L45)

- `adata.stock.market.list_market_current(code_list=None)`：`code_list` 为空时会直接返回空 DataFrame（该脚本目前传参为空会拿不到数据）。
  - 正确示例：`adata.stock.market.list_market_current(code_list=["000001", "600519"])`

## 股票（adata.stock）

### 1) 基础信息（adata.stock.info）

入口：`adata.stock.info`

#### 股票列表/代码相关

- `adata.stock.info.all_code(wait_time=100)`
  - 含义：获取全市场股票代码、简称、交易所、上市日期
  - 参数：
    - `wait_time`：请求间隔/等待参数（用于降低频率限制）
  - 返回列：`['stock_code', 'short_name', 'exchange', 'list_date']`

- `adata.stock.info.market_rank_sina(wait_time=100)`
  - 含义：新浪涨幅榜股票列表（作为 `all_code` 的兜底来源之一）

#### 交易日历

- `adata.stock.info.trade_calendar(year=None)`
  - 含义：获取交易日历
  - 参数：
    - `year`：年份（缺省为当年）
  - 返回列：`['trade_date', 'trade_status', 'day_week']`

#### 指数

- `adata.stock.info.all_index_code()`
  - 含义：获取 A 股指数列表（主要来源：东方财富）
  - 返回列：`['index_code', 'concept_code', 'name', 'source']`

- `adata.stock.info.index_constituent(index_code=None, wait_time=None)`
  - 含义：获取指数成分股（实现里当前走百度源）
  - 参数：
    - `index_code`：指数代码（如 `000300` 等）
    - `wait_time`：请求间隔/等待参数
  - 返回列：`['index_code', 'stock_code', 'short_name']`

#### 概念/板块

概念代码在不同数据源中形态不同：

- 同花顺：
  - `index_code`：8 开头（概念指数代码）
  - `concept_code`：3 开头（网页概念代码）
- 东方财富：
  - `concept_code`：多为 `BKxxxx` 或接口返回的板块 code

接口：

- `adata.stock.info.all_concept_code_ths()`
  - 含义：同花顺概念列表（名称、概念指数代码、概念代码）

- `adata.stock.info.all_concept_code_east(wait_time=None)`
  - 含义：东方财富概念列表

- `adata.stock.info.concept_constituent_ths(concept_code=None, name=None, index_code=None, wait_time=None)`
  - 含义：同花顺概念成分股
  - 优先级：`index_code > name > concept_code`（三选一）
  - 返回列：`['stock_code', 'short_name']`（源码模板中还可能含 `concept_code`，以实际返回为准）

- `adata.stock.info.concept_constituent_east(concept_code=None, wait_time=None)`
  - 含义：东方财富概念成分股
  - 参数：
    - `concept_code`：如 `BK0966`
  - 返回列：`['stock_code', 'short_name']`

- `adata.stock.info.get_concept_ths(stock_code: str = '000001')`
- `adata.stock.info.get_concept_east(stock_code: str = '000001')`
- `adata.stock.info.get_concept_baidu(stock_code='000001')`
  - 含义：根据股票代码获取所属概念
  - 参数：
    - `stock_code`：6 位股票代码字符串；部分实现支持传入列表
  - 返回列：一般包含 `stock_code / concept_code / name / reason / source`

- `adata.stock.info.get_plate_east(stock_code: str = '000001', plate_type=None)`
  - 含义：根据股票代码获取所属板块信息
  - 参数：
    - `plate_type`：`None` 返回全部；`1` 行业；`2` 地域板块；`3` 概念（内部映射到中文类型）

#### 股本/行业

- `adata.stock.info.get_stock_shares(stock_code: str = '000033', is_history=True)`
  - 含义：股本结构（来源：东方财富）
  - 参数：
    - `stock_code`：6 位股票代码
    - `is_history`：是否返回历史（True 返回多行；False 取最新一行）
  - 返回列：`['stock_code', 'change_date', 'total_shares', 'limit_shares', 'list_a_shares', 'change_reason']`

- `adata.stock.info.get_industry_sw(stock_code='000001')`
  - 含义：申万行业（来源：百度股市通）
  - 参数：
    - `stock_code`：支持单个代码或代码列表
  - 返回列：`['stock_code', 'sw_code', 'industry_name', 'industry_type', 'source']`

- `adata.stock.info.get_dynamic_core_index(stock_code='000001')`
  - 含义：动态核心指标
  - 状态：源码内标记 `TODO`，当前实现直接 `return`，不建议使用

### 2) 行情/资金流（adata.stock.market）

入口：`adata.stock.market`

#### 个股行情

- `adata.stock.market.get_market(stock_code: str = '000001', start_date='1990-01-01', end_date=None, k_type=1, adjust_type: int = 1)`
  - 含义：K 线行情（默认前复权）
  - 参数：
    - `stock_code`：6 位股票代码
    - `start_date`：开始日期（`YYYY-MM-DD`）
    - `end_date`：结束日期（`YYYY-MM-DD` 或 None）
    - `k_type`：周期类型：`1`日，`2`周，`3`月，`4`季，`5`=5min，`15`=15min，`30`=30min，`60`=60min
    - `adjust_type`：`0`不复权，`1`前复权，`2`后复权（该版本注释提示“目前只有前复权可用”）

- `adata.stock.market.get_market_min(stock_code: str = '000001')`
  - 含义：当日分时行情

- `adata.stock.market.get_market_bar(stock_code: str = '000001')`
  - 含义：分时成交明细（逐笔/分时成交）

- `adata.stock.market.get_market_five(stock_code: str = '000001')`
  - 含义：五档行情

- `adata.stock.market.list_market_current(code_list=None)`
  - 含义：多股票最新行情快照
  - 参数：
    - `code_list`：股票代码列表（如 `["000001","600519"]`）
  - 返回字段（源码注释）：`stock_code, short_name, price, change, change_pct, volume, amount`

#### 个股资金流

- `adata.stock.market.get_capital_flow(stock_code: str = '000001', start_date=None, end_date=None)`
  - 含义：日度资金流向（东财；窗口期较短，源码提示约 120 天/最近两年内，实际以接口为准）

- `adata.stock.market.get_capital_flow_min(stock_code: str = '000001')`
  - 含义：当日分时资金流向

- `adata.stock.market.all_capital_flow_east(days_type=1)`
  - 含义：全市场资金流（东财）
  - 备注：`days_type` 含义需以实际返回为准

#### 概念/板块行情

- `adata.stock.market.get_market_concept_current_ths(index_code: str = '886013', k_type: int = 1)`
  - 含义：同花顺概念当前行情

- `adata.stock.market.get_market_concept_ths(index_code: str = '886013', k_type: int = 1, adjust_type: int = 1)`
  - 含义：同花顺概念 K 线行情

- `adata.stock.market.get_market_concept_min_ths(index_code='886041')`
  - 含义：同花顺概念当日分时

- `adata.stock.market.get_market_concept_current_east(index_code: str = 'BK0612')`
  - 含义：东方财富概念当前行情（板块 code 形如 `BK0612`）

- `adata.stock.market.get_market_concept_east(index_code: str = 'BK0612', k_type: int = 1)`
  - 含义：东方财富概念 K 线行情

- `adata.stock.market.get_market_concept_min_east(index_code='BK0612')`
  - 含义：东方财富概念当日分时

#### 概念资金流

- `adata.stock.market.concept_capital_flow_east(days_type=1)`
- `adata.stock.market.get_concept_flow(index_code='BK0816', days_type=1)`
  - 含义：概念/板块资金流（东财）
  - 备注：`index_code` 形如 `BKxxxx`

#### 指数行情

- `adata.stock.market.get_market_index(index_code: str = '000001', start_date='2020-01-01', k_type: int = 1)`
  - 含义：指数 K 线行情

- `adata.stock.market.get_market_index_current(index_code: str = '000001')`
  - 含义：指数当前快照

- `adata.stock.market.get_market_index_min(index_code='000001')`
  - 含义：指数当日分时

#### 分红

- `adata.stock.market.get_dividend(stock_code='000001')`
  - 含义：个股分红信息

#### 问财辅助（可选）

部分同花顺/问财相关功能暴露为通用方法（多个子模块共享）：

- `adata.stock.market.get_wencai_server_time()`
- `adata.stock.market.wencai_hexin_v(js_path='hexin.js')`

### 3) 财务/核心指标（adata.stock.finance）

入口：`adata.stock.finance`

- `adata.stock.finance.get_core_index(stock_code='300059')`
  - 含义：核心指标（财务类汇总指标）

## 基金（adata.fund）

### 1) 基金信息（adata.fund.info）

入口：`adata.fund.info`

- `adata.fund.info.all_etf_exchange_traded_info(wait_time=None)`
  - 含义：获取全部场内 ETF 信息

同样也包含问财辅助：

- `adata.fund.info.get_wencai_server_time()`
- `adata.fund.info.wencai_hexin_v(js_path='hexin.js')`

### 2) 基金行情（adata.fund.market）

入口：`adata.fund.market`

- `adata.fund.market.get_market_etf(fund_code: str = '512880', k_type: int = 1, start_date='', end_date='')`
  - 含义：ETF K 线行情

- `adata.fund.market.get_market_etf_current(fund_code: str = '512880', k_type: int = 1)`
  - 含义：ETF 当前行情（命名上是 current）

- `adata.fund.market.get_market_etf_min(fund_code='512880')`
  - 含义：ETF 当日分时

## 可转债（adata.bond）

### 1) 债券信息（adata.bond.info）

入口：`adata.bond.info`

- `adata.bond.info.all_convert_code()`
  - 含义：获取所有可转债代码信息

### 2) 债券行情（adata.bond.market）

入口：`adata.bond.market`

- `adata.bond.market.list_market_current(code_list=None)`
  - 含义：多个可转债最新行情信息
  - 参数：
    - `code_list`：可转债代码列表

## 舆情/资金面（adata.sentiment）

入口：`adata.sentiment`

### 1) 融资融券余额

- `adata.sentiment.securities_margin(start_date=None)`
  - 含义：融资融券余额（默认查询最近一年）
  - 参数：
    - `start_date`：开始日期（格式以接口实现为准；通常支持 `YYYY-MM-DD` 或 `YYYYMMDD`）

### 2) 解禁

- `adata.sentiment.stock_lifting_last_month()`
  - 含义：最近一个月股票解禁列表

### 3) 热度/榜单（adata.sentiment.hot）

入口：`adata.sentiment.hot`

- `adata.sentiment.hot.pop_rank_100_east()`
  - 含义：东方财富人气榜 100

- `adata.sentiment.hot.hot_rank_100_ths()`
  - 含义：同花顺热股 100

- `adata.sentiment.hot.hot_concept_20_ths(plate_type=1)`
  - 含义：同花顺热门概念板块

- `adata.sentiment.hot.list_a_list_daily(report_date=None)`
  - 含义：每日龙虎榜（默认当天）
  - 参数：
    - `report_date`：日期（格式以实现为准）

- `adata.sentiment.hot.get_a_list_info(stock_code, report_date=None)`
  - 含义：单个股票龙虎榜详情（买 5/卖 5）

同样也包含问财辅助：

- `adata.sentiment.hot.get_wencai_server_time()`
- `adata.sentiment.hot.wencai_hexin_v(js_path='hexin.js')`

### 4) 北向资金（adata.sentiment.north）

入口：`adata.sentiment.north`

- `adata.sentiment.north.north_flow(start_date=None)`
  - 含义：北向资金历史数据（从 start_date 到最新）

- `adata.sentiment.north.north_flow_current()`
  - 含义：北向资金最新交易日数据

- `adata.sentiment.north.north_flow_min()`
  - 含义：北向资金当日分时数据

同样也包含问财辅助：

- `adata.sentiment.north.get_wencai_server_time()`
- `adata.sentiment.north.wencai_hexin_v(js_path='hexin.js')`

### 5) 通达信扫雷（adata.sentiment.mine）

入口：`adata.sentiment.mine`

- `adata.sentiment.mine.mine_clearance_tdx(stock_code='600811')`
  - 含义：通达信扫雷信息

