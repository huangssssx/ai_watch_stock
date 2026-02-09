# pytdx 接口文档（本地库反查版）

本文档基于本机已安装的 `pytdx==1.72` 进行源码反查，结合 `inspect.signature`、`parser/*` 的字段解析逻辑整理而成，适用于本项目在本地直接调用 `pytdx` 的场景。

## 安装与入口

- Python 包路径：`/Users/huangchuanjian/Library/Python/3.9/lib/python/site-packages/pytdx/`
- 常用入口：
  - `from pytdx.hq import TdxHq_API`
  - `from pytdx.exhq import TdxExHq_API`

## 本项目封装

- 连接/复用封装：[pytdx_client.py](file:///Users/huangchuanjian/workspace/my_projects/ai_watch_stock/backend/utils/pytdx_client.py)
- 最小用例：[test.py](file:///Users/huangchuanjian/workspace/my_projects/ai_watch_stock/backend/scripts/test.py)

## 通用约定

### 市场与周期

- `market`（A 股 HQ 行情）：
  - `0`：深圳（SZ）
  - `1`：上海（SH）
- `category`（K 线周期，见 `pytdx.params.TDXParams`）：
  - `0/1/2/3`：5/15/30 分钟、1 小时
  - `4/5/6`：日/周/月
  - `7/8`：1 分钟（扩展/普通）
  - `9/10/11`：日/季/年（其中 `9` 常用于日线）
- `start` / `count`：
  - `start`：从最近数据向前的偏移（协议层的“起始块”概念，通常用 0 表示最新）
  - `count`：返回条数（上限由服务端与库常量决定；A 股 K 线单次最多约 800）
- `date`：
  - HQ：`YYYYMMDD` 整数（如 `20170811`）
  - EXHQ：同样使用 `YYYYMMDD` 整数

### 返回值形态

- 多数接口返回 `list[OrderedDict]` 或 `dict` 或 `int`。
- 你可以用 `api.to_df(ret)` 把返回转换成 `pandas.DataFrame`（`list`/`dict` 更友好；`int` 会变成单列 `value`）。
- 部分字段在源码里明确标注为 `reversed_bytes* / unused / unknown`：此类字段含义在库中未解释，本文会标记为“未明确”。
- 成交量/成交额单位：pytdx 对某些字段做了特殊解码（见 `pytdx.helper.get_volume`）；在本项目已验证的 A 股 HQ 场景下，各接口的单位结论已写在对应条目内；其他市场/接口未实测的不做单位承诺。

## 1) HQ 行情（TdxHq_API）

入口：`pytdx.hq.TdxHq_API`

### 连接与通用能力

#### `connect(self, ip='101.227.73.20', port=7709, time_out=5.0, bindport=None, bindip='0.0.0.0')`

- 作用：建立到通达信行情服务器的 TCP 连接，并执行必要的 setup；可用于 `with api.connect(...): ...`
- 参数：
  - `ip`：服务器地址
  - `port`：服务器端口
  - `time_out`：连接超时秒数
  - `bindport`：绑定本地端口（可选）
  - `bindip`：绑定本地 IP（默认 `0.0.0.0`）
- 返回：
  - 成功：返回 `self`（因此 `if api.connect(...):` 会为真）
  - 失败：返回 `False`（当 `raise_exception=False` 时）

#### `disconnect(self)` / `close(self)`

- 作用：断开连接并关闭 socket；`close` 是 `disconnect` 的别名
- 返回：无

#### `setup(self)`

- 作用：发送通达信协议的初始化指令（由库内部 `SetupCmd1/2/3` 完成）
- 何时调用：默认 `connect()` 会自动调用（除非你把 `api.need_setup=False`）
- 返回：无

#### `do_heartbeat(self)`

- 作用：发送一个轻量请求维持连接活性（内部随机调用一次 `get_security_count`）
- 返回：无（该方法本身不返回请求结果）

#### `get_traffic_stats(self)`

- 作用：读取 socket 的流量统计信息
- 返回：`dict`，字段意义：
  - `send_pkg_num` / `recv_pkg_num`：发送/接收包次数
  - `send_pkg_bytes` / `recv_pkg_bytes`：发送/接收总字节数
  - `first_pkg_send_time`：首次发送时间
  - `total_seconds`：统计窗口秒数
  - `send_bytes_per_second` / `recv_bytes_per_second`：平均速率
  - `last_api_send_bytes` / `last_api_recv_bytes`：最近一次 API 调用的字节数

#### `send_raw_pkg(self, pkg)`

- 作用：发送原始协议包（调试/抓包/协议研究用途）
- 参数：
  - `pkg`：`bytes`/`bytearray`
- 返回：原始解析结果（由 `RawParser` 决定，通常是 `bytes` 或解析后的结构）

#### `to_df(self, v)`

- 作用：将返回值转换为 `DataFrame`
- 参数：
  - `v`：`list` / `dict` / `int` 等
- 返回：
  - `list` → DataFrame（每个元素一行）
  - `dict` → DataFrame（一行）
  - 其他 → DataFrame（单行单列 `value`）

### 行情与数据接口

#### `get_security_bars(self, category, market, code, start, count)`

- 作用：获取证券（股票/基金等）K 线（含分钟/日线等）
- 参数：
  - `category`：K 线周期（见“通用约定”）
  - `market`：0=深市，1=沪市
  - `code`：6 位证券代码字符串（如 `"000001"`）
  - `start`：起始偏移（0 通常表示最新）
  - `count`：条数
- 返回：`list[OrderedDict]`，每个元素字段意义：
  - `open` / `close` / `high` / `low`：开/收/高/低
  - `vol`：成交量
    - A 股日线经对照验证：当 `category=4` 时，`vol` 单位为“股”；当 `category=9` 时，`vol` 单位为“手”
  - `amount`：成交额/成交金额（库中命名为 `dbvol`）
    - A 股日线经对照验证：`amount` 单位为“元”（不随 `category=4/9` 改变）
    - 量价一致性校验：`amount / vol` 若约为股价，则 `vol` 为“股”；若约为股价的 100 倍，则 `vol` 为“手”（计算 VWAP 时需除以 100）
  - `year` / `month` / `day` / `hour` / `minute`：时间分量
  - `datetime`：`YYYY-MM-DD HH:MM` 字符串

#### `get_index_bars(self, category, market, code, start, count)`

- 作用：获取指数 K 线（形态与 `get_security_bars` 类似，额外包含上涨/下跌家数）
- 参数：同 `get_security_bars`
- 返回：`list[OrderedDict]`，字段意义：
  - 价格/成交字段：同 `get_security_bars`
  - `up_count`：上涨家数
  - `down_count`：下跌家数

#### `get_security_quotes(self, all_stock, code=None)`

- 作用：获取一个或多个证券的实时快照（含五档）
- 参数：
  - `all_stock`：支持三种形式：
    - `get_security_quotes(market, code)`（此时 `all_stock`=market，`code`=代码）
    - `get_security_quotes((market, code))`
    - `get_security_quotes([(market1, code1), (market2, code2)])`
  - `code`：当你用第一种形式时填写
- 返回：`list[OrderedDict]`，每个元素字段意义（源码字段名）：
  - 基本标识：
    - `market`：市场（0/1）
    - `code`：代码
    - `active1` / `active2`：状态位（库未给出解释）
  - 价格：
    - `price`：现价（快照时刻的最新价）
    - `last_close`：昨收（上一交易日收盘价，日内固定）
    - `open`：今开（当日开盘价，日内固定）
    - `high`：日内最高（开盘至快照时刻的最高价）
    - `low`：日内最低（开盘至快照时刻的最低价）
  - 时间：
    - `servertime`：服务器时间（快照时间；由 `reversed_bytes0` 格式化得来）
    - `reversed_bytes0`：原始时间戳/字段（未明确）
  - 成交：
    - `vol`：总成交量（开盘至快照时刻的累计成交量）
      - A 股实测：单位为“手”
    - `amount`：成交额/成交金额（开盘至快照时刻的累计成交额；由 `get_volume` 解码）
      - A 股实测：单位为“元”
      - 量价一致性：`amount / (vol * 100)` 与 `price` 同量级（接近 VWAP）
    - `cur_vol`：现量（快照时刻对应的“最近一笔/最近一次撮合”的成交量）
      - A 股实测：单位为“手”，且总是满足 `cur_vol <= vol`
    - `b_vol`：外盘/主动买入成交量（开盘至快照时刻累计）
      - A 股实测：单位为“手”
    - `s_vol`：内盘/主动卖出成交量（开盘至快照时刻累计）
      - A 股实测：单位为“手”
    - A 股实测补充：`b_vol + s_vol` 大多数情况下等于 `vol`，但偶尔会出现相差 1 手的情况（不建议把等式当作强约束）
  - 五档（价格与数量）：
    - `bid1..bid5`：买一到买五价
    - `ask1..ask5`：卖一到卖五价
    - `bid_vol1..bid_vol5`：买一到买五量（快照时刻盘口挂单量，非日内累计）
    - `ask_vol1..ask_vol5`：卖一到卖五量（快照时刻盘口挂单量，非日内累计）
  - 其他：
    - `reversed_bytes1/2/3/4/5/6/7/8`：保留字段（未明确）
    - `reversed_bytes9`：涨速（快照时刻的指标；源码注释：`# 涨速`，单位为百分之一）

#### `get_security_count(self, market)`

- 作用：获取指定市场证券数量
- 参数：
  - `market`：0=深市，1=沪市
- 返回：`int`，证券数量

#### `get_security_list(self, market, start)`

- 作用：分页获取证券代码列表（每页返回若干条；`start` 是分页偏移）
- 参数：
  - `market`：0=深市，1=沪市
  - `start`：分页起点（常见用法：0、100、200...）
- 返回：`list[OrderedDict]`，字段意义：
  - `code`：证券代码
  - `name`：名称（GBK 解码）
  - `pre_close`：字段名为 `pre_close`（由 `get_volume` 解码）
    - A 股实测：该字段经常与 `get_security_quotes.last_close` 不一致（偏差可达 1000 倍以上），不可作为“昨收/前收”使用；如需昨收请用 `get_security_quotes.last_close`
  - `volunit`：成交量单位标识（源码未给出映射）
    - A 股股票实测：该字段恒为 `100`（与“1 手 = 100 股”的交易单位一致）
  - `decimal_point`：小数位数（价格精度）
  - A 股实测补充：上交所 `market=1` 时，`start` 在较小取值（如 `0/100/200/500`）会返回 `None`；从 `start≈800` 起返回 `list`

#### `get_minute_time_data(self, market, code)`

- 作用：获取当日分时（价格-量）
- 参数：
  - `market`：0/1
  - `code`：6 位代码
- 返回：`list[OrderedDict]`，字段意义：
  - `price`：价格
  - `vol`：该点对应的量
    - A 股实测：单位为“手”（对当日全量分时求和可与日内总成交量对齐）

#### `get_history_minute_time_data(self, market, code, date)`

- 作用：获取历史某日分时（价格-量）
- 参数：
  - `market`：0/1
  - `code`：6 位代码
  - `date`：`YYYYMMDD`（int 或数字字符串）
- 返回：与 `get_minute_time_data` 同结构：`list[{price, vol}]`
  - A 股实测：`vol` 单位与 `get_minute_time_data` 一致，均为“手”

#### `get_transaction_data(self, market, code, start, count)`

- 作用：获取当日逐笔成交（分时成交明细）
- 参数：
  - `market`：0/1
  - `code`：6 位代码
  - `start`：起始偏移
  - `count`：条数
- 返回：`list[OrderedDict]`，字段意义：
  - `time`：`HH:MM`
  - `price`：成交价
  - `vol`：成交量
    - A 股实测：单位为“手”（对当日全量逐笔求和可与 `get_security_quotes.vol` 对齐，允许存在 1 手级别误差）
  - `num`：成交笔数/撮合笔数（库未进一步解释）
  - `buyorsell`：买卖方向标识（协议原始字段，pytdx 不做枚举映射）
    - A 股实测：取值为 `0/1/2/8`（含义未在库中定义，因此本文不做“买/卖/中性”映射承诺）

#### `get_history_transaction_data(self, market, code, start, count, date)`

- 作用：获取历史某日逐笔成交
- 参数：
  - `market`：0/1
  - `code`：6 位代码
  - `start` / `count`：分页
  - `date`：`YYYYMMDD`（int 或数字字符串）
- 返回：与 `get_transaction_data` 类似，但不含 `num` 字段：
  - `time` / `price` / `vol` / `buyorsell`
  - A 股实测：`vol` 单位为“手”（口径同 `get_transaction_data`）
  - A 股实测：`buyorsell` 取值范围与 `get_transaction_data` 一致（`0/1/2/8`）

#### `get_company_info_category(self, market, code)`

- 作用：获取公司信息目录（有哪些文本文件、偏移与长度）
- 参数：
  - `market`：0/1
  - `code`：6 位代码
- 返回：`list[OrderedDict]`，字段意义：
  - `name`：条目名称（GBK）
  - `filename`：文件名（GBK）
  - `start`：起始偏移
  - `length`：长度

#### `get_company_info_content(self, market, code, filename, start, length)`

- 作用：读取公司信息文本内容（配合 `get_company_info_category`）
- 参数：
  - `market`：0/1
  - `code`：6 位代码
  - `filename`：目录返回的 `filename`（会被补齐到 80 字节）
  - `start`：起始偏移
  - `length`：读取长度
- 返回：`str`（GBK 解码后的文本内容）

#### `get_finance_info(self, market, code)`

- 作用：获取财务摘要（股本、资产负债、利润等一组字段）
- 参数：
  - `market`：0/1
  - `code`：6 位代码
- 返回：`OrderedDict`，字段意义（按源码字段名；多数数值在库内做了固定倍率缩放，源码中常见 `*10000`）：
  - 基本：
    - `market` / `code`
    - `province`：省份代码（通达信内部编码）
    - `industry`：行业代码（通达信内部编码）
    - `updated_date`：财务更新日期（`YYYYMMDD`）
    - `ipo_date`：上市日期（`YYYYMMDD`）
  - 股本（经对照验证，单位为“股”，且是 `updated_date` 对应的时点快照值）：
    - 对照来源：Tushare Pro `daily_basic`（`float_share/total_share` 单位为“万股”，需 `*10000` 换算为“股”后可对齐）
    - `liutongguben`：流通股本
    - `zongguben`：总股本
    - `guojiagu`：国家股
    - `faqirenfarengu`：发起人法人股
    - `farengu`：法人股
    - `bgu` / `hgu`：B 股 / H 股
    - `zhigonggu`：职工股
  - 资产负债与经营（同样多数字段 `*10000`）：
    - `zongzichan`：总资产
    - `liudongzichan`：流动资产
    - `gudingzichan`：固定资产
    - `wuxingzichan`：无形资产
    - `gudongrenshu`：股东人数
    - `liudongfuzhai`：流动负债
    - `changqifuzhai`：长期负债
    - `zibengongjijin`：资本公积金
    - `jingzichan`：净资产
    - `zhuyingshouru`：主营收入
    - `zhuyinglirun`：主营利润
    - `yingshouzhangkuan`：应收账款
    - `yingyelirun`：营业利润
    - `touzishouyu`：投资收益
    - `jingyingxianjinliu`：经营现金流
    - `zongxianjinliu`：总现金流
    - `cunhuo`：存货
    - `lirunzonghe`：利润总额
    - `shuihoulirun`：税后利润
    - `jinglirun`：净利润
    - `weifenpeilirun`：未分配利润
    - `meigujingzichan`：每股净资产（源码从 `baoliu1` 取值）
      - A 股经对照验证：与 Tushare Pro `daily_basic` 的 `close/pb` 同量级且可对齐（误差通常在 0-2% 内）
    - `baoliu2`：保留字段 2（未明确）

#### `get_xdxr_info(self, market, code)`

- 作用：获取除权除息/股本变动等事件（XDXR）
- 参数：
  - `market`：0/1
  - `code`：6 位代码
- 返回：`list[OrderedDict]`，字段意义：
  - 公共字段：
    - `year` / `month` / `day`
    - `category`：事件类别编号
    - `name`：事件类别名称（库内映射，例：`1=除权除息`、`2=送配股上市`、`5=股本变化`、`10=可转债上市` 等）
  - 事件相关字段（不同 `category` 含义不同；无关字段为 `None`）：
    - `fenhong`：分红
    - `peigujia`：配股价
    - `songzhuangu`：送转股
    - `peigu`：配股
    - `suogu`：缩股
    - `panqianliutong` / `panhouliutong`：盘前/盘后流通股本
    - `qianzongguben` / `houzongguben`：盘前/盘后总股本
    - `fenshu`：份数（权证相关）
    - `xingquanjia`：行权价（权证相关）

#### `get_block_info_meta(self, blockfile)`

- 作用：读取板块文件的元信息（尺寸与 hash）
- 参数：
  - `blockfile`：服务端板块文件名（例如 `block.dat`、`block_gn.dat` 等）
- 返回：`dict`
  - `size`：文件大小
  - `hash_value`：hash（`bytes`）

#### `get_block_info(self, blockfile, start, size)`

- 作用：读取板块文件的一个数据段（原始 `bytes`）
- 参数：
  - `blockfile`：文件名
  - `start`：起始偏移
  - `size`：读取长度
- 返回：`bytes`（源码直接 `body_buf[4:]`）

#### `get_and_parse_block_info(self, blockfile)`

- 作用：下载完整板块文件并解析为可用结构（内部会分块下载）
- 参数：
  - `blockfile`：文件名
- 返回：`list[OrderedDict]`（平铺模式），字段意义（见 `pytdx.reader.block_reader.BlockReader`）：
  - `blockname`：板块名（GBK）
  - `block_type`：板块类型编号
  - `code_index`：板块内序号
  - `code`：证券代码

#### `get_report_file(self, filename, offset)`

- 作用：从代理服务器下载报告文件的一个 chunk
- 参数：
  - `filename`：远端文件路径（如 `tdxfin/gpcw.txt`）
  - `offset`：偏移
- 返回：`dict`
  - `chunksize`：本次返回的 chunk 字节数（0 表示结束/无数据）
  - `chunkdata`：chunk 原始字节（当 `chunksize>0` 时存在）

#### `get_report_file_by_size(self, filename, filesize=0, reporthook=None)`

- 作用：按大小（或直到结束）循环调用 `get_report_file` 下载完整文件
- 参数：
  - `filename`：远端文件路径
  - `filesize`：预期大小；未知时传 0（将读到服务端返回 0 chunk）
  - `reporthook`：回调函数 `hook(downloaded, filesize)`（可选）
- 返回：`bytearray`（文件内容）

#### `get_k_data(self, code, start_date, end_date)`

- 作用：便捷方法：用 `get_security_bars` 批量拼接日线并返回 DataFrame（见源码注释链接）
- 参数：
  - `code`：6 位代码
  - `start_date` / `end_date`：`YYYY-MM-DD` 字符串
- 返回：`pandas.DataFrame`，常见列：
  - `open` / `close` / `high` / `low`
  - `vol` / `amount`
  - `date`：`YYYY-MM-DD`
  - `code`：代码

## 2) 扩展行情（TdxExHq_API）

入口：`pytdx.exhq.TdxExHq_API`

说明：扩展行情覆盖期货/港股/外盘等，`market/category/code` 的取值需要先通过 `get_markets()`、`get_instrument_info()` 等接口探测。
  - 本机 `pytdx==1.72` 实测：EXHQ 可以成功连接到扩展行情服务器且 `get_instrument_count()` 可返回 `int`，但 `get_markets()` 与 `get_instrument_info()` 返回 `None`，因此无法在本机通过 EXHQ 探测可用的 `market/code`；本节中涉及口径的字段均未做进一步实测校验

### 连接与通用能力

#### `connect / disconnect / close / setup / do_heartbeat / get_traffic_stats / send_raw_pkg / to_df`

- 这些方法与 `TdxHq_API` 来自同一个 `BaseSocketClient`，语义一致：
  - `connect`：连接并 `setup`
  - `disconnect/close`：断开
  - `setup`：扩展行情的初始化（`ExSetupCmd1`）
  - `do_heartbeat`：内部调用 `get_instrument_count()`
  - `get_traffic_stats` / `send_raw_pkg` / `to_df`：同上

### 市场与代码探测

#### `get_markets(self)`

- 作用：获取可用市场列表（用于确定 `market` 值）
- 参数：无
- 返回：
  - 本机实测：返回 `None`

#### `get_instrument_count(self)`

- 作用：获取扩展行情中可用标的总数
- 参数：无
- 返回：`int`：数量

#### `get_instrument_info(self, start, count=100)`

- 作用：分页获取标的（代码/名称/描述等），用于查找某个品种的 `code`
- 参数：
  - `start`：起始偏移
  - `count`：条数
- 返回：
  - 本机实测：返回 `None`

### 快照/分时/K线/逐笔

#### `get_instrument_quote(self, market, code)`

- 作用：获取单个标的的实时快照（含五档、持仓等；适用于期货/港股等）
- 参数：
  - `market`：市场编号（来自 `get_markets`）
  - `code`：代码（9 字符内，如 `IF1709`、`00020`、`BABA` 等）
- 返回：`list[OrderedDict]`（通常 1 条），字段意义（按源码字段名）：
  - 价格：
    - `pre_close`：昨收/昨结
    - `open`：今开
    - `high`：最高
    - `low`：最低
    - `price`：现价
  - 成交/持仓：
    - `kaicang`：开仓量（字段名直译；本机未实测，原因见本节开头说明）
    - `zongliang`：总量
    - `xianliang`：现量
    - `neipan` / `waipan`：内盘/外盘
    - `chicang`：持仓
  - 五档：
    - `bid1..bid5` / `ask1..ask5`：买一到买五/卖一到卖五价
    - `bid_vol1..bid_vol5` / `ask_vol1..ask_vol5`：对应数量

#### `get_instrument_bars(self, category, market, code, start=0, count=700)`

- 作用：获取扩展行情标的的 K 线
- 参数：
  - `category`：K 线周期（扩展行情的取值由服务端定义；本机无法通过 `get_markets/get_instrument_info` 探测，因此本文不对可用取值做枚举承诺）
  - `market`：市场编号
  - `code`：标的代码
  - `start`：起始偏移
  - `count`：条数
- 返回：`list[OrderedDict]`，字段意义：
  - `open` / `high` / `low` / `close`
  - `position`：持仓量
  - `trade`：成交量/成交笔数（本机未实测，原因见本节开头说明）
  - `price`：价格字段（源码保留；本机未实测，原因见本节开头说明）
  - `amount`：成交额/金额（源码从一段 float 取值；本机未实测，原因见本节开头说明）
  - `year` / `month` / `day` / `hour` / `minute` / `datetime`

#### `get_minute_time_data(self, market, code)`

- 作用：获取当日分时（扩展行情版，含均价、成交额等）
- 参数：
  - `market`：市场编号
  - `code`：代码
- 返回：`list[OrderedDict]`，字段意义：
  - `hour` / `minute`：时间
  - `price`：价格
  - `avg_price`：均价
  - `volume`：成交量
  - `open_interest`：源码字段名为 `open_interest`，但变量名叫 `amount`（本机未实测，原因见本节开头说明）

#### `get_history_minute_time_data(self, market, code, date)`

- 作用：获取历史某日分时（扩展行情版）
- 参数：
  - `market`：市场编号
  - `code`：代码
  - `date`：`YYYYMMDD`
- 返回：与 `get_minute_time_data` 相同结构

#### `get_transaction_data(self, market, code, start=0, count=1800)`

- 作用：获取当日逐笔成交（扩展行情版，含增仓/方向/性质）
- 参数：
  - `market`：市场编号
  - `code`：代码
  - `start` / `count`：分页
- 返回：`list[OrderedDict]`，字段意义：
  - `date`：datetime（以“今天日期 + raw_time + second”拼出）
  - `hour` / `minute` / `second`
  - `price`：价格（注意：这里是整数原始值，是否需要除以 100/1000 取决于市场；本机未实测，原因见本节开头说明）
  - `volume`：成交量
  - `zengcang`：增仓（可为负）
  - `nature`：原始性质字段（为兼容保留）
  - `nature_mark`：`nature // 10000`
  - `nature_value`：`nature % 10000`
  - `nature_name`：解析后的中文性质（如 多开/空平/双开/换手…）
  - `direction`：方向（1=买/多，-1=卖/空，0=中性；港股市场有特殊分支）

#### `get_history_transaction_data(self, market, code, date, start=0, count=1800)`

- 作用：获取历史某日逐笔成交（扩展行情版）
- 参数：同上（含 `date=YYYYMMDD`）
- 返回：字段与 `get_transaction_data` 基本一致；额外注意：
  - 返回中同时保留了历史拼写错误字段：`natrue_name`（兼容用，建议用 `nature_name`）

#### `get_history_instrument_bars_range(self, market, code, start, end)`

- 作用：按日期范围拉取扩展行情 K 线（range 版）
- 参数：
  - `market`：市场编号
  - `code`：代码
  - `start`：开始日期（`YYYYMMDD`）
  - `end`：结束日期（`YYYYMMDD`）
- 返回：`list[OrderedDict]`，字段意义：
  - `datetime` / `year` / `month` / `day` / `hour` / `minute`
  - `open` / `high` / `low` / `close`
  - `position`：持仓
  - `trade`：成交
  - `settlementprice`：结算价

### 批量快照

#### `get_instrument_quote_list(self, market, category, start=0, count=80)`

- 作用：批量获取某市场的快照列表（当前源码仅实现了港股与期货两类）
- 参数：
  - `market`：市场编号
  - `category`：类别（源码仅支持 `2=港股`、`3=期货`，其他会抛 `NotImplementedError`）
  - `start` / `count`：分页
- 返回：`list[OrderedDict]`
  - 当 `category == 2`（港股）字段意义（源码字段名）：
    - `market` / `code`
    - `HuoYueDu`：活跃度（本机未实测，原因见本节开头说明）
    - `ZuoShou`：昨收
    - `JinKai`：今开
    - `ZuiGao` / `ZuiDi`：最高/最低
    - `XianJia`：现价
    - `MaiRuJia`：买入价（本机未实测，原因见本节开头说明）
    - `ZongLiang`：总量
    - `XianLiang`：现量
    - `ZongJinE`：总金额/成交额
    - `Nei` / `Wai`：内/外（本机未实测，原因见本节开头说明；源码注释：`Nei/Wai = 内外比？`）
    - `MaiRuJia1..5` / `MaiRuLiang1..5`：买一到买五价/量
    - `MaiChuJia1..5` / `MaiChuLiang1..5`：卖一到卖五价/量
  - 当 `category == 3`（期货）字段意义（源码字段名）：
    - `market` / `code`
    - `BiShu`：笔数
    - `ZuoJie`：昨结
    - `JinKai`：今开
    - `ZuiGao` / `ZuiDi`：最高/最低
    - `MaiChu`：卖出价（本机未实测，原因见本节开头说明）
    - `KaiCang`：开仓
    - `ZongLiang`：总量
    - `XianLiang`：现量
    - `ZongJinE`：总金额/成交额
    - `NeiPan` / `WaiPan`：内盘/外盘
    - `ChiCangLiang`：持仓量
    - `MaiRuJia` / `MaiRuLiang`：买入价/量
    - `MaiChuJia` / `MaiChuLiang`：卖出价/量

## 3) 连接池与 IP 池（pytdx.pool.*）

> 说明：这部分不是“行情数据接口”，但属于 pytdx 提供的可直接调用的 API/工具类，常用于提升稳定性（自动切换服务器 IP、自动重试）。

### HQ 连接池（TdxHqPool_API）

入口：`pytdx.pool.hqpool.TdxHqPool_API`

#### `__init__(self, hq_cls, ippool)`

- 作用：构造一个 HQ 连接池代理对象
  - 内部会创建 2 个 `hq_cls(multithread=True, heartbeat=True)` 实例：`api`（主连接）与 `hot_failover_api`（热备连接）
  - 并通过反射把 `hq_cls` 中所有以 `get*` 开头的方法（以及 `do_heartbeat`、`to_df`）动态挂载到 `TdxHqPool_API` 自身
- 参数：
  - `hq_cls`：通常传 `pytdx.hq.TdxHq_API`
  - `ippool`：IP 池对象（见下文 `BaseIPPool`/`RandomIPPool`/`AvailableIPPool`）
- 返回：`None`

#### `perform_reflect(self, api_obj)`

- 作用：扫描 `api_obj` 的可调用方法名，把 `get*`（以及 `do_heartbeat`、`to_df`）动态绑定到 `TdxHqPool_API` 上
- 参数：
  - `api_obj`：通常是内部创建的 `TdxHq_API` 实例（`self.api`）
- 返回：无（通过 `setattr` 完成方法挂载）

#### `connect(self, ipandport, hot_failover_ipandport)`

- 作用：连接主/热备两路 HQ 服务器，并启动/初始化 IP 池
- 参数：
  - `ipandport`：主连接 `(ip, port)` 元组
  - `hot_failover_ipandport`：热备连接 `(ip, port)` 元组
- 返回：`self`（支持 `with api.connect(...): ...`）

#### `disconnect(self)` / `close(self)`

- 作用：断开主/热备连接，并释放 IP 池资源（`teardown`）；`close` 是 `disconnect` 的别名
- 返回：无

#### `do_hq_api_call(self, method_name, *args, **kwargs)`

- 作用：统一代理调用底层 `self.api.<method_name>`，失败时自动切换到热备连接并重试
- 参数：
  - `method_name`：字符串形式的方法名（例如 `"get_security_bars"`）
  - `*args/**kwargs`：透传给真实 API 的参数
- 返回：与真实 API 方法完全一致（同 `TdxHq_API` 对应方法的返回）
- 异常：
  - 多次重试仍失败会抛 `TdxHqApiCallMaxRetryTimesReachedException`

#### 动态反射出来的 `get*` 方法

- 说明：`TdxHqPool_API` 会把 `TdxHq_API` 的全部 `get*` 接口（以及 `do_heartbeat`、`to_df`）“原样暴露”出来。
- 参数/返回：与 [HQ 行情（TdxHq_API）](#1-hq-行情tdxhq_api) 中同名方法一致。

### IP 池（ippool.py）

入口：`pytdx.pool.ippool`

#### `BaseIPPool`

- `__init__(self, hq_class)`：记录 HQ API 类（通常是 `TdxHq_API`）
- `setup(self)` / `teardown(self)`：生命周期钩子（基类为空实现）
- `sync_get_top_n(self, num)`：同步选出前 N 个候选 `(ip, port)`（基类为空实现）
- `add_to_pool(self, ip)`：把 `(ip, port)` 加回池（基类为空实现）

#### `RandomIPPool(BaseIPPool)`

- 作用：对给定 `ips` 做随机洗牌，按随机顺序返回候选 IP
- `__init__(self, hq_class, ips)`
  - `ips`：`list[tuple[str,int]]`，例如 `[("101.227.73.20", 7709), ...]`
- `get_ips(self)`：返回洗牌后的 `ips`
- `sync_get_top_n(self, num)`：返回前 `num` 个
- `add_to_pool(self, ip)`：如果不在列表中则追加

#### `AvailableIPPool(BaseIPPool)`

- 作用：周期性测速并排序，优先返回“可连接且心跳耗时更低”的 IP
- `__init__(self, hq_class, ips)`
  - `ips`：同上
  - `wait_interval`：默认 20 分钟更新一次（`20 * 60`）
- `setup(self)`：启动后台线程，循环调用 `get_all_available_ips`
- `teardown(self)`：停止后台线程并回收
- `get_ips(self)`：如果已有排序结果则返回排序后的 IP 列表；否则返回原始 `ips`
- `sync_get_top_n(self, num)`：同步测速一次并返回前 `num` 个
- `add_to_pool(self, ip)`：如果不在列表中则追加
