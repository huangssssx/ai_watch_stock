## ETF实时日线

接口：rt_etf_k


描述：获取ETF实时日k线行情，支持按ETF代码或代码通配符一次性提取全部ETF实时日k线行情


积分：本接口是单独开权限的数据，单独申请权限请参考权限列表










输入参数

| 名称 | 类型 | 必选 | 描述 |
| --- | --- | --- | --- |
| ts_code | str | Y | 支持通配符方式，e.g. 5*.SH、15*.SZ、159101.SZ |
| topic | str | Y | 分类参数，取上海ETF时，需要输入'HQ_FND_TICK'，参考下面例子 |






注：ts_code代码一定要带.SH/.SZ/.BJ后缀










输出参数

| 名称 | 类型 | 默认显示 | 描述 |
| --- | --- | --- | --- |
| ts_code | str | Y | ETF代码 |
| name | None | Y | ETF名称 |
| pre_close | float | Y | 昨收价 |
| high | float | Y | 最高价 |
| open | float | Y | 开盘价 |
| low | float | Y | 最低价 |
| close | float | Y | 收盘价（最新价） |
| vol | int | Y | 成交量（股） |
| amount | int | Y | 成交金额（元） |
| num | int | Y | 开盘以来成交笔数 |
| ask_volume1 | int | N | 委托卖盘（股） |
| bid_volume1 | int | N | 委托买盘（股） |
| trade_time | str | N | 交易时间 |










接口示例

```python
#获取今日所有深市ETF实时日线和成交笔数
df = pro.rt_etf_k(ts_code='1*.SZ')

#获取今日沪市所有ETF实时日线和成交笔数
df = pro.rt_etf_k(ts_code='5*.SH', topic='HQ_FND_TICK')
```










数据示例

```python
ts_code      name      pre_close     high     open     low    close        vol     amount    num
0    520860.SH      港股通科      1.024    1.054    1.048   1.041    1.048   15071600   15780985    307
```