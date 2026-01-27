## Libor拆借利率

接口：libor

描述：Libor拆借利率

限量：单次最大4000行数据，总量不限制，可通过设置开始和结束日期分段获取

积分：用户积累120积分可以调取，具体请参阅积分获取办法 

Libor（London Interbank Offered Rate ），即伦敦同业拆借利率，是指伦敦的第一流银行之间短期资金借贷的利率，是国际金融市场中大多数浮动利率的基础利率。作为银行从市场上筹集资金进行转贷的融资成本，贷款协议中议定的LIBOR通常是由几家指定的参考银行，在规定的时间（一般是伦敦时间上午11：00）报价的平均利率。

输入参数

| 名称 | 类型 | 必选 | 描述 |
| --- | --- | --- | --- |
| date | str | N | 日期 (日期输入格式：YYYYMMDD，下同) |
| start_date | str | N | 开始日期 |
| end_date | str | N | 结束日期 |
| curr_type | str | N | 货币代码  (USD美元  EUR欧元  JPY日元  GBP英镑  CHF瑞郎，默认是USD) |

输出参数

| 名称 | 类型 | 默认显示 | 描述 |
| --- | --- | --- | --- |
| date | str | Y | 日期 |
| curr_type | str | Y | 货币 |
| on | float | Y | 隔夜 |
| 1w | float | Y | 1周 |
| 1m | float | Y | 1个月 |
| 2m | float | Y | 2个月 |
| 3m | float | Y | 3个月 |
| 6m | float | Y | 6个月 |
| 12m | float | Y | 12个月 |

接口调用

```python
pro = ts.pro_api()

df = pro.libor(curr_type='USD', start_date='20180101', end_date='20181130')
```

数据样例

```python
date     curr_type       on       1w       1m       2m       3m       6m  \
0    20181130       USD  2.17750  2.22131  2.34694  2.51006  2.73613  2.89463   
```