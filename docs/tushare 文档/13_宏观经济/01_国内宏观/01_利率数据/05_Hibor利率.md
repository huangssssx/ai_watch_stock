## Hibor利率

接口：hibor

描述：Hibor利率

限量：单次最大4000行数据，总量不限制，可通过设置开始和结束日期分段获取

积分：用户积累120积分可以调取，具体请参阅积分获取办法 

HIBOR (Hongkong InterBank Offered Rate)，是香港银行同行业拆借利率。指香港货币市场上，银行与银行之间的一年期以下的短期资金借贷利率，从伦敦同业拆借利率（LIBOR）变化出来的。

输入参数

| 名称 | 类型 | 必选 | 描述 |
| --- | --- | --- | --- |
| date | str | N | 日期  (日期输入格式：YYYYMMDD，下同) |
| start_date | str | N | 开始日期 |
| end_date | str | N | 结束日期 |

输出参数

| 名称 | 类型 | 默认显示 | 描述 |
| --- | --- | --- | --- |
| date | str | Y | 日期 |
| on | float | Y | 隔夜 |
| 1w | float | Y | 1周 |
| 2w | float | Y | 2周 |
| 1m | float | Y | 1个月 |
| 2m | float | Y | 2个月 |
| 3m | float | Y | 3个月 |
| 6m | float | Y | 6个月 |
| 12m | float | Y | 12个月 |

接口调用

```python
pro = ts.pro_api()

df = pro.hibor(start_date='20180101', end_date='20181130')
```

数据样例

```python
date       on       1w       2w       1m       2m       3m       6m  \
0    20181130  1.52500  1.10125  1.08000  1.20286  1.83030  2.03786  2.32821   
```