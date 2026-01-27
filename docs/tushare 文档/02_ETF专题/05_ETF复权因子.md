## 基金复权因子

接口：fund_adj

描述：获取基金复权因子，用于计算基金复权行情

限量：单次最大提取2000行记录，可循环提取，数据总量不限制

积分：用户积600积分可调取，超过5000积分以上频次相对较高。具体请参阅积分获取办法 

复权行情实现参考：

后复权 = 当日最新价 × 当日复权因子

前复权 = 当日最新价 ÷ 最新复权因子

输入参数

| 名称 | 类型 | 必选 | 描述 |
| --- | --- | --- | --- |
| ts_code | str | N | TS基金代码（支持多只基金输入） |
| trade_date | str | N | 交易日期（格式：yyyymmdd，下同） |
| start_date | str | N | 开始日期 |
| end_date | str | N | 结束日期 |
| offset | str | N | 开始行数 |
| limit | str | N | 最大行数 |

输出参数

| 名称 | 类型 | 默认显示 | 描述 |
| --- | --- | --- | --- |
| ts_code | str | Y | ts基金代码 |
| trade_date | str | Y | 交易日期 |
| adj_factor | float | Y | 复权因子 |

接口使用

```python
pro = ts.pro_api()

df = pro.fund_adj(ts_code='513100.SH', start_date='20190101', end_date='20190926')
```

数据示例

```python
ts_code    trade_date  adj_factor
0    513100.SH   20190926         1.0
```