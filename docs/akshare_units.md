# AKShare 常用接口单位对照（手 / 股）

## 背景

AKShare 很多接口返回的列名是 `成交量`、`成交额`，但**不一定在列名里标明单位**。由于上游数据源差异，`成交量` 可能是“手”（1 手 = 100 股）也可能是“股”。在计算 VWAP、换手率、量价比等指标时，如果误判单位，结果可能出现 **100 倍数量级错误**，直接导致策略“全误杀/全误选”。

本项目默认采用**数量级自适应校验（Ratio Check）**，禁止根据“记忆/经验”假设单位固定。

## 单位对照表（常用）

| 接口 | 常见字段 | 文档/列名是否标单位 | 实务风险 | 建议 |
|---|---|---:|---:|---|
| `ak.stock_zh_a_spot_em()` | `成交量` `成交额` | 否（列名通常不含“手/股”） | 高 | 计算前必须做 Ratio Check |
| `ak.stock_zh_a_hist()` | `成交量` `成交额` | 否（列名通常不含“手/股”） | 高 | 计算前必须做 Ratio Check |
| `ak.stock_zh_a_hist_min_em()` | `成交量` `成交额` `均价` | 否（列名通常不含“手/股”） | 高 | 计算前必须做 Ratio Check；优先使用接口自带 `均价`（如可用） |
| `ak.stock_zh_a_tick_tx_js()` / `ak.stock_zh_a_tick_tx()` | `成交量` `成交金额` | 否（列名通常不含“手/股”） | 高 | 计算前必须做 Ratio Check |
| `ak.stock_zh_a_tick_163()` | `成交量` `成交额` | 否（列名通常不含“手/股”） | 高 | 计算前必须做 Ratio Check |
| 港股/美股相关接口 | `成交量` | 多数场景为“股” | 中 | 若参与关键计算，仍建议做 sanity check（至少一次比例校验） |

文档入口参考：
- 股票数据文档：https://akshare.akfamily.xyz/data/stock/stock.html

## 推荐实现：VWAP 自适应（Ratio Check）

```python
def safe_vwap(amount: float, volume: float, current_price: float) -> float:
    if not volume:
        return current_price

    raw_vwap = amount / volume
    if current_price > 0:
        ratio = raw_vwap / current_price
        if 80 < ratio < 120:
            return raw_vwap / 100.0
        if 0.8 < ratio < 1.2:
            return raw_vwap

    return raw_vwap / 100.0 if raw_vwap > current_price * 50 else raw_vwap
```

## 最佳实践（项目约定）

- 只要出现 `成交量`、`成交额` 参与计算：VWAP、换手率、量价、资金强度等，必须走 Ratio Check。
- 不依赖“接口名/字段名”推断单位；同一接口在不同数据源/时期也可能变化。
- 当接口同时提供 `均价` 等派生字段时，优先使用并保留交叉校验（例如 `abs(均价 - safe_vwap) / 均价` 不应长期异常）。

