## 目标（你补充的细节）
- 过滤出“筹码近期表现出萌动迹象”的股票：不是单纯低位收敛，而是“低位收敛 + 最近出现扩散/拐头/加速”。

## 现状评估（基于代码）
- 目前脚本已有趋势字段：chip_90_delta / chip_70_delta（基于 trend_days 的差分）。
- 仅靠 delta 的正负能表达“开始松动”，但不足以表达你要的“萌动形态”（例如需要：先长期收敛、再短期抬升）。

## 改造方案：在 filter_chip_concentration 增加“萌动模式”
### 1) 新增萌动判定指标（基于 proxy 序列）
对每只股票，在 calculate_chip_concentration_operator 内部拿到完整 proxy 序列后，额外计算：
- **base_low（基准收敛）**：近 base_days 内 chip_70/chip_90 的分位数或最小值（如 rolling_min 或 rolling_quantile 20%）。
- **bounce_ratio（抬升幅度）**： (当前 chip - base_low) / base_low。
- **slope（短期斜率）**：最近 momentum_days 对 chip 做线性回归斜率（或简单差分均值）。
- **up_days（连续性）**：最近 momentum_days 中有多少天 chip 上升（避免单日噪声）。

### 2) 萌动过滤规则（可调参）
在满足“仍处于低位（收敛基底）”前提下，增加萌动条件：
- 低位：chip_70 <= max_70 且 chip_90 <= max_90
- 萌动（满足其一或组合）：
  - slope >= min_slope
  - bounce_ratio >= min_bounce_ratio
  - up_days >= min_up_days
并输出新增字段（便于你复盘）：chip_70_slope、chip_90_slope、chip_70_bounce、chip_90_bounce、chip_up_days。

### 3) 参数入口（环境变量）
- CHIP_TREND_MODE 可扩展为："dormant"（只收敛）、"up"（简单拐头）、"sprout"（萌动模式）
- 新增：CHIP_BASE_DAYS、CHIP_MOMENTUM_DAYS、CHIP_MIN_BOUNCE_70/90、CHIP_MIN_SLOPE_70/90、CHIP_MIN_UP_DAYS

## 验证方案（会执行代码并产出新 CSV）
1) **一致性校验**：抽样 5-10 只，验证 CSV 中新增字段与直接计算一致（排除串值/错位）。
2) **形态校验**：对比你提供的 AK 筹码（如 000488、002404）：确认“收敛→近期抬升”的方向一致（不强求数值一致）。
3) **参数敏感性**：给 3 组 preset：潜伏、萌动弱、萌动强，比较筛出数量与特征分布。

## 交付物
- 更新后的脚本输出列：chip_90/chip_70 + delta + slope + bounce + up_days
- 一份抽样对照结果（CSV 行 vs 直接计算）
- 一组推荐的“萌动参数”默认值（你可以继续微调）

确认后我将开始实现上述“萌动模式”并跑完验证。