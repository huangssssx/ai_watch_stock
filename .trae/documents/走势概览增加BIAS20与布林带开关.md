## 目标
- 在“看盘 / 走势概览”Tab 增加 2 个开关：BIAS20、布林带（BOLL）。
- 任一开关开启时，**对该 Tab 内所有股票卡片图同步显示对应指标曲线**，用于判断高估/低估、是否触及布林轨道等状态。
- 严格遵循约束：**优先前端实现，不改后端/DB/AI投喂逻辑**（只复用现有接口）。

## 口径设计（确保是“当天实时”的 BIAS/BOLL，而不是“当日20分钟”）
- 需要两份数据：
  1) 当日分时序列（用于走势 x 轴与实时 close）：`GET /stocks/{symbol}/daily`（实际是分钟分时）
  2) 历史日线 close（用于构造近 20 交易日窗口）：`GET /stocks/{symbol}/history?period=daily`
- 计算逻辑（对每个分时点都产出指标，从而得到“当天指标曲线”）：
  - 先从分时数据得到“今日日期”，并**统一做日期归一化**（只保留 YYYYMMDD）：
    - `normalizeDate(x) = String(x).replace(/\D/g,'').slice(0,8)`
  - 从日线 history 中筛选 `date < today` 的记录，取最近 19 个 close 作为 `prev19`（确保不把今日重复算入）。
  - 对每个分时点 close_t：
    - window20 = prev19 + [close_t]
    - MA20_t = mean(window20)
    - STD20_t = std(window20, ddof=1)
    - BOLL_mid/upper/lower 与 BIAS20 按标准公式计算
- 这样计算出来的 BOLL/BIAS 会随分时 close 更新，是真正的“当天实时状态”。

## 交互与视觉设计（贴合实际看盘）
- 全局开关（在 StockCharts 顶部控制条）：
  - Switch：显示 BIAS20
  - Switch：显示 布林带
  - 开关状态用 localStorage 记忆（刷新保持）。
- BOLL 绘制（与价格同轴叠加，提升辨识度）：
  - 中轨（mid）：深灰虚线
  - 上/下轨：浅灰细实线，opacity≈0.6
- BIAS20 绘制（避免量纲冲突）：
  - 卡片内上下分区：上=价格(+BOLL)，下=BIAS20 小图
  - 小图加入参考线：0、+5/+8、-5/-8（红/绿虚线，0 为黑色实线）
- Tooltip（看盘友好格式）：
  - 价格与布林三线：2 位小数，带“元”
  - BIAS20：1 位小数，带“%”
  - 指标名称用中文：如“布林上轨/中轨/下轨”“BIAS20”。

## 降级策略（宁缺毋滥，避免误导）
- 若 `prev19` 不足 19（新股/停牌/历史缺失）或日线接口失败：
  - **不绘制 BIAS/BOLL（不退化到“分时20bar口径”）**
  - 仍展示价格走势
  - 在卡片右上角加一个轻量提示图标（配 Tooltip 文案：“历史日线不足，暂不显示 BIAS20/BOLL”）。

## 稳定性与性能（多股票并发场景）
- 请求并发控制：
  - 对 `/history?period=daily` 增加并发上限（例如同时最多 3 个），用前端 Promise 队列实现，避免股票数多时请求风暴。
  - 失败后轻量重试 1 次；仍失败则按降级策略处理。
- 日线短期缓存：
  - 对 `/history?period=daily` 做 sessionStorage 缓存（TTL 1 小时），key=symbol。
  - 页面刷新/来回切 tab 时复用缓存，减少重复请求与等待。

## 代码落点（文件级）
- 修改：`frontend/src/components/StockCharts.tsx`
  - 增加控制条开关
  - 拉取分时 + 日线（含并发控制、缓存）
  - 将指标字段注入到分时点数据中（用于 Recharts 直接绘制）
  - 增加 BOLL 线、BIAS 小图、参考线、Tooltip 格式化
- 新增：`frontend/src/utils/indicators.ts`
  - 纯 TS 指标计算与日期归一化工具（可复用到 StockDetail）
- 修改（可选但建议）：`frontend/src/types.ts`
  - 为 `StockPricePoint` 增加可选字段声明：`boll_upper/boll_mid/boll_lower/bias20`（保持类型清晰，build 一次过）。

## 验证与验收（实现后执行）
- 构建验证：前端 `npm run build` 通过。
- 场景验收（至少 3 类）：
  1) 正常股票：prev19 充足，打开开关后所有卡片显示 BOLL/BIAS 且走势合理
  2) 新股/停牌：prev19 不足，开关开启但不画指标，提示图标出现
  3) 网络/接口失败：日线失败时不影响价格图渲染，提示图标出现
- 指标正确性抽检：任选一只股票、任选一个分时点，手工用 prev19+close_t 计算 MA20/BIAS/BOLL，与图表 tooltip 数值对照一致。

## 可选后续迭代（不影响本次交付）
- 将同一套开关/指标能力复用到 StockDetail（分时/日线/周线/月线）。
- 将 BOLL 的 N/k、BIAS 阈值做成可配置项（默认 N=20,k=2,阈值±5/±8）。