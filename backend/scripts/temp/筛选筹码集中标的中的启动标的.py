import pandas as pd
import os
import sys
import time
from datetime import datetime, timedelta

# 添加项目根目录到路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
# 导入tushare客户端
from backend.utils.tushare_client import pro

# 检查pro是否初始化成功
if pro is None:
    print("错误：Tushare客户端初始化失败，无法获取资金流向数据！")
    sys.exit(1)

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
CACHE_DIR = os.path.join(PROJECT_ROOT, "backend", "cache", "chip_concentration")
os.makedirs(CACHE_DIR, exist_ok=True)

FORCE_TRADE_DATE = os.environ.get("AI_WATCH_TRADE_DATE") or os.environ.get("WATCH_TRADE_DATE")
VERBOSE = (os.environ.get("AI_WATCH_VERBOSE") or "1").strip() not in ("0", "false", "False", "")
SELF_TEST = (os.environ.get("AI_WATCH_SELF_TEST") or "").strip() in ("1", "true", "True")
CACHE_LIMIT = int((os.environ.get("AI_WATCH_CACHE_LIMIT") or "0").strip() or "0")

EXPECTED_CACHE_COLUMNS = [
    "ts_code",
    "strategy",
    "cost_range_start",
    "cost_range_end",
    "cost_range_change",
    "cost_range_change_rate",
    "cost_range_slope",
    "cost_range_percent",
    "winner_rate_start",
    "winner_rate_end",
    "winner_rate_change",
    "winner_rate_change_rate",
    "winner_rate_slope",
    "weight_avg_start",
    "weight_avg_end",
    "weight_avg_change",
    "weight_avg_change_rate",
    "weight_avg_slope",
    "weight_avg_diff_percent",
    "start_date",
    "end_date",
    "latest_price",
    "stock_name",
]

STRATEGIES = {
    "稳健型": {
        "name": "稳健型",
        "description": "适合长周期横盘蓄势的股票，风险较低",
        "params": {
            "cost_range_slope_threshold": -0.01,
            "cost_range_change_rate_threshold": -5,
            "winner_rate_slope_threshold": 0.1,
            "winner_rate_change_rate_threshold": 10,
            "weight_avg_change_rate_threshold": 15,
            "min_cost_range_percent": 5,
            "max_cost_range_percent": 30,
            "min_winner_rate": 50,
            "max_weight_avg_diff_percent": 10,
        },
    },
    "激进型": {
        "name": "激进型",
        "description": "适合即将爆发的股票，捕捉启动瞬间",
        "params": {
            "cost_range_slope_threshold": -0.05,
            "cost_range_change_rate_threshold": -10,
            "winner_rate_slope_threshold": 0.5,
            "winner_rate_change_rate_threshold": 50,
            "weight_avg_change_rate_threshold": 20,
            "min_cost_range_percent": 3,
            "max_cost_range_percent": 20,
            "min_winner_rate": 60,
            "max_weight_avg_diff_percent": 15,
        },
    },
    "平衡型": {
        "name": "平衡型",
        "description": "平衡风险和收益，适合大多数市场环境",
        "params": {
            "cost_range_slope_threshold": -0.02,
            "cost_range_change_rate_threshold": -7,
            "winner_rate_slope_threshold": 0.2,
            "winner_rate_change_rate_threshold": 20,
            "weight_avg_change_rate_threshold": 15,
            "min_cost_range_percent": 4,
            "max_cost_range_percent": 25,
            "min_winner_rate": 55,
            "max_weight_avg_diff_percent": 12,
        },
    },
    "暴力型": {
        "name": "暴力型",
        "description": "捕捉暴力连板和V型反转的股票，风险较高",
        "params": {
            "cost_range_slope_threshold": -0.1,
            "cost_range_change_rate_threshold": -15,
            "winner_rate_slope_threshold": 1.0,
            "winner_rate_change_rate_threshold": 100,
            "weight_avg_change_rate_threshold": 25,
            "min_cost_range_percent": 2,
            "max_cost_range_percent": 15,
            "min_winner_rate": 70,
            "max_weight_avg_diff_percent": 20,
        },
    },
}


class RateLimiter:
    def __init__(self, max_calls_per_minute: int):
        self._interval = 60.0 / max(1, int(max_calls_per_minute))
        self._last_at = 0.0

    def wait(self):
        now = time.monotonic()
        sleep_s = self._interval - (now - self._last_at)
        if sleep_s > 0:
            time.sleep(sleep_s)
        self._last_at = time.monotonic()


limiter = RateLimiter(max_calls_per_minute=200)

def _get_today_yyyymmdd():
    if FORCE_TRADE_DATE:
        s = str(FORCE_TRADE_DATE).strip()
        if len(s) == 8 and s.isdigit():
            return s
    return datetime.now().strftime("%Y%m%d")

def _log(msg: str):
    if VERBOSE:
        print(msg)


def _ensure_csv_header(file_path: str, columns: list[str]):
    try:
        if os.path.exists(file_path) and os.path.getsize(file_path) > 0:
            try:
                df0 = pd.read_csv(file_path, nrows=1)
                if df0 is not None and not df0.empty:
                    return
            except Exception:
                pass
        pd.DataFrame(columns=list(columns)).to_csv(file_path, index=False, encoding="utf-8")
    except Exception:
        pass


def _append_raw_cyq_perf(cache_file: str, chip_df: pd.DataFrame):
    if chip_df is None or chip_df.empty:
        return
    try:
        if os.path.exists(cache_file) and os.path.getsize(cache_file) > 0:
            chip_df.to_csv(cache_file, mode="a", index=False, header=False, encoding="utf-8")
        else:
            chip_df.to_csv(cache_file, index=False, encoding="utf-8")
    except Exception:
        pass


def _get_open_trading_dates(start_date: str, end_date: str):
    limiter.wait()
    df = pro.trade_cal(exchange="SSE", start_date=str(start_date), end_date=str(end_date))
    if df is None or df.empty:
        return []
    df = df[df["is_open"] == 1]
    if df.empty:
        return []
    dates = [str(x) for x in df["cal_date"].tolist() if x is not None]
    dates = sorted(set(dates))
    return dates


# 计算最近的交易日
def get_latest_trading_date():
    try:
        today = _get_today_yyyymmdd()
        start_date = (datetime.strptime(today, "%Y%m%d") - timedelta(days=60)).strftime("%Y%m%d")
        dates = _get_open_trading_dates(start_date=start_date, end_date=today)
        out = dates[-1] if dates else today
        _log(f"交易日计算: today={today}, latest_trading_date={out}, force={FORCE_TRADE_DATE or ''}")
        return out
    except Exception as e:
        print(f"获取交易日失败: {e}")
        return _get_today_yyyymmdd()


def get_recent_trading_dates(n_days=20, anchor_date=None):
    anchor = str(anchor_date).strip() if anchor_date else _get_today_yyyymmdd()
    start_date = (datetime.strptime(anchor, "%Y%m%d") - timedelta(days=max(120, int(n_days) * 7))).strftime("%Y%m%d")
    dates = _get_open_trading_dates(start_date=start_date, end_date=anchor)
    if not dates:
        return [anchor]
    if len(dates) <= int(n_days):
        return dates
    return dates[-int(n_days) :]


def get_all_stock_codes():
    try:
        limiter.wait()
        df = pro.stock_basic(exchange="", list_status="L", fields="ts_code,name")
        print(f"获取到 {len(df)} 只股票")
        return df
    except Exception as e:
        print(f"获取股票列表失败: {e}")
        return pd.DataFrame()


def get_chip_data(ts_code, start_date, end_date):
    try:
        limiter.wait()
        df = pro.cyq_perf(ts_code=ts_code, start_date=start_date, end_date=end_date)
        return df if df is not None else pd.DataFrame()
    except Exception as e:
        print(f"获取 {ts_code} 筹码数据失败: {e}")
        return pd.DataFrame()


def _slope(y_values):
    ys = []
    for v in y_values:
        if v is None:
            continue
        try:
            fv = float(v)
        except Exception:
            continue
        if fv != fv:
            continue
        ys.append(fv)
    n = len(ys)
    if n < 2:
        return 0.0
    xs = list(range(n))
    x_mean = sum(xs) / n
    y_mean = sum(ys) / n
    num = sum((x - x_mean) * (y - y_mean) for x, y in zip(xs, ys))
    den = sum((x - x_mean) ** 2 for x in xs)
    return num / den if den != 0 else 0.0


def is_chip_concentrated(chip_df, strategy_name="平衡型"):
    strategy = STRATEGIES.get(strategy_name, STRATEGIES["平衡型"])
    params = strategy["params"]

    required_cols = {"trade_date", "ts_code", "cost_95pct", "cost_5pct", "winner_rate", "weight_avg"}
    missing = required_cols.difference(set(chip_df.columns.tolist()))
    if missing:
        return False, {}

    chip_df = chip_df.sort_values("trade_date", ascending=False)
    recent_data = chip_df.head(20).copy()
    if recent_data.empty:
        return False, {}

    recent_data.loc[:, "cost_range"] = recent_data["cost_95pct"] - recent_data["cost_5pct"]

    x_ordered = recent_data.iloc[::-1].reset_index(drop=True)
    slope_cost_range = _slope(x_ordered["cost_range"].tolist())
    slope_winner_rate = _slope(x_ordered["winner_rate"].tolist())
    slope_weight_avg = _slope(x_ordered["weight_avg"].tolist())

    first_cost_range = recent_data["cost_range"].iloc[-1]
    last_cost_range = recent_data["cost_range"].iloc[0]
    cost_range_change = last_cost_range - first_cost_range
    cost_range_change_rate = (cost_range_change / first_cost_range) * 100 if first_cost_range != 0 else 0

    first_winner_rate = recent_data["winner_rate"].iloc[-1]
    last_winner_rate = recent_data["winner_rate"].iloc[0]
    winner_rate_change = last_winner_rate - first_winner_rate
    winner_rate_change_rate = (winner_rate_change / first_winner_rate) * 100 if first_winner_rate != 0 else 0

    first_weight_avg = recent_data["weight_avg"].iloc[-1]
    last_weight_avg = recent_data["weight_avg"].iloc[0]
    weight_avg_change = last_weight_avg - first_weight_avg
    weight_avg_change_rate = (weight_avg_change / first_weight_avg) * 100 if first_weight_avg != 0 else 0

    start_date = recent_data["trade_date"].iloc[-1]
    end_date = recent_data["trade_date"].iloc[0]

    latest_price = recent_data["cost_95pct"].iloc[0]
    cost_range_percent = (last_cost_range / latest_price) * 100 if latest_price != 0 else 0
    weight_avg_diff_percent = ((latest_price - last_weight_avg) / last_weight_avg) * 100 if last_weight_avg != 0 else 0

    is_concentrated = (
        slope_cost_range < params["cost_range_slope_threshold"]
        and cost_range_change_rate < params["cost_range_change_rate_threshold"]
        and slope_winner_rate > params["winner_rate_slope_threshold"]
        and winner_rate_change_rate > params["winner_rate_change_rate_threshold"]
        and abs(weight_avg_change_rate) < params["weight_avg_change_rate_threshold"]
        and params["min_cost_range_percent"] < cost_range_percent < params["max_cost_range_percent"]
        and last_winner_rate > params["min_winner_rate"]
        and abs(weight_avg_diff_percent) < params["max_weight_avg_diff_percent"]
    )

    indicators = {
        "ts_code": recent_data["ts_code"].iloc[0],
        "strategy": strategy["name"],
        "cost_range_start": first_cost_range,
        "cost_range_end": last_cost_range,
        "cost_range_change": cost_range_change,
        "cost_range_change_rate": cost_range_change_rate,
        "cost_range_slope": slope_cost_range,
        "cost_range_percent": cost_range_percent,
        "winner_rate_start": first_winner_rate,
        "winner_rate_end": last_winner_rate,
        "winner_rate_change": winner_rate_change,
        "winner_rate_change_rate": winner_rate_change_rate,
        "winner_rate_slope": slope_winner_rate,
        "weight_avg_start": first_weight_avg,
        "weight_avg_end": last_weight_avg,
        "weight_avg_change": weight_avg_change,
        "weight_avg_change_rate": weight_avg_change_rate,
        "weight_avg_slope": slope_weight_avg,
        "weight_avg_diff_percent": weight_avg_diff_percent,
        "start_date": start_date,
        "end_date": end_date,
        "latest_price": latest_price,
    }

    return is_concentrated, indicators


def ensure_chip_concentration_cache(strategy_name="平衡型", latest_trading_date=None):
    latest_trading_date = latest_trading_date or get_latest_trading_date()
    cache_file = os.path.join(CACHE_DIR, f"筹码忽然集中_{strategy_name}_{latest_trading_date}.csv")
    raw_cache_file = os.path.join(CACHE_DIR, f"cyq_perf_raw_{latest_trading_date}.csv")
    processed_file = cache_file + ".processed"

    if (
        os.path.exists(cache_file)
        and os.path.getsize(cache_file) > 0
        and os.path.exists(raw_cache_file)
        and os.path.getsize(raw_cache_file) > 0
        and not os.path.exists(processed_file)
    ):
        return cache_file, latest_trading_date

    dates = get_recent_trading_dates(20, anchor_date=latest_trading_date)
    start_date = dates[0]
    latest_date = dates[-1]
    if str(start_date) > str(latest_date):
        start_date, latest_date = latest_date, start_date

    stock_list = get_all_stock_codes()
    if stock_list.empty:
        raise SystemExit("错误：无法获取股票列表")
    if CACHE_LIMIT > 0:
        stock_list = stock_list.head(int(CACHE_LIMIT)).reset_index(drop=True)

    processed = set()
    if os.path.exists(processed_file):
        try:
            with open(processed_file, "r", encoding="utf-8") as f:
                processed = set(line.strip() for line in f if line.strip())
        except Exception:
            processed = set()

    total = len(stock_list)
    if os.path.exists(cache_file) and os.path.getsize(cache_file) > 0 and len(processed) >= total:
        return cache_file, latest_trading_date

    buf = []

    _ensure_csv_header(cache_file, EXPECTED_CACHE_COLUMNS)
    print(f"开始生成筹码缓存：{cache_file}")
    print(f"日期范围: 开始 {start_date}, 结束 {latest_date}")
    print(f"每分钟最多 200 次请求，已处理 {len(processed)}/{total}")
    _log(f"缓存目录: {CACHE_DIR}")
    _log(f"processed_file: {processed_file if os.path.exists(processed_file) else ''}")
    _log(f"raw_cache_file: {raw_cache_file}")
    _log(f"最近交易日样本: {dates[:3]} ... {dates[-3:]}")

    for i, row in stock_list.iterrows():
        ts_code = row["ts_code"]
        name = row.get("name", "")
        if ts_code in processed:
            continue

        chip_df = get_chip_data(ts_code, start_date, latest_date)
        if not chip_df.empty:
            _append_raw_cyq_perf(raw_cache_file, chip_df)
            ok, indicators = is_chip_concentrated(chip_df, strategy_name=strategy_name)
            if ok:
                indicators["stock_name"] = name
                pd.DataFrame([indicators]).to_csv(
                    cache_file,
                    mode="a",
                    index=False,
                    header=not os.path.exists(cache_file),
                    encoding="utf-8",
                )

        buf.append(ts_code)
        if len(buf) >= 50:
            try:
                with open(processed_file, "a", encoding="utf-8") as f:
                    for code in buf:
                        f.write(f"{code}\n")
            except Exception:
                pass
            for code in buf:
                processed.add(code)
            buf = []

        if (i + 1) % 200 == 0:
            print(f"进度: {i+1}/{total}")

    if buf:
        try:
            with open(processed_file, "a", encoding="utf-8") as f:
                for code in buf:
                    f.write(f"{code}\n")
        except Exception:
            pass

    _ensure_csv_header(cache_file, EXPECTED_CACHE_COLUMNS)

    return cache_file, latest_trading_date


def _self_test():
    print("开始自检...")
    today = _get_today_yyyymmdd()
    latest = get_latest_trading_date()
    dates = get_recent_trading_dates(20)
    start_date = dates[0]
    end_date = dates[-1]
    if str(start_date) > str(end_date):
        start_date, end_date = end_date, start_date
    print(f"自检: today={today}, latest_trading_date={latest}, date_range={start_date}-{end_date}")
    stock_list = get_all_stock_codes()
    if stock_list.empty:
        raise SystemExit("自检失败：stock_basic 为空")
    stock_list = stock_list.head(3).reset_index(drop=True)
    print(f"自检: stock_basic rows={len(stock_list)}")
    sample_code = stock_list.iloc[0]["ts_code"]
    chip_df = get_chip_data(sample_code, start_date, end_date)
    print(f"自检: cyq_perf sample ts_code={sample_code}, rows={len(chip_df)}")
    if not chip_df.empty:
        ok, indicators = is_chip_concentrated(chip_df, strategy_name="平衡型")
        print(f"自检: is_chip_concentrated={ok}, indicators_keys={len(indicators.keys())}")
    cache_file = os.path.join(CACHE_DIR, f"__self_test_cache_{latest}.csv")
    _ensure_csv_header(cache_file, EXPECTED_CACHE_COLUMNS)
    df0 = pd.read_csv(cache_file)
    miss0 = set(EXPECTED_CACHE_COLUMNS).difference(set(df0.columns.tolist()))
    if miss0:
        raise SystemExit(f"自检失败：缓存CSV表头缺少列: {sorted(miss0)}")
    if not chip_df.empty:
        ok, indicators = is_chip_concentrated(chip_df, strategy_name="平衡型")
        if indicators:
            row = {k: indicators.get(k) for k in EXPECTED_CACHE_COLUMNS}
            row["stock_name"] = stock_list.iloc[0].get("name", "")
            pd.DataFrame([row]).to_csv(cache_file, mode="a", index=False, header=False, encoding="utf-8")
    df1 = pd.read_csv(cache_file)
    miss1 = set(EXPECTED_CACHE_COLUMNS).difference(set(df1.columns.tolist()))
    if miss1:
        raise SystemExit(f"自检失败：缓存CSV读回缺少列: {sorted(miss1)}")
    print(f"自检: cache_csv cols_ok, rows={len(df1)}")
    print("自检完成")

def _fetch_money_flow(ts_codes, trade_date_str):
    batch_size = 200
    result = []
    for i in range(0, len(ts_codes), batch_size):
        batch_codes = ts_codes[i : i + batch_size]
        df = pro.moneyflow_dc(ts_code=",".join(batch_codes), trade_date=trade_date_str)
        result.append(df)
        time.sleep(0.5)
    if result:
        return pd.concat(result, ignore_index=True)
    return pd.DataFrame()


def _is_moneyflow_zero(df: pd.DataFrame):
    if df is None or df.empty:
        return True
    if "net_amount" not in df.columns:
        return False
    series = pd.to_numeric(df["net_amount"], errors="coerce").fillna(0)
    return bool((series == 0).all())


def get_money_flow_data(ts_codes, trade_date):
    try:
        trade_date_str = str(trade_date)
        dates = get_recent_trading_dates(20, anchor_date=trade_date_str)
        fallback_dates = [d for d in dates if str(d) <= trade_date_str]
        if not fallback_dates:
            fallback_dates = dates[:]
        tried = []
        if trade_date_str not in fallback_dates:
            fallback_dates.append(trade_date_str)
        for d in sorted(set(fallback_dates), reverse=True):
            tried.append(str(d))
            df = _fetch_money_flow(ts_codes, str(d))
            if not _is_moneyflow_zero(df):
                if str(d) != trade_date_str:
                    print(f"资金流向回退到最近交易日: {d}")
                return df
        print(f"资金流向可用日期不足，尝试过: {tried[:5]}{'...' if len(tried) > 5 else ''}")
        return pd.DataFrame()
    except Exception as e:
        print(f"获取资金流向数据失败: {e}")
        return pd.DataFrame()

# 获取股票成交量数据
def get_volume_data(ts_codes, trade_date, days=5):
    """获取股票成交量数据，计算成交量放大情况"""
    try:
        # 确保trade_date是字符串类型
        trade_date_str = str(trade_date)
        # 计算开始日期
        end_date = trade_date_str
        start_date = (datetime.strptime(trade_date_str, '%Y%m%d') - timedelta(days=days)).strftime('%Y%m%d')
        
        # 分批获取数据
        batch_size = 200
        result = []
        
        for i in range(0, len(ts_codes), batch_size):
            batch_codes = ts_codes[i:i+batch_size]
            df = pro.daily(ts_code=','.join(batch_codes), start_date=start_date, end_date=end_date)
            result.append(df)
            time.sleep(0.5)  # 避免请求过于频繁
        
        if result:
            return pd.concat(result, ignore_index=True)
        return pd.DataFrame()
    except Exception as e:
        print(f"获取成交量数据失败: {e}")
        return pd.DataFrame()

# 获取北向资金持股数据（已移除）
def get_hsgt_hold_data(ts_codes, trade_date):
    """获取北向资金持股数据（已移除）"""
    return pd.DataFrame()

if SELF_TEST:
    _self_test()
    raise SystemExit(0)

effective_trading_date = get_latest_trading_date()
input_file, latest_trading_date = ensure_chip_concentration_cache(
    strategy_name="平衡型",
    latest_trading_date=effective_trading_date,
)
output_file = os.path.join(CACHE_DIR, f"筹码集中启动标的_{latest_trading_date}.csv")

# 检查输入文件是否存在
if not os.path.exists(input_file):
    print(f"错误：输入文件 {input_file} 不存在！")
    exit(1)

# 读取CSV文件
print(f"正在读取文件：{input_file}")
try:
    df = pd.read_csv(input_file)
except Exception as e:
    raise SystemExit(f"错误：读取输入CSV失败: {e}")

# 打印原始数据行数
print(f"原始数据行数：{len(df)}")
need_cols = {"ts_code", "cost_range_change_rate", "winner_rate_change_rate", "weight_avg_diff_percent"}
missing_cols = need_cols.difference(set(df.columns.tolist()))
if missing_cols:
    raise SystemExit(f"错误：输入CSV缺少必要列: {sorted(missing_cols)}")

# 筛选条件
print("\n筛选条件：")
print("1. 成本范围变化率 < -10% （筹码集中程度更高）")
print("2. 胜率变化率 > 30% （市场情绪向好更明显）")
print("3. 股价与加权平均成本差异 < 5% （主力未大幅获利，拉抬动力足）")

# 应用筛选条件
filtered_df = df[
    (df['cost_range_change_rate'] < -10) &  # 成本范围变化率 < -10%
    (df['winner_rate_change_rate'] > 30) &  # 胜率变化率 > 30%
    (df['weight_avg_diff_percent'] < 5)      # 股价与加权平均成本差异 < 5%
]

# 打印筛选后的数据行数
print(f"\n筛选后数据行数：{len(filtered_df)}")

# 按成本范围变化率降序排序（负值越小，集中程度越高）
filtered_df = filtered_df.sort_values(by='cost_range_change_rate', ascending=True)

# 重置索引
filtered_df = filtered_df.reset_index(drop=True)

print(f"\n使用最近交易日数据：{latest_trading_date}")

# 如果有筛选结果，进行资金流向和成交量核查
if len(filtered_df) > 0:
    # 提取股票代码列表
    stock_codes = filtered_df['ts_code'].tolist()
    print(f"\n正在获取 {len(stock_codes)} 只股票的资金流向和成交量数据...")
    
    # 获取资金流向数据
    money_flow_df = get_money_flow_data(stock_codes, latest_trading_date)
    
    # 获取成交量数据
    volume_df = get_volume_data(stock_codes, latest_trading_date)
    
    # 获取北向资金数据
    hsgt_hold_df = get_hsgt_hold_data(stock_codes, latest_trading_date)
    
    # 合并数据
    if not money_flow_df.empty:
        filtered_df = filtered_df.merge(money_flow_df[['ts_code', 'net_amount', 'net_amount_rate']], on='ts_code', how='left')
    else:
        filtered_df['net_amount'] = 0
        filtered_df['net_amount_rate'] = 0
    
    # 计算成交量放大情况
    if not volume_df.empty:
        # 计算每只股票的平均成交量和当日成交量
        volume_stats = volume_df.groupby('ts_code').agg(
            avg_volume=('vol', 'mean'),
            latest_volume=('vol', 'last')
        ).reset_index()
        # 计算成交量放大倍数
        volume_stats['volume_ratio'] = volume_stats['latest_volume'] / volume_stats['avg_volume']
        # 合并到主数据框
        filtered_df = filtered_df.merge(volume_stats[['ts_code', 'volume_ratio', 'latest_volume']], on='ts_code', how='left')
    else:
        filtered_df['volume_ratio'] = 1.0
        filtered_df['latest_volume'] = 0
    
    # 合并北向资金数据
    if not hsgt_hold_df.empty:
        filtered_df = filtered_df.merge(hsgt_hold_df[['ts_code', 'hold_amount', 'hold_ratio']], on='ts_code', how='left')
    else:
        filtered_df['hold_amount'] = 0
        filtered_df['hold_ratio'] = 0
    
    # 应用资金流向和成交量筛选条件
    print("\n应用资金流向和成交量筛选条件：")
    print("1. 主力净流入 > 0 （资金流入）")
    print("2. 成交量放大倍数 > 1.2 （有量）")
    
    final_df = filtered_df[
        (filtered_df['net_amount'] > 0) &  # 主力净流入
        (filtered_df['volume_ratio'] > 1.2)  # 成交量放大
    ]
    
    # 按主力净流入和成交量放大倍数排序
    final_df = final_df.sort_values(by=['net_amount', 'volume_ratio'], ascending=[False, False])
    final_df = final_df.reset_index(drop=True)
    
    # 保存最终结果
    print(f"\n正在保存最终筛选结果到：{output_file}")
    final_df.to_csv(output_file, index=False, encoding='utf-8')
    
    # 打印前10行结果
    print("\n最终筛选结果前10行：")
    print(final_df.head(10))
    
    # 打印总结
    print(f"\n筛选完成！共筛选出 {len(final_df)} 只符合条件的股票。")
    print(f"结果已保存到：{output_file}")
    
    # 打印资金流向和成交量统计
    if not final_df.empty:
        print("\n资金流向和成交量统计：")
        print(f"平均主力净流入：{final_df['net_amount'].mean():.2f} 万元")
        print(f"平均成交量放大倍数：{final_df['volume_ratio'].mean():.2f} 倍")
else:
    # 保存空结果
    print(f"\n正在保存筛选结果到：{output_file}")
    filtered_df.to_csv(output_file, index=False, encoding='utf-8')
    
    # 打印总结
    print(f"\n筛选完成！共筛选出 {len(filtered_df)} 只符合条件的股票。")
    print(f"结果已保存到：{output_file}")
