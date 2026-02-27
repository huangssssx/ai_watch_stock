import os
import sys
import time
from typing import Optional

import pandas as pd

# 黑名单：剔除不参与计算/回测的标的（例如新股/异常标的等）
blacklist = ["603284", "688712", "688816", "688818"]
df_quote_snapshots = pd.DataFrame()
# 让脚本可以从 backend/scripts 直接运行并 import backend/utils 下的工具
backend_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if backend_dir not in sys.path:
    sys.path.insert(0, backend_dir)

from utils.stock_codes import get_all_a_share_codes
from utils.pytdx_client import tdx
from utils.chips import get_chip_concentration_proxy, ChipProxyParams

def _infer_liutongguben_factor(price: pd.Series, liutongguben: pd.Series) -> pd.Series:
    p = pd.to_numeric(price, errors="coerce")
    s = pd.to_numeric(liutongguben, errors="coerce")
    cap_raw = p * s
    cap_scaled = cap_raw * 10000
    need_scale = (p > 0) & (s > 0) & (cap_raw < 1e8) & (cap_scaled >= 1e8)
    return pd.Series(need_scale).map(lambda x: 10000 if bool(x) else 1).astype(int)

def get_security_quotes(df_stock_codes:pd.DataFrame):
    """
     获取指定股票的实时行情
    """
    stock_codes = list(zip(df_stock_codes['market'], df_stock_codes['code'])  )
    # 按照指定数量分组，生成二维数组
    bitch_size=80
    stock_codes_batches = [stock_codes[i:i+bitch_size] for i in range(0, len(stock_codes),bitch_size)]

    # 收集所有的实时快照
    all_quote_snapshots = []
    for batch in stock_codes_batches:
        quote_snapshots = tdx.get_security_quotes(batch)
        if not quote_snapshots:
            continue
        all_quote_snapshots.extend(quote_snapshots)
    df_quote_snapshots = pd.DataFrame(all_quote_snapshots)
    return df_quote_snapshots    

def get_finance_info(df_stock_codes:pd.DataFrame):
    """
     获取指定股票的财务摘要（股本、资产负债、利润等一组字段）
    """
    date = pd.Timestamp.today()
    cache_file = f"all_finance_info_cache_{date.strftime('%Y%m%d')}.csv"

    if  os.path.exists(cache_file):
        return  pd.read_csv(cache_file)

    stock_codes = list(zip(df_stock_codes['market'], df_stock_codes['code'])  )
    # 收集所有的财务摘要
    all_finance_info = []
    for stock in stock_codes:
        res = tdx.get_finance_info(stock[0], stock[1])
        if res:
            all_finance_info.append(res)
        # 等待 1 秒，避免对服务器压力过大
        # time.sleep(1)
    df_finance_info = pd.DataFrame(all_finance_info)
    df_finance_info.to_csv(cache_file, index=False)
    return df_finance_info    

# 流通盘计算
def calcalte_circulating_stock(df_quote_snapshots: pd.DataFrame,df_finance_info: pd.DataFrame) -> pd.DataFrame:
    """
    计算流通盘（这里按“流通市值/流通盘(元)”口径：流通盘 = price * liutongguben）
    """
    df_left = df_quote_snapshots.copy()
    df_right = df_finance_info.copy()

    df_left["market"] = df_left["market"].astype(int)
    df_left["code"] = df_left["code"].astype(str).str.zfill(6)
    df_right["market"] = df_right["market"].astype(int)
    df_right["code"] = df_right["code"].astype(str).str.zfill(6)

    df = pd.merge(df_left, df_right, on=["market", "code"], how="inner")
    price_rt = pd.to_numeric(df.get("price", 0), errors="coerce")
    price_ref = pd.to_numeric(df.get("last_close", 0), errors="coerce")
    df["price"] = price_rt.where(price_rt > 0, price_ref)
    df["liutongguben_raw"] = pd.to_numeric(df.get("liutongguben", 0), errors="coerce")
    df["liutongguben_factor"] = _infer_liutongguben_factor(df["price"], df["liutongguben_raw"])
    df["liutongguben"] = df["liutongguben_raw"] * df["liutongguben_factor"]

    df["流通盘"] = df["price"] * df["liutongguben"]
    df["circulating"] = df["流通盘"]
    return df

# 流通盘过滤
def filter_circulating_stock(df: pd.DataFrame) -> pd.DataFrame:
    """
    过滤出流通盘股票（0 < 流通盘 < 50亿）
    """
    s = pd.to_numeric(df.get("circulating", pd.NA), errors="coerce")
    return df[(s > 0) & (s < 5_000_000_000)]

def calculate_daily_turnover_operator(row:pd.Series,N:int=20):
    """
    计算“多数时间缩量”口径的日换手率占比：
    - 近 N 天中，日换手率 <= 5% 的天数占比
    """
    market = row["market"]
    code_num = int(row["code"])
    code = str(code_num).zfill(6)
    bars = tdx.get_security_bars(9,market, code, 0, N)
    if bars is None or len(bars) < N:
        return None
    df = pd.DataFrame(bars)
    if "datetime" in df.columns:
        df = df.sort_values("datetime")
    df["vol"] = pd.to_numeric(df.get("vol", 0), errors="coerce")
    df["amount"] = pd.to_numeric(df.get("amount", 0), errors="coerce")
    df["close"] = pd.to_numeric(df.get("close", 0), errors="coerce")
    liutongguben = pd.to_numeric(row.get("liutongguben", 0), errors="coerce")
    if pd.isna(liutongguben) or float(liutongguben) <= 0:
        return None

    safe = (df["vol"] > 0) & (df["close"] > 0)
    raw_vwap = (df.loc[safe, "amount"] / df.loc[safe, "vol"]).astype(float)
    vwap_ratio = raw_vwap / df.loc[safe, "close"].astype(float)
    ratio_median = pd.to_numeric(vwap_ratio, errors="coerce").replace([float("inf"), float("-inf")], pd.NA).dropna().median()

    vol_shares = df["vol"]
    if pd.notna(ratio_median) and 80 <= float(ratio_median) <= 120:
        vol_shares = df["vol"] * 100

    df["turnover"] = vol_shares / liutongguben
    s = pd.to_numeric(df["turnover"], errors="coerce").dropna()
    if s.empty:
        return None
    return float((s <= 0.05).mean())

def filter_daily_turnover(df: pd.DataFrame, N: int = 20) -> pd.DataFrame:
    """ 
    - 🔒 日常换手率＜5%
    - 日成交量： get_security_bars （日线）→ vol （取近 N 天）
    - 流通股本： get_finance_info → liutongguben
    - 计算： turnover = vol / liutongguben （同样需要对 vol 单位做量级校验：股/手）
    
    “多数时间缩量”口径（更贴近你描述） ：
    - 计算 turnover_daily 后，统计 <=5% 的天数占比
    - 判断： count(turnover_daily<=0.05)/N >= 0.7 （比如 70%）
    """
    if df is None or df.empty:
        return df
    t0 = time.time()
    ratios = []
    total = int(len(df))
    for idx, row in df.iterrows():
        ratios.append(calculate_daily_turnover_operator(row, N))
        done = len(ratios)
        if done % 200 == 0 or done == total:
            dt = time.time() - t0
            print(f"[换手率] progress {done}/{total} elapsed={dt:.1f}s")
    df["turnover_low_ratio"] = ratios
    df["turnover_low_ratio"] = pd.to_numeric(df["turnover_low_ratio"], errors="coerce")
    df["turnover_low_ok"] = df["turnover_low_ratio"].fillna(0) >= 0.7
    return df[df["turnover_low_ok"]]

def calculate_chip_concentration_operator(
    row: pd.Series,
    lookback_days: int = 220,
    window: int = 60,
    smooth: int = 5,
    trend_days: int = 10,
    base_days: int = 30,
    momentum_days: int = 7,
) -> tuple[
    Optional[float],
    Optional[float],
    Optional[float],
    Optional[float],
    Optional[float],
    Optional[float],
    Optional[float],
    Optional[float],
    Optional[float],
    Optional[float],
]:
    code_num = int(row["code"])
    code = str(code_num).zfill(6)
    end_dt = pd.Timestamp.today().normalize()
    start_dt = end_dt - pd.Timedelta(days=int(lookback_days))

    df = get_chip_concentration_proxy(
        code=code,
        start_date=start_dt.strftime("%Y-%m-%d"),
        end_date=end_dt.strftime("%Y-%m-%d"),
        params=ChipProxyParams(window=window, smooth=smooth),
        as_df=True,
    )
    if df is None or df.empty:
        return None, None, None, None, None, None, None, None, None, None
    df = df.dropna(subset=["proxy_90_concentration_smooth", "proxy_70_concentration_smooth"])
    if df.empty:
        return None, None, None, None, None, None, None, None, None, None
    last = df.iloc[-1]
    v90 = pd.to_numeric(last.get("proxy_90_concentration_smooth", None), errors="coerce")
    v70 = pd.to_numeric(last.get("proxy_70_concentration_smooth", None), errors="coerce")
    v90_out = float(v90) if pd.notna(v90) else None
    v70_out = float(v70) if pd.notna(v70) else None

    d90_out: Optional[float] = None
    d70_out: Optional[float] = None
    td = int(trend_days)
    if td > 0 and len(df) >= td + 1:
        prev = df.iloc[-(td + 1)]
        p90 = pd.to_numeric(prev.get("proxy_90_concentration_smooth", None), errors="coerce")
        p70 = pd.to_numeric(prev.get("proxy_70_concentration_smooth", None), errors="coerce")
        if pd.notna(v90) and pd.notna(p90):
            d90_out = float(v90 - p90)
        if pd.notna(v70) and pd.notna(p70):
            d70_out = float(v70 - p70)

    def _sprout_metrics(s: pd.Series, base_days_n: int, momentum_days_n: int) -> tuple[Optional[float], Optional[float], Optional[float]]:
        x = pd.to_numeric(s, errors="coerce").replace([float("inf"), float("-inf")], pd.NA).dropna()
        if x.empty:
            return None, None, None
        m = int(momentum_days_n)
        b = int(base_days_n)
        if m < 2:
            m = 2
        if b < 5:
            b = 5
        tail = x.tail(b + m)
        if len(tail) < m:
            return None, None, None
        cur = float(tail.iloc[-1])
        recent = tail.tail(m)
        slope = float(recent.iloc[-1] - recent.iloc[0]) / float(max(1, len(recent) - 1))
        diffs = recent.diff().dropna()
        up_days = float((diffs > 0).sum())
        base = tail.iloc[: max(1, len(tail) - m)]
        base_low = pd.to_numeric(base, errors="coerce").dropna().min()
        bounce = None
        if pd.notna(base_low) and float(base_low) > 0:
            bounce = (cur - float(base_low)) / float(base_low)
        return slope, bounce, up_days

    slope90, bounce90, up90 = _sprout_metrics(df["proxy_90_concentration_smooth"], base_days, momentum_days)
    slope70, bounce70, up70 = _sprout_metrics(df["proxy_70_concentration_smooth"], base_days, momentum_days)

    return v90_out, v70_out, d90_out, d70_out, slope90, slope70, bounce90, bounce70, up90, up70


def filter_chip_concentration(
    df: pd.DataFrame,
    max_70: float = 0.06,
    max_90: float = 0.13,
    lookback_days: int = 220,
    window: int = 60,
    smooth: int = 5,
    trend_mode: Optional[str] = None,
    trend_days: int = 10,
    min_delta_70: float = 0.0,
    min_delta_90: float = 0.0,
    base_days: int = 30,
    momentum_days: int = 7,
    min_bounce_70: float = 0.0,
    min_bounce_90: float = 0.0,
    min_slope_70: float = 0.0,
    min_slope_90: float = 0.0,
    min_up_days: int = 0,
) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    t0 = time.time()
    vals_90 = []
    vals_70 = []
    deltas_90 = []
    deltas_70 = []
    slopes_90 = []
    slopes_70 = []
    bounces_90 = []
    bounces_70 = []
    up_days_90 = []
    up_days_70 = []
    total = int(len(df))
    for idx, row in df.iterrows():
        v90, v70, d90, d70, s90, s70, b90, b70, u90, u70 = calculate_chip_concentration_operator(
            row,
            lookback_days=lookback_days,
            window=window,
            smooth=smooth,
            trend_days=trend_days,
            base_days=base_days,
            momentum_days=momentum_days,
        )
        vals_90.append(v90)
        vals_70.append(v70)
        deltas_90.append(d90)
        deltas_70.append(d70)
        slopes_90.append(s90)
        slopes_70.append(s70)
        bounces_90.append(b90)
        bounces_70.append(b70)
        up_days_90.append(u90)
        up_days_70.append(u70)
        done = len(vals_70)
        if done % 100 == 0 or done == total:
            dt = time.time() - t0
            print(f"[筹码集中度] progress {done}/{total} elapsed={dt:.1f}s")
    df = df.copy()
    df["chip_90"] = pd.to_numeric(pd.Series(vals_90, index=df.index), errors="coerce")
    df["chip_70"] = pd.to_numeric(pd.Series(vals_70, index=df.index), errors="coerce")
    df["chip_90_delta"] = pd.to_numeric(pd.Series(deltas_90, index=df.index), errors="coerce")
    df["chip_70_delta"] = pd.to_numeric(pd.Series(deltas_70, index=df.index), errors="coerce")
    df["chip_90_slope"] = pd.to_numeric(pd.Series(slopes_90, index=df.index), errors="coerce")
    df["chip_70_slope"] = pd.to_numeric(pd.Series(slopes_70, index=df.index), errors="coerce")
    df["chip_90_bounce"] = pd.to_numeric(pd.Series(bounces_90, index=df.index), errors="coerce")
    df["chip_70_bounce"] = pd.to_numeric(pd.Series(bounces_70, index=df.index), errors="coerce")
    df["chip_90_up_days"] = pd.to_numeric(pd.Series(up_days_90, index=df.index), errors="coerce")
    df["chip_70_up_days"] = pd.to_numeric(pd.Series(up_days_70, index=df.index), errors="coerce")

    ok = (df["chip_70"] > 0) & (df["chip_70"] <= float(max_70)) & (df["chip_90"] > 0) & (df["chip_90"] <= float(max_90))

    mode = (trend_mode or "").strip().lower()
    if mode in ("up", "down"):
        d70 = df["chip_70_delta"]
        d90 = df["chip_90_delta"]
        m70 = float(min_delta_70)
        m90 = float(min_delta_90)

        if mode == "up":
            if m70 > 0 and m90 > 0:
                ok = ok & (d70 >= m70) & (d90 >= m90)
            elif m70 > 0:
                ok = ok & (d70 >= m70)
            elif m90 > 0:
                ok = ok & (d90 >= m90)
            else:
                ok = ok & ((d70 > 0) | (d90 > 0))
        else:
            if m70 > 0 and m90 > 0:
                ok = ok & (d70 <= -m70) & (d90 <= -m90)
            elif m70 > 0:
                ok = ok & (d70 <= -m70)
            elif m90 > 0:
                ok = ok & (d90 <= -m90)
            else:
                ok = ok & ((d70 < 0) | (d90 < 0))
    elif mode == "sprout":
        s70 = df["chip_70_slope"]
        s90 = df["chip_90_slope"]
        b70 = df["chip_70_bounce"]
        b90 = df["chip_90_bounce"]
        u70 = df["chip_70_up_days"]

        conds = []
        if float(min_bounce_70) > 0:
            conds.append(b70 >= float(min_bounce_70))
        if float(min_slope_70) > 0:
            conds.append(s70 >= float(min_slope_70))
        if float(min_bounce_90) > 0:
            conds.append(b90 >= float(min_bounce_90))
        if float(min_slope_90) > 0:
            conds.append(s90 >= float(min_slope_90))

        if conds:
            sprout_ok = conds[0]
            for c in conds[1:]:
                sprout_ok = sprout_ok | c
        else:
            d70 = df["chip_70_delta"]
            d90 = df["chip_90_delta"]
            sprout_ok = (d70 > 0) | (d90 > 0)

        if int(min_up_days) > 0:
            sprout_ok = sprout_ok & (u70 >= int(min_up_days))

        ok = ok & sprout_ok

    return df[ok]


def filter_ma20_support_operator(row:pd.Series,N:int=40)->bool:
    """
    20日均线支撑：现价≥MA20×0.98
    """
    market = row["market"]
    code_num = int(row["code"])
    code = str(code_num).zfill(6)
    bars = tdx.get_security_bars(9,market, code, 0, N)
    if bars is None or len(bars) < 20:
        return False
    df = pd.DataFrame(bars)
    if "datetime" in df.columns:
        df = df.sort_values("datetime")
    df["close"] = pd.to_numeric(df.get("close", 0), errors="coerce")
    df["ma20"] = df["close"].rolling(window=20).mean()
    is_ma20_support = df["close"].iloc[-1] >= df["ma20"].iloc[-1] * 0.98
    return is_ma20_support

# 20日均线支撑
def filter_ma20_support(df:pd.DataFrame)->pd.DataFrame:
    """
    20日均线支撑：现价≥MA20×0.98
    """
    if df is None or df.empty:
        return df
    t0 = time.time()
    oks = []
    total = int(len(df))
    for idx, row in df.iterrows():
        oks.append(filter_ma20_support_operator(row))
        done = len(oks)
        if done % 200 == 0 or done == total:
            dt = time.time() - t0
            print(f"[MA20支撑] progress {done}/{total} elapsed={dt:.1f}s")
    df["ma20_support_ok"] = oks
    print(df)
    return df[df["ma20_support_ok"]]


def filter_ma20_oscillation_operator(row:pd.Series,N:int=40)->bool:
    market = row["market"]
    code_num = int(row["code"])
    code = str(code_num).zfill(6)
    bars = tdx.get_security_bars(9,market, code, 0, N)
    if bars is None or len(bars) == 0:
        return False
    df = pd.DataFrame(bars)
    if "datetime" in df.columns:
        df = df.sort_values("datetime")
    df["close"] = pd.to_numeric(df.get("close", 0), errors="coerce")
    df["high"] = pd.to_numeric(df.get("high", 0), errors="coerce")
    df["low"] = pd.to_numeric(df.get("low", 0), errors="coerce")
    close_s = pd.to_numeric(df["close"], errors="coerce").dropna().iloc[-20:]
    if len(close_s) < 20:
        return False
    base_close = float(close_s.iloc[0])
    last_close = float(close_s.iloc[-1])
    if base_close <= 0 or last_close <= 0:
        return False

    chg20 = last_close / base_close - 1.0
    ret_s = close_s.pct_change().dropna()
    max_daily_up = float(ret_s.max()) if not ret_s.empty else 0.0

    high_s = pd.to_numeric(df["high"], errors="coerce").dropna().iloc[-20:]
    low_s = pd.to_numeric(df["low"], errors="coerce").dropna().iloc[-20:]
    if len(high_s) < 20 or len(low_s) < 20:
        return False
    min_low = low_s[low_s > 0].min()
    if pd.isna(min_low) or float(min_low) <= 0:
        return False
    range_amp = float(high_s.max()) / float(min_low) - 1.0

    return bool((-0.12 <= chg20 <= 0.15) and (range_amp <= 0.18) and (max_daily_up <= 0.08))


# 近20日整体震荡：涨跌幅 -12%~+15% 且区间振幅<=30%
def filter_ma20_oscillation(df:pd.DataFrame)->pd.DataFrame:
    """
    近20日整体震荡：涨跌幅 -12%~+15% 且区间振幅<=30%
    """
    if df is None or df.empty:
        return df
    t0 = time.time()
    oks = []
    total = int(len(df))
    for idx, row in df.iterrows():
        oks.append(filter_ma20_oscillation_operator(row))
        done = len(oks)
        if done % 200 == 0 or done == total:
            dt = time.time() - t0
            print(f"[20日震荡] progress {done}/{total} elapsed={dt:.1f}s")
    df["ma20_oscillation_ok"] = oks
    print(df)
    return df[df["ma20_oscillation_ok"]]

# 脉冲放量：近10天最大成交量相对均值比≥3.0 的算子 
def filter_pulse_volume_operator(row:pd.Series,N:int=10)->bool:
    market = row["market"]
    code_num = int(row["code"])
    code = str(code_num).zfill(6)
    bars = tdx.get_security_bars(9,market, code, 0, N)
    if bars is None or len(bars) == 0:
        return False
    df = pd.DataFrame(bars)
    if "datetime" in df.columns:
        df = df.sort_values("datetime")
    if "vol" in df.columns:
        vol_raw = df["vol"]
    elif "volume" in df.columns:
        vol_raw = df["volume"]
    else:
        vol_raw = pd.Series([0] * int(len(df)))
    vol_s = pd.to_numeric(vol_raw, errors="coerce").dropna()
    vol_s = vol_s[vol_s > 0]
    if int(len(vol_s)) < N:
        return False
    avg_vol_10d = float(vol_s.mean())
    if avg_vol_10d <= 0:
        return False
    max_vol_ratio = float(vol_s.max()) / avg_vol_10d
    if max_vol_ratio < 1:
        return False
    max_idx = int(vol_s.values.argmax())
    pulse_age = (int(len(vol_s)) - 1) - max_idx
    return bool(1 <= pulse_age and pulse_age <= 2)



# 脉冲放量：近10天最大成交量相对均值比≥2.5的算子
def filter_pulse_volume(df:pd.DataFrame)->pd.DataFrame:
    """
    脉冲放量：近10天最大成交量相对均值比≥2.5 的算子     
    """
    if df is None or df.empty:
        return df
    t0 = time.time()
    oks = []
    total = int(len(df))
    for idx, row in df.iterrows():
        oks.append(filter_pulse_volume_operator(row))
        done = len(oks)
        if done % 200 == 0 or done == total:
            dt = time.time() - t0
            print(f"[脉冲放量] progress {done}/{total} elapsed={dt:.1f}s")
    df["pulse_volume_ok"] = oks
    print(df)
    return df[df["pulse_volume_ok"]]

def main():
    t0 = time.time()

    def _log(stage: str, df: pd.DataFrame):
        n = 0 if df is None else int(len(df))
        dt = time.time() - t0
        if n == 0:
            print(f"[{dt:8.1f}s] {stage}: rows=0")
            return
        cols = []
        for c in ["market", "code", "price", "circulating", "turnover_low_ratio", "ma20_support_ok", "ma20_oscillation_ok", "pulse_volume_ok"]:
            if c in df.columns:
                cols.append(c)
        head_preview = df[cols].head(3) if cols else df.head(3)
        print(f"[{dt:8.1f}s] {stage}: rows={n}")
        print(head_preview)

    print("开始全市场 A 股股票列表拉取...")
    df_stock_codes = get_all_a_share_codes()
    df_stock_codes["code"] = df_stock_codes["code"].astype(str).str.zfill(6)
    _log("A股代码列表", df_stock_codes)

    df_stock_codes = df_stock_codes[~df_stock_codes["code"].isin(blacklist)]
    _log("剔除黑名单后代码列表", df_stock_codes)

    df_quote_snapshots = get_security_quotes(df_stock_codes)
    _log("实时快照", df_quote_snapshots)

    df_finance_info = get_finance_info(df_stock_codes)
    _log("财务摘要", df_finance_info)

    df_quote_snapshots = calcalte_circulating_stock(df_quote_snapshots, df_finance_info)
    _log("合并并计算流通盘", df_quote_snapshots)

    df_quote_snapshots = filter_circulating_stock(df_quote_snapshots)
    _log("过滤流通盘", df_quote_snapshots)

    df_quote_snapshots = filter_daily_turnover(df_quote_snapshots)
    _log("过滤换手率占比", df_quote_snapshots)

    chip_window = int(os.getenv("CHIP_WINDOW", "60"))
    chip_smooth = int(os.getenv("CHIP_SMOOTH", "5"))
    chip_trend_mode_raw = os.getenv("CHIP_TREND_MODE", "").strip().lower()
    chip_trend_mode = chip_trend_mode_raw if chip_trend_mode_raw else None
    chip_trend_days = int(os.getenv("CHIP_TREND_DAYS", "10"))
    chip_min_delta_70 = float(os.getenv("CHIP_MIN_DELTA_70", "0"))
    chip_min_delta_90 = float(os.getenv("CHIP_MIN_DELTA_90", "0"))
    chip_base_days = int(os.getenv("CHIP_BASE_DAYS", "30"))
    chip_momentum_days = int(os.getenv("CHIP_MOMENTUM_DAYS", "7"))
    chip_min_bounce_70 = float(os.getenv("CHIP_MIN_BOUNCE_70", "0"))
    chip_min_bounce_90 = float(os.getenv("CHIP_MIN_BOUNCE_90", "0"))
    chip_min_slope_70 = float(os.getenv("CHIP_MIN_SLOPE_70", "0"))
    chip_min_slope_90 = float(os.getenv("CHIP_MIN_SLOPE_90", "0"))
    chip_min_up_days = int(os.getenv("CHIP_MIN_UP_DAYS", "0"))

    df_quote_snapshots = filter_chip_concentration(
        df_quote_snapshots,
        window=chip_window,
        smooth=chip_smooth,
        trend_mode=chip_trend_mode,
        trend_days=chip_trend_days,
        min_delta_70=chip_min_delta_70,
        min_delta_90=chip_min_delta_90,
        base_days=chip_base_days,
        momentum_days=chip_momentum_days,
        min_bounce_70=chip_min_bounce_70,
        min_bounce_90=chip_min_bounce_90,
        min_slope_70=chip_min_slope_70,
        min_slope_90=chip_min_slope_90,
        min_up_days=chip_min_up_days,
    )
    _log("过滤筹码集中度", df_quote_snapshots)

    df_quote_snapshots = filter_ma20_support(df_quote_snapshots)
    _log("过滤MA20支撑", df_quote_snapshots)

    df_quote_snapshots = filter_ma20_oscillation(df_quote_snapshots)
    _log("过滤20日整体震荡", df_quote_snapshots)
    
    # df_quote_snapshots.to_csv("ma20_oscillation.csv",index=False)
    # df_quote_snapshots = filter_pulse_volume(df_quote_snapshots)
    # _log("过滤脉冲放量", df_quote_snapshots)

    df_quote_snapshots.to_csv("ma20_pulse_volume.csv", index=False)
    print("完成，最终数量：", len(df_quote_snapshots))

if __name__ == "__main__":
    main()
