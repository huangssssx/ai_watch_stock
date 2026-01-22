import akshare as ak
import efinance as ef
import pandas as pd
import numpy as np
import time
import datetime
import traceback

# --- Helper Functions ---
_ERROR_COUNT = 0
_ERROR_TRACE_PRINTED = 0
_MAX_TRACEBACKS = 3

def _log_error(where: str, e: Exception):
    global _ERROR_COUNT, _ERROR_TRACE_PRINTED
    _ERROR_COUNT += 1
    print(f"❌ ERROR[{_ERROR_COUNT}] {where}: {type(e).__name__}: {e}")
    if _ERROR_TRACE_PRINTED < _MAX_TRACEBACKS:
        print(traceback.format_exc())
        _ERROR_TRACE_PRINTED += 1

def normalize(series):
    if series.empty: return series
    min_val = series.min()
    max_val = series.max()
    if max_val == min_val: return pd.Series([1.0]*len(series), index=series.index)
    return (series - min_val) / (max_val - min_val)

def get_rpp(close, high_60, low_60):
    if high_60 == low_60: return 0.5
    return (close - low_60) / (high_60 - low_60)

def _safe_vwap(amount, volume, current_price):
    """
    自适应计算 VWAP，自动修正 '手' vs '股' 的单位问题
    """
    if volume == 0: return current_price
    
    # 尝试1: 假设单位是股
    raw_vwap = amount / volume
    
    # 检查数量级差异
    if current_price > 0:
        ratio = raw_vwap / current_price
        if 80 < ratio < 120: # 偏差约100倍，说明 Volume 是手 (Amount是元, Vol是手) -> 需除以100
            return raw_vwap / 100.0
        elif 0.8 < ratio < 1.2: # 偏差不大，说明 Volume 是股
            return raw_vwap
            
    # 兜底：如果无法判断，假设是手（A股 spot 接口通常返回手）
    # 但为了保险，还是返回修正后的
    return raw_vwap / 100.0 if raw_vwap > current_price * 50 else raw_vwap

def _chunked(items, size: int):
    if not items:
        return
    for i in range(0, len(items), size):
        yield items[i : i + size]

def _fetch_latest_quotes_once(codes):
    df = ef.stock.get_latest_quote(codes)
    if df is None or df.empty:
        return pd.DataFrame()
    df = df.copy()
    df["代码"] = df["代码"].astype(str).str.zfill(6)
    for col in ["最新价", "今开", "昨日收盘", "最高", "最低", "涨跌幅", "换手率", "量比", "成交量", "成交额"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df

def _fetch_latest_quotes(codes):
    if not codes:
        return pd.DataFrame()
    out = []
    stack = [codes]
    while stack:
        chunk = stack.pop()
        try:
            out.append(_fetch_latest_quotes_once(chunk))
            time.sleep(0.05)
        except Exception as e:
            if len(chunk) <= 10:
                _log_error(f"ef.stock.get_latest_quote({len(chunk)})", e)
                continue
            mid = len(chunk) // 2
            stack.append(chunk[:mid])
            stack.append(chunk[mid:])
            time.sleep(0.2)
    if not out:
        return pd.DataFrame()
    df = pd.concat(out, ignore_index=True)
    df = df.dropna(subset=["代码"]).drop_duplicates(subset=["代码"], keep="last")
    return df

def _fetch_base_info_once(codes):
    df = ef.stock.get_base_info(codes)
    if df is None or (hasattr(df, "empty") and df.empty):
        return pd.DataFrame()
    if isinstance(df, pd.Series):
        df = df.to_frame().T
    df = df.copy()
    if "股票代码" in df.columns:
        df["股票代码"] = df["股票代码"].astype(str).str.zfill(6)
    return df

def _fetch_base_info(codes):
    if not codes:
        return pd.DataFrame()
    out = []
    for chunk in _chunked(codes, 80):
        try:
            out.append(_fetch_base_info_once(chunk))
            time.sleep(0.05)
        except Exception as e:
            _log_error(f"ef.stock.get_base_info({len(chunk)})", e)
            time.sleep(0.2)
            continue
    if not out:
        return pd.DataFrame()
    df = pd.concat(out, ignore_index=True)
    if "股票代码" in df.columns:
        df = df.dropna(subset=["股票代码"]).drop_duplicates(subset=["股票代码"], keep="last")
    return df

def _fetch_quote_history_once(codes):
    hist_dict = ef.stock.get_quote_history(codes)
    if not hist_dict:
        return {}
    return hist_dict

def _fetch_quote_history(codes):
    if not codes:
        return {}
    out = {}
    for chunk in _chunked(codes, 30):
        try:
            part = _fetch_quote_history_once(chunk)
            out.update(part)
            time.sleep(0.05)
        except Exception as e:
            _log_error(f"ef.stock.get_quote_history({len(chunk)})", e)
            time.sleep(0.2)
            continue
    return out

def fetch_stock_data(code, name, sector, hist: pd.DataFrame = None):
    """
    Worker function to fetch data for a single stock.
    Returns dict or None.
    """
    try:
        # 1. Get Daily Data (for Trend & RPP)
        if hist is None:
            hist_dict = ef.stock.get_quote_history([code])
            if not hist_dict or code not in hist_dict:
                return None
            hist = hist_dict[code]

        if hist is None or hist.empty or len(hist) < 60: return None
        
        # efinance columns: 股票名称, 股票代码, 日期, 开盘, 收盘, 最高, 最低, 成交量, 成交额...
        # Map to expected columns
        last_row = hist.iloc[-1]
        close = float(last_row['收盘'])
        
        # RPP Calculation
        window_60 = hist.tail(60)
        high_60 = window_60['最高'].max()
        low_60 = window_60['最低'].min()
        rpp = get_rpp(close, high_60, low_60)
        
        # Trend: Price > MA20
        ma20 = window_60['收盘'].tail(20).mean()
        
        return {
            "code": code,
            "name": name,
            "sector": sector,
            "close": close,
            "rpp": rpp,
            "ma20": ma20,
            "vol_prev": float(last_row['成交量'])
        }
    except Exception as e:
        _log_error(f"fetch_stock_data({code})", e)
        return None

# --- Main Logic ---

print("🔥 启动 V2.0 板块资金选股引擎 (单线程安全版 - efinance加强)...")
start_time = time.time()

spot_df = pd.DataFrame()
try:
    universe = ak.stock_info_a_code_name()
    if universe is not None and not universe.empty:
        universe = universe.rename(columns={"code": "代码", "name": "名称"})
        universe["代码"] = universe["代码"].astype(str).str.zfill(6)
        universe = universe[~universe["名称"].astype(str).str.contains("ST|退", na=False)]
        universe_codes = universe["代码"].tolist()
    else:
        universe_codes = []
except Exception as e:
    _log_error("ak.stock_info_a_code_name()", e)
    universe_codes = []

if universe_codes:
    print(f"📡 拉取全市场实时快照 (via efinance.get_latest_quote, 分批)...")
    try:
        for chunk in _chunked(universe_codes, 150):
            try:
                part = _fetch_latest_quotes_once(chunk)
                if not part.empty:
                    spot_df = pd.concat([spot_df, part], ignore_index=True)
                time.sleep(0.05)
            except Exception as e:
                _log_error(f"ef.stock.get_latest_quote({len(chunk)})", e)
                time.sleep(0.2)
                continue
        if not spot_df.empty:
            spot_df = spot_df.dropna(subset=["代码"]).drop_duplicates(subset=["代码"], keep="last")
    except Exception as e:
        _log_error("build_spot_df()", e)
        spot_df = pd.DataFrame()

if spot_df.empty:
    df = pd.DataFrame([{"代码": "-", "名称": "-", "点评": "实时行情获取失败，通常是数据源连接被中断"}])
    print(f"耗时: {time.time() - start_time:.2f}s")
    if _ERROR_COUNT > 0:
        print(f"❗ 本次运行捕获异常次数: {_ERROR_COUNT}")
    raise SystemExit(0)

scan_pool = spot_df.copy()
if "成交额" in scan_pool.columns:
    scan_pool = scan_pool[scan_pool["成交额"].fillna(0) > 0]
scan_pool = scan_pool.sort_values(by="成交额", ascending=False).head(800)

base_info = _fetch_base_info(scan_pool["代码"].astype(str).tolist())
if not base_info.empty and "股票代码" in base_info.columns:
    base_info = base_info.rename(columns={"股票代码": "代码", "股票名称": "名称", "所处行业": "板块"})
    scan_pool = scan_pool.merge(base_info[["代码", "板块"]], on="代码", how="left")
else:
    scan_pool["板块"] = ""

sector_list = (
    scan_pool.dropna(subset=["板块"])
    .groupby("板块")["成交额"]
    .sum()
    .sort_values(ascending=False)
    .head(8)
    .index.tolist()
)
sector_list = [s for s in sector_list if isinstance(s, str) and s.strip()]
print(f"🎯 锁定热门板块: {sector_list}")

candidates = []
if sector_list:
    cand_df = scan_pool[scan_pool["板块"].isin(sector_list)].copy()
else:
    cand_df = scan_pool.copy()
    cand_df["板块"] = "全市场"

cand_df = cand_df.sort_values(by="成交额", ascending=False).head(300)
for _, row in cand_df.iterrows():
    candidates.append(
        {
            "code": str(row["代码"]).zfill(6),
            "name": row.get("名称", ""),
            "sector": row.get("板块", "") or "全市场",
        }
    )

print(f"🔍 初始候选池: {len(candidates)} 只股票")

# 3. 顺序获取数据 (Sequential Fetching)
# 改为单线程 + 延时，防止封IP
analyzed_stocks = []

if candidates:
    codes = [c["code"] for c in candidates]
    hist_map = _fetch_quote_history(codes)
    total_tasks = len(candidates)
    for i, c in enumerate(candidates):
        hist = hist_map.get(c["code"])
        res = fetch_stock_data(c["code"], c["name"], c["sector"], hist=hist)
        if res:
            analyzed_stocks.append(res)
        if i % 10 == 0:
            print(f"  进度: {i}/{total_tasks}...", end="\r")
        time.sleep(0.02)

print(f"\n✅ 数据获取完成，有效股票: {len(analyzed_stocks)}")

# 4. 实时行情校验 (The Filter)
final_list = []
if not spot_df.empty and analyzed_stocks:
    # 转为字典加速查找
    spot_map = spot_df.set_index('代码').to_dict('index')
    
    for stock in analyzed_stocks:
        code = stock['code']
        if code not in spot_map: continue
        
        real = spot_map[code]
        
        # --- 核心过滤逻辑 V2.0 ---
        
        try:
            current_price = float(real.get('最新价', 0) or 0)
            open_price = float(real.get('今开', 0) or 0)
            prev_close = float(real.get('昨日收盘', 0) or real.get('昨收', 0) or 0)
            high_price = float(real.get('最高', 0) or 0)
            volume = float(real.get('成交量', 0) or 0)
            amount = float(real.get('成交额', 0) or 0)
            turnover = float(real.get('换手率', 0) or 0)
            lb = float(real.get('量比', 0) or 0)
        except Exception as e:
            _log_error(f"parse_spot_row({code})", e)
            continue
            
        if current_price == 0: continue
        
        # 1. 相对位置 RPP < 0.4 (低位)
        if stock['rpp'] >= 0.4: continue
        
        # 2. 趋势支撑 (价格 > MA20)
        # if current_price < stock['ma20']: continue 
        
        # 3. 实时强度 (Price > Open) -> 拒绝假阴线
        if current_price <= open_price: continue
        
        # 4. 资金实锤 (Price > VWAP)
        # 使用自适应 VWAP 计算，防止单位陷阱
        if volume > 0:
            vwap = _safe_vwap(amount, volume, current_price)
            if current_price < vwap: continue
            
            # V2.1 优化：乖离率限制 < 1.5%
            # 防止追高接盘
            vwap_dev = (current_price - vwap) / vwap
            if vwap_dev > 0.015: continue
            
        # 5. 量能确认 (量比 > 1.2 或 换手 > 1%)
        if lb < 1.2: continue
        
        # 6. 风控：拒绝涨停 (Limit Up)
        if current_price >= prev_close * 1.095: continue
        
        # 7. 涨幅区间 (1% < Chg < 6%)
        chg_pct = (current_price - prev_close) / prev_close * 100
        if chg_pct < 1.0 or chg_pct > 6.0: continue
        
        # --- 评分系统 ---
        # 低位分 (30) + 资金分 (40) + 强度分 (30)
        score_pos = (1 - stock['rpp']) * 30
        score_fund = min(lb / 3.0, 1.0) * 40
        score_mom = min(chg_pct / 5.0, 1.0) * 30
        
        total_score = score_pos + score_fund + score_mom
        
        stock['最新价'] = current_price
        stock['涨跌幅'] = chg_pct
        stock['量比'] = lb
        stock['VWAP'] = round(vwap, 2)
        stock['评分'] = int(total_score)
        
        # 点评生成
        comments = []
        if stock['rpp'] < 0.1: comments.append("极低位")
        elif stock['rpp'] < 0.3: comments.append("相对底部")
        
        if current_price > vwap * 1.01: comments.append("站稳均价")
        if lb > 2.0: comments.append(f"放量{lb}倍")
        
        stock['点评'] = ",".join(comments)
        
        final_list.append(stock)

# 5. 输出结果
df = pd.DataFrame(final_list)
if not df.empty:
    df = df.sort_values(by="评分", ascending=False).head(30)
    # 格式化输出
    out_cols = ['code', 'name', 'sector', '最新价', '涨跌幅', '量比', 'rpp', '评分', '点评']
    df = df[out_cols]
    df.columns = ['代码', '名称', '板块', '最新价', '涨跌幅', '量比', 'RPP位置', '综合评分', '点评']
    
    print("\n🏆 最终精选 (Top 30):")
    # print(df.to_string()) 

# 必须赋值给 df 变量供系统读取
df = df if not df.empty else pd.DataFrame(columns=['代码', '名称', '点评'])
print(f"耗时: {time.time() - start_time:.2f}s")
if _ERROR_COUNT > 0:
    print(f"❗ 本次运行捕获异常次数: {_ERROR_COUNT}")
