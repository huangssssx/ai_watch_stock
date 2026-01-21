import akshare as ak
import pandas as pd
import numpy as np
import time
import datetime
import traceback
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed

_errors = []

def _record_error(stage, key, exc):
    _errors.append(
        {
            "stage": str(stage),
            "key": str(key),
            "exc": repr(exc),
            "traceback": traceback.format_exc(),
        }
    )

def _summarize_errors(max_traces=3):
    if not _errors:
        return ""
    stage_counter = Counter([e.get("stage") for e in _errors])
    exc_counter = Counter([e.get("exc") for e in _errors])
    lines = []
    lines.append(f"错误统计: total={len(_errors)} stages={dict(stage_counter)} top_excs={dict(exc_counter.most_common(5))}")
    shown = 0
    for e in _errors:
        if shown >= int(max_traces):
            break
        lines.append(f"[{shown+1}] stage={e.get('stage')} key={e.get('key')} exc={e.get('exc')}")
        tb = (e.get("traceback") or "").strip()
        if tb:
            lines.append(tb)
        shown += 1
    return "\n".join(lines)

# --- Helper Functions ---
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

def fetch_stock_data(code, name, sector):
    """
    Worker function to fetch data for a single stock.
    Returns dict or None.
    """
    try:
        # 1. Get Daily Data (for Trend & RPP)
        # We need historical data to calculate RPP (Relative Position)
        hist = ak.stock_zh_a_hist(symbol=code, period="daily", adjust="qfq")
        if hist is None or hist.empty or len(hist) < 60: return None
        
        last_row = hist.iloc[-1]
        close = last_row['收盘']
        
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
            "vol_prev": last_row['成交量']
        }
    except Exception as e:
        _record_error("hist", code, e)
        return None

# --- Main Logic ---

print("🔥 启动 V2.0 板块资金选股引擎...")
start_time = time.time()

# 1. 获取热门板块 (Real-time)
try:
    sectors = ak.stock_board_industry_name_em()
    if sectors is not None and not sectors.empty:
        # 过滤掉 ST 板块
        sectors = sectors[~sectors['板块名称'].str.contains("ST")]
        # 按涨幅排序
        top_sectors = sectors.sort_values(by="涨跌幅", ascending=False).head(8)
        sector_list = top_sectors['板块名称'].tolist()
        print(f"🎯 锁定热门板块: {sector_list}")
    else:
        sector_list = []
except Exception as e:
    _record_error("sectors", "stock_board_industry_name_em", e)
    raise RuntimeError(f"板块获取失败: {repr(e)}\n{_summarize_errors()}") from e

# 2. 构建候选池 (Candidate Pool)
candidates = []
if sector_list:
    for sector in sector_list:
        try:
            cons = ak.stock_board_industry_cons_em(symbol=sector)
            if cons is not None and not cons.empty:
                for _, row in cons.iterrows():
                    candidates.append({
                        "code": str(row['代码']), 
                        "name": row['名称'], 
                        "sector": sector
                    })
            time.sleep(0.2) # Avoid blocking
        except Exception as e:
            _record_error("sector_cons", sector, e)
            continue

print(f"🔍 初始候选池: {len(candidates)} 只股票")
if sector_list and not candidates:
    raise RuntimeError(f"候选池构建失败：板块={len(sector_list)} 但候选=0\n{_summarize_errors()}")

# 3. 并发获取数据 (Concurrent Fetching)
# 限制最大线程数，防止封IP
MAX_WORKERS = 8 
analyzed_stocks = []

if candidates:
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(fetch_stock_data, c['code'], c['name'], c['sector']): c for c in candidates}
        
        for i, future in enumerate(as_completed(futures)):
            try:
                res = future.result()
            except Exception as e:
                _record_error("future", "fetch_stock_data", e)
                res = None
            if res:
                analyzed_stocks.append(res)
            if i % 50 == 0:
                print(f"  进度: {i}/{len(candidates)}...")

print(f"\n✅ 数据获取完成，有效股票: {len(analyzed_stocks)}")
if candidates and not analyzed_stocks:
    raise RuntimeError(f"日线历史数据拉取全部失败：candidates={len(candidates)} analyzed=0\n{_summarize_errors()}")

# 4. 实时行情校验 (The Filter)
# 为了获取最新的 Price, Open, VWAP (Amount/Vol)，我们需要拉取一次全市场 Spot
print("📡 拉取全市场实时快照...")
try:
    spot_df = ak.stock_zh_a_spot_em()
    if spot_df is not None and not spot_df.empty:
        spot_df['代码'] = spot_df['代码'].astype(str)
    else:
        raise RuntimeError("实时快照为空")
except Exception as e:
    _record_error("spot", "stock_zh_a_spot_em", e)
    raise RuntimeError(f"实时行情失败: {repr(e)}\n{_summarize_errors()}") from e

final_list = []
_filter_stats = Counter()
if not spot_df.empty and analyzed_stocks:
    # 转为字典加速查找
    spot_map = spot_df.set_index('代码').to_dict('index')
    
    for stock in analyzed_stocks:
        code = stock['code']
        if code not in spot_map: continue
        
        real = spot_map[code]
        
        # --- 核心过滤逻辑 V2.0 ---
        
        try:
            current_price = float(real.get('最新价', 0))
            open_price = float(real.get('今开', 0))
            prev_close = float(real.get('昨收', 0))
            high_price = float(real.get('最高', 0))
            volume = float(real.get('成交量', 0))
            amount = float(real.get('成交额', 0))
            turnover = float(real.get('换手率', 0))
            lb = float(real.get('量比', 0))
        except Exception as e:
            _record_error("spot_parse", code, e)
            _filter_stats["spot_parse_error"] += 1
            continue
            
        if current_price == 0:
            _filter_stats["price_zero"] += 1
            continue
        
        # 1. 相对位置 RPP < 0.4 (低位)
        if stock['rpp'] >= 0.4:
            _filter_stats["rpp_high"] += 1
            continue
        
        # 2. 趋势支撑 (价格 > MA20)
        # if current_price < stock['ma20']: continue 
        
        # 3. 实时强度 (Price > Open) -> 拒绝假阴线
        if current_price <= open_price:
            _filter_stats["below_open"] += 1
            continue
        
        # 4. 资金实锤 (Price > VWAP)
        # 使用自适应 VWAP 计算，防止单位陷阱
        vwap = current_price
        if volume > 0:
            vwap = _safe_vwap(amount, volume, current_price)
            if current_price < vwap:
                _filter_stats["below_vwap"] += 1
                continue
            
            # V2.1 优化：乖离率限制 < 1.5%
            # 防止追高接盘
            vwap_dev = (current_price - vwap) / vwap
            if vwap_dev > 0.015:
                _filter_stats["vwap_dev_high"] += 1
                continue
            
        # 5. 量能确认 (量比 > 1.2 或 换手 > 1%)
        if lb < 1.2:
            _filter_stats["lb_low"] += 1
            continue
        
        # 6. 风控：拒绝涨停 (Limit Up)
        if current_price >= prev_close * 1.095:
            _filter_stats["limit_up"] += 1
            continue
        
        # 7. 涨幅区间 (1% < Chg < 6%)
        chg_pct = (current_price - prev_close) / prev_close * 100
        if chg_pct < 1.0 or chg_pct > 6.0:
            _filter_stats["chg_out_of_range"] += 1
            continue
        
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
        _filter_stats["selected"] += 1

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
else:
    print("📊 过滤统计:", dict(_filter_stats))
if _errors:
    print(_summarize_errors())

# 必须赋值给 df 变量供系统读取
df = df if not df.empty else pd.DataFrame(columns=['代码', '名称', '点评'])
print(f"耗时: {time.time() - start_time:.2f}s")
