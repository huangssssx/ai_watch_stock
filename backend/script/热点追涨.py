import akshare as ak
import pandas as pd
import numpy as np
import time
import datetime
import traceback
import sys
import os
import sqlite3
import json

def _resolve_project_root():
    start_paths = []
    if "__file__" in globals():
        start_paths.append(os.path.abspath(__file__))
    start_paths.append(os.getcwd())
    for start in start_paths:
        cur = os.path.abspath(start)
        if os.path.isfile(cur):
            cur = os.path.dirname(cur)
        while True:
            if os.path.exists(os.path.join(cur, "backend", "stock_watch.db")):
                return cur
            parent = os.path.dirname(cur)
            if parent == cur:
                break
            cur = parent
    return os.getcwd()

project_root = _resolve_project_root()
if project_root not in sys.path:
    sys.path.append(project_root)

try:
    from backend.utils.tushare_client import pro
    import tushare as ts
except ImportError:
    print("❌ Failed to import tushare_client. Ensure you are running from project root or backend is in python path.")
    pro = None

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

def get_ts_code(code):
    """Convert 6-digit code to ts_code (e.g. 600519 -> 600519.SH)"""
    if code.startswith('6'):
        return f"{code}.SH"
    elif code.startswith('0') or code.startswith('3'):
        return f"{code}.SZ"
    elif code.startswith('8') or code.startswith('4'):
        return f"{code}.BJ"
    return code

def fetch_stock_data(code, name, sector):
    """
    Worker function to fetch data for a single stock using Tushare.
    Returns dict or None.
    """
    if pro is None:
        return None
        
    try:
        ts_code = get_ts_code(code)
        
        # 1. Get Daily Data (for Trend & RPP)
        # Tushare daily interface
        end_date = datetime.datetime.now().strftime("%Y%m%d")
        start_date = (datetime.datetime.now() - datetime.timedelta(days=120)).strftime("%Y%m%d")
        
        # pro.daily returns: ts_code, trade_date, open, high, low, close, pre_close, change, pct_chg, vol, amount
        df = pro.daily(ts_code=ts_code, start_date=start_date, end_date=end_date)
        
        if df is None or df.empty or len(df) < 20: 
            return None
            
        # Tushare returns data in descending order (newest first). 
        # We need to sort ascending for calculation or handle index carefully.
        # Let's sort ascending by date
        hist = df.sort_values('trade_date', ascending=True)
        
        # Ensure we have enough data
        if len(hist) < 20: return None
        
        last_row = hist.iloc[-1]
        close = float(last_row['close'])
        
        # RPP Calculation (Use last 60 days max)
        window_60 = hist.tail(60)
        high_60 = window_60['high'].max()
        low_60 = window_60['low'].min()
        rpp = get_rpp(close, high_60, low_60)
        
        # Trend: Price > MA20
        ma20 = window_60['close'].tail(20).mean()
        
        return {
            "code": code,
            "name": name,
            "sector": sector,
            "close": close,
            "rpp": rpp,
            "ma20": ma20,
            "vol_prev": float(last_row['vol']) # Tushare vol is in 'Hand' (usually), need to check
        }
    except Exception as e:
        _log_error(f"fetch_stock_data({code})", e)
        return None

# --- Main Logic ---

print("🔥 启动 V2.0 板块资金选股引擎 (Tushare Pro版)...")
start_time = time.time()

# 1. 获取热门板块 (Real-time)
# Fallback to akshare with retry, as efinance lacks explicit sector list
try:
    sectors = None
    for _ in range(3):
        try:
            sectors = ak.stock_board_industry_name_em()
            if sectors is not None and not sectors.empty: break
        except:
            time.sleep(1)
            
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
    print(f"❌ 板块获取失败: {e}")
    sector_list = []

# 2. 构建候选池 (Candidate Pool)
candidates = []
if sector_list:
    for sector in sector_list:
        try:
            cons = ak.stock_board_industry_cons_em(symbol=sector)
            if cons is not None and not cons.empty:
                for _, row in cons.iterrows():
                    candidates.append({
                        "code": str(row['代码']).zfill(6), 
                        "name": row['名称'], 
                        "sector": sector
                    })
            time.sleep(0.5) # Avoid blocking
        except Exception as e:
            _log_error(f"stock_board_industry_cons_em({sector})", e)
            continue

print(f"🔍 初始候选池: {len(candidates)} 只股票")

# 3. 顺序获取数据 (Sequential Fetching)
# 改为单线程 + 延时，防止封IP
analyzed_stocks = []

if candidates:
    total_tasks = len(candidates)
    for i, c in enumerate(candidates):
        res = fetch_stock_data(c['code'], c['name'], c['sector'])
        if res:
            analyzed_stocks.append(res)
        
        if i % 10 == 0:
            print(f"  进度: {i}/{total_tasks}...", end="\r")
        
        # 关键修改：增加延时，保护账号
        time.sleep(0.1) # efinance is faster, but still be nice

print(f"\n✅ 数据获取完成，有效股票: {len(analyzed_stocks)}")

# 4. 实时行情校验 (The Filter)
# 为了获取最新的 Price, Open, VWAP (Amount/Vol)，我们需要拉取一次全市场 Spot
print("📡 拉取实时快照 (via Tushare)...")
try:
    # Tushare's ts.get_realtime_quotes works with list of codes
    # But it might fail if list is too long. Let's chunk it.
    stock_codes = [s['code'] for s in analyzed_stocks]
    spot_df = pd.DataFrame()
    
    chunk_size = 50
    for i in range(0, len(stock_codes), chunk_size):
        chunk = stock_codes[i:i+chunk_size]
        try:
            # ts.get_realtime_quotes returns: name, open, pre_close, price, high, low, bid, ask, volume, amount...
            df_chunk = ts.get_realtime_quotes(chunk)
            if df_chunk is not None and not df_chunk.empty:
                spot_df = pd.concat([spot_df, df_chunk], ignore_index=True)
            time.sleep(0.1)
        except Exception as e:
            _log_error(f"ts.get_realtime_quotes chunk {i}", e)

    if spot_df is not None and not spot_df.empty:
        # Standardize to match logic
        # ts columns: code, name, price, bid, ask, volume, amount, time...
        # Note: ts returns strings!
        pass
    else:
        spot_df = pd.DataFrame()
except Exception as e:
    _log_error("ts.get_realtime_quotes()", e)
    spot_df = pd.DataFrame()


final_list = []
if not spot_df.empty and analyzed_stocks:
    # 转为字典加速查找
    spot_map = spot_df.set_index('code').to_dict('index')
    
    for stock in analyzed_stocks:
        code = stock['code']
        if code not in spot_map: continue
        
        real = spot_map[code]
        
        # --- 核心过滤逻辑 V2.0 ---
        
        try:
            current_price = float(real.get('price', 0))
            open_price = float(real.get('open', 0))
            prev_close = float(real.get('pre_close', 0))
            high_price = float(real.get('high', 0))
            volume = float(real.get('volume', 0)) # 手
            amount = float(real.get('amount', 0)) # 元
            
            # Tushare doesn't return turnover or lb (volume ratio) directly in realtime_quotes
            # We might need to approximate or skip
            # LB = (Vol / 240) / (MA5_Vol / 240) ? No MA5_Vol here.
            # We can use previous volume from daily data as a proxy for "average volume"?
            # Or just skip LB check if not available.
            # Let's try to calculate simple LB if we have vol_prev (which is yesterday's volume)
            # LB approx = (Current Vol / Minutes_passed) / (Yesterday Vol / 240)
            # This is rough.
            
            # Calculate minutes passed since 9:30
            now = datetime.datetime.now()
            market_open = now.replace(hour=9, minute=30, second=0, microsecond=0)
            minutes_passed = (now - market_open).total_seconds() / 60
            if minutes_passed < 1: minutes_passed = 1
            if minutes_passed > 240: minutes_passed = 240
            
            vol_prev = stock.get('vol_prev', 0)
            if vol_prev > 0:
                # Tushare daily vol is in Hand (usually), realtime vol is in Hand (usually).
                # Assuming both are Hand.
                lb = (volume / minutes_passed) / (vol_prev / 240)
            else:
                lb = 1.0 # Default
            
            # Turnover not available in realtime quotes usually
            turnover = 0 

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
        if prev_close > 0:
            chg_pct = (current_price - prev_close) / prev_close * 100
        else:
            chg_pct = 0
            
        if chg_pct < 1.0 or chg_pct > 6.0: continue
        
        # --- 评分系统 ---
        # 低位分 (30) + 资金分 (40) + 强度分 (30)
        score_pos = (1 - stock['rpp']) * 30
        score_fund = min(lb / 3.0, 1.0) * 40
        score_mom = min(chg_pct / 5.0, 1.0) * 30
        
        total_score = score_pos + score_fund + score_mom
        
        stock['最新价'] = current_price
        stock['涨跌幅'] = round(chg_pct, 2)
        stock['量比'] = round(lb, 2)
        stock['VWAP'] = round(vwap, 2)
        stock['评分'] = int(total_score)
        
        # 点评生成
        comments = []
        if stock['rpp'] < 0.1: comments.append("极低位")
        elif stock['rpp'] < 0.3: comments.append("相对底部")
        
        if current_price > vwap * 1.01: comments.append("站稳均价")
        if lb > 2.0: comments.append(f"放量{lb:.1f}倍")
        
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

# --- Save to Database ---
try:
    db_path = os.path.join(project_root, 'backend', 'stock_watch.db')
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    def build_db_script_content():
        return """import pandas as pd
import traceback
import datetime
import time

print("开始运行选股策略...")

result_rows = []

def get_rpp(close, high_60, low_60):
    if high_60 == low_60:
        return 0.5
    return (close - low_60) / (high_60 - low_60)

def _safe_vwap(amount, volume, current_price):
    if volume == 0:
        return current_price
    raw_vwap = amount / volume
    if current_price > 0:
        ratio = raw_vwap / current_price
        if 80 < ratio < 120:
            return raw_vwap / 100.0
        elif 0.8 < ratio < 1.2:
            return raw_vwap
    return raw_vwap / 100.0 if raw_vwap > current_price * 50 else raw_vwap

def get_ts_code(code):
    if code.startswith("6"):
        return f"{code}.SH"
    elif code.startswith("0") or code.startswith("3"):
        return f"{code}.SZ"
    elif code.startswith("8") or code.startswith("4"):
        return f"{code}.BJ"
    return code

def fetch_stock_data(code, name, sector):
    if pro is None:
        return None
    try:
        ts_code = get_ts_code(code)
        end_date = datetime.datetime.now().strftime("%Y%m%d")
        start_date = (datetime.datetime.now() - datetime.timedelta(days=120)).strftime("%Y%m%d")
        df = pro.daily(ts_code=ts_code, start_date=start_date, end_date=end_date)
        if df is None or df.empty or len(df) < 20:
            return None
        hist = df.sort_values("trade_date", ascending=True)
        if len(hist) < 20:
            return None
        last_row = hist.iloc[-1]
        close = float(last_row["close"])
        window_60 = hist.tail(60)
        high_60 = window_60["high"].max()
        low_60 = window_60["low"].min()
        rpp = get_rpp(close, high_60, low_60)
        ma20 = window_60["close"].tail(20).mean()
        return {
            "code": code,
            "name": name,
            "sector": sector,
            "close": close,
            "rpp": rpp,
            "ma20": ma20,
            "vol_prev": float(last_row["vol"])
        }
    except Exception:
        return None

try:
    sectors = None
    for _ in range(3):
        try:
            sectors = ak.stock_board_industry_name_em()
            if sectors is not None and not sectors.empty:
                break
        except Exception:
            time.sleep(1)
    if sectors is None or sectors.empty:
        print("无数据：板块为空")
    else:
        sectors = sectors[~sectors["板块名称"].str.contains("ST")]
        top_sectors = sectors.sort_values(by="涨跌幅", ascending=False).head(8)
        sector_list = top_sectors["板块名称"].tolist()
        candidates = []
        for sector in sector_list:
            try:
                cons = ak.stock_board_industry_cons_em(symbol=sector)
                if cons is not None and not cons.empty:
                    for _, row in cons.iterrows():
                        candidates.append({
                            "code": str(row["代码"]).zfill(6),
                            "name": row["名称"],
                            "sector": sector
                        })
                time.sleep(0.5)
            except Exception:
                continue

        analyzed_stocks = []
        total_tasks = len(candidates)
        for i, c in enumerate(candidates):
            res = fetch_stock_data(c["code"], c["name"], c["sector"])
            if res:
                analyzed_stocks.append(res)
            if i % 10 == 0:
                print(f"进度: {i}/{total_tasks}...")
            time.sleep(0.1)

        stock_codes = [s["code"] for s in analyzed_stocks]
        spot_df = pd.DataFrame()
        chunk_size = 50
        for i in range(0, len(stock_codes), chunk_size):
            chunk = stock_codes[i:i+chunk_size]
            try:
                df_chunk = ts.get_realtime_quotes(chunk)
                if df_chunk is not None and not df_chunk.empty:
                    spot_df = pd.concat([spot_df, df_chunk], ignore_index=True)
                time.sleep(0.1)
            except Exception:
                pass

        final_list = []
        if not spot_df.empty and analyzed_stocks:
            spot_map = spot_df.set_index("code").to_dict("index")
            for stock in analyzed_stocks:
                code = stock["code"]
                if code not in spot_map:
                    continue
                real = spot_map[code]
                try:
                    current_price = float(real.get("price", 0))
                    open_price = float(real.get("open", 0))
                    prev_close = float(real.get("pre_close", 0))
                    volume = float(real.get("volume", 0))
                    amount = float(real.get("amount", 0))
                    now = datetime.datetime.now()
                    market_open = now.replace(hour=9, minute=30, second=0, microsecond=0)
                    minutes_passed = (now - market_open).total_seconds() / 60
                    if minutes_passed < 1:
                        minutes_passed = 1
                    if minutes_passed > 240:
                        minutes_passed = 240
                    vol_prev = stock.get("vol_prev", 0)
                    if vol_prev > 0:
                        lb = (volume / minutes_passed) / (vol_prev / 240)
                    else:
                        lb = 1.0
                except Exception:
                    continue

                if current_price == 0:
                    continue
                if stock["rpp"] >= 0.4:
                    continue
                if current_price <= open_price:
                    continue
                if volume > 0:
                    vwap = _safe_vwap(amount, volume, current_price)
                    if current_price < vwap:
                        continue
                    vwap_dev = (current_price - vwap) / vwap
                    if vwap_dev > 0.015:
                        continue
                if lb < 1.2:
                    continue
                if current_price >= prev_close * 1.095:
                    continue
                if prev_close > 0:
                    chg_pct = (current_price - prev_close) / prev_close * 100
                else:
                    chg_pct = 0
                if chg_pct < 1.0 or chg_pct > 6.0:
                    continue

                score_pos = (1 - stock["rpp"]) * 30
                score_fund = min(lb / 3.0, 1.0) * 40
                score_mom = min(chg_pct / 5.0, 1.0) * 30
                total_score = score_pos + score_fund + score_mom

                comments = []
                if stock["rpp"] < 0.1:
                    comments.append("极低位")
                elif stock["rpp"] < 0.3:
                    comments.append("相对底部")
                if volume > 0 and current_price > vwap * 1.01:
                    comments.append("站稳均价")
                if lb > 2.0:
                    comments.append(f"放量{lb:.1f}倍")

                final_list.append({
                    "代码": stock["code"],
                    "名称": stock["name"],
                    "板块": stock["sector"],
                    "最新价": current_price,
                    "涨跌幅": round(chg_pct, 2),
                    "量比": round(lb, 2),
                    "RPP位置": stock["rpp"],
                    "综合评分": int(total_score),
                    "点评": ",".join(comments)
                })

        df = pd.DataFrame(final_list)
        if not df.empty:
            df = df.sort_values(by="综合评分", ascending=False).head(30)
            for _, row in df.iterrows():
                symbol = str(row["代码"]).strip()
                name = str(row["名称"]).strip()
                result_rows.append({
                    "symbol": symbol,
                    "name": name,
                    "score": int(row["综合评分"]),
                    "reason": str(row["点评"])
                })

except Exception as e:
    print("脚本异常:", str(e))
    print(traceback.format_exc())

df = pd.DataFrame(result_rows)
print(f"完成：共 {len(df)} 条结果")
"""

    current_script_content = build_db_script_content()
    
    # 2. Find or Create Screener ID
    # Use name as unique identifier
    cursor.execute("SELECT id FROM stock_screeners WHERE name = '热点追涨'")
    row = cursor.fetchone()
    
    if row:
        screener_id = row[0]
        if current_script_content:
            cursor.execute("""
                UPDATE stock_screeners 
                SET script_content = ?, updated_at = datetime('now')
                WHERE id = ?
            """, (current_script_content, screener_id))
    else:
        # Create a new screener entry
        print("Creating new screener entry in DB...")
        cursor.execute("""
            INSERT INTO stock_screeners (name, description, script_content, is_active, created_at, updated_at)
            VALUES (?, ?, ?, ?, datetime('now'), datetime('now'))
        """, ("热点追涨", "自动追涨热门板块低位个股 (Tushare版)", current_script_content or "", 1))
        screener_id = cursor.lastrowid
    
    conn.commit()
    
    # 3. Save Results
    if not df.empty:
        # Convert DataFrame to JSON
        # Ensure we save the list of dicts
        result_json = df.to_json(orient='records', force_ascii=False)
        count = len(df)
        
        cursor.execute("""
            INSERT INTO screener_results (screener_id, run_at, result_json, count)
            VALUES (?, datetime('now'), ?, ?)
        """, (screener_id, result_json, count))
        conn.commit()
        print(f"✅ 选股结果已同步到数据库 (ScreenerID: {screener_id}, Count: {count})")
        
        # Update last run info
        cursor.execute("""
            UPDATE stock_screeners 
            SET last_run_at = datetime('now'), last_run_status = 'success', last_run_log = 'Found ' || ? || ' stocks'
            WHERE id = ?
        """, (count, screener_id))
        conn.commit()
    else:
        print("⚠️ 无选股结果，跳过数据库保存。")
        cursor.execute("""
            UPDATE stock_screeners 
            SET last_run_at = datetime('now'), last_run_status = 'success', last_run_log = 'No stocks found'
            WHERE id = ?
        """, (screener_id,))
        conn.commit()
        
    conn.close()
    
except Exception as e:
    print(f"❌ Database save failed: {e}")
    traceback.print_exc()

print(f"耗时: {time.time() - start_time:.2f}s")
if _ERROR_COUNT > 0:
    print(f"❗ 本次运行捕获异常次数: {_ERROR_COUNT}")
