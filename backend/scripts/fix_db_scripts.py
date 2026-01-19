import sqlite3
import pandas as pd # Just for syntax highlighting in IDE, not used by script directly
import datetime

db_path = 'backend/stock_watch.db'

# === Script 1: VWAP ===
code_vwap = """import datetime
import pandas as pd
import akshare as ak

symbol = context.get("symbol", "")
if not symbol:
    df = pd.DataFrame([{"提示": "未获取到股票代码"}])
else:
    df_min = pd.DataFrame()
    used_date = None
    for i in range(10):
        d = (pd.Timestamp.now() - pd.Timedelta(days=i)).strftime("%Y-%m-%d")
        start_dt = f"{d} 09:30:00"
        end_dt = f"{d} 15:00:00"
        try:
            tmp = ak.stock_zh_a_hist_min_em(
                symbol=symbol,
                start_date=start_dt,
                end_date=end_dt,
                period="1",
                adjust="",
            )
        except Exception:
            tmp = pd.DataFrame()
        if tmp is not None and not tmp.empty:
            df_min = tmp
            used_date = d
            break

    if df_min is None or df_min.empty:
        df = pd.DataFrame([{"提示": "近 10 天未获取到 1 分钟分时数据", "股票代码": symbol}])
    else:
        df_min = df_min.sort_values("时间").reset_index(drop=True)
        last_row = df_min.iloc[-1]

        if "成交额" in df_min.columns:
            total_amount = float(pd.to_numeric(df_min["成交额"], errors="coerce").fillna(0).sum())
        else:
            total_amount = 0.0

        if "成交量" in df_min.columns:
            total_vol_raw = float(pd.to_numeric(df_min["成交量"], errors="coerce").fillna(0).sum())
        else:
            total_vol_raw = 0.0

        # === 自适应单位检查 (Hand vs Share) ===
        def _safe_float(x):
            try:
                if pd.isna(x): return None
                return float(x)
            except: return None

        current_price = _safe_float(last_row.get("收盘"))
        
        # 默认假设：如果是 A 股，通常接口返回的是“手”
        is_hands = True 
        
        # Ratio Check: Amount / Volume vs Price
        if total_vol_raw > 0 and current_price and current_price > 0:
            raw_vwap = total_amount / total_vol_raw
            ratio = raw_vwap / current_price
            
            # 如果 raw_vwap 约为股价的 100 倍 (80-120)，说明 Volume 是手
            if 80 <= ratio <= 120:
                is_hands = True
            # 如果 raw_vwap 接近股价 (0.8-1.2)，说明 Volume 是股
            elif 0.8 <= ratio <= 1.2:
                is_hands = False
            # 其他情况保持默认
        
        if is_hands:
            total_shares = max(total_vol_raw * 100.0, 1.0)
            vol_unit_desc = "手"
        else:
            total_shares = max(total_vol_raw, 1.0)
            vol_unit_desc = "股"
            
        vwap = float(total_amount / total_shares)
        # ======================================

        if "最高" in df_min.columns:
            day_high = _safe_float(pd.to_numeric(df_min["最高"], errors="coerce").max())
        else:
            day_high = current_price

        if "最低" in df_min.columns:
            day_low = _safe_float(pd.to_numeric(df_min["最低"], errors="coerce").min())
        else:
            day_low = current_price

        amp = None
        if (day_high is not None) and (day_low is not None) and (current_price is not None) and current_price != 0:
            amp = round((day_high - day_low) / abs(current_price) * 100.0, 2)

        out_data = {
            "日期": used_date,
            "时间": pd.to_datetime(last_row.get("时间")).strftime("%Y-%m-%d %H:%M:%S") if last_row.get("时间") is not None else None,
            "最新价": current_price,
            "日内VWAP": round(vwap, 3) if vwap is not None else None,
            "日内最高": day_high,
            "日内最低": day_low,
            "日内振幅%": amp,
            "累计成交量": int(total_vol_raw),
            "单位": vol_unit_desc,
            "累计成交额": total_amount,
        }
        df = pd.DataFrame([out_data])"""

# === Script 2: 5 Min Data ===
code_5min = """import datetime
import pandas as pd
import akshare as ak

# 1. 设置时间范围
# 获取最近 5 天以确保覆盖足够的交易日（即使遇到周末）
today = datetime.datetime.now()
start_date = (today - datetime.timedelta(days=5)).strftime("%Y-%m-%d 09:30:00")
end_date = today.strftime("%Y-%m-%d 15:00:00")

# 2. 调用接口：period='5'
try:
    df = ak.stock_zh_a_hist_min_em(
        symbol=context['symbol'],
        start_date=start_date,
        end_date=end_date,
        period="5",  # 核心修改：改为 5
        adjust=""
    )
    
    if not df.empty:
        # 3. 数据清洗
        # 标准化时间格式
        df["时间"] = pd.to_datetime(df["时间"]).dt.strftime('%Y-%m-%d %H:%M:%S')
        
        # 按时间排序
        df = df.sort_values(by="时间")
        
        # 4. (可选) 计算 5分钟级别的技术指标
        # 例如：计算 5分钟级别的 MA20 (短线生命线)
        df['MA5'] = df['收盘'].rolling(window=5).mean()
        df['MA20'] = df['收盘'].rolling(window=20).mean()
        
        # === 单位自适应修复 ===
        # 检查是否需要转换单位 (将手转为股，统一输出)
        if "成交量" in df.columns and "成交额" in df.columns and "收盘" in df.columns:
            # 取最后一行非空数据进行检查
            last_valid = df.iloc[-1]
            vol = float(last_valid["成交量"])
            amt = float(last_valid["成交额"])
            price = float(last_valid["收盘"])
            
            if vol > 0 and price > 0:
                ratio = (amt / vol) / price
                # 如果 ratio ~ 100，说明 vol 是手，需要 * 100 转为股
                if 80 <= ratio <= 120:
                    df["成交量"] = df["成交量"] * 100
        # ====================
        
        # 截取最近的 48 个点 (即最近 4 小时的走势，5 * 48 = 240分钟)
        df = df.tail(48)
        
        # 只保留需要的字段，并重命名以消除歧义
        df = df[["时间", "开盘", "最高", "最低", "收盘", "成交量", "MA5", "MA20"]]
        df.rename(columns={"成交量": "成交量(股)"}, inplace=True)
        
except:
    df = pd.DataFrame()"""

try:
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Update VWAP script
    print("Updating '今日分时 VWAP/日内强弱'...")
    cursor.execute("UPDATE indicator_definitions SET python_code = ? WHERE name = ?", (code_vwap, '今日分时 VWAP/日内强弱'))
    if cursor.rowcount == 0:
        print("WARNING: '今日分时 VWAP/日内强弱' not found!")
    else:
        print("Success.")

    # Update 5min script
    print("Updating '5 分钟数据（分时历史）'...")
    cursor.execute("UPDATE indicator_definitions SET python_code = ? WHERE name = ?", (code_5min, '5 分钟数据（分时历史）'))
    if cursor.rowcount == 0:
        print("WARNING: '5 分钟数据（分时历史）' not found!")
    else:
        print("Success.")
        
    conn.commit()
    conn.close()
    print("Database updated successfully.")
except Exception as e:
    print(f"Error updating database: {e}")
