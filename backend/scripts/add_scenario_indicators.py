import sys
import os
import json

# Add backend directory to sys.path
current_dir = os.path.dirname(os.path.abspath(__file__))
backend_dir = os.path.dirname(current_dir)
sys.path.append(backend_dir)

from database import SessionLocal
from models import IndicatorDefinition

def add_indicators():
    db = SessionLocal()
    
    indicators = [
        {
            "name": "场景-超短线 (5分钟/日内)",
            "code": r"""
symbol = context.get("symbol", "")
if not symbol:
    df = pd.DataFrame([{"提示": "未获取到股票代码"}])
else:
    try:
        # 1. Fetch 5-min data (approx 2 days)
        # Using stock_zh_a_hist_min_em is reliable for recent data
        df = ak.stock_zh_a_hist_min_em(symbol=symbol, period="5", adjust="qfq")
        df = df.tail(48) # Last 4 hours (48 * 5min)
    except Exception:
        df = pd.DataFrame()

    if df.empty:
        df = pd.DataFrame([{"提示": "未获取到5分钟数据", "股票代码": symbol}])
    else:
        # Standardize columns
        df = df.rename(columns={"时间": "日期", "成交量": "Volume", "收盘": "Close"})
        close = pd.to_numeric(df["Close"], errors='coerce')
        vol = pd.to_numeric(df["Volume"], errors='coerce')
        
        # 1. MA Trend (5 vs 20 on 5min chart)
        ma5 = close.rolling(5).mean()
        ma20 = close.rolling(20).mean()
        
        # 2. MACD (12, 26, 9)
        macd, signal, hist = talib.MACD(close, fastperiod=12, slowperiod=26, signalperiod=9)
        
        # 3. Volume Ratio (vs MA20 Volume)
        vol_ma20 = vol.rolling(20).mean()
        vol_ratio = vol / vol_ma20
        
        # Get latest snapshot
        last_idx = df.index[-1]
        c = close.iloc[-1]
        m5 = ma5.iloc[-1]
        m20 = ma20.iloc[-1]
        v_r = vol_ratio.iloc[-1]
        md = macd.iloc[-1]
        sig = signal.iloc[-1]
        
        trend = "看涨" if m5 > m20 else "看跌"
        vol_status = "放量" if v_r > 1.5 else ("缩量" if v_r < 0.7 else "平量")
        macd_status = "金叉区" if md > sig else "死叉区"
        
        out = {
            "时间": df.iloc[-1]["日期"],
            "现价": c,
            "5分趋势": trend,
            "均线状态": "多头" if c > m5 > m20 else "空头",
            "量能": f"{vol_status}({v_r:.1f}x)",
            "MACD信号": macd_status
        }
        df = pd.DataFrame([out])
"""
        },
        {
            "name": "场景-中短线 (日线波段)",
            "code": r"""
symbol = context.get("symbol", "")
if not symbol:
    df = pd.DataFrame([{"提示": "未获取到股票代码"}])
else:
    import datetime
    end_dt = pd.Timestamp.now()
    start_dt = end_dt - pd.Timedelta(days=120) # 4 months
    start_str = start_dt.strftime("%Y%m%d")
    end_str = end_dt.strftime("%Y%m%d")
    
    try:
        df = ak.stock_zh_a_hist(symbol=symbol, period="daily", start_date=start_str, end_date=end_str, adjust="qfq")
    except:
        df = pd.DataFrame()
        
    if df.empty:
        df = pd.DataFrame([{"提示": "无日线数据"}])
    else:
        close = pd.to_numeric(df["收盘"])
        high = pd.to_numeric(df["最高"])
        low = pd.to_numeric(df["最低"])
        
        # MA
        ma5 = close.rolling(5).mean()
        ma10 = close.rolling(10).mean()
        ma20 = close.rolling(20).mean()
        
        # KDJ
        low_min = low.rolling(9).min()
        high_max = high.rolling(9).max()
        rsv = (close - low_min) / (high_max - low_min) * 100
        k = rsv.ewm(alpha=1/3, adjust=False).mean()
        d = k.ewm(alpha=1/3, adjust=False).mean()
        j = 3 * k - 2 * d
        
        # RSI
        rsi = talib.RSI(close, timeperiod=14)
        
        c = close.iloc[-1]
        m5 = ma5.iloc[-1]
        m20 = ma20.iloc[-1]
        j_val = j.iloc[-1]
        rsi_val = rsi.iloc[-1]
        
        # Signals
        ma_trend = "多头排列" if m5 > ma10.iloc[-1] > m20 else ("空头排列" if m5 < ma10.iloc[-1] < m20 else "纠缠")
        kdj_sig = "超买" if j_val > 100 else ("超卖" if j_val < 0 else "中性")
        rsi_sig = "强势" if rsi_val > 60 else ("弱势" if rsi_val < 40 else "盘整")
        
        out = {
            "日期": df.iloc[-1]["日期"],
            "收盘": c,
            "均线趋势": ma_trend,
            "KDJ状态": f"{kdj_sig}(J={j_val:.1f})",
            "RSI状态": f"{rsi_sig}({rsi_val:.1f})",
            "建议": "持有/做多" if ma_trend == "多头排列" and rsi_val > 50 else "观望/减仓"
        }
        df = pd.DataFrame([out])
"""
        },
        {
            "name": "场景-中长线 (周线趋势)",
            "code": r"""
symbol = context.get("symbol", "")
if not symbol:
    df = pd.DataFrame([{"提示": "未获取到股票代码"}])
else:
    # Get 2 years of daily data and resample
    import datetime
    end_dt = pd.Timestamp.now()
    start_dt = end_dt - pd.Timedelta(days=730)
    start_str = start_dt.strftime("%Y%m%d")
    end_str = end_dt.strftime("%Y%m%d")
    
    try:
        df = ak.stock_zh_a_hist(symbol=symbol, period="daily", start_date=start_str, end_date=end_str, adjust="qfq")
    except:
        df = pd.DataFrame()
        
    if df.empty:
        df = pd.DataFrame([{"提示": "无数据"}])
    else:
        df["日期"] = pd.to_datetime(df["日期"])
        df.set_index("日期", inplace=True)
        # Resample to Weekly (W-FRI)
        weekly = df.resample("W-FRI").agg({
            "收盘": "last", "开盘": "first", "最高": "max", "最低": "min", "成交量": "sum"
        }).dropna()
        
        if weekly.empty:
            df = pd.DataFrame([{"提示": "周线生成失败"}])
        else:
            close = weekly["收盘"]
            
            # Weekly MA
            ma10 = close.rolling(10).mean() # ~MA50 daily
            ma30 = close.rolling(30).mean() # ~MA150 daily
            
            # Weekly MACD
            macd, signal, hist = talib.MACD(close, fastperiod=12, slowperiod=26, signalperiod=9)
            
            c = close.iloc[-1]
            m10 = ma10.iloc[-1]
            m30 = ma30.iloc[-1]
            md = macd.iloc[-1]
            
            trend = "长期向上" if m10 > m30 else "长期向下/震荡"
            pos = "均线上方" if c > m10 else "均线压制"
            macd_trend = "红柱增强" if hist.iloc[-1] > hist.iloc[-2] > 0 else ("绿柱增强" if hist.iloc[-1] < hist.iloc[-2] < 0 else "震荡")
            
            out = {
                "周线日期": weekly.index[-1].strftime("%Y-%m-%d"),
                "收盘": c,
                "长期趋势": trend,
                "位置": pos,
                "周MACD": macd_trend
            }
            df = pd.DataFrame([out])
"""
        },
        {
            "name": "场景-长线 (月线与估值)",
            "code": r"""
symbol = context.get("symbol", "")
if not symbol:
    df = pd.DataFrame([{"提示": "未获取到股票代码"}])
else:
    # 1. Fetch Daily for Monthly Resample (5 years)
    import datetime
    end_dt = pd.Timestamp.now()
    start_dt = end_dt - pd.Timedelta(days=365*5)
    start_str = start_dt.strftime("%Y%m%d")
    end_str = end_dt.strftime("%Y%m%d")
    
    try:
        df = ak.stock_zh_a_hist(symbol=symbol, period="daily", start_date=start_str, end_date=end_str, adjust="qfq")
    except:
        df = pd.DataFrame()
        
    if df.empty:
        df = pd.DataFrame([{"提示": "无数据"}])
    else:
        df["日期"] = pd.to_datetime(df["日期"])
        df.set_index("日期", inplace=True)
        monthly = df.resample("M").agg({"收盘": "last"}).dropna()
        
        # Monthly MA
        close = monthly["收盘"]
        ma20 = close.rolling(20).mean() # 20-month line (Bull/Bear line)
        
        # 2. Valuation (PE/PB) - Try to fetch latest
        # ak.stock_a_indicator_lg(symbol="...")
        try:
            # This API returns all history, heavy. Just get recent? No param for date.
            # Alternative: stock_zh_a_spot_em has PE-TTM
            spot = ak.stock_zh_a_spot_em()
            spot_row = spot[spot["代码"] == symbol]
            if not spot_row.empty:
                pe_ttm = spot_row.iloc[0]["市盈率-动态"]
                pb = spot_row.iloc[0]["市净率"]
                total_mv = spot_row.iloc[0]["总市值"]
            else:
                pe_ttm = None
                pb = None
                total_mv = None
        except:
            pe_ttm = None
            pb = None
            total_mv = None
            
        # Calc percentile (approximate with close price position as proxy for valuation if PE missing)
        max_p = close.max()
        min_p = close.min()
        cur_p = close.iloc[-1]
        pos_pct = (cur_p - min_p) / (max_p - min_p) * 100
        
        m20_val = ma20.iloc[-1] if not pd.isna(ma20.iloc[-1]) else 0
        trend = "牛市结构" if cur_p > m20_val else "熊市结构"
        
        out = {
            "月线日期": monthly.index[-1].strftime("%Y-%m-%d"),
            "月线趋势": trend,
            "5年位置": f"{pos_pct:.1f}% (0=最低, 100=最高)",
            "PE(TTM)": pe_ttm if pe_ttm else "N/A",
            "PB": pb if pb else "N/A",
            "总市值(亿)": round(total_mv / 1e8, 2) if total_mv else "N/A"
        }
        df = pd.DataFrame([out])
"""
        },
        {
            "name": "场景-超长线 (历史大底)",
            "code": r"""
symbol = context.get("symbol", "")
if not symbol:
    df = pd.DataFrame([{"提示": "未获取到股票代码"}])
else:
    # Fetch as much history as reasonable (since 2010 or listing)
    try:
        # adjust='qfq' to see real returns
        df = ak.stock_zh_a_hist(symbol=symbol, period="daily", start_date="20100101", adjust="qfq")
    except:
        df = pd.DataFrame()
        
    if df.empty:
        df = pd.DataFrame([{"提示": "无数据"}])
    else:
        close = pd.to_numeric(df["收盘"])
        
        # All-time High/Low
        ath = close.max()
        atl = close.min()
        cur = close.iloc[-1]
        
        # Drawdown from ATH
        dd = (cur - ath) / ath * 100
        
        # Gain from ATL
        gain = (cur - atl) / atl * 100
        
        # Annualized Volatility (approx)
        pct_change = close.pct_change()
        volatility = pct_change.std() * (252 ** 0.5) * 100
        
        # CAGR (Compound Annual Growth Rate)
        days = (pd.to_datetime(df.iloc[-1]["日期"]) - pd.to_datetime(df.iloc[0]["日期"])).days
        years = days / 365.25
        if years > 0:
            cagr = (cur / close.iloc[0]) ** (1 / years) - 1
            cagr_pct = cagr * 100
        else:
            cagr_pct = 0
            
        out = {
            "历史最高": ath,
            "历史最低": atl,
            "距最高回撤": f"{dd:.1f}%",
            "距最低涨幅": f"{gain:.1f}%",
            "年化波动率": f"{volatility:.1f}%",
            "年化收益(CAGR)": f"{cagr_pct:.1f}%",
            "上市年数": f"{years:.1f}年"
        }
        df = pd.DataFrame([out])
"""
        }
    ]

    for ind in indicators:
        # Check existence
        existing = db.query(IndicatorDefinition).filter(IndicatorDefinition.name == ind["name"]).first()
        if existing:
            print(f"Updating existing indicator: {ind['name']}")
            existing.python_code = ind["code"]
            # Reset akshare_api to generic safe default if needed
            existing.akshare_api = "stock_zh_a_hist"
        else:
            print(f"Creating new indicator: {ind['name']}")
            new_ind = IndicatorDefinition(
                name=ind["name"],
                akshare_api="stock_zh_a_hist", # Dummy, code handles fetch
                params_json=json.dumps({"symbol": "{symbol}"}),
                post_process_json=None,
                python_code=ind["code"],
                is_pinned=False
            )
            db.add(new_ind)
    
    try:
        db.commit()
        print("All scenario indicators added/updated successfully.")
    except Exception as e:
        print(f"Error committing to DB: {e}")
        db.rollback()
    finally:
        db.close()

if __name__ == "__main__":
    add_indicators()
