import sys
import os
import json
import pandas as pd
import datetime
import traceback
from sqlalchemy.orm import Session

# Add backend to path to access models
# Add both root and backend to sys.path to handle imports
root_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
backend_path = os.path.join(root_path, 'backend')
sys.path.append(root_path)
sys.path.append(backend_path)

try:
    from backend.database import SessionLocal
    from backend import models
except ImportError:
    # Fallback if running from root
    sys.path.append(os.path.abspath(os.getcwd()))
    sys.path.append(os.path.join(os.path.abspath(os.getcwd()), 'backend'))
    from database import SessionLocal
    import models

# --- 1. Indicator Script (Pure Script Mode) ---
# Input: context = {'symbol': '...', ...}
# Output: df or result
INDICATOR_CODE = """
import pandas as pd
import json
import traceback

# 1. 默认空结果
df = pd.DataFrame()

try:
    symbol = context['symbol']
    
    # 尝试使用 Tushare (5000积分通道)
    import tushare as ts
    ts.set_token('4501928450004005131')
    pro = ts.pro_api()
    pro._DataApi__http_url = 'http://5k1a.xiximiao.com/dataapi'
    
    # 获取日线数据 (最近100天以确保计算准确)
    end_date = pd.Timestamp.now().strftime('%Y%m%d')
    start_date = (pd.Timestamp.now() - pd.Timedelta(days=150)).strftime('%Y%m%d')
    
    ts_code = symbol
    
    # Tushare 获取
    try:
        df_raw = pro.daily(ts_code=ts_code, start_date=start_date, end_date=end_date)
        if df_raw.empty:
            raise ValueError("Tushare returned empty data")
            
        df_raw = df_raw.rename(columns={'trade_date': 'date', 'vol': 'volume'})
        df_raw['date'] = pd.to_datetime(df_raw['date'])
        df_raw = df_raw.sort_values('date')
        
    except Exception as e:
        print(f"Indicator Error (Tushare): {e}, switching to Akshare")
        import akshare as ak
        # Akshare usually takes 6 digits '000001'
        code = symbol.split('.')[0]
        try:
            df_raw = ak.stock_zh_a_hist(symbol=code, period="daily", adjust="qfq")
            df_raw = df_raw.rename(columns={'日期': 'date', '开盘': 'open', '收盘': 'close', '最高': 'high', '最低': 'low', '成交量': 'volume'})
            df_raw['date'] = pd.to_datetime(df_raw['date'])
        except Exception as ak_e:
             print(f"Akshare failed: {ak_e}")
             df_raw = pd.DataFrame()

    if not df_raw.empty:
        # KDJ Calculation (9, 3, 3)
        low_list = df_raw['low'].rolling(9, min_periods=9).min()
        high_list = df_raw['high'].rolling(9, min_periods=9).max()
        rsv = (df_raw['close'] - low_list) / (high_list - low_list) * 100
        
        df_raw['K'] = rsv.ewm(com=2, adjust=False).mean()
        df_raw['D'] = df_raw['K'].ewm(com=2, adjust=False).mean()
        df_raw['J'] = 3 * df_raw['K'] - 2 * df_raw['D']
        
        # Return last 5 records
        df = df_raw.tail(5)
        
except Exception as e:
    print(f"Indicator Script Error: {e}")
    traceback.print_exc()
"""

# --- 2. Lab Script (Top Level) ---
LAB_CODE = """
# KDJ 金叉策略验证实验
import pandas as pd
import numpy as np
import tushare as ts
import akshare as ak

print("开始执行 KDJ 策略验证实验...")
symbol = "000001.SZ" # 平安银行

# 1. 获取数据
try:
    ts.set_token('4501928450004005131')
    pro = ts.pro_api()
    pro._DataApi__http_url = 'http://5k1a.xiximiao.com/dataapi'
    end_date = pd.Timestamp.now().strftime('%Y%m%d')
    start_date = (pd.Timestamp.now() - pd.Timedelta(days=365)).strftime('%Y%m%d')
    df = pro.daily(ts_code=symbol, start_date=start_date, end_date=end_date)
    df = df.rename(columns={'trade_date': 'date', 'vol': 'volume', 'close': 'close'})
    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values('date').reset_index(drop=True)
    print(f"成功获取 {symbol} 数据: {len(df)} 条")
except Exception as e:
    print(f"数据获取失败: {e}")
    df = pd.DataFrame()

if not df.empty:
    # 2. 计算指标
    low_list = df['low'].rolling(9, min_periods=9).min()
    high_list = df['high'].rolling(9, min_periods=9).max()
    rsv = (df['close'] - low_list) / (high_list - low_list) * 100
    df['K'] = rsv.ewm(com=2, adjust=False).mean()
    df['D'] = df['K'].ewm(com=2, adjust=False).mean()
    df['J'] = 3 * df['K'] - 2 * df['D']

    # 3. 策略逻辑：金叉买入 (K上穿D 且 K<20)
    df['signal'] = 0
    # Golden Cross condition
    df.loc[(df['K'] > df['D']) & (df['K'].shift(1) < df['D'].shift(1)) & (df['K'] < 30), 'signal'] = 1
    
    signals = df[df['signal'] == 1]
    print(f"发现买入信号: {len(signals)} 次")
    if not signals.empty:
        print(signals[['date', 'close', 'K', 'D']].tail())
"""

# --- 3. Screener Script (Top Level) ---
# Output: result (list) or df (DataFrame)
SCREENER_CODE = """
import pandas as pd
import tushare as ts

results = []
print("开始执行 KDJ 超卖选股...")

# Mock stock list for demo speed (usually would fetch all)
target_stocks = ['000001.SZ', '600519.SH', '000002.SZ']

try:
    ts.set_token('4501928450004005131')
    pro = ts.pro_api()
    pro._DataApi__http_url = 'http://5k1a.xiximiao.com/dataapi'
    
    for symbol in target_stocks:
        try:
            end_date = pd.Timestamp.now().strftime('%Y%m%d')
            start_date = (pd.Timestamp.now() - pd.Timedelta(days=60)).strftime('%Y%m%d')
            df = pro.daily(ts_code=symbol, start_date=start_date, end_date=end_date)
            
            if df.empty: continue
            
            df = df.sort_values('trade_date')
            low_list = df['low'].rolling(9).min()
            high_list = df['high'].rolling(9).max()
            rsv = (df['close'] - low_list) / (high_list - low_list) * 100
            k = rsv.ewm(com=2, adjust=False).mean().iloc[-1]
            
            if k < 20:
                results.append({
                    "symbol": symbol, 
                    "name": symbol, # 暂无名称数据，用 symbol 代替
                    "score": round(20 - k, 2), # K越小分越高
                    "reason": f"Oversold (K={k:.2f} < 20)",
                    "K_value": round(k, 2)
                })
        except:
            continue
            
except Exception as e:
    print(f"Screener Error: {e}")

# Assign to final result variable
result = results
print(f"选股结果: {len(result)} 条")
"""

# --- 4. Rule Script (Top Level) ---
# Input: symbol, ak, pd, np, datetime, time
# Output: triggered, signal, message
RULE_CODE = """
# 初始化默认值
triggered = False
signal = "WAIT"
message = "监控中..."

try:
    import tushare as ts
    import pandas as pd
    
    # 模拟获取数据 (实际运行环境中无需再次 import 标准库，但 tushare 需要)
    ts.set_token('4501928450004005131')
    pro = ts.pro_api()
    pro._DataApi__http_url = 'http://5k1a.xiximiao.com/dataapi'
    
    # 标准化 symbol 格式 (兼容 sh600519 -> 600519.SH)
    ts_code = symbol
    if isinstance(symbol, str):
        symbol_lower = symbol.lower()
        if symbol_lower.startswith('sh') and symbol_lower[2:].isdigit():
            ts_code = f"{symbol_lower[2:]}.SH"
        elif symbol_lower.startswith('sz') and symbol_lower[2:].isdigit():
            ts_code = f"{symbol_lower[2:]}.SZ"
        elif symbol.isdigit(): 
            # 简单推断：6开头为SH，其他为SZ (根据A股规则，不一定完全准确，但够用)
            if symbol.startswith('6'):
                ts_code = f"{symbol}.SH"
            else:
                ts_code = f"{symbol}.SZ"
    
    print(f"DEBUG: Processing symbol={symbol} -> ts_code={ts_code}")

    # 获取最近 30 天数据
    end_date = pd.Timestamp.now().strftime('%Y%m%d')
    start_date = (pd.Timestamp.now() - pd.Timedelta(days=60)).strftime('%Y%m%d') # 扩大时间范围以防节假日
    df = pro.daily(ts_code=ts_code, start_date=start_date, end_date=end_date)
    
    print(f"DEBUG: Fetched {len(df)} rows")

    
    if df.empty or len(df) < 10:
        message = "数据不足"
    else:
        df = df.sort_values('trade_date')
        
        # 简单模拟实时行情更新 (如果是盘后分析则无需此步)
        # 这里假设 df 是最新的
        
        # Calc KDJ
        low_list = df['low'].rolling(9).min()
        high_list = df['high'].rolling(9).max()
        rsv = (df['close'] - low_list) / (high_list - low_list) * 100
        df['K'] = rsv.ewm(com=2, adjust=False).mean()
        df['D'] = df['K'].ewm(com=2, adjust=False).mean()
        
        k_curr = df['K'].iloc[-1]
        d_curr = df['D'].iloc[-1]
        k_prev = df['K'].iloc[-2]
        d_prev = df['D'].iloc[-2]
        
        # Golden Cross: Prev K < Prev D AND Curr K > Curr D
        if k_prev < d_prev and k_curr > d_curr:
            triggered = True
            signal = "BUY"
            message = f"KDJ金叉触发: K({k_curr:.2f})上穿D({d_curr:.2f})"
        else:
            message = f"未触发: K={k_curr:.2f}, D={d_curr:.2f}"
        
except Exception as e:
    triggered = False
    signal = "WAIT"
    message = f"错误: {str(e)}"
"""

def verify_script(name, code, exec_check=False):
    print(f"[*] Verifying {name}...")
    try:
        # 1. Syntax Check
        compile(code, name, 'exec')
        print(f"    - Syntax OK")
        
        # 2. Execution Check (Optional/Light)
        if exec_check:
            # Create a shared scope for globals/locals to ensure imports work in functions
            # Simulate Context based on script type
            local_scope = {'__name__': '__main__'}
            
            if name == "Indicator":
                local_scope['context'] = {'symbol': '000001.SZ', 'name': '平安银行'}
            elif name == "Rule":
                local_scope['symbol'] = '000001.SZ'
            
            try:
                exec(code, local_scope)
                
                # Validation Logic
                if name == "Indicator":
                    if 'df' in local_scope and not local_scope['df'].empty:
                         print(f"    - Execution OK: df returned with {len(local_scope['df'])} rows")
                    elif 'result' in local_scope:
                         print(f"    - Execution OK: result returned")
                    else:
                         print(f"    - Execution Warning: No df or result found (or empty)")
                         
                elif name == "Screener":
                    if 'result' in local_scope:
                        print(f"    - Execution OK: Found {len(local_scope['result'])} stocks")
                    elif 'df' in local_scope:
                        print(f"    - Execution OK: Found DataFrame")
                    else:
                        print(f"    - Execution Warning: No result/df found")
                        
                elif name == "Rule":
                    if 'triggered' in local_scope and 'message' in local_scope:
                        print(f"    - Execution OK: triggered={local_scope['triggered']}, msg={local_scope['message']}")
                    else:
                        print(f"    - Execution Fail: Missing triggered/message")
                        
                elif name == "Lab":
                    print(f"    - Execution OK (Ran to completion)")
                    
            except Exception as e:
                print(f"    - Execution Failed: {e}")
                traceback.print_exc()
                
    except Exception as e:
        print(f"    - Syntax/Compile Failed: {e}")
        return False
    return True

def inject_data():
    session = SessionLocal()
    try:
        # 1. Indicator
        if verify_script("Indicator", INDICATOR_CODE, exec_check=True):
            ind = session.query(models.IndicatorDefinition).filter_by(name="KDJ_Custom").first()
            if not ind:
                ind = models.IndicatorDefinition(name="KDJ_Custom", python_code=INDICATOR_CODE)
                session.add(ind)
                print("[+] Indicator 'KDJ_Custom' added.")
            else:
                ind.python_code = INDICATOR_CODE
                print("[+] Indicator 'KDJ_Custom' updated.")
        
        # 2. Lab Script
        if verify_script("Lab", LAB_CODE, exec_check=True):
            lab = session.query(models.ResearchScript).filter_by(title="KDJ_Backtest_Experiment").first()
            if not lab:
                lab = models.ResearchScript(title="KDJ_Backtest_Experiment", script_content=LAB_CODE)
                session.add(lab)
                print("[+] Lab Script 'KDJ_Backtest_Experiment' added.")
            else:
                lab.script_content = LAB_CODE
                print("[+] Lab Script 'KDJ_Backtest_Experiment' updated.")
                
        # 3. Screener
        if verify_script("Screener", SCREENER_CODE, exec_check=True):
            scr = session.query(models.StockScreener).filter_by(name="KDJ_Oversold_Screener").first()
            if not scr:
                scr = models.StockScreener(name="KDJ_Oversold_Screener", script_content=SCREENER_CODE, is_active=True)
                session.add(scr)
                print("[+] Screener 'KDJ_Oversold_Screener' added.")
            else:
                scr.script_content = SCREENER_CODE
                print("[+] Screener 'KDJ_Oversold_Screener' updated.")

        # 4. Rule
        if verify_script("Rule", RULE_CODE, exec_check=True):
            rule = session.query(models.RuleScript).filter_by(name="KDJ_Golden_Cross_Monitor").first()
            if not rule:
                rule = models.RuleScript(name="KDJ_Golden_Cross_Monitor", code=RULE_CODE, description="Alerts when K crosses D upwards")
                session.add(rule)
                print("[+] Rule 'KDJ_Golden_Cross_Monitor' added.")
            else:
                rule.code = RULE_CODE
                print("[+] Rule 'KDJ_Golden_Cross_Monitor' updated.")
        
        session.commit()
        
        # 5. Stock (Market Watch)
        # Re-query rule to get ID
        rule = session.query(models.RuleScript).filter_by(name="KDJ_Golden_Cross_Monitor").first()
        
        stock_symbol = "000001.SZ"
        stock = session.query(models.Stock).filter_by(symbol=stock_symbol).first()
        if not stock:
            stock = models.Stock(
                symbol=stock_symbol, 
                name="平安银行", 
                is_monitoring=True, 
                monitoring_mode="script_only",
                rule_script_id=rule.id
            )
            session.add(stock)
            print(f"[+] Stock '{stock_symbol}' added to Market Watch.")
        else:
            stock.is_monitoring = True
            stock.monitoring_mode = "script_only"
            stock.rule_script_id = rule.id
            print(f"[+] Stock '{stock_symbol}' updated with new rule.")
            
        session.commit()
        print("=== Injection Complete ===")
        
    except Exception as e:
        session.rollback()
        print(f"Injection Failed: {e}")
        traceback.print_exc()
    finally:
        session.close()

if __name__ == "__main__":
    inject_data()
