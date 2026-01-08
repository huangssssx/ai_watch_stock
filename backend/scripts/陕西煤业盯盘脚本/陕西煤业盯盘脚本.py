# --------------------------------------------------------------------------------
# 脚本名称：陕西煤业（601225）硬规则盯盘策略（完整版）
# 策略来源：明日操作核心指标与行动指南（空仓专属）
# 核心优化：补充期货/资金流向/时间维度/补仓逻辑，匹配指南阈值
# 依赖库：akshare >= 1.10.0, pandas >= 2.0.0 (需提前安装：pip install akshare pandas)
# --------------------------------------------------------------------------------
try:
    ak
except NameError:
    import os
    import sys
    backend_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    if backend_dir not in sys.path:
        sys.path.insert(0, backend_dir)
    from pymr_compat import ensure_py_mini_racer
    ensure_py_mini_racer()
    import akshare as ak

try:
    pd
except NameError:
    import pandas as pd

try:
    datetime
except NameError:
    import datetime

# 1. 初始化核心变量
triggered = False
signal = "WAIT"
message = "监控中：等待行情更新..."
position_held = False  # 标记是否已持仓（空仓初始为False）

# 策略参数配置（严格匹配操作指南）
TARGET_SYMBOL = "601225"      # 陕西煤业
SUPPORT_PRICE = 22.0          # 刚性支撑位/止损位
BEST_BUY_LOW = 22.0           # 最佳买入区间下沿
BEST_BUY_HIGH = 22.5          # 最佳买入区间上沿
SUB_BUY_HIGH = 23.0           # 次优买入区间上沿
TAKE_PROFIT_1 = 26.0          # 第一止盈位
TAKE_PROFIT_2 = 28.0          # 第二止盈位
STOP_LOSS_PCT = -3.0          # 单日跌幅止损阈值 (%)
INDEX_BEST_PCT = 2.0          # 最佳入场板块涨幅阈值 (%)
INDEX_SUB_PCT = 3.0           # 次优入场板块涨幅阈值 (%)
INDEX_RISK_PCT = -5.0         # 板块系统性风险阈值 (%)
FUTURE_RISK_PCT = -3.0        # 期货下跌风险阈值 (%)
TURNOVER_ENTRY = 0.8          # 买入换手率阈值 (%)
TURNOVER_ADD = 0.6            # 补仓缩量企稳换手率阈值 (%)
MAIN_FLOW_THRESHOLD = 5000    # 主力净流入阈值 (万元)
VOLUME_THRESHOLD = 80000      # 成交额阈值 (万元)
MONITOR_MINUTES = 30          # 开盘监控窗口 (分钟)

# 2. 辅助函数：获取当前交易时间（判断开盘30分钟）
def get_trading_minute():
    """返回当前处于开盘后的分钟数（9:30开盘为0）"""
    now = datetime.datetime.now()
    if now.hour < 9 or (now.hour == 9 and now.minute < 30):
        return -1  # 未开盘
    elif now.hour >= 15:
        return 390  # 收盘
    # 计算开盘后分钟数
    total_min = (now.hour - 9) * 60 + (now.minute - 30)
    # 扣除午休（11:30-13:00）
    if total_min >= 120 and total_min < 210:
        total_min = 120
    elif total_min >= 210:
        total_min -= 90
    return total_min

# 3. 辅助函数：获取期货数据（焦煤/焦炭主力合约）
def get_futures_data():
    """获取焦煤、焦炭主力合约涨跌幅"""
    try:
        fetcher = None
        if hasattr(ak, "futures_zh_spot_em"):
            fetcher = ak.futures_zh_spot_em
        if fetcher is None:
            return {"焦煤主力": 0.0, "焦炭主力": 0.0, "期货红盘": True, "期货风险": False}

        df_future = fetcher()
        if df_future is None or getattr(df_future, "empty", True):
            return {"焦煤主力": 0.0, "焦炭主力": 0.0, "期货红盘": True, "期货风险": False}

        code_col = None
        for c in ["代码", "symbol", "合约代码", "contract", "品种代码"]:
            if c in df_future.columns:
                code_col = c
                break
        pct_col = None
        for c in ["涨跌幅", "涨跌幅%", "pct", "PCT", "change_pct", "涨跌幅(%)"]:
            if c in df_future.columns:
                pct_col = c
                break
        if code_col is None or pct_col is None:
            return {"焦煤主力": 0.0, "焦炭主力": 0.0, "期货红盘": True, "期货风险": False}

        jm_row = df_future[df_future[code_col].astype(str) == "JM.m"]
        j_row = df_future[df_future[code_col].astype(str) == "J.m"]
        
        jm_pct = 0.0
        j_pct = 0.0
        if not jm_row.empty:
            jm_pct = pd.to_numeric(jm_row.iloc[0][pct_col], errors='coerce')
        if not j_row.empty:
            j_pct = pd.to_numeric(j_row.iloc[0][pct_col], errors='coerce')
        return {
            "焦煤主力": jm_pct,
            "焦炭主力": j_pct,
            "期货红盘": jm_pct >= 0 and j_pct >= 0,
            "期货风险": jm_pct <= FUTURE_RISK_PCT or j_pct <= FUTURE_RISK_PCT
        }
    except Exception as e:
        print(f"期货数据获取失败: {e}")
        return {"焦煤主力": 0.0, "焦炭主力": 0.0, "期货红盘": True, "期货风险": False}

# 4. 辅助函数：获取主力资金流向（陕西煤业）
def get_main_flow(symbol, spot_row=None):
    """获取个股主力资金净流入（万元）和成交额（万元）"""
    try:
        symbol_code = str(symbol).strip()
        if symbol_code.startswith(("sh", "sz", "bj")):
            symbol_code = symbol_code[2:]
        symbol_code = symbol_code.zfill(6)

        volume_wan = 0.0
        try:
            if spot_row is not None and (not getattr(spot_row, "empty", True)) and ("成交额" in spot_row.columns):
                volume_yuan = pd.to_numeric(spot_row.iloc[0]["成交额"], errors="coerce")
                if pd.notna(volume_yuan):
                    volume_wan = float(volume_yuan) / 10000.0
        except Exception:
            pass

        main_in_wan = 0.0
        if main_in_wan == 0.0 and hasattr(ak, "stock_individual_fund_flow"):
            try:
                df_hist = ak.stock_individual_fund_flow(symbol_code)
                if df_hist is not None and (not df_hist.empty) and ("主力净流入-净额" in df_hist.columns):
                    net_yuan = pd.to_numeric(df_hist.iloc[0]["主力净流入-净额"], errors="coerce")
                    if pd.notna(net_yuan):
                        main_in_wan = float(net_yuan) / 10000.0
            except Exception as e:
                print(f"资金流向(日频)获取失败: {e}")

        return {"主力净流入": main_in_wan, "成交额": volume_wan}
    except Exception as e:
        print(f"资金流向获取失败: {e}")
    return {"主力净流入": 0.0, "成交额": 0.0}

try:
    symbol
except NameError:
    symbol = TARGET_SYMBOL

try:
    globals().update(locals())
    # 5. 预处理 Symbol
    current_symbol_code = symbol if isinstance(symbol, str) else ""
    if current_symbol_code.startswith(("sh", "sz", "bj")):
        current_symbol_code = current_symbol_code[2:]
    
    if current_symbol_code != TARGET_SYMBOL:
        print(f"提示：当前监控标的 {current_symbol_code} 非策略指定标的 {TARGET_SYMBOL}，逻辑可能不完全适用。")

    # 6. 核心数据拉取
    # 6.1 个股实时行情
    df_spot = ak.stock_zh_a_spot_em()
    target_row = df_spot[df_spot['代码'] == current_symbol_code]
    
    if target_row.empty:
        message = f"未触发：无法获取 {current_symbol_code} 实时行情"
    else:
        # 提取个股核心指标
        price_current = pd.to_numeric(target_row.iloc[0]['最新价'], errors='coerce')
        pct_change = pd.to_numeric(target_row.iloc[0]['涨跌幅'], errors='coerce')
        turnover_rate = pd.to_numeric(target_row.iloc[0]['换手率'], errors='coerce')
        
        if pd.isna(price_current):
            raise ValueError("当前价格数据无效 (NaN)")

        # 6.2 板块指数（煤炭板块）
        index_pct_change = 0.0
        try:
            if hasattr(ak, "stock_board_industry_name_em"):
                df_board = ak.stock_board_industry_name_em()
                if df_board is not None and (not df_board.empty) and ("板块名称" in df_board.columns) and ("涨跌幅" in df_board.columns):
                    board_row = df_board[df_board["板块名称"].astype(str).str.contains("煤炭", na=False)]
                    if not board_row.empty:
                        index_pct_change = pd.to_numeric(board_row.iloc[0]["涨跌幅"], errors="coerce")
            if (not pd.notna(index_pct_change)) or index_pct_change == 0.0:
                df_index = ak.stock_zh_index_spot_em()
                if df_index is not None and (not df_index.empty) and ("代码" in df_index.columns) and ("涨跌幅" in df_index.columns):
                    index_row = df_index[df_index["代码"].astype(str) == "399998"]
                    if not index_row.empty:
                        index_pct_change = pd.to_numeric(index_row.iloc[0]["涨跌幅"], errors="coerce")
        except Exception as e:
            print(f"板块指数获取失败: {e}")

        # 6.3 期货数据
        future_data = get_futures_data()
        
        # 6.4 资金流向数据
        flow_data = get_main_flow(current_symbol_code, target_row)
        
        # 6.5 交易时间判断
        trading_min = get_trading_minute()
        in_monitor_window = (0 <= trading_min <= MONITOR_MINUTES)  # 开盘30分钟内

        # 7. 调试日志（完整关键数据）
        print("="*50)
        print(f"监控标的：{current_symbol_code} | 时间窗口：{'开盘30分钟内' if in_monitor_window else '非关键窗口'}")
        print(f"现价：{price_current} 元 | 涨幅：{pct_change}% | 换手率：{turnover_rate}%")
        print(f"板块涨幅（煤炭板块）：{index_pct_change}%")
        print(f"期货：焦煤{future_data['焦煤主力']}% | 焦炭{future_data['焦炭主力']}%")
        print(f"资金：主力净流入{flow_data['主力净流入']:.0f}万 | 成交额{flow_data['成交额']:.0f}万")
        print("="*50)

        # 8. 核心条件判断
        # --- 卖出逻辑（优先级最高）---
        # 8.1 止损/止盈核心条件
        is_break_support = price_current < SUPPORT_PRICE  # 跌破22元
        is_hard_stop = pct_change <= STOP_LOSS_PCT        # 跌幅≥3%
        is_index_risk = index_pct_change <= INDEX_RISK_PCT # 板块回调≥5%
        is_future_risk = future_data['期货风险']           # 期货跌超3%
        is_take_profit_1 = price_current >= TAKE_PROFIT_1 # 止盈26元
        is_take_profit_2 = price_current >= TAKE_PROFIT_2 # 止盈28元

        # 8.2 卖出信号判断
        if is_break_support:
            triggered = True
            signal = "STRONG_SELL"
            message = f"触发止损：现价 {price_current} 元跌破支撑位 {SUPPORT_PRICE} 元"
        elif is_hard_stop:
            triggered = True
            signal = "STRONG_SELL"
            message = f"触发风控：盘中跌幅 {pct_change}% 超过阈值 {STOP_LOSS_PCT}%"
        elif is_index_risk:
            triggered = True
            signal = "STRONG_SELL"
            message = f"板块风险：中证煤炭跌幅 {index_pct_change}% 超 {INDEX_RISK_PCT}%，清仓避险"
        elif is_future_risk:
            triggered = True
            signal = "STRONG_SELL"
            message = f"期货风险：焦煤/焦炭跌超 {FUTURE_RISK_PCT}%，被动离场"
        elif is_take_profit_2:
            triggered = True
            signal = "SELL_ALL"
            message = f"触发清仓：现价 {price_current} 元触及第二止盈位 {TAKE_PROFIT_2} 元"
        elif is_take_profit_1:
            triggered = True
            signal = "SELL_HALF"
            message = f"触发减仓：现价 {price_current} 元触及第一止盈位 {TAKE_PROFIT_1} 元，建议减仓50%"

        # --- 买入/补仓逻辑（未持仓时判断买入，持仓时判断补仓）---
        else:
            # 8.3 买入条件（空仓状态）
            if not position_held:
                # 基础买入条件：开盘30分钟内 + 换手率达标 + 成交额≥8亿 + 期货红盘
                base_buy_ok = (in_monitor_window 
                               and turnover_rate >= TURNOVER_ENTRY 
                               and flow_data['成交额'] >= VOLUME_THRESHOLD 
                               and future_data['期货红盘'])
                
                # 最佳入场场景：22-22.5元 + 板块≥2% + 主力净流入≥5000万
                is_best_buy = (BEST_BUY_LOW <= price_current <= BEST_BUY_HIGH) \
                              and (index_pct_change >= INDEX_BEST_PCT) \
                              and (flow_data['主力净流入'] >= MAIN_FLOW_THRESHOLD)
                
                # 次优入场场景：22.5-23元 + 板块≥3% + 主力净流入≥5000万
                is_sub_buy = (BEST_BUY_HIGH < price_current <= SUB_BUY_HIGH) \
                             and (index_pct_change >= INDEX_SUB_PCT) \
                             and (flow_data['主力净流入'] >= MAIN_FLOW_THRESHOLD)
                
                if base_buy_ok and is_best_buy:
                    triggered = True
                    signal = "STRONG_BUY"
                    message = f"最佳入场：价{price_current}在[{BEST_BUY_LOW},{BEST_BUY_HIGH}]，板块{index_pct_change}%，主力净流入{flow_data['主力净流入']:.0f}万"
                elif base_buy_ok and is_sub_buy:
                    triggered = True
                    signal = "BUY"
                    message = f"次优入场：价{price_current}在({BEST_BUY_HIGH},{SUB_BUY_HIGH}]，板块{index_pct_change}%，建议小仓位（50%）"
                elif price_current < BEST_BUY_LOW:
                    message = f"观望：现价 {price_current} 低于买入区间，支撑位松动，不抄底"
                elif price_current > SUB_BUY_HIGH:
                    message = f"观望：现价 {price_current} 高于次优区间上限 {SUB_BUY_HIGH}，不追高"
                else:
                    # 价格在区间但其他条件不满足
                    reason = []
                    if not in_monitor_window:
                        reason.append("非开盘30分钟窗口")
                    if turnover_rate < TURNOVER_ENTRY:
                        reason.append(f"换手率{turnover_rate}%不足{TURNOVER_ENTRY}%")
                    if flow_data['成交额'] < VOLUME_THRESHOLD:
                        reason.append(f"成交额{flow_data['成交额']:.0f}万不足{VOLUME_THRESHOLD}万")
                    if not future_data['期货红盘']:
                        reason.append("期货翻绿")
                    message = f"观望：价格在区间，但{'; '.join(reason)}"
            
            # 8.4 补仓逻辑（已持仓状态）
            else:
                # 补仓条件：回调至22元 + 缩量企稳（换手率≤0.6%） + 板块未跌 + 期货红盘
                is_add_ok = (abs(price_current - SUPPORT_PRICE) <= 0.1) \
                            and (turnover_rate <= TURNOVER_ADD) \
                            and (index_pct_change >= 0) \
                            and (future_data['期货红盘'])
                
                if is_add_ok:
                    triggered = True
                    signal = "ADD"
                    message = f"补仓信号：价{price_current}企稳22元，换手率{turnover_rate}%缩量，可补仓（不超首次仓位）"
                else:
                    message = f"持仓观望：补仓条件未满足（当前换手{turnover_rate}%，板块{index_pct_change}%）"

except Exception as e:
    # 9. 异常捕获
    triggered = False
    signal = "WAIT"
    message = f"脚本运行错误: {str(e)}"
    print(f"Error trace: {e}")

# 10. 仓位提示（补充操作纪律）
signal_tips = {
    "STRONG_BUY": "建议入场仓位：稳健15%/激进20%",
    "BUY": "建议入场仓位：稳健7.5%/激进10%（次优场景减半）",
    "ADD": "补仓后总仓位不超30%",
    "SELL_HALF": "建议减仓50%，剩余仓位看28元",
    "SELL_ALL": "建议全部清仓",
    "STRONG_SELL": "立即止损，保持空仓"
}
if signal in signal_tips:
    message += f" | {signal_tips[signal]}"

# 脚本结束，系统会自动读取 triggered, signal, message
