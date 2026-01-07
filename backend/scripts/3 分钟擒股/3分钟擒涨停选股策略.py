import traceback
import os
import pandas as pd
import numpy as np
import akshare as ak

os.environ.setdefault("NO_PROXY", "*")

RUN_MODE = "adaptive"

# ========== 策略核心参数配置（完全对齐原文） ==========
VOL_RATIO_LOW = 1.3    # 量比下限 30%
VOL_RATIO_HIGH = 2.0   # 量比上限 100%
MA_DIST_MAX = 0.08     # 5/10日线最大距离 8%
HIGH_POS_RATIO = 1.8   # 高位股阈值 80%
CALLBACK_MAX = 0.03    # 分时最大回调 3%
RECOVER_RATIO = 0.8    # 回调后15分钟回升比例 80%
END_RISE_LOW = 0.005   # 尾盘涨幅下限 0.5%
END_RISE_HIGH = 0.01   # 尾盘涨幅上限 1%
END_VOL_RATIO = 1.2    # 尾盘量能放大 20%
END_RISE_LOW_INTRA = 0.002   # 盘中窗口涨幅下限 0.2%
END_RISE_HIGH_INTRA = 0.02   # 盘中窗口涨幅上限 2%
END_VOL_RATIO_INTRA = 1.1    # 盘中窗口量能放大 10%
BOARD_RISE_MIN = 0.01  # 板块涨幅阈值 1%
BOARD_LIMIT_MIN = 2    # 板块涨停股数阈值 2只

# 1. 初始化空结果
df = pd.DataFrame()

def get_main_board(symbol: str) -> str:
    """获取个股主营板块（容错处理）"""
    try:
        stock_info = ak.stock_individual_info_em(symbol=symbol)
        return stock_info.loc[stock_info['item'] == '行业', 'value'].iloc[0]
    except:
        return ""

def calculate_ma_slope(ma_series: pd.Series) -> float:
    """计算均线斜率（判断是否向上倾斜）"""
    if len(ma_series) < 5:
        return 0
    x = np.arange(len(ma_series))
    slope = np.polyfit(x, ma_series.values, 1)[0]
    return slope

def _get_industry_board_metrics(board_name: str, cache: dict) -> dict:
    if not board_name:
        return {}
    if board_name in cache:
        return cache[board_name]
    board_pct = None
    limit_count = None
    try:
        spot_df = ak.stock_board_industry_spot_em(symbol=board_name)
        if spot_df is not None and not spot_df.empty and "item" in spot_df.columns and "value" in spot_df.columns:
            spot_map = spot_df.set_index("item")["value"].to_dict()
            board_pct_raw = spot_map.get("涨跌幅", None)
            if board_pct_raw is not None:
                board_pct = float(board_pct_raw) / 100.0
    except Exception:
        board_pct = None
    try:
        cons_df = ak.stock_board_industry_cons_em(symbol=board_name)
        if cons_df is not None and not cons_df.empty and "涨跌幅" in cons_df.columns:
            pct_series = pd.to_numeric(cons_df["涨跌幅"], errors="coerce")
            limit_count = int((pct_series >= 9.8).sum())
    except Exception:
        limit_count = None
    cache[board_name] = {"board_pct": board_pct, "limit_count": limit_count}
    return cache[board_name]

try:
    print("开始执行：3分钟擒涨停选股策略（严格对齐原文）...")
    
    # ========== 步骤1：获取市场快照，初步筛选标的 ==========
    snapshot_df = ak.stock_zh_a_spot_em()
    if snapshot_df is None or snapshot_df.empty:
        print("未获取到市场快照数据")
        raise Exception("市场快照数据为空")
    
    # 字段清洗与预过滤
    snapshot_df.rename(
        columns={"代码": "symbol", "名称": "name", "最新价": "price", "涨跌幅": "pct_chg", "成交额": "amount"},
        inplace=True
    )
    for col in ["pct_chg", "amount"]:
        snapshot_df[col] = pd.to_numeric(snapshot_df[col], errors="coerce")
    
    # 预过滤条件：非ST、涨幅0-9.5%、成交额>5000万
    snapshot_df = snapshot_df[
        (~snapshot_df["name"].str.contains("ST", na=False)) &
        (snapshot_df["pct_chg"] > 0) & (snapshot_df["pct_chg"] < 9.5) &
        (snapshot_df["amount"] > 50000000)
    ].sort_values("amount", ascending=False).head(200)
    print(f"初步入围 {len(snapshot_df)} 只候选股，开始深度逻辑分析...")

    # ========== 步骤2：获取板块数据，用于联动校验 ==========
    board_dict = {}
    board_cache = {}
    try:
        board_df = ak.stock_zh_a_board_spot_em()
        board_df.rename(
            columns={"板块名称": "board_name", "涨跌幅": "board_pct", "涨停家数": "limit_count"},
            inplace=True
        )
        board_df["board_pct"] = pd.to_numeric(board_df["board_pct"], errors="coerce") / 100.0
        board_df["limit_count"] = pd.to_numeric(board_df["limit_count"], errors="coerce")
        board_dict = board_df.set_index("board_name")[["board_pct", "limit_count"]].to_dict("index")
        print(f"板块数据源: stock_zh_a_board_spot_em, 板块数: {len(board_dict)}")
    except Exception as e:
        print(f"板块数据源: industry_lazy (原因: {e})")

    diag = {
        "candidates": int(len(snapshot_df)),
        "scanned": 0,
        "exception": 0,
        "daily_fetch_error": 0,
        "minute_fetch_error": 0,
        "error_samples": [],
        "pass_vol_total": 0,
        "pass_ma": 0,
        "pass_not_high": 0,
        "pass_vol": 0,
        "pass_callback": 0,
        "pass_end": 0,
        "pass_board": 0,
        "pass_all": 0,
        "stage_vol": 0,
        "stage_vol_ma": 0,
        "stage_day": 0,
        "stage_day_strength": 0,
        "stage_all": 0,
    }

    results = []
    near_results = []
    board_check_enabled = bool(board_dict)
    for _, row in snapshot_df.iterrows():
        symbol = row["symbol"]
        name = row["name"]
        try:
            is_strict = RUN_MODE == "strict"
            diag["scanned"] += 1
            # ========== 子步骤1：获取日线数据 ==========
            try:
                hist_df = ak.stock_zh_a_hist(symbol=symbol, period="daily", adjust="qfq")
            except Exception as e:
                diag["daily_fetch_error"] += 1
                if len(diag["error_samples"]) < 10:
                    diag["error_samples"].append({"symbol": symbol, "step": "daily", "error": str(e)})
                continue
            if hist_df is None or len(hist_df) < 20:
                continue
            hist_df.rename(
                columns={"日期": "date", "收盘": "close", "成交量": "volume", "最高": "high", "最低": "low"},
                inplace=True
            )
            for col in ["close", "volume", "high", "low"]:
                hist_df[col] = pd.to_numeric(hist_df[col], errors="coerce")
            today_d = hist_df.iloc[-1]  # 当日日线数据
            prev_5d = hist_df.iloc[-6:-1]  # 前5日数据

            # ========== 子步骤2：日线维度条件校验 ==========
            # 1. 量能总量校验：今日量能/前5日均量 1.3-2.0
            prev_5d_vol_avg = prev_5d["volume"].mean()
            if prev_5d_vol_avg == 0:
                continue
            vol_ratio = today_d["volume"] / prev_5d_vol_avg
            cond_vol_total = (vol_ratio >= VOL_RATIO_LOW) & (vol_ratio <= VOL_RATIO_HIGH)
            if cond_vol_total:
                diag["pass_vol_total"] += 1

            # 2. 均线条件：5日线向上+股价全天在5日线+5/10日线距离≤3%
            hist_df["ma5"] = hist_df["close"].rolling(window=5).mean()
            hist_df["ma10"] = hist_df["close"].rolling(window=10).mean()
            ma5 = hist_df["ma5"].iloc[-1]
            ma10 = hist_df["ma10"].iloc[-1]
            # 均线斜率>0 表示向上倾斜
            ma5_slope = calculate_ma_slope(hist_df["ma5"].iloc[-5:])
            # 股价全天在5日线（用日线高低价判断，更贴近全天逻辑）
            cond_price_on_ma5 = (today_d["low"] >= ma5)
            # 5/10日线距离
            ma_dist = (ma5 - ma10) / ma10 if ma10 > 0 else 1
            cond_ma = (ma5_slope > 0) & (ma5 > ma10) & cond_price_on_ma5 & (ma_dist <= MA_DIST_MAX)
            if cond_ma:
                diag["pass_ma"] += 1

            # 3. 高位股避坑：近20日涨幅≤80%
            low_20d = hist_df["close"].iloc[-20:].min()
            pos_ratio = today_d["close"] / low_20d
            cond_not_high = (pos_ratio <= HIGH_POS_RATIO)
            if cond_not_high:
                diag["pass_not_high"] += 1

            board_name = ""
            cond_board = True if not board_check_enabled else False
            if board_check_enabled and (cond_vol_total & cond_ma & cond_not_high):
                board_name = get_main_board(symbol)
                board_data = board_dict.get(board_name, None)
                if board_data:
                    board_pct = board_data.get("board_pct", None)
                    limit_count = board_data.get("limit_count", None)
                    if board_pct is not None:
                        if limit_count is None:
                            cond_board = (board_pct >= BOARD_RISE_MIN)
                        else:
                            cond_board = (board_pct >= BOARD_RISE_MIN) & (limit_count >= BOARD_LIMIT_MIN)
                if cond_board:
                    diag["pass_board"] += 1

            # ========== 子步骤3：分时数据校验（策略核心，新增） ==========
            # 获取当日分时数据（1分钟级，A股交易时间：9:30-11:30, 13:00-15:00）
            minute_df = None
            try:
                minute_df = ak.stock_zh_a_hist_min_em(symbol=symbol, period="1", adjust="")
            except Exception as e:
                diag["minute_fetch_error"] += 1
                if len(diag["error_samples"]) < 10:
                    diag["error_samples"].append({"symbol": symbol, "step": "minute", "error": str(e)})
                if (not is_strict) and (cond_vol_total & cond_ma & cond_not_high & cond_board):
                    vol_score = (1 - abs(vol_ratio - 1.65)) * 50
                    ma_score = (1 - ma_dist) * 50
                    score = round(vol_score + ma_score, 2)
                    near_results.append({
                        "symbol": symbol,
                        "name": name,
                        "score": score,
                        "reason": f"分钟数据获取失败，降级日线候选；量比{vol_ratio:.2f}倍(符合1.3-2.0)；均线条件达标；近20日涨幅{pos_ratio-1:.1%}(≤80%)；板块联动未知",
                        "price": today_d.get("close", None),
                        "pct_chg": row.get("pct_chg", None),
                        "vol_ratio": round(vol_ratio, 2),
                        "board_name": board_name,
                        "data_mode": "DayOnly"
                    })
                continue
            if minute_df is None or minute_df.empty or "时间" not in minute_df.columns:
                if (not is_strict) and (cond_vol_total & cond_ma & cond_not_high & cond_board):
                    vol_score = (1 - abs(vol_ratio - 1.65)) * 50
                    ma_score = (1 - ma_dist) * 50
                    score = round(vol_score + ma_score, 2)
                    near_results.append({
                        "symbol": symbol,
                        "name": name,
                        "score": score,
                        "reason": f"分钟数据为空，降级日线候选；量比{vol_ratio:.2f}倍(符合1.3-2.0)；均线条件达标；近20日涨幅{pos_ratio-1:.1%}(≤80%)；板块联动未知",
                        "price": today_d.get("close", None),
                        "pct_chg": row.get("pct_chg", None),
                        "vol_ratio": round(vol_ratio, 2),
                        "board_name": board_name,
                        "data_mode": "DayOnly"
                    })
                continue
            minute_df["time_dt"] = pd.to_datetime(minute_df["时间"], errors="coerce")
            minute_df = minute_df[minute_df["time_dt"].notna()].copy()
            if minute_df.empty:
                if (not is_strict) and (cond_vol_total & cond_ma & cond_not_high & cond_board):
                    vol_score = (1 - abs(vol_ratio - 1.65)) * 50
                    ma_score = (1 - ma_dist) * 50
                    score = round(vol_score + ma_score, 2)
                    near_results.append({
                        "symbol": symbol,
                        "name": name,
                        "score": score,
                        "reason": f"分钟时间字段为空，降级日线候选；量比{vol_ratio:.2f}倍(符合1.3-2.0)；均线条件达标；近20日涨幅{pos_ratio-1:.1%}(≤80%)；板块联动未知",
                        "price": today_d.get("close", None),
                        "pct_chg": row.get("pct_chg", None),
                        "vol_ratio": round(vol_ratio, 2),
                        "board_name": board_name,
                        "data_mode": "DayOnly"
                    })
                continue
            last_dt = minute_df["time_dt"].dt.date.max()
            minute_df = minute_df[minute_df["time_dt"].dt.date == last_dt].copy()
            if minute_df is None or len(minute_df) < 120:
                if (not is_strict) and (cond_vol_total & cond_ma & cond_not_high & cond_board):
                    vol_score = (1 - abs(vol_ratio - 1.65)) * 50
                    ma_score = (1 - ma_dist) * 50
                    score = round(vol_score + ma_score, 2)
                    near_results.append({
                        "symbol": symbol,
                        "name": name,
                        "score": score,
                        "reason": f"分钟数据不足，降级日线候选；量比{vol_ratio:.2f}倍(符合1.3-2.0)；均线条件达标；近20日涨幅{pos_ratio-1:.1%}(≤80%)；板块联动未知",
                        "price": today_d.get("close", None),
                        "pct_chg": row.get("pct_chg", None),
                        "vol_ratio": round(vol_ratio, 2),
                        "board_name": board_name,
                        "data_mode": "DayOnly"
                    })
                continue
            minute_df.rename(
                columns={"收盘": "min_close", "成交量": "min_volume"},
                inplace=True
            )
            minute_df["min_close"] = pd.to_numeric(minute_df["min_close"], errors="coerce")
            minute_df["min_volume"] = pd.to_numeric(minute_df["min_volume"], errors="coerce")
            minute_df["time"] = minute_df["time_dt"].dt.strftime("%H:%M")

            morning_mask = minute_df["time"].between("09:30", "10:30")
            end_mask = minute_df["time"].between("14:30", "15:00")
            pre_end_mask = minute_df["time"].between("14:00", "14:30")
            
            morning_vol = minute_df[morning_mask]["min_volume"].sum()
            end_slice = minute_df[end_mask]
            pre_end_slice = minute_df[pre_end_mask]
            if end_slice.empty:
                end_slice = minute_df.tail(30) if len(minute_df) >= 30 else minute_df
            if pre_end_slice.empty:
                pre_end_slice = minute_df.iloc[-60:-30] if len(minute_df) >= 60 else minute_df.head(0)
            end_vol = end_slice["min_volume"].sum()
            pre_end_vol = pre_end_slice["min_volume"].sum()
            total_vol = minute_df["min_volume"].sum()

            # 1. 分时量能校验：早盘占比 + 尾盘量≥早盘
            has_full_day = bool(minute_df["time"].max() >= "14:55")
            if is_strict and not has_full_day:
                continue
            cond_morning_vol = True
            cond_end_vol_ratio = True
            if has_full_day:
                if RUN_MODE == "strict":
                    cond_morning_vol = (morning_vol / total_vol >= 0.25) & (morning_vol / total_vol <= 0.45) if total_vol > 0 else False
                    cond_end_vol_ratio = (end_vol >= morning_vol * 0.4) if morning_vol > 0 else False
                elif RUN_MODE == "adaptive":
                    cond_morning_vol = (morning_vol / total_vol >= 0.2) & (morning_vol / total_vol <= 0.5) if total_vol > 0 else False
                    cond_end_vol_ratio = (end_vol >= morning_vol * 0.3) if morning_vol > 0 else False
            elif is_strict:
                cond_morning_vol = False
                cond_end_vol_ratio = False
            cond_vol = cond_vol_total & cond_morning_vol & cond_end_vol_ratio
            if cond_vol:
                diag["pass_vol"] += 1

            # 2. 分时回调与承接校验
            # 当日分时最高价与对应时间
            high_price = minute_df["min_close"].max()
            high_time = minute_df.loc[minute_df["min_close"] == high_price, "time"].iloc[0]
            # 最高价之后的分时数据（回调阶段）
            after_high_mask = minute_df["time"] >= high_time
            after_high_df = minute_df[after_high_mask].reset_index(drop=True)
            max_callback = None
            recover_ratio = None
            if len(after_high_df) < 15:  # 至少15分钟数据判断回调
                cond_callback = False
            else:
                # 最大回调幅度≤3%
                max_callback = (high_price - after_high_df["min_close"].min()) / high_price
                # 回调后15分钟内回升到80%以上
                callback_low_price = after_high_df["min_close"].min()
                callback_low_idx = after_high_df["min_close"].idxmin()
                recover_15min_df = after_high_df.iloc[callback_low_idx:callback_low_idx+15] if callback_low_idx +15 < len(after_high_df) else after_high_df.iloc[callback_low_idx:]
                recover_price = recover_15min_df["min_close"].max()
                recover_ratio = (recover_price - callback_low_price) / (high_price - callback_low_price) if high_price > callback_low_price else 0
                recover_threshold = (RECOVER_RATIO if has_full_day else 0.3) if RUN_MODE == "strict" else (0.5 if has_full_day else 0.3)
                cond_callback = (max_callback <= CALLBACK_MAX) & (recover_ratio >= recover_threshold)
            if cond_callback:
                diag["pass_callback"] += 1

            # 3. 尾盘异动校验：涨幅0.5%-1% + 量能放大20%
            end_first_price = float(end_slice["min_close"].iloc[0]) if not end_slice.empty else 0
            end_last_price = float(end_slice["min_close"].iloc[-1]) if not end_slice.empty else 0
            end_rise = (end_last_price - end_first_price) / end_first_price if end_first_price > 0 else 0
            end_vol_ratio = end_vol / pre_end_vol if pre_end_vol > 0 else 0
            if RUN_MODE == "strict" and has_full_day:
                cond_end = (end_rise >= END_RISE_LOW) & (end_rise <= END_RISE_HIGH) & (end_vol_ratio >= END_VOL_RATIO)
            else:
                cond_end = (end_rise >= END_RISE_LOW_INTRA) & (end_rise <= END_RISE_HIGH_INTRA) & (end_vol_ratio >= END_VOL_RATIO_INTRA)
            if cond_end:
                diag["pass_end"] += 1

            # 分时承接总条件
            cond_strength = (cond_callback & cond_end) if is_strict else (cond_callback | cond_end)

            if cond_vol:
                diag["stage_vol"] += 1
            if cond_vol & cond_ma:
                diag["stage_vol_ma"] += 1
            if cond_vol & cond_ma & cond_not_high:
                diag["stage_day"] += 1
            if cond_vol & cond_ma & cond_not_high & cond_strength:
                diag["stage_day_strength"] += 1
            if cond_vol & cond_ma & cond_not_high & cond_strength & cond_board:
                diag["stage_all"] += 1

            # ========== 子步骤5：综合条件判断 ==========
            if cond_vol & cond_ma & cond_not_high & cond_strength & cond_board:
                diag["pass_all"] += 1
                # 计算综合评分
                vol_score = (1 - abs(vol_ratio - 1.65)) * 50
                ma_score = (1 - ma_dist) * 50
                score = round(vol_score + ma_score, 2)
                
                results.append({
                    "symbol": symbol,
                    "name": name,
                    "score": score,
                    "reason": f"量比{vol_ratio:.2f}倍(符合1.3-2.0)；5日线向上且股价站稳；近20日涨幅{pos_ratio-1:.1%}(≤80%)；分时承接强；板块联动达标",
                    "price": today_d["close"],
                    "pct_chg": row["pct_chg"],
                    "vol_ratio": round(vol_ratio, 2),
                    "board_name": board_name,
                    "data_mode": "Day+Minute"
                })
            elif cond_vol & cond_ma & cond_not_high & cond_board:
                vol_score = (1 - abs(vol_ratio - 1.65)) * 50
                ma_score = (1 - ma_dist) * 50
                extra = (10 if cond_end else 0) + (10 if cond_callback else 0)
                score = round(vol_score + ma_score + extra, 2)
                max_callback_str = "NA"
                recover_ratio_str = "NA"
                try:
                    if isinstance(max_callback, (int, float, np.floating)) and not pd.isna(max_callback):
                        max_callback_str = f"{float(max_callback):.2%}"
                    if isinstance(recover_ratio, (int, float, np.floating)) and not pd.isna(recover_ratio):
                        recover_ratio_str = f"{float(recover_ratio):.2%}"
                except Exception:
                    pass
                near_results.append({
                    "symbol": symbol,
                    "name": name,
                    "score": score,
                    "reason": f"严格条件未全满足；量比{vol_ratio:.2f}倍；end_rise={end_rise:.2%}；end_vol_ratio={end_vol_ratio:.2f}；max_callback={max_callback_str}；recover_ratio={recover_ratio_str}；板块联动未知",
                    "price": today_d.get("close", None),
                    "pct_chg": row["pct_chg"],
                    "vol_ratio": round(vol_ratio, 2),
                    "board_name": board_name,
                    "data_mode": "NearMiss"
                })

        except Exception as e:
            # 个股分析失败跳过，不中断整体流程
            diag["exception"] += 1
            if len(diag["error_samples"]) < 10:
                diag["error_samples"].append({"symbol": symbol, "step": "loop", "error": str(e)})
            continue

    # ========== 步骤3：结果整理 ==========
    if results:
        df = pd.DataFrame(results)
        df = df.sort_values("score", ascending=False).head(100)
        print(f"筛选完成，命中 {len(df)} 只标的！")
        print(df[["symbol", "name", "score", "reason"]].head(10))
    elif near_results:
        df = pd.DataFrame(near_results)
        df = df.sort_values("score", ascending=False).head(100)
        print(f"严格条件命中 0，只输出接近标的 {len(df)} 只（NearMiss）")
        print(df[["symbol", "name", "score", "reason"]].head(10))
    else:
        print("未发现完全符合条件的标的")
    print("诊断统计:", diag)

except Exception as e:
    print(f"脚本执行崩溃: {e}")
    print(traceback.format_exc())
    df = pd.DataFrame()

# 暴露结果变量
print("\n最终筛选结果已存入 df 变量")
