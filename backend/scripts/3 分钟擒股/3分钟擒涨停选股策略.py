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

def _get_tier_info(level: int):
    if level == 1:
        return "T1", "档位T1：日线+分时+板块数据齐全且核心条件全部达标，信号最强，适合进攻型交易。"
    if level == 2:
        return "T2", "档位T2：数据齐全但分时承接或尾盘强度略有缺口，强度略弱于T1，可结合盘面确认。"
    if level == 3:
        return "T3", "档位T3：分钟级数据缺失或不完整，仅依赖日线与板块，信号偏中性，需提高容错和风控。"
    if level == 4:
        return "T4", "档位T4：仅满足日线基础条件，板块或分时信息严重缺失，更适合作为观察名单。"
    return "T5", "档位T5：数据支撑较弱或环境噪音较大，只做弱关注，不建议直接执行交易。"

def _make_record(symbol: str, name: str, score: float, reason: str, level: int, note: str, **kwargs):
    tier, tier_tip = _get_tier_info(level)
    tier_context = tier_tip if not note else f"{tier_tip} 注意事项：{note}"
    rec = {
        "symbol": symbol,
        "name": name,
        "score": float(score),
        "tier": tier,
        "tier_level": int(level),
        "tier_tip": tier_tip,
        "tier_context": tier_context,
        "note": note,
        "reason": reason,
    }
    rec.update(kwargs)
    return rec

def _fetch_daily_hist(symbol: str):
    tried = []
    for adjust in ["qfq", "hfq", ""]:
        try:
            hist_df = ak.stock_zh_a_hist(symbol=symbol, period="daily", adjust=adjust)
            return hist_df, (adjust if adjust else "none")
        except Exception as e:
            tried.append((adjust if adjust else "none", str(e)))
    raise RuntimeError(f"daily_fetch_failed: {tried[:2]}")

def _fetch_minute_hist(symbol: str):
    tried = []
    for period in ["1", "5"]:
        try:
            minute_df = ak.stock_zh_a_hist_min_em(symbol=symbol, period=period, adjust="")
            return minute_df, f"{period}m"
        except Exception as e:
            tried.append((period, str(e)))
    raise RuntimeError(f"minute_fetch_failed: {tried[:2]}")

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
        "daily_fallback_hfq": 0,
        "daily_fallback_none": 0,
        "minute_fetch_error": 0,
        "minute_fallback_5m": 0,
        "error_samples": [],
        "tier1": 0,
        "tier2": 0,
        "tier3": 0,
        "tier4": 0,
        "tier5": 0,
    }

    board_check_enabled = bool(board_dict)
    all_results = []

    amount_series = pd.to_numeric(snapshot_df.get("amount"), errors="coerce")
    pct_series = pd.to_numeric(snapshot_df.get("pct_chg"), errors="coerce")
    amount_rank = amount_series.rank(ascending=False, method="min") if amount_series is not None else None

    for idx, row in snapshot_df.iterrows():
        symbol = row["symbol"]
        name = row["name"]
        diag["scanned"] += 1

        note_parts = []
        if not board_check_enabled:
            note_parts.append("板块数据源不可用，已跳过板块联动校验")

        try:
            try:
                hist_df, daily_mode = _fetch_daily_hist(symbol)
                if daily_mode != "qfq":
                    if daily_mode == "hfq":
                        diag["daily_fallback_hfq"] += 1
                        note_parts.append("日线已降级为后复权(hfq)，可能与前复权口径略有差异")
                    else:
                        diag["daily_fallback_none"] += 1
                        note_parts.append("日线已降级为不复权(none)，可能与复权口径略有差异")
            except Exception as e:
                diag["daily_fetch_error"] += 1
                if len(diag["error_samples"]) < 10:
                    diag["error_samples"].append({"symbol": symbol, "step": "daily", "error": str(e)})
                ar = float(amount_rank.loc[idx]) if amount_rank is not None and idx in amount_rank.index and not pd.isna(amount_rank.loc[idx]) else None
                amt = row.get("amount", None)
                pct = row.get("pct_chg", None)
                amt_val = float(amt) if amt is not None and not pd.isna(amt) else 0.0
                pct_val = float(pct) if pct is not None and not pd.isna(pct) else 0.0
                score = round((1.0 / (ar or 9999)) * 60 + max(min(pct_val, 9.5), 0) / 9.5 * 40, 2)
                note_parts.append("日线数据获取失败，仅快照候选，需人工复核")
                all_results.append(
                    _make_record(
                        symbol=symbol,
                        name=name,
                        score=score,
                        reason=f"快照候选：涨跌幅{pct_val:.2f}%；成交额{amt_val/1e8:.2f}亿；日线缺失",
                        level=5,
                        note="；".join(note_parts),
                        price=row.get("price", None),
                        pct_chg=pct_val,
                        amount=amt_val,
                        vol_ratio=None,
                        board_name="",
                        data_mode="SnapshotOnly",
                    )
                )
                diag["tier5"] += 1
                continue

            if hist_df is None or len(hist_df) < 20:
                note_parts.append("日线数据不足，仅快照候选，需人工复核")
                ar = float(amount_rank.loc[idx]) if amount_rank is not None and idx in amount_rank.index and not pd.isna(amount_rank.loc[idx]) else None
                amt = row.get("amount", None)
                pct = row.get("pct_chg", None)
                amt_val = float(amt) if amt is not None and not pd.isna(amt) else 0.0
                pct_val = float(pct) if pct is not None and not pd.isna(pct) else 0.0
                score = round((1.0 / (ar or 9999)) * 60 + max(min(pct_val, 9.5), 0) / 9.5 * 40, 2)
                all_results.append(
                    _make_record(
                        symbol=symbol,
                        name=name,
                        score=score,
                        reason=f"快照候选：涨跌幅{pct_val:.2f}%；成交额{amt_val/1e8:.2f}亿；日线不足",
                        level=5,
                        note="；".join(note_parts),
                        price=row.get("price", None),
                        pct_chg=pct_val,
                        amount=amt_val,
                        vol_ratio=None,
                        board_name="",
                        data_mode="SnapshotOnly",
                    )
                )
                diag["tier5"] += 1
                continue

            hist_df.rename(columns={"日期": "date", "收盘": "close", "成交量": "volume", "最高": "high", "最低": "low"}, inplace=True)
            for col in ["close", "volume", "high", "low"]:
                hist_df[col] = pd.to_numeric(hist_df[col], errors="coerce")

            today_d = hist_df.iloc[-1]
            prev_5d = hist_df.iloc[-6:-1]
            prev_5d_vol_avg = float(prev_5d["volume"].mean()) if prev_5d is not None else 0.0
            if prev_5d_vol_avg <= 0 or pd.isna(prev_5d_vol_avg):
                continue

            vol_ratio = float(today_d["volume"]) / prev_5d_vol_avg if today_d.get("volume", None) is not None else 0.0
            hist_df["ma5"] = hist_df["close"].rolling(window=5).mean()
            hist_df["ma10"] = hist_df["close"].rolling(window=10).mean()
            ma5 = float(hist_df["ma5"].iloc[-1]) if not pd.isna(hist_df["ma5"].iloc[-1]) else 0.0
            ma10 = float(hist_df["ma10"].iloc[-1]) if not pd.isna(hist_df["ma10"].iloc[-1]) else 0.0
            ma5_slope = float(calculate_ma_slope(hist_df["ma5"].iloc[-5:]))
            ma_dist = (ma5 - ma10) / ma10 if ma10 > 0 else 1.0
            low_20d = float(hist_df["close"].iloc[-20:].min())
            pos_ratio = float(today_d["close"]) / low_20d if low_20d > 0 else 10.0

            cond_vol_total_t3 = (vol_ratio >= VOL_RATIO_LOW) & (vol_ratio <= VOL_RATIO_HIGH)
            cond_vol_total_t4 = (vol_ratio >= 1.1) & (vol_ratio <= 2.5)

            cond_price_on_ma5 = (float(today_d["low"]) >= ma5) if ma5 > 0 and today_d.get("low", None) is not None else False
            cond_ma_t3 = (ma5_slope > 0) & (ma5 > ma10) & cond_price_on_ma5 & (ma_dist <= MA_DIST_MAX)
            cond_ma_t4 = (ma5_slope > 0) & (ma5 >= ma10) & (float(today_d["close"]) >= ma5 * 0.98 if ma5 > 0 else False) & (ma_dist <= 0.12)

            cond_not_high_t3 = (pos_ratio <= HIGH_POS_RATIO)
            cond_not_high_t4 = (pos_ratio <= 2.2)

            daily_ok_t3 = bool(cond_vol_total_t3 & cond_ma_t3 & cond_not_high_t3)
            daily_ok_t4 = bool(cond_vol_total_t4 & cond_ma_t4 & cond_not_high_t4)

            board_name = ""
            board_checked = False
            board_pct = None
            board_limit = None
            cond_board = True
            if board_check_enabled and (daily_ok_t3 or daily_ok_t4):
                board_name = get_main_board(symbol)
                board_data = board_dict.get(board_name, None)
                if board_data and board_data.get("board_pct", None) is not None:
                    board_pct = float(board_data.get("board_pct"))
                    board_limit = board_data.get("limit_count", None)
                    board_limit = int(board_limit) if board_limit is not None and not pd.isna(board_limit) else None
                    board_checked = True
                else:
                    metrics = _get_industry_board_metrics(board_name, board_cache)
                    if metrics and metrics.get("board_pct", None) is not None:
                        board_pct = float(metrics.get("board_pct"))
                        lc = metrics.get("limit_count", None)
                        board_limit = int(lc) if lc is not None else None
                        board_checked = True

                if board_checked and board_pct is not None:
                    if board_limit is None:
                        cond_board = bool(board_pct >= BOARD_RISE_MIN)
                    else:
                        cond_board = bool((board_pct >= BOARD_RISE_MIN) & (board_limit >= BOARD_LIMIT_MIN))
                else:
                    note_parts.append("板块数据缺失，已跳过板块联动校验")
                    cond_board = True

            minute_df = None
            minute_status = "ok"
            minute_granularity = "1m"
            try:
                minute_df, minute_granularity = _fetch_minute_hist(symbol)
                if minute_granularity == "5m":
                    diag["minute_fallback_5m"] += 1
                    note_parts.append("分钟数据已降级为5分钟K，分时细节可能丢失")
            except Exception as e:
                diag["minute_fetch_error"] += 1
                minute_status = "fetch_error"
                if len(diag["error_samples"]) < 10:
                    diag["error_samples"].append({"symbol": symbol, "step": "minute", "error": str(e)})

            if minute_df is None or getattr(minute_df, "empty", True) or "时间" not in getattr(minute_df, "columns", []):
                minute_status = "missing" if minute_status == "ok" else minute_status
                if daily_ok_t3 and cond_board:
                    note_parts.append("分钟数据缺失，已降级到日线档位")
                    vol_score = (1 - abs(vol_ratio - 1.65)) * 50
                    ma_score = (1 - ma_dist) * 50
                    score = round(vol_score + ma_score, 2)
                    all_results.append(
                        _make_record(
                            symbol=symbol,
                            name=name,
                            score=score,
                            reason=f"量比{vol_ratio:.2f}倍；日线均线达标；近20日涨幅{pos_ratio-1:.1%}；分钟缺失",
                            level=3,
                            note="；".join(note_parts),
                            price=float(today_d["close"]),
                            pct_chg=float(row.get("pct_chg", 0) or 0),
                            vol_ratio=round(vol_ratio, 2),
                            board_name=board_name,
                            board_pct=board_pct,
                            board_limit=board_limit,
                            minute_status=minute_status,
                            minute_granularity=minute_granularity,
                            data_mode="DayOnly",
                        )
                    )
                    diag["tier3"] += 1
                elif daily_ok_t4 and cond_board:
                    note_parts.append("分钟数据缺失，已降级到更宽松日线档位")
                    vol_score = (1 - abs(vol_ratio - 1.65)) * 45
                    ma_score = (1 - ma_dist) * 45
                    score = round(vol_score + ma_score, 2)
                    all_results.append(
                        _make_record(
                            symbol=symbol,
                            name=name,
                            score=score,
                            reason=f"量比{vol_ratio:.2f}倍；日线基础达标；近20日涨幅{pos_ratio-1:.1%}；分钟缺失",
                            level=4,
                            note="；".join(note_parts),
                            price=float(today_d["close"]),
                            pct_chg=float(row.get("pct_chg", 0) or 0),
                            vol_ratio=round(vol_ratio, 2),
                            board_name=board_name,
                            board_pct=board_pct,
                            board_limit=board_limit,
                            minute_status=minute_status,
                            minute_granularity=minute_granularity,
                            data_mode="DayOnlyLoose",
                        )
                    )
                    diag["tier4"] += 1
                continue

            minute_df["time_dt"] = pd.to_datetime(minute_df["时间"], errors="coerce")
            minute_df = minute_df[minute_df["time_dt"].notna()].copy()
            if minute_df.empty:
                minute_status = "time_invalid"
                if daily_ok_t3 and cond_board:
                    note_parts.append("分钟时间字段异常，已降级到日线档位")
                    vol_score = (1 - abs(vol_ratio - 1.65)) * 50
                    ma_score = (1 - ma_dist) * 50
                    score = round(vol_score + ma_score, 2)
                    all_results.append(
                        _make_record(
                            symbol=symbol,
                            name=name,
                            score=score,
                            reason=f"量比{vol_ratio:.2f}倍；日线均线达标；近20日涨幅{pos_ratio-1:.1%}；分钟异常",
                            level=3,
                            note="；".join(note_parts),
                            price=float(today_d["close"]),
                            pct_chg=float(row.get("pct_chg", 0) or 0),
                            vol_ratio=round(vol_ratio, 2),
                            board_name=board_name,
                            board_pct=board_pct,
                            board_limit=board_limit,
                            minute_status=minute_status,
                            minute_granularity=minute_granularity,
                            data_mode="DayOnly",
                        )
                    )
                    diag["tier3"] += 1
                continue

            last_dt = minute_df["time_dt"].dt.date.max()
            minute_df = minute_df[minute_df["time_dt"].dt.date == last_dt].copy()
            min_points = 120 if minute_granularity == "1m" else 24
            if minute_df is None or len(minute_df) < min_points:
                minute_status = "too_short"
                if daily_ok_t3 and cond_board:
                    note_parts.append("分钟数据不足，已降级到日线档位")
                    vol_score = (1 - abs(vol_ratio - 1.65)) * 50
                    ma_score = (1 - ma_dist) * 50
                    score = round(vol_score + ma_score, 2)
                    all_results.append(
                        _make_record(
                            symbol=symbol,
                            name=name,
                            score=score,
                            reason=f"量比{vol_ratio:.2f}倍；日线均线达标；近20日涨幅{pos_ratio-1:.1%}；分钟不足",
                            level=3,
                            note="；".join(note_parts),
                            price=float(today_d["close"]),
                            pct_chg=float(row.get("pct_chg", 0) or 0),
                            vol_ratio=round(vol_ratio, 2),
                            board_name=board_name,
                            board_pct=board_pct,
                            board_limit=board_limit,
                            minute_status=minute_status,
                            minute_granularity=minute_granularity,
                            data_mode="DayOnly",
                        )
                    )
                    diag["tier3"] += 1
                continue

            minute_df.rename(columns={"收盘": "min_close", "成交量": "min_volume"}, inplace=True)
            minute_df["min_close"] = pd.to_numeric(minute_df["min_close"], errors="coerce")
            minute_df["min_volume"] = pd.to_numeric(minute_df["min_volume"], errors="coerce")
            minute_df["time"] = minute_df["time_dt"].dt.strftime("%H:%M")

            morning_mask = minute_df["time"].between("09:30", "10:30")
            end_mask = minute_df["time"].between("14:30", "15:00")
            pre_end_mask = minute_df["time"].between("14:00", "14:30")

            morning_vol = float(minute_df[morning_mask]["min_volume"].sum())
            end_slice = minute_df[end_mask]
            pre_end_slice = minute_df[pre_end_mask]
            if end_slice.empty:
                end_slice = minute_df.tail(30) if len(minute_df) >= 30 else minute_df
            if pre_end_slice.empty:
                pre_end_slice = minute_df.iloc[-60:-30] if len(minute_df) >= 60 else minute_df.head(0)
            end_vol = float(end_slice["min_volume"].sum())
            pre_end_vol = float(pre_end_slice["min_volume"].sum())
            total_vol = float(minute_df["min_volume"].sum())

            has_full_day = bool(str(minute_df["time"].max()) >= "14:55")
            if not has_full_day:
                note_parts.append("分时未覆盖全日，已采用盘中阈值")

            cond_morning_vol_strict = (morning_vol / total_vol >= 0.25) & (morning_vol / total_vol <= 0.45) if total_vol > 0 else False
            cond_end_vol_ratio_strict = (end_vol >= morning_vol * 0.4) if morning_vol > 0 else False
            cond_morning_vol_adaptive = (morning_vol / total_vol >= 0.2) & (morning_vol / total_vol <= 0.5) if total_vol > 0 else False
            cond_end_vol_ratio_adaptive = (end_vol >= morning_vol * 0.3) if morning_vol > 0 else False

            cond_vol_strict = bool(cond_vol_total_t3 & (cond_morning_vol_strict if has_full_day else False) & (cond_end_vol_ratio_strict if has_full_day else False))
            cond_vol_adaptive = bool(cond_vol_total_t3 & (cond_morning_vol_adaptive if total_vol > 0 else False) & (cond_end_vol_ratio_adaptive if morning_vol > 0 else False))

            high_price = float(minute_df["min_close"].max())
            high_time = str(minute_df.loc[minute_df["min_close"] == high_price, "time"].iloc[0])
            after_high_df = minute_df[minute_df["time"] >= high_time].reset_index(drop=True)
            max_callback = None
            recover_ratio = None

            if len(after_high_df) < 15:
                cond_callback_strict = False
                cond_callback_adaptive = False
            else:
                max_callback = (high_price - float(after_high_df["min_close"].min())) / high_price if high_price > 0 else 1.0
                callback_low_price = float(after_high_df["min_close"].min())
                callback_low_idx = int(after_high_df["min_close"].idxmin())
                recover_15min_df = after_high_df.iloc[callback_low_idx:callback_low_idx+15] if callback_low_idx + 15 < len(after_high_df) else after_high_df.iloc[callback_low_idx:]
                recover_price = float(recover_15min_df["min_close"].max()) if not recover_15min_df.empty else callback_low_price
                recover_ratio = (recover_price - callback_low_price) / (high_price - callback_low_price) if high_price > callback_low_price else 0.0
                cond_callback_strict = bool((max_callback <= CALLBACK_MAX) & (recover_ratio >= RECOVER_RATIO))
                cond_callback_adaptive = bool((max_callback <= max(CALLBACK_MAX, 0.05)) & (recover_ratio >= 0.5 if has_full_day else 0.3))

            end_first_price = float(end_slice["min_close"].iloc[0]) if not end_slice.empty else 0.0
            end_last_price = float(end_slice["min_close"].iloc[-1]) if not end_slice.empty else 0.0
            end_rise = (end_last_price - end_first_price) / end_first_price if end_first_price > 0 else 0.0
            end_vol_ratio = end_vol / pre_end_vol if pre_end_vol > 0 else 0.0

            cond_end_strict = bool((end_rise >= END_RISE_LOW) & (end_rise <= END_RISE_HIGH) & (end_vol_ratio >= END_VOL_RATIO)) if has_full_day else False
            cond_end_adaptive = bool((end_rise >= END_RISE_LOW_INTRA) & (end_rise <= END_RISE_HIGH_INTRA) & (end_vol_ratio >= END_VOL_RATIO_INTRA))

            cond_strength_strict = bool(cond_callback_strict & cond_end_strict)
            cond_strength_adaptive = bool(cond_callback_adaptive | cond_end_adaptive)

            board_ok_for_rank = cond_board if board_checked else True

            tier1_ok = bool(daily_ok_t3 & cond_vol_strict & cond_strength_strict & has_full_day & board_checked & board_ok_for_rank)
            tier2_ok = bool(daily_ok_t3 & cond_vol_adaptive & cond_strength_adaptive & board_ok_for_rank)

            vol_score = (1 - abs(vol_ratio - 1.65)) * 50
            ma_score = (1 - ma_dist) * 50
            base_score = float(vol_score + ma_score)

            if tier1_ok:
                score = round(base_score + 10, 2)
                all_results.append(
                    _make_record(
                        symbol=symbol,
                        name=name,
                        score=score,
                        reason=f"量比{vol_ratio:.2f}倍；均线达标；近20日涨幅{pos_ratio-1:.1%}；分时承接强；板块联动达标",
                        level=1,
                        note="；".join(note_parts) if note_parts else "",
                        price=float(today_d["close"]),
                        pct_chg=float(row.get("pct_chg", 0) or 0),
                        vol_ratio=round(vol_ratio, 2),
                        board_name=board_name,
                        board_pct=board_pct,
                        board_limit=board_limit,
                        minute_status=minute_status,
                        minute_granularity=minute_granularity,
                        has_full_day=has_full_day,
                        end_rise=round(end_rise, 4),
                        end_vol_ratio=round(end_vol_ratio, 2),
                        max_callback=round(float(max_callback), 4) if max_callback is not None else None,
                        recover_ratio=round(float(recover_ratio), 4) if recover_ratio is not None else None,
                        data_mode="Day+Minute+Board",
                    )
                )
                diag["tier1"] += 1
            elif tier2_ok:
                score = round(base_score, 2)
                if not board_checked:
                    note_parts.append("板块联动未校验，已降级到T2")
                all_results.append(
                    _make_record(
                        symbol=symbol,
                        name=name,
                        score=score,
                        reason=f"量比{vol_ratio:.2f}倍；均线达标；近20日涨幅{pos_ratio-1:.1%}；分时承接中等",
                        level=2,
                        note="；".join(note_parts) if note_parts else "",
                        price=float(today_d["close"]),
                        pct_chg=float(row.get("pct_chg", 0) or 0),
                        vol_ratio=round(vol_ratio, 2),
                        board_name=board_name,
                        board_pct=board_pct,
                        board_limit=board_limit,
                        minute_status=minute_status,
                        minute_granularity=minute_granularity,
                        has_full_day=has_full_day,
                        end_rise=round(end_rise, 4),
                        end_vol_ratio=round(end_vol_ratio, 2),
                        max_callback=round(float(max_callback), 4) if max_callback is not None else None,
                        recover_ratio=round(float(recover_ratio), 4) if recover_ratio is not None else None,
                        data_mode="Day+Minute",
                    )
                )
                diag["tier2"] += 1
            elif daily_ok_t3 and cond_board:
                note_parts.append("分时强度未达标，已降级到日线档位")
                score = round(base_score * 0.95, 2)
                all_results.append(
                    _make_record(
                        symbol=symbol,
                        name=name,
                        score=score,
                        reason=f"量比{vol_ratio:.2f}倍；均线达标；近20日涨幅{pos_ratio-1:.1%}；分时未达标",
                        level=3,
                        note="；".join(note_parts),
                        price=float(today_d["close"]),
                        pct_chg=float(row.get("pct_chg", 0) or 0),
                        vol_ratio=round(vol_ratio, 2),
                        board_name=board_name,
                        board_pct=board_pct,
                        board_limit=board_limit,
                        minute_status=minute_status,
                        minute_granularity=minute_granularity,
                        has_full_day=has_full_day,
                        data_mode="DayOnlyFromMinute",
                    )
                )
                diag["tier3"] += 1
            elif daily_ok_t4 and cond_board:
                note_parts.append("分时与日线强度不足，已降级到更宽松日线档位")
                score = round(base_score * 0.9, 2)
                all_results.append(
                    _make_record(
                        symbol=symbol,
                        name=name,
                        score=score,
                        reason=f"量比{vol_ratio:.2f}倍；日线基础达标；近20日涨幅{pos_ratio-1:.1%}",
                        level=4,
                        note="；".join(note_parts),
                        price=float(today_d["close"]),
                        pct_chg=float(row.get("pct_chg", 0) or 0),
                        vol_ratio=round(vol_ratio, 2),
                        board_name=board_name,
                        board_pct=board_pct,
                        board_limit=board_limit,
                        minute_status=minute_status,
                        minute_granularity=minute_granularity,
                        has_full_day=has_full_day,
                        data_mode="DayOnlyLooseFromMinute",
                    )
                )
                diag["tier4"] += 1

        except Exception as e:
            diag["exception"] += 1
            if len(diag["error_samples"]) < 10:
                diag["error_samples"].append({"symbol": symbol, "step": "loop", "error": str(e)})
            continue

    if all_results:
        df = pd.DataFrame(all_results)
        df = df.sort_values(["tier_level", "score"], ascending=[True, False]).head(100)
        print(f"筛选完成，输出 {len(df)} 只标的（含降级档位）！")
        cols = [c for c in ["symbol", "name", "tier", "score", "note", "reason"] if c in df.columns]
        if cols:
            print(df[cols].head(10))
    else:
        print("未发现符合条件的标的")
    print("诊断统计:", diag)

except Exception as e:
    print(f"脚本执行崩溃: {e}")
    print(traceback.format_exc())
    df = pd.DataFrame()

# 暴露结果变量
print("\n最终筛选结果已存入 df 变量")
