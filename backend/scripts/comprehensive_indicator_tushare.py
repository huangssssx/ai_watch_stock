import talib
import datetime as _datetime
import pandas as pd
import numpy as np

symbol = context.get("symbol", "")
stock_name = context.get("name", "") or ""
now_dt = pd.Timestamp.now()

def _is_na(x):
    try:
        return pd.isna(x)
    except Exception:
        return x is None

def _to_float(x):
    if _is_na(x):
        return None
    try:
        return float(x)
    except Exception:
        try:
            return float(str(x).replace(",", ""))
        except Exception:
            return None

def _to_int(x):
    if _is_na(x):
        return None
    try:
        return int(float(x))
    except Exception:
        return None

def _round(x, nd=3):
    v = _to_float(x)
    if v is None:
        return None
    return round(v, nd)

def _to_str(x):
    if _is_na(x):
        return None
    try:
        s = str(x).strip()
    except Exception:
        return None
    return s if s else None

def _clip_str(s, max_len=400):
    if s is None:
        return None
    ss = _to_str(s)
    if ss is None:
        return None
    return ss if len(ss) <= int(max_len) else (ss[: int(max_len)] + "…")

def _split_terms(s):
    ss = _to_str(s)
    if ss is None:
        return []
    for sep in ["；", ";", "、", "，", ",", "|", "/", " "]:
        ss = ss.replace(sep, "|")
    parts = [p.strip() for p in ss.split("|")]
    return [p for p in parts if p]

def _clip_record(obj, max_len=400):
    if isinstance(obj, dict):
        out = {}
        for k, v in obj.items():
            if isinstance(v, str):
                out[k] = _clip_str(v, max_len=max_len)
            else:
                out[k] = v
        return out
    return obj

def _clean_obj(obj):
    if isinstance(obj, dict):
        return {k: _clean_obj(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_clean_obj(v) for v in obj]
    if _is_na(obj):
        return None
    if isinstance(obj, (float, int)) and (not np.isfinite(obj)):
        return None
    if isinstance(obj, (np.floating, np.float32, np.float64)):
        v = float(obj)
        return v if np.isfinite(v) else None
    if isinstance(obj, (np.integer,)):
        return int(obj)
    if isinstance(obj, (_datetime.datetime, _datetime.date)):
        try:
            return obj.isoformat(sep=" ")
        except TypeError:
            return obj.isoformat()
    if isinstance(obj, pd.Timestamp):
        return obj.strftime("%Y-%m-%d %H:%M:%S")
    return obj

def _to_ts_code(sym: str):
    s = _to_str(sym)
    if not s:
        return None
    if s.endswith((".SH", ".SZ", ".BJ")):
        return s
    if s.startswith("6"):
        return s + ".SH"
    if s.startswith(("0", "3")):
        return s + ".SZ"
    if s.startswith(("4", "8")):
        return s + ".BJ"
    return None

def _get_last_trade_date():
    if pro is None:
        return None
    end_date = now_dt.strftime("%Y%m%d")
    start_date = (now_dt - pd.Timedelta(days=40)).strftime("%Y%m%d")
    try:
        df = pro.trade_cal(start_date=start_date, end_date=end_date, is_open="1")
    except Exception:
        df = None
    if df is None or df.empty:
        return None
    if "cal_date" not in df.columns:
        return None
    df = df.copy()
    df["cal_date"] = df["cal_date"].astype(str)
    df = df.sort_values("cal_date")
    return str(df.iloc[-1]["cal_date"])

def _safe_vwap(amount_raw, volume_raw, current_price):
    a = _to_float(amount_raw)
    v = _to_float(volume_raw)
    p = _to_float(current_price)
    if a is None or v is None or v <= 0:
        return p
    raw = a / v
    if p is not None and p > 0:
        ratio = raw / p
        if 0.08 <= ratio <= 0.12:
            return raw * 10.0
        if 80.0 <= ratio <= 120.0:
            return raw / 100.0
        if 0.8 <= ratio <= 1.2:
            return raw
    if p is not None and p > 0:
        if raw < p * 0.2:
            return raw * 10.0
        if raw > p * 50.0:
            return raw / 100.0
    return raw

def _infer_amount_volume_multipliers(amount_raw, volume_raw, current_price):
    a = _to_float(amount_raw)
    v = _to_float(volume_raw)
    p = _to_float(current_price)
    if a is None or v is None or v <= 0 or p is None or p <= 0:
        return 1.0, 1.0
    raw = a / v
    ratio = raw / p
    if 0.08 <= ratio <= 0.12:
        return 1000.0, 100.0
    if 80.0 <= ratio <= 120.0:
        return 1.0, 100.0
    if 0.8 <= ratio <= 1.2:
        return 1.0, 1.0
    if raw < p * 0.2:
        return 1000.0, 100.0
    if raw > p * 50.0:
        return 1.0, 100.0
    return 1.0, 1.0

def _attach_describe(obj, text):
    if obj is None or not isinstance(obj, dict):
        obj = {}
    if "describe" not in obj:
        obj["describe"] = text
    return obj

def _pick(d, path, default=None):
    cur = d
    for p in path.split("."):
        if not isinstance(cur, dict):
            return default
        cur = cur.get(p)
    return cur if cur is not None else default

def _non_empty(v):
    if v is None:
        return False
    if isinstance(v, dict):
        if "error" in v:
            return False
        if "hint" in v:
            return False
        if set(v.keys()) == {"describe"}:
            return False
        return len(v) > 0
    if isinstance(v, list):
        return len(v) > 0
    return True

ts_code = _to_ts_code(symbol)
last_trade_date = _get_last_trade_date()

meta = {
    "symbol": symbol,
    "ts_code": ts_code,
    "name": stock_name,
    "generated_at": now_dt.strftime("%Y-%m-%d %H:%M:%S"),
    "covers_indicators": [
        "最近 20 日：日线（K 线）",
        "量能指标 (VOL/OBV/MFI/ADL)",
        "次新股属性 (上市时长/流通盘/筹码)",
        "交易规则约束 (涨跌停/T+1)",
        "基本面概览 (行业/股本/财务指标)",
        "公司画像与题材 (主营/概念/互动)",
        "资金流向（个股近 100 个交易日内）",
        "5 分钟数据（分时历史）",
        "近 10 天换手率",
        "实时估值 (PE/PB/量比)",
        "ATR （平均真实波幅）",
        "盘口五档/涨停跌停/外内盘",
        "实时快照（单股过滤版）",
        "今日分时 VWAP/日内强弱",
        "新闻获取",
        "筹码集中度",
        "最近 3 次上榜龙虎榜主力动向分析（年度回溯）",
        "融资融券",
        "个股资金流向分析（主力/超大单/大单）",
        "基金持仓分析",
        "大单追踪",
        "趋势系统 (MA/MACD/RSI)",
        "突破与风险 (ATR/通道)",
        "相对强弱 (RS vs HS300)",
        "中短线-突破与波动（布林+唐奇安+ATR）",
        "中短线-相对强弱（对沪深300）",
        "中短线-震荡与反转 (KDJ/WR/BIAS)",
    ],
}

snapshot = {}
order_book = {}
daily = {}
volume_indicators = {}
intraday = {}
intraday_min_df = None
relative = {}
money_flow = {}
chip = {}
technicals = {}
signals = {}
lhb = {}
margin = {}
fund_holding = {}
big_deal = {}
share_structure = {}
fundamental = {}
trading_rules = {}
new_stock = {}
company = {}
disclosure = {}

if not symbol:
    result = {
        "meta": meta,
        "error": "symbol_missing",
        "snapshot": snapshot,
        "order_book": order_book,
        "daily": daily,
        "volume": volume_indicators,
        "share_structure": share_structure,
        "fundamental": fundamental,
        "trading_rules": trading_rules,
        "new_stock": new_stock,
        "company": company,
        "disclosure": disclosure,
        "intraday": intraday,
        "technicals": technicals,
        "relative": relative,
        "money_flow": money_flow,
        "chip": chip,
        "signals": signals,
        "lhb": lhb,
        "margin": margin,
        "fund_holding": fund_holding,
        "big_deal": big_deal,
    }
else:
    rt_row = None
    try:
        rt_df = ts.get_realtime_quotes([symbol]) if ts is not None else pd.DataFrame()
        if rt_df is not None and not rt_df.empty:
            rt_row = rt_df.iloc[0].to_dict()
            price = _to_float(rt_row.get("price"))
            pre_close = _to_float(rt_row.get("pre_close"))
            change_pct = None
            if price is not None and pre_close:
                change_pct = _round((price / pre_close - 1.0) * 100.0, 2)
            snapshot = {
                "price": _round(price, 3),
                "change_pct": change_pct,
                "open": _round(rt_row.get("open"), 3),
                "prev_close": _round(pre_close, 3),
                "high": _round(rt_row.get("high"), 3),
                "low": _round(rt_row.get("low"), 3),
                "volume": _to_float(rt_row.get("volume")),
                "amount": _to_float(rt_row.get("amount")),
            }
            if not meta.get("name"):
                meta["name"] = _to_str(rt_row.get("name"))

            vwap = _safe_vwap(snapshot.get("amount"), snapshot.get("volume"), snapshot.get("price"))
            vwap_bias_pct = None
            if snapshot.get("price") is not None and vwap:
                vwap_bias_pct = _round((float(snapshot["price"]) / vwap - 1.0) * 100.0, 2)
            intraday = {
                "date": now_dt.strftime("%Y-%m-%d"),
                "last_time": now_dt.strftime("%Y-%m-%d %H:%M:%S"),
                "price": snapshot.get("price"),
                "vwap": _round(vwap),
                "vwap_bias_pct": vwap_bias_pct,
                "day_high": snapshot.get("high"),
                "day_low": snapshot.get("low"),
                "total_volume_raw": snapshot.get("volume"),
                "total_amount_raw": snapshot.get("amount"),
            }
    except Exception as e:
        snapshot = {"error": str(e)}

    try:
        if pro is None or ts_code is None or last_trade_date is None:
            raise RuntimeError("tushare_pro_not_ready")
        basic = pro.stock_basic(ts_code=ts_code, fields="ts_code,name,industry,area,list_date")
        daily_basic = pro.daily_basic(
            ts_code=ts_code,
            trade_date=last_trade_date,
            fields="ts_code,trade_date,turnover_rate,volume_ratio,pe,pe_ttm,pb,total_mv,circ_mv,total_share,float_share,free_share",
        )
        b0 = basic.iloc[0].to_dict() if basic is not None and not basic.empty else {}
        d0 = daily_basic.iloc[0].to_dict() if daily_basic is not None and not daily_basic.empty else {}
        if not meta.get("name"):
            meta["name"] = _to_str(b0.get("name"))
        if isinstance(snapshot, dict):
            snapshot["turnover_rate"] = _round(d0.get("turnover_rate"), 2)
            snapshot["volume_ratio"] = _round(d0.get("volume_ratio"), 3)
            snapshot["pe_dynamic"] = _round(d0.get("pe_ttm") if d0.get("pe_ttm") is not None else d0.get("pe"), 2)
            snapshot["pb"] = _round(d0.get("pb"), 3)
            total_mv = _to_float(d0.get("total_mv"))
            circ_mv = _to_float(d0.get("circ_mv"))
            snapshot["total_mv"] = None if total_mv is None else total_mv * 10000.0
            snapshot["circ_mv"] = None if circ_mv is None else circ_mv * 10000.0

        share_structure = {
            "industry": _to_str(b0.get("industry")),
            "listing_date": _to_str(b0.get("list_date")),
            "total_shares": _to_float(d0.get("total_share")),
            "float_shares": _to_float(d0.get("float_share")),
            "free_float_shares": _to_float(d0.get("free_share")),
            "free_float_mv": _to_float(snapshot.get("circ_mv")) if isinstance(snapshot, dict) else None,
            "raw": {"stock_basic": b0, "daily_basic": d0},
        }

        listing_dt = pd.to_datetime(share_structure.get("listing_date"), errors="coerce")
        days_since_listing = None if pd.isna(listing_dt) else int((pd.Timestamp.now().normalize() - listing_dt.normalize()).days)
        new_stock = {
            "listing_date": share_structure.get("listing_date"),
            "days_since_listing": days_since_listing,
            "is_recent_ipo": bool(days_since_listing is not None and days_since_listing <= 365),
            "float_mv_yi": _round((_to_float(snapshot.get("circ_mv")) / 1e8) if isinstance(snapshot, dict) else None, 3),
        }

        company = {
            "profile": {
                "name": meta.get("name"),
                "industry": share_structure.get("industry"),
                "region": _to_str(b0.get("area")),
                "listing_date": share_structure.get("listing_date"),
            },
            "concepts": [],
        }

        c_fn = getattr(pro, "concept_detail", None)
        if callable(c_fn):
            cdf = c_fn(ts_code=ts_code)
            if cdf is not None and not cdf.empty and "concept_name" in cdf.columns:
                company["concepts"] = [x for x in cdf["concept_name"].astype(str).tolist() if x]
    except Exception as e:
        if not share_structure:
            share_structure = {"error": str(e)}
        if not company:
            company = {"error": str(e)}

    try:
        if pro is None or ts_code is None:
            raise RuntimeError("tushare_pro_not_ready")
        fi_fn = getattr(pro, "fina_indicator", None)
        fi_df = fi_fn(ts_code=ts_code) if callable(fi_fn) else pd.DataFrame()
        if fi_df is not None and not fi_df.empty:
            if "end_date" in fi_df.columns:
                fi_df = fi_df.copy()
                fi_df["end_date"] = fi_df["end_date"].astype(str)
                fi_df = fi_df.sort_values("end_date").tail(4)
            else:
                fi_df = fi_df.tail(4)
            fundamental = {"financial_analysis_last_4": fi_df.to_dict("records")}
        else:
            fundamental = {"hint": "无财务分析指标数据"}
    except Exception as e:
        fundamental = {"error": str(e)}

    try:
        if rt_row is None:
            raise RuntimeError("realtime_quote_missing")
        buy = []
        sell = []
        for i in range(1, 6):
            buy.append({"price": _round(rt_row.get(f"b{i}_p"), 3), "vol": _to_float(rt_row.get(f"b{i}_v"))})
            sell.append({"price": _round(rt_row.get(f"a{i}_p"), 3), "vol": _to_float(rt_row.get(f"a{i}_v"))})
        order_book = {
            "latest": _round(rt_row.get("price"), 3),
            "avg_price": None,
            "high": _round(rt_row.get("high"), 3),
            "low": _round(rt_row.get("low"), 3),
            "open": _round(rt_row.get("open"), 3),
            "prev_close": _round(rt_row.get("pre_close"), 3),
            "buy": buy,
            "sell": sell,
        }
    except Exception as e:
        order_book = {"error": str(e)}

    try:
        if pro is None or ts_code is None or last_trade_date is None:
            raise RuntimeError("tushare_pro_not_ready")
        limit_fn = getattr(pro, "stk_limit", None)
        limit_df = limit_fn(ts_code=ts_code, trade_date=last_trade_date) if callable(limit_fn) else pd.DataFrame()
        if limit_df is not None and not limit_df.empty:
            rr = limit_df.iloc[0].to_dict()
            limit_up = _to_float(rr.get("up_limit"))
            limit_down = _to_float(rr.get("down_limit"))
        else:
            limit_up = None
            limit_down = None
        prev_close = None
        if isinstance(snapshot, dict):
            prev_close = _to_float(snapshot.get("prev_close"))
        if prev_close is None and isinstance(order_book, dict):
            prev_close = _to_float(order_book.get("prev_close"))
        limit_up_pct = None if (not prev_close or limit_up is None) else _round((limit_up / prev_close - 1.0) * 100.0, 2)
        limit_down_pct = None if (not prev_close or limit_down is None) else _round((limit_down / prev_close - 1.0) * 100.0, 2)
        trading_rules = {
            "t_plus_1": True,
            "limit_up": _round(limit_up, 3),
            "limit_down": _round(limit_down, 3),
            "limit_up_pct": limit_up_pct,
            "limit_down_pct": limit_down_pct,
        }
        if isinstance(order_book, dict):
            if limit_up is not None:
                order_book["limit_up"] = _round(limit_up, 3)
            if limit_down is not None:
                order_book["limit_down"] = _round(limit_down, 3)
    except Exception as e:
        trading_rules = {"error": str(e)}

    try:
        disclosure = {}
        rep_df = pd.DataFrame()
        end_str = now_dt.strftime("%Y%m%d")
        start_str = (now_dt - pd.Timedelta(days=120)).strftime("%Y%m%d")

        anns_fn = getattr(pro, "anns", None) if pro is not None else None
        if callable(anns_fn) and ts_code is not None:
            try:
                rep_df = anns_fn(ts_code=ts_code, start_date=start_str, end_date=end_str)
            except Exception:
                rep_df = pd.DataFrame()

        if (rep_df is None or rep_df.empty) and ts is not None:
            notices_fn = getattr(ts, "get_notices", None)
            if callable(notices_fn):
                try:
                    rep_df = notices_fn(symbol)
                except Exception:
                    rep_df = pd.DataFrame()

        if rep_df is not None and not rep_df.empty:
            rep_df = rep_df.copy()
            dt_col = None
            for c in ["ann_date", "f_ann_date", "date", "datetime", "pub_time"]:
                if c in rep_df.columns:
                    dt_col = c
                    break
            if dt_col:
                rep_df[dt_col] = pd.to_datetime(rep_df[dt_col], errors="coerce")
                rep_df = rep_df[rep_df[dt_col].notna()].sort_values(dt_col, ascending=False)
            rep_df = rep_df.head(10)
            recs = []
            for _, rr in rep_df.iterrows():
                rec = {
                    "title": _clip_str(rr.get("title") or rr.get("ann_title") or rr.get("summary") or rr.get("name"), 500),
                    "time": rr.get(dt_col).strftime("%Y-%m-%d %H:%M:%S") if dt_col and rr.get(dt_col) == rr.get(dt_col) else None,
                    "url": _to_str(rr.get("url")),
                }
                recs.append(_clip_record(rec, max_len=600))
            disclosure["announcements_top10"] = recs
        else:
            disclosure["announcements_hint"] = "无公告数据"
        disclosure["irm_hint"] = "互动易问答接口未配置"
    except Exception as e:
        disclosure = {"error": str(e)}

    try:
        if pro is None or ts_code is None:
            raise RuntimeError("tushare_pro_not_ready")
        end_dt = now_dt
        start_dt = end_dt - pd.Timedelta(days=420)
        start_str = start_dt.strftime("%Y%m%d")
        end_str = end_dt.strftime("%Y%m%d")
        df_daily = pro.daily(ts_code=ts_code, start_date=start_str, end_date=end_str)
        db_df = pro.daily_basic(ts_code=ts_code, start_date=start_str, end_date=end_str, fields="ts_code,trade_date,turnover_rate") if getattr(pro, "daily_basic", None) else pd.DataFrame()
        if df_daily is not None and not df_daily.empty:
            df_daily = df_daily.copy()
            df_daily["trade_date"] = df_daily["trade_date"].astype(str)
            df_daily = df_daily.sort_values("trade_date").reset_index(drop=True)
            for c in ["open", "close", "high", "low", "vol", "amount"]:
                if c in df_daily.columns:
                    df_daily[c] = pd.to_numeric(df_daily[c], errors="coerce")
            if db_df is not None and not db_df.empty and "trade_date" in db_df.columns and "turnover_rate" in db_df.columns:
                db_df = db_df.copy()
                db_df["trade_date"] = db_df["trade_date"].astype(str)
                db_df["turnover_rate"] = pd.to_numeric(db_df["turnover_rate"], errors="coerce")
                df_daily = pd.merge(df_daily, db_df[["trade_date", "turnover_rate"]], on="trade_date", how="left")

            close = df_daily["close"].values
            high = df_daily["high"].values
            low = df_daily["low"].values
            vol_raw = df_daily["vol"].values if "vol" in df_daily.columns else np.array([])
            amt_raw = df_daily["amount"].values if "amount" in df_daily.columns else np.array([])
            cur_px = _to_float(close[-1]) if len(close) else None
            amt_mul, vol_mul = (1.0, 1.0)
            if len(vol_raw) and len(amt_raw) and cur_px is not None:
                amt_mul, vol_mul = _infer_amount_volume_multipliers(amt_raw[-1], vol_raw[-1], cur_px)
            vol = vol_raw * vol_mul if len(vol_raw) else vol_raw
            amt = amt_raw * amt_mul if len(amt_raw) else amt_raw

            ma5 = talib.SMA(close, timeperiod=5)
            ma6 = talib.SMA(close, timeperiod=6)
            ma10 = talib.SMA(close, timeperiod=10)
            ma12 = talib.SMA(close, timeperiod=12)
            ma20 = talib.SMA(close, timeperiod=20)
            ma24 = talib.SMA(close, timeperiod=24)
            ma60 = talib.SMA(close, timeperiod=60)
            ma120 = talib.SMA(close, timeperiod=120)
            ema20 = talib.EMA(close, timeperiod=20)

            macd, macd_signal, macd_hist = talib.MACD(close, fastperiod=12, slowperiod=26, signalperiod=9)
            rsi6 = talib.RSI(close, timeperiod=6)
            rsi12 = talib.RSI(close, timeperiod=12)
            rsi14 = talib.RSI(close, timeperiod=14)
            rsi24 = talib.RSI(close, timeperiod=24)
            k, d = talib.STOCH(high, low, close, fastk_period=9, slowk_period=3, slowk_matype=0, slowd_period=3, slowd_matype=0)
            j = 3 * k - 2 * d
            boll_up, boll_mid, boll_low = talib.BBANDS(close, timeperiod=20, nbdevup=2, nbdevdn=2, matype=0)
            atr14 = talib.ATR(high, low, close, timeperiod=14)
            wr14 = talib.WILLR(high, low, close, timeperiod=14)
            adx14 = talib.ADX(high, low, close, timeperiod=14)

            donchian_hi_20 = pd.Series(high).rolling(20).max().values
            donchian_lo_20 = pd.Series(low).rolling(20).min().values
            donchian_hi_55 = pd.Series(high).rolling(55).max().values
            donchian_lo_55 = pd.Series(low).rolling(55).min().values

            vwap_20 = None
            if len(vol_raw) and len(amt_raw):
                vol_20 = pd.Series(vol_raw).rolling(20).sum().values
                amt_20 = pd.Series(amt_raw).rolling(20).sum().values
                vwap_20 = _round(_safe_vwap(amt_20[-1], vol_20[-1], cur_px), 3)

            tail = df_daily.tail(20).copy()
            if not tail.empty:
                tail["pct_change"] = tail["close"].pct_change() * 100.0
                daily_items = []
                for _, rr in tail.iterrows():
                    amt_yuan = _to_float(rr.get("amount"))
                    amt_yuan = None if amt_yuan is None else amt_yuan * amt_mul
                    vol_share = _to_float(rr.get("vol"))
                    vol_share = None if vol_share is None else vol_share * vol_mul
                    daily_items.append(
                        {
                            "date": rr.get("trade_date"),
                            "open": _round(rr.get("open"), 3),
                            "high": _round(rr.get("high"), 3),
                            "low": _round(rr.get("low"), 3),
                            "close": _round(rr.get("close"), 3),
                            "volume": vol_share,
                            "amount": amt_yuan,
                            "amount_yi": _round((amt_yuan / 1e8) if amt_yuan is not None else None, 3),
                            "turnover_rate": _round(rr.get("turnover_rate"), 2),
                            "pct_change": _round(rr.get("pct_change"), 2),
                        }
                    )
                turnover_last_10 = []
                t10 = df_daily.tail(10).copy()
                for _, rr in t10.iterrows():
                    turnover_last_10.append(
                        {
                            "date": rr.get("trade_date"),
                            "close": _round(rr.get("close"), 3),
                            "turnover_rate": _round(rr.get("turnover_rate"), 2),
                        }
                    )
                daily = {"last_20": daily_items, "turnover_last_10": turnover_last_10}

            technicals = {
                "latest": {
                    "ma5": _round(ma5[-1]),
                    "ma6": _round(ma6[-1]),
                    "ma10": _round(ma10[-1]),
                    "ma12": _round(ma12[-1]),
                    "ma20": _round(ma20[-1]),
                    "ma24": _round(ma24[-1]),
                    "ma60": _round(ma60[-1]),
                    "ma120": _round(ma120[-1]),
                    "ema20": _round(ema20[-1]),
                    "macd": _round(macd[-1]),
                    "macd_signal": _round(macd_signal[-1]),
                    "macd_hist": _round(macd_hist[-1]),
                    "rsi_6": _round(rsi6[-1]),
                    "rsi_12": _round(rsi12[-1]),
                    "rsi_14": _round(rsi14[-1]),
                    "rsi_24": _round(rsi24[-1]),
                    "kdj_k": _round(k[-1]),
                    "kdj_d": _round(d[-1]),
                    "kdj_j": _round(j[-1]),
                    "boll_upper": _round(boll_up[-1]),
                    "boll_mid": _round(boll_mid[-1]),
                    "boll_lower": _round(boll_low[-1]),
                    "atr_14": _round(atr14[-1]),
                    "wr_14": _round(wr14[-1]),
                    "adx_14": _round(adx14[-1]),
                    "donchian_high_20": _round(donchian_hi_20[-1]),
                    "donchian_low_20": _round(donchian_lo_20[-1]),
                    "donchian_high_55": _round(donchian_hi_55[-1]),
                    "donchian_low_55": _round(donchian_lo_55[-1]),
                    "vwap_20": vwap_20,
                }
            }

            if len(vol):
                vol_s = pd.Series(vol)
                vol_ma5 = vol_s.rolling(5).mean()
                vol_ma10 = vol_s.rolling(10).mean()
                vol_ma20 = vol_s.rolling(20).mean()
                vol_std20 = vol_s.rolling(20).std()

                cur_vol = _to_float(vol_s.iloc[-1])
                vol_ma5_v = _to_float(vol_ma5.iloc[-1])
                vol_ma10_v = _to_float(vol_ma10.iloc[-1])
                vol_ma20_v = _to_float(vol_ma20.iloc[-1])
                vol_std20_v = _to_float(vol_std20.iloc[-1])

                vol_ratio_5 = None if (cur_vol is None or not vol_ma5_v) else _round(cur_vol / vol_ma5_v, 3)
                vol_ratio_10 = None if (cur_vol is None or not vol_ma10_v) else _round(cur_vol / vol_ma10_v, 3)
                vol_ratio_20 = None if (cur_vol is None or not vol_ma20_v) else _round(cur_vol / vol_ma20_v, 3)

                vol_z20 = None
                if cur_vol is not None and vol_ma20_v is not None and vol_std20_v is not None and vol_std20_v:
                    vol_z20 = _round((cur_vol - vol_ma20_v) / vol_std20_v, 3)

                obv_v = None
                obv_ma20_v = None
                mfi14_v = None
                ad_v = None
                adosc_v = None
                vpt_v = None

                try:
                    obv_arr = talib.OBV(close, vol)
                    if len(obv_arr):
                        obv_v = _round(obv_arr[-1], 3)
                        obv_ma20 = pd.Series(obv_arr).rolling(20).mean()
                        obv_ma20_v = _round(obv_ma20.iloc[-1], 3)
                except Exception:
                    pass

                try:
                    mfi14 = talib.MFI(high, low, close, vol, timeperiod=14)
                    if len(mfi14):
                        mfi14_v = _round(mfi14[-1], 3)
                except Exception:
                    pass

                try:
                    ad_arr = talib.AD(high, low, close, vol)
                    if len(ad_arr):
                        ad_v = _round(ad_arr[-1], 3)
                except Exception:
                    pass

                try:
                    adosc_arr = talib.ADOSC(high, low, close, vol, fastperiod=3, slowperiod=10)
                    if len(adosc_arr):
                        adosc_v = _round(adosc_arr[-1], 3)
                except Exception:
                    pass

                try:
                    close_s = pd.Series(close)
                    ret = close_s.pct_change().fillna(0.0)
                    vpt = (ret * vol_s).cumsum()
                    vpt_v = _round(vpt.iloc[-1], 3)
                except Exception:
                    pass

                volume_indicators = {
                    "latest": {
                        "volume": cur_vol,
                        "volume_ma5": vol_ma5_v,
                        "volume_ma10": vol_ma10_v,
                        "volume_ma20": vol_ma20_v,
                        "volume_ratio_5": vol_ratio_5,
                        "volume_ratio_10": vol_ratio_10,
                        "volume_ratio_20": vol_ratio_20,
                        "volume_zscore_20": vol_z20,
                        "obv": obv_v,
                        "obv_ma20": obv_ma20_v,
                        "mfi_14": mfi14_v,
                        "ad": ad_v,
                        "adosc_3_10": adosc_v,
                        "vpt": vpt_v,
                    }
                }

            last_close = _to_float(close[-1])
            bias6_pct = None
            bias12_pct = None
            bias24_pct = None
            ma6_v = technicals.get("latest", {}).get("ma6")
            ma12_v = technicals.get("latest", {}).get("ma12")
            ma24_v = technicals.get("latest", {}).get("ma24")
            if last_close is not None and ma6_v not in (None, 0):
                bias6_pct = _round((last_close / ma6_v - 1.0) * 100.0, 2)
            if last_close is not None and ma12_v not in (None, 0):
                bias12_pct = _round((last_close / ma12_v - 1.0) * 100.0, 2)
            if last_close is not None and ma24_v not in (None, 0):
                bias24_pct = _round((last_close / ma24_v - 1.0) * 100.0, 2)
            t = "SIDEWAYS"
            if last_close is not None and technicals.get("latest", {}).get("ma20") is not None and technicals.get("latest", {}).get("ma60") is not None:
                ma20_v = technicals["latest"]["ma20"]
                ma60_v = technicals["latest"]["ma60"]
                if last_close > ma20_v and ma20_v > ma60_v:
                    t = "UP"
                elif last_close < ma20_v and ma20_v < ma60_v:
                    t = "DOWN"

            overbought = None
            oversold = None
            rsi14_v = technicals.get("latest", {}).get("rsi_14")
            if rsi14_v is not None:
                overbought = bool(rsi14_v >= 70)
                oversold = bool(rsi14_v <= 30)

            signals = {
                "trend": t,
                "overbought": overbought,
                "oversold": oversold,
                "breakout_20d_high": bool(last_close is not None and technicals["latest"]["donchian_high_20"] is not None and last_close >= technicals["latest"]["donchian_high_20"]),
                "breakdown_20d_low": bool(last_close is not None and technicals["latest"]["donchian_low_20"] is not None and last_close <= technicals["latest"]["donchian_low_20"]),
                "breakout_55d_high": bool(last_close is not None and technicals["latest"]["donchian_high_55"] is not None and last_close >= technicals["latest"]["donchian_high_55"]),
                "breakdown_55d_low": bool(last_close is not None and technicals["latest"]["donchian_low_55"] is not None and last_close <= technicals["latest"]["donchian_low_55"]),
                "trend_system": {
                    "ma_alignment": "BULLISH" if (last_close is not None and technicals["latest"]["ma5"] is not None and technicals["latest"]["ma10"] is not None and technicals["latest"]["ma20"] is not None and last_close > technicals["latest"]["ma5"] > technicals["latest"]["ma10"] > technicals["latest"]["ma20"]) else ("BEARISH" if (last_close is not None and technicals["latest"]["ma5"] is not None and technicals["latest"]["ma10"] is not None and technicals["latest"]["ma20"] is not None and last_close < technicals["latest"]["ma5"] < technicals["latest"]["ma10"] < technicals["latest"]["ma20"]) else "MIXED"),
                    "macd_state": "BULLISH" if (technicals["latest"]["macd"] is not None and technicals["latest"]["macd_signal"] is not None and technicals["latest"]["macd"] > technicals["latest"]["macd_signal"]) else ("BEARISH" if (technicals["latest"]["macd"] is not None and technicals["latest"]["macd_signal"] is not None and technicals["latest"]["macd"] < technicals["latest"]["macd_signal"]) else None),
                    "rsi14": technicals["latest"].get("rsi_14"),
                },
                "breakout_risk": {
                    "atr_pct": _round((technicals["latest"]["atr_14"] / last_close) * 100.0, 2) if (technicals["latest"]["atr_14"] is not None and last_close) else None,
                    "boll_position": "ABOVE_UPPER" if (last_close is not None and technicals["latest"]["boll_upper"] is not None and last_close > technicals["latest"]["boll_upper"]) else ("BELOW_LOWER" if (last_close is not None and technicals["latest"]["boll_lower"] is not None and last_close < technicals["latest"]["boll_lower"]) else "INSIDE"),
                    "donchian_breakout": bool(last_close is not None and technicals["latest"]["donchian_high_20"] is not None and last_close >= technicals["latest"]["donchian_high_20"]),
                },
                "short_term": {
                    "kdj_j": technicals["latest"].get("kdj_j"),
                    "wr_14": technicals["latest"].get("wr_14"),
                    "bias6_pct": bias6_pct,
                    "bias12_pct": bias12_pct,
                    "bias24_pct": bias24_pct,
                    "oscillator_state": "OVERBOUGHT" if (technicals["latest"].get("kdj_j") is not None and technicals["latest"]["kdj_j"] > 100) else ("OVERSOLD" if (technicals["latest"].get("kdj_j") is not None and technicals["latest"]["kdj_j"] < 0) else None),
                },
            }
    except Exception as e:
        daily = {"error": str(e)}

    try:
        if pro is None or ts_code is None:
            raise RuntimeError("tushare_pro_not_ready")
        rt_min_fn = getattr(pro, "rt_min", None)
        df_min = rt_min_fn(ts_code=ts_code, freq="1MIN") if callable(rt_min_fn) else pd.DataFrame()
        if df_min is not None and not df_min.empty:
            df_min = df_min.copy()
            time_col = "trade_time" if "trade_time" in df_min.columns else ("datetime" if "datetime" in df_min.columns else None)
            if time_col:
                df_min[time_col] = pd.to_datetime(df_min[time_col], errors="coerce")
                df_min = df_min[df_min[time_col].notna()].sort_values(time_col).reset_index(drop=True)
                used_date = df_min[time_col].dt.date.iloc[-1]
                day_df = df_min[df_min[time_col].dt.date == used_date].copy()
            else:
                day_df = df_min.copy()
                used_date = now_dt.date()
            for c in ["open", "high", "low", "close", "vol", "amount"]:
                if c in day_df.columns:
                    day_df[c] = pd.to_numeric(day_df[c], errors="coerce")
            intraday_min_df = day_df
            last_row = day_df.iloc[-1]
            total_amount = float(pd.to_numeric(day_df.get("amount"), errors="coerce").fillna(0).sum()) if "amount" in day_df.columns else 0.0
            total_vol = float(pd.to_numeric(day_df.get("vol"), errors="coerce").fillna(0).sum()) if "vol" in day_df.columns else 0.0
            current_price = _to_float(last_row.get("close"))
            vwap = _safe_vwap(total_amount, total_vol, current_price)
            day_high = _to_float(pd.to_numeric(day_df.get("high"), errors="coerce").max()) if "high" in day_df.columns else current_price
            day_low = _to_float(pd.to_numeric(day_df.get("low"), errors="coerce").min()) if "low" in day_df.columns else current_price
            vwap_bias_pct = None
            if current_price is not None and vwap:
                vwap_bias_pct = _round((current_price / vwap - 1.0) * 100.0, 2)
            last_time = None
            if "trade_time" in day_df.columns:
                last_time = pd.to_datetime(last_row.get("trade_time"), errors="coerce")
            if last_time is None and "datetime" in day_df.columns:
                last_time = pd.to_datetime(last_row.get("datetime"), errors="coerce")
            intraday = {
                "date": str(used_date),
                "last_time": last_time.strftime("%Y-%m-%d %H:%M:%S") if last_time is not None and last_time == last_time else None,
                "price": _round(current_price),
                "vwap": _round(vwap),
                "vwap_bias_pct": vwap_bias_pct,
                "day_high": _round(day_high),
                "day_low": _round(day_low),
                "total_volume_raw": _round(total_vol, 3),
                "total_amount_raw": _round(total_amount, 3),
            }
    except Exception as e:
        if isinstance(intraday, dict):
            intraday["min_1m_error"] = str(e)
        else:
            intraday = {"error": str(e)}

    try:
        if pro is None or ts_code is None:
            raise RuntimeError("tushare_pro_not_ready")
        rt_min_fn = getattr(pro, "rt_min", None)
        df_5m = rt_min_fn(ts_code=ts_code, freq="5MIN") if callable(rt_min_fn) else pd.DataFrame()
        if (df_5m is None or df_5m.empty) and ts is not None:
            pro_bar_fn = getattr(ts, "pro_bar", None)
            if callable(pro_bar_fn) and last_trade_date is not None:
                try:
                    df_5m = pro_bar_fn(ts_code=ts_code, start_date=last_trade_date, end_date=last_trade_date, freq="5min", asset="E")
                except TypeError:
                    try:
                        df_5m = pro_bar_fn(ts_code=ts_code, start_date=last_trade_date, end_date=last_trade_date, freq="5min")
                    except Exception:
                        df_5m = pd.DataFrame()
                except Exception:
                    df_5m = pd.DataFrame()
        if (df_5m is None or df_5m.empty) and ts is not None:
            k_fn = getattr(ts, "get_k_data", None)
            if callable(k_fn):
                try:
                    start_k = (now_dt - pd.Timedelta(days=10)).strftime("%Y-%m-%d")
                    end_k = now_dt.strftime("%Y-%m-%d")
                    k_df = k_fn(symbol, start=start_k, end=end_k, ktype="5")
                except TypeError:
                    try:
                        k_df = k_fn(symbol, ktype="5")
                    except Exception:
                        k_df = pd.DataFrame()
                except Exception:
                    k_df = pd.DataFrame()
                if k_df is not None and not k_df.empty:
                    k_df = k_df.copy()
                    dt_col = "date" if "date" in k_df.columns else None
                    if dt_col:
                        k_df["datetime"] = pd.to_datetime(k_df[dt_col], errors="coerce")
                    if "volume" in k_df.columns and "vol" not in k_df.columns:
                        k_df["vol"] = pd.to_numeric(k_df["volume"], errors="coerce")
                    df_5m = k_df
        if (df_5m is None or df_5m.empty) and ts is not None and last_trade_date is not None and len(str(last_trade_date)) == 8:
            date_for_tick = f"{str(last_trade_date)[:4]}-{str(last_trade_date)[4:6]}-{str(last_trade_date)[6:]}"
            ticks_df = pd.DataFrame()
            ticks_fn = getattr(ts, "get_tick_data", None)
            if callable(ticks_fn):
                try:
                    ticks_df = ticks_fn(symbol, date=date_for_tick)
                except Exception:
                    ticks_df = pd.DataFrame()
            if ticks_df is not None and not ticks_df.empty and "time" in ticks_df.columns and "price" in ticks_df.columns:
                ticks_df = ticks_df.copy()
                ticks_df["dt"] = pd.to_datetime(date_for_tick + " " + ticks_df["time"].astype(str), errors="coerce")
                ticks_df = ticks_df[ticks_df["dt"].notna()].sort_values("dt").reset_index(drop=True)
                ticks_df["price"] = pd.to_numeric(ticks_df["price"], errors="coerce")
                vol_col = "volume" if "volume" in ticks_df.columns else ("vol" if "vol" in ticks_df.columns else None)
                if vol_col is None:
                    ticks_df["vol"] = np.nan
                else:
                    ticks_df["vol"] = pd.to_numeric(ticks_df[vol_col], errors="coerce")
                rs = ticks_df.set_index("dt").resample("5min")
                df_5m = pd.DataFrame(
                    {
                        "datetime": rs["price"].first().index,
                        "open": rs["price"].first().values,
                        "high": rs["price"].max().values,
                        "low": rs["price"].min().values,
                        "close": rs["price"].last().values,
                        "vol": rs["vol"].sum().values,
                    }
                )
                df_5m = df_5m.dropna(subset=["close"]).reset_index(drop=True)
        if (df_5m is None or df_5m.empty) and intraday_min_df is not None and not intraday_min_df.empty:
            df_tmp = intraday_min_df.copy()
            time_col = "trade_time" if "trade_time" in df_tmp.columns else ("datetime" if "datetime" in df_tmp.columns else None)
            if time_col:
                df_tmp["dt"] = pd.to_datetime(df_tmp[time_col], errors="coerce")
                df_tmp = df_tmp[df_tmp["dt"].notna()].sort_values("dt").reset_index(drop=True)
                for c in ["open", "high", "low", "close", "vol", "amount"]:
                    if c in df_tmp.columns:
                        df_tmp[c] = pd.to_numeric(df_tmp[c], errors="coerce")
                agg = {}
                for c, fn in [("open", "first"), ("high", "max"), ("low", "min"), ("close", "last"), ("vol", "sum"), ("amount", "sum")]:
                    if c in df_tmp.columns:
                        agg[c] = fn
                if agg:
                    df_5m = df_tmp.set_index("dt").resample("5min").agg(agg).reset_index()
                    df_5m = df_5m.rename(columns={"dt": "datetime"})
        if df_5m is not None and not df_5m.empty:
            df_5m = df_5m.copy()
            time_col = "trade_time" if "trade_time" in df_5m.columns else ("datetime" if "datetime" in df_5m.columns else None)
            if time_col:
                df_5m[time_col] = pd.to_datetime(df_5m[time_col], errors="coerce")
                df_5m = df_5m[df_5m[time_col].notna()].sort_values(time_col).reset_index(drop=True)
                df_5m["time_str"] = df_5m[time_col].dt.strftime("%Y-%m-%d %H:%M:%S")
            else:
                df_5m["time_str"] = None
            if "close" in df_5m.columns:
                df_5m["close"] = pd.to_numeric(df_5m["close"], errors="coerce")
                df_5m["MA5"] = df_5m["close"].rolling(window=5).mean()
                df_5m["MA20"] = df_5m["close"].rolling(window=20).mean()
            df_5m = df_5m.tail(48)
            rows = []
            for _, rr in df_5m.iterrows():
                rows.append(
                    {
                        "time": rr.get("time_str"),
                        "open": _round(rr.get("open")),
                        "high": _round(rr.get("high")),
                        "low": _round(rr.get("low")),
                        "close": _round(rr.get("close")),
                        "volume": _to_float(rr.get("vol")),
                        "ma5": _round(rr.get("MA5")),
                        "ma20": _round(rr.get("MA20")),
                    }
                )
            if not isinstance(intraday, dict):
                intraday = {}
            intraday["bars_5m_last_48"] = rows
    except Exception as e:
        if isinstance(intraday, dict):
            intraday["bars_5m_error"] = str(e)

    try:
        if pro is None or ts_code is None:
            raise RuntimeError("tushare_pro_not_ready")
        end_dt = pd.Timestamp.now()
        start_dt = end_dt - pd.Timedelta(days=120)
        start_str = start_dt.strftime("%Y%m%d")
        end_str = end_dt.strftime("%Y%m%d")
        df_stock = pro.daily(ts_code=ts_code, start_date=start_str, end_date=end_str)
        idx_fn = getattr(pro, "index_daily", None)
        df_index = idx_fn(ts_code="000300.SH", start_date=start_str, end_date=end_str) if callable(idx_fn) else pd.DataFrame()
        if df_stock is not None and not df_stock.empty and df_index is not None and not df_index.empty:
            df_stock = df_stock.copy()
            df_index = df_index.copy()
            df_stock["trade_date"] = df_stock["trade_date"].astype(str)
            df_index["trade_date"] = df_index["trade_date"].astype(str)
            df_stock["close"] = pd.to_numeric(df_stock["close"], errors="coerce")
            df_index["close"] = pd.to_numeric(df_index["close"], errors="coerce")
            m = pd.merge(df_stock[["trade_date", "close"]], df_index[["trade_date", "close"]], on="trade_date", how="inner", suffixes=("_stock", "_hs300"))
            m = m.dropna()
            if not m.empty:
                rs = m["close_stock"] / m["close_hs300"]
                rs_ma20 = rs.rolling(20).mean()
                relative = {
                    "rs": _round(rs.iloc[-1], 6),
                    "rs_ma20": _round(rs_ma20.iloc[-1], 6),
                    "status": "STRONG" if rs_ma20.iloc[-1] == rs_ma20.iloc[-1] and rs.iloc[-1] > rs_ma20.iloc[-1] else "WEAK",
                }
    except Exception as e:
        relative = {"error": str(e)}

    try:
        if pro is None or ts_code is None:
            raise RuntimeError("tushare_pro_not_ready")
        end_dt = pd.Timestamp.now()
        start_dt = end_dt - pd.Timedelta(days=160)
        start_str = start_dt.strftime("%Y%m%d")
        end_str = end_dt.strftime("%Y%m%d")
        mf_fn = getattr(pro, "moneyflow", None)
        flow_df = mf_fn(ts_code=ts_code, start_date=start_str, end_date=end_str) if callable(mf_fn) else pd.DataFrame()
        px_df = pro.daily(ts_code=ts_code, start_date=start_str, end_date=end_str, fields="ts_code,trade_date,close,pct_chg,amount") if getattr(pro, "daily", None) else pd.DataFrame()
        if flow_df is not None and not flow_df.empty:
            flow_df = flow_df.copy()
            if "trade_date" in flow_df.columns:
                flow_df["trade_date"] = flow_df["trade_date"].astype(str)
            if px_df is not None and not px_df.empty and "trade_date" in px_df.columns:
                px_df = px_df.copy()
                px_df["trade_date"] = px_df["trade_date"].astype(str)
                for c in ["close", "pct_chg", "amount"]:
                    if c in px_df.columns:
                        px_df[c] = pd.to_numeric(px_df[c], errors="coerce")
                flow_df = pd.merge(flow_df, px_df, on="trade_date", how="left")

            for c in flow_df.columns:
                if c.endswith("_amount") or c in ["net_mf_amount", "buy_sm_amount", "sell_sm_amount", "close", "pct_chg"]:
                    flow_df[c] = pd.to_numeric(flow_df[c], errors="coerce")

            def _net(col_buy, col_sell):
                if col_buy in flow_df.columns and col_sell in flow_df.columns:
                    return flow_df[col_buy].fillna(0) - flow_df[col_sell].fillna(0)
                return pd.Series([np.nan] * len(flow_df))

            huge_net = _net("buy_elg_amount", "sell_elg_amount")
            big_net = _net("buy_lg_amount", "sell_lg_amount")
            main_net = huge_net.fillna(0) + big_net.fillna(0)

            flow_df = flow_df.sort_values("trade_date", ascending=False).reset_index(drop=True)
            latest = flow_df.iloc[0].to_dict()

            def _to_wan_series(s):
                s0 = pd.to_numeric(s, errors="coerce")
                if "amount" in flow_df.columns and flow_df["amount"].notna().any():
                    daily_amt_yuan = flow_df["amount"].iloc[0] * 1000.0
                    if pd.notna(daily_amt_yuan) and abs(float(s0.iloc[0] or 0)) > daily_amt_yuan / 10.0:
                        return s0 / 10000.0
                return s0

            huge_net_wan = _to_wan_series(huge_net)
            big_net_wan = _to_wan_series(big_net)
            main_net_wan = _to_wan_series(main_net)

            history_last_100 = []
            for i, rr in flow_df.head(100).iterrows():
                history_last_100.append(
                    {
                        "date": rr.get("trade_date"),
                        "close": _round(rr.get("close")),
                        "chg_pct": _round(rr.get("pct_chg"), 2),
                        "main_net_in_10k": _round(main_net_wan.iloc[i], 2) if i < len(main_net_wan) else None,
                        "main_ratio_pct": None,
                        "huge_net_in_10k": _round(huge_net_wan.iloc[i], 2) if i < len(huge_net_wan) else None,
                        "big_net_in_10k": _round(big_net_wan.iloc[i], 2) if i < len(big_net_wan) else None,
                    }
                )
            main_net_5d = _round(float(main_net_wan.head(5).sum()), 2) if len(main_net_wan) else None
            money_flow = {
                "latest_date": latest.get("trade_date"),
                "close": _round(latest.get("close")),
                "chg_pct": _round(latest.get("pct_chg"), 2),
                "main_net_in_10k": _round(main_net_wan.iloc[0], 2) if len(main_net_wan) else None,
                "main_ratio_pct": None,
                "huge_net_in_10k": _round(huge_net_wan.iloc[0], 2) if len(huge_net_wan) else None,
                "big_net_in_10k": _round(big_net_wan.iloc[0], 2) if len(big_net_wan) else None,
                "main_net_5d_10k": main_net_5d,
                "history_last_100": history_last_100,
            }
        else:
            money_flow = {"hint": "无资金流向数据"}
    except Exception as e:
        money_flow = {"error": str(e)}

    try:
        CONFIG = {"institution_ratio_threshold": 3.0, "sell_threshold_yi": 1.0, "back_days": 365, "top_n_days": 3}
        if pro is None or ts_code is None:
            raise RuntimeError("tushare_pro_not_ready")
        now = pd.Timestamp.now()
        end_date = now.strftime("%Y%m%d")
        start_date = (now - pd.Timedelta(days=CONFIG["back_days"])).strftime("%Y%m%d")
        top_fn = getattr(pro, "top_list", None)
        raw_df = pd.DataFrame()
        need_fallback_by_trade_date = False
        if callable(top_fn):
            try:
                raw_df = top_fn(ts_code=ts_code, start_date=start_date, end_date=end_date)
            except Exception as e:
                if "trade_date" in str(e) or "必填参数" in str(e):
                    need_fallback_by_trade_date = True
                    raw_df = pd.DataFrame()
                else:
                    raise

        if need_fallback_by_trade_date and callable(top_fn):
            cal_df = pro.trade_cal(start_date=start_date, end_date=end_date, is_open="1")
            if cal_df is not None and not cal_df.empty and "cal_date" in cal_df.columns:
                dates = cal_df["cal_date"].astype(str).sort_values().tolist()
            else:
                dates = []
            dates = dates[-120:]
            frames = []
            hit_dates = []
            for d in reversed(dates):
                tmp = pd.DataFrame()
                try:
                    tmp = top_fn(trade_date=d, ts_code=ts_code)
                except TypeError:
                    try:
                        tmp = top_fn(trade_date=d)
                    except Exception:
                        tmp = pd.DataFrame()
                except Exception:
                    try:
                        tmp = top_fn(trade_date=d)
                    except Exception:
                        tmp = pd.DataFrame()

                if tmp is None or tmp.empty:
                    continue
                tmp = tmp.copy()
                if "ts_code" in tmp.columns:
                    tmp = tmp[tmp["ts_code"].astype(str) == str(ts_code)]
                if tmp.empty:
                    continue
                frames.append(tmp)
                hit_dates.append(d)
                if len(hit_dates) >= int(CONFIG["top_n_days"]):
                    break
            raw_df = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
        if raw_df is not None and not raw_df.empty:
            raw_df = raw_df.copy()
            if "trade_date" in raw_df.columns:
                raw_df["trade_date"] = raw_df["trade_date"].astype(str)
                raw_df = raw_df.sort_values("trade_date", ascending=False)
            days = []
            if "trade_date" in raw_df.columns:
                for d0 in raw_df["trade_date"].tolist():
                    if d0 not in days:
                        days.append(d0)
                    if len(days) >= CONFIG["top_n_days"]:
                        break
            df_use = raw_df[raw_df["trade_date"].isin(days)].copy() if days else raw_df.head(10).copy()
            if "net_buy" in df_use.columns:
                df_use["net_buy_yi"] = pd.to_numeric(df_use["net_buy"], errors="coerce") / 10000.0
            else:
                df_use["net_buy_yi"] = np.nan
            ratio_col = "net_rate" if "net_rate" in df_use.columns else None

            def judge_main_force(net_buy_yi, buy_ratio):
                try:
                    net_buy = 0.0 if _is_na(net_buy_yi) else float(net_buy_yi)
                    ratio = 0.0 if _is_na(buy_ratio) else float(buy_ratio)
                    if net_buy > 0 and ratio > CONFIG["institution_ratio_threshold"]:
                        return "机构主导", "持有/低吸"
                    if net_buy > 0 and ratio <= CONFIG["institution_ratio_threshold"]:
                        return "游资主导", "警惕回调/止盈"
                    if net_buy < 0 and abs(net_buy) > CONFIG["sell_threshold_yi"]:
                        return "主力出货", "减仓/观望"
                    return "主力洗盘", "观望"
                except Exception:
                    return "数据异常", "暂无建议"

            items = []
            for _, rr in df_use.iterrows():
                ratio_v = rr.get(ratio_col) if ratio_col else None
                movement, suggestion = judge_main_force(rr.get("net_buy_yi"), ratio_v)
                items.append(
                    {
                        "date": rr.get("trade_date"),
                        "close": _round(rr.get("close"), 3),
                        "chg_pct": _round(rr.get("pct_chg"), 2),
                        "net_buy_yi": _round(rr.get("net_buy_yi"), 3),
                        "net_ratio_pct": _round(ratio_v, 2),
                        "turnover_rate": _round(rr.get("turnover_rate"), 2),
                        "reason": rr.get("reason") or rr.get("explain") or rr.get("name"),
                        "movement": movement,
                        "suggestion": suggestion,
                    }
                )
            lhb = {"items": items} if items else {"hint": "近期未上榜"}
        else:
            lhb = {"hint": "无龙虎榜数据"}
    except Exception as e:
        err = str(e)
        if ("没有该接口访问权限" in err) or ("权限" in err) or ("接口名" in err):
            lhb = {"hint": "龙虎榜接口权限不足", "reason": err}
        else:
            lhb = {"error": err}

    try:
        if pro is None or ts_code is None:
            raise RuntimeError("tushare_pro_not_ready")
        fh_df = pd.DataFrame()
        fh_fn = getattr(pro, "fund_holdings", None)
        if callable(fh_fn):
            try:
                fh_df = fh_fn(ts_code=ts_code)
            except Exception:
                fh_df = pd.DataFrame()

        if fh_df is None or fh_df.empty:
            def _latest_quarter_end(dt: pd.Timestamp):
                y = int(dt.year)
                m = int(dt.month)
                if m <= 3:
                    return f"{y-1}1231"
                if m <= 6:
                    return f"{y}0331"
                if m <= 9:
                    return f"{y}0630"
                return f"{y}0930"

            period = _latest_quarter_end(now_dt)
            alt_fn = getattr(pro, "top10_floatholders", None)
            if callable(alt_fn):
                try:
                    fh_df = alt_fn(ts_code=ts_code, period=period)
                except TypeError:
                    try:
                        fh_df = alt_fn(ts_code=ts_code)
                    except Exception:
                        fh_df = pd.DataFrame()
                except Exception:
                    try:
                        fh_df = alt_fn(ts_code=ts_code)
                    except Exception:
                        fh_df = pd.DataFrame()

        if fh_df is not None and not fh_df.empty:
            fh_df = fh_df.copy()
            ratio_col = None
            for c in ["hold_ratio", "hold_ratio_pct", "holdratio", "hold_ratio(%)", "hold_ratio_percent"]:
                if c in fh_df.columns:
                    ratio_col = c
                    break
            if ratio_col:
                fh_df[ratio_col] = pd.to_numeric(fh_df[ratio_col], errors="coerce").fillna(0)
                fh_df = fh_df.sort_values(ratio_col, ascending=False).reset_index(drop=True)
            total_ratio = float(fh_df[ratio_col].sum()) if ratio_col else None
            fund_count = int(len(fh_df))
            if total_ratio is None:
                concentration = None
            elif total_ratio > 20:
                concentration = "高度集中"
            elif total_ratio > 10:
                concentration = "中度集中"
            elif total_ratio > 5:
                concentration = "有持仓"
            else:
                concentration = "持仓较少"
            show = fh_df.head(30).copy()
            fund_holding = {
                "summary": {
                    "total_ratio_pct": _round(total_ratio, 2) if total_ratio is not None else None,
                    "fund_count": fund_count,
                    "concentration": concentration,
                    "source": "top10_floatholders",
                },
                "top30": show.to_dict("records"),
            }
        else:
            fund_holding = {"hint": "无基金/机构持仓数据"}
    except Exception as e:
        err = str(e)
        if ("没有该接口访问权限" in err) or ("权限" in err) or ("接口名" in err):
            fund_holding = {"hint": "基金/机构持仓接口权限不足", "reason": err}
        else:
            fund_holding = {"error": err}

    try:
        if pro is None or ts_code is None:
            raise RuntimeError("tushare_pro_not_ready")
        md_fn = getattr(pro, "margin_detail", None)
        found_df = None
        found_date = None
        if callable(md_fn):
            for i in range(15):
                d = (pd.Timestamp.now() - pd.Timedelta(days=i)).strftime("%Y%m%d")
                tmp = md_fn(ts_code=ts_code, trade_date=d)
                if tmp is not None and not tmp.empty:
                    found_df = tmp
                    found_date = d
                    break
        if found_df is not None and not found_df.empty:
            rr = found_df.iloc[0].to_dict()
            margin = {"date": found_date}
            for k in ["rzye", "rqyl", "rqye", "rzmre", "rzche", "rqmcl", "rqchl"]:
                if k in rr:
                    margin[k] = _to_float(rr.get(k))
        else:
            margin = {"hint": "无两融数据"}
    except Exception as e:
        margin = {"error": str(e)}

    try:
        if ts is None:
            raise RuntimeError("tushare_module_not_ready")
        date_for_tick = None
        if last_trade_date and len(last_trade_date) == 8:
            date_for_tick = f"{last_trade_date[:4]}-{last_trade_date[4:6]}-{last_trade_date[6:]}"
        ticks_df = ts.get_tick_data(symbol, date=date_for_tick) if date_for_tick else pd.DataFrame()
        if ticks_df is not None and not ticks_df.empty:
            ticks_df = ticks_df.copy()
            for c in ["price", "volume"]:
                if c in ticks_df.columns:
                    ticks_df[c] = pd.to_numeric(ticks_df[c], errors="coerce")
            if "price" in ticks_df.columns and "volume" in ticks_df.columns:
                ticks_df["amount_wan"] = (ticks_df["price"] * ticks_df["volume"]) / 10000.0
            else:
                ticks_df["amount_wan"] = np.nan
            top_df = ticks_df.sort_values("amount_wan", ascending=False).head(30).copy()
            items = []
            buy_wan = 0.0
            sell_wan = 0.0
            for _, rr in top_df.iterrows():
                nature = rr.get("type")
                amt_wan = _to_float(rr.get("amount_wan")) or 0.0
                if nature == "买盘":
                    buy_wan += amt_wan
                elif nature == "卖盘":
                    sell_wan += amt_wan
                items.append(
                    {
                        "time": rr.get("time"),
                        "price": _round(rr.get("price"), 3),
                        "hands": _to_int(rr.get("volume")),
                        "nature": nature,
                        "amount_wan": _round(rr.get("amount_wan"), 2),
                    }
                )
            big_deal = {
                "summary": {"buy_wan": _round(buy_wan, 2), "sell_wan": _round(sell_wan, 2), "net_wan": _round(buy_wan - sell_wan, 2)},
                "top30": items,
            }
        else:
            big_deal = {"hint": "无大单分笔数据"}
    except Exception as e:
        big_deal = {"error": str(e)}

    try:
        cyq_fn = getattr(pro, "cyq_perf", None) if pro is not None else None
        if callable(cyq_fn) and ts_code is not None:
            cyq_df = cyq_fn(ts_code=ts_code)
        else:
            cyq_df = pd.DataFrame()
        if cyq_df is not None and not cyq_df.empty:
            cyq_df = cyq_df.copy()
            dt_col = "trade_date" if "trade_date" in cyq_df.columns else None
            if dt_col:
                cyq_df[dt_col] = cyq_df[dt_col].astype(str)
                cyq_df = cyq_df.sort_values(dt_col)
            last = cyq_df.iloc[-1].to_dict()
            chip = {"latest": last}
        else:
            chip = {"hint": "无筹码数据"}
    except Exception as e:
        chip = {"error": str(e)}

    try:
        news_df = pd.DataFrame()
        news_fn = getattr(pro, "news", None) if pro is not None else None
        if callable(news_fn) and ts_code is not None:
            end_str = now_dt.strftime("%Y%m%d")
            start_str = (now_dt - pd.Timedelta(days=30)).strftime("%Y%m%d")
            try:
                news_df = news_fn(ts_code=ts_code, start_date=start_str, end_date=end_str)
            except Exception as e:
                err = str(e)
                if "没有该接口访问权限" in err or "权限" in err:
                    news_df = pd.DataFrame()
                else:
                    raise

        if (news_df is None or news_df.empty) and pro is not None:
            cctv_fn = getattr(pro, "cctv_news", None)
            if callable(cctv_fn):
                try:
                    news_df = cctv_fn(date=now_dt.strftime("%Y%m%d"))
                except Exception:
                    news_df = pd.DataFrame()

        if (news_df is None or news_df.empty) and ts is not None:
            latest_news_fn = getattr(ts, "get_latest_news", None)
            if callable(latest_news_fn):
                try:
                    news_df = latest_news_fn(top=50, show_content=False)
                except TypeError:
                    try:
                        news_df = latest_news_fn(top=50)
                    except Exception:
                        news_df = pd.DataFrame()
                except Exception:
                    news_df = pd.DataFrame()

        if news_df is not None and not news_df.empty and meta.get("name"):
            original_news_df = news_df
            key = str(meta.get("name"))
            if "title" in news_df.columns:
                mask = news_df["title"].astype(str).str.contains(key, na=False)
            elif "content" in news_df.columns:
                mask = news_df["content"].astype(str).str.contains(key, na=False)
            else:
                mask = None
            if mask is not None and mask.any():
                news_df = news_df[mask].copy()
            elif original_news_df is not None and not original_news_df.empty:
                news_df = original_news_df
        if news_df is not None and not news_df.empty:
            news_df = news_df.copy()
            dt_col = "datetime" if "datetime" in news_df.columns else ("pub_time" if "pub_time" in news_df.columns else None)
            if dt_col is None and "time" in news_df.columns:
                dt_col = "time"
            if dt_col:
                news_df[dt_col] = pd.to_datetime(news_df[dt_col], errors="coerce")
                news_df = news_df.sort_values(dt_col, ascending=False)
            news_df = news_df.head(5)
            items = []
            for _, rr in news_df.iterrows():
                items.append(
                    _clip_record(
                        {
                            "发布时间": rr.get(dt_col),
                            "新闻标题": rr.get("title") or rr.get("content") or rr.get("name"),
                            "文章来源": rr.get("src") or rr.get("source"),
                            "url": rr.get("url"),
                        },
                        max_len=800,
                    )
                )
            meta["news_top5"] = items
        else:
            meta["news_hint"] = "无新闻数据"
    except Exception as e:
        err = str(e)
        if ("没有该接口访问权限" in err) or ("权限" in err) or ("接口名" in err):
            meta["news_hint"] = "新闻接口权限不足"
            meta["news_reason"] = err
        else:
            meta["news_error"] = err

    snapshot = _attach_describe(snapshot, "实时快照：价格/涨跌幅/估值/成交等概览")
    order_book = _attach_describe(order_book, "盘口信息：五档买卖/涨跌停等")
    daily = _attach_describe(daily, "日线数据：最近K线、涨跌幅、换手率等")
    volume_indicators = _attach_describe(volume_indicators, "量能指标：成交量均线/量能强弱/OBV/MFI/AD 等")
    share_structure = _attach_describe(share_structure, "股本结构：行业/上市日期/总股本/流通股等")
    fundamental = _attach_describe(fundamental, "基本面：财务分析指标（如可用）与概览")
    trading_rules = _attach_describe(trading_rules, "交易规则：涨跌停价/涨跌停幅度推算/T+1")
    new_stock = _attach_describe(new_stock, "次新股属性：上市时长/是否次新/流通市值等")
    company = _attach_describe(company, "公司画像：题材/概念等（如可用）")
    disclosure = _attach_describe(disclosure, "公告/互动：公告摘要（如可用）")
    intraday = _attach_describe(intraday, "分时数据：分钟级、VWAP、日内高低等")
    technicals = _attach_describe(technicals, "技术指标：MA/MACD/RSI/BOLL/ATR/唐奇安等")
    relative = _attach_describe(relative, "相对强弱：相对沪深300的RS与状态")
    money_flow = _attach_describe(money_flow, "资金流向：主力/超大单/大单净流入与历史")
    chip = _attach_describe(chip, "筹码分布：筹码指标（如可用）")
    signals = _attach_describe(signals, "信号汇总：趋势、突破风险、震荡反转等")
    lhb = _attach_describe(lhb, "龙虎榜：近3次上榜主力动向与建议")
    margin = _attach_describe(margin, "两融：融资融券关键数据与提示")
    fund_holding = _attach_describe(fund_holding, "基金持仓：机构持仓Top与提示")
    big_deal = _attach_describe(big_deal, "大单追踪：分笔成交Top与买卖汇总")

    result = {
        "meta": meta,
        "snapshot": snapshot,
        "order_book": order_book,
        "daily": daily,
        "volume": volume_indicators,
        "share_structure": share_structure,
        "fundamental": fundamental,
        "trading_rules": trading_rules,
        "new_stock": new_stock,
        "company": company,
        "disclosure": disclosure,
        "intraday": intraday,
        "technicals": technicals,
        "relative": relative,
        "money_flow": money_flow,
        "chip": chip,
        "signals": signals,
        "lhb": lhb,
        "margin": margin,
        "fund_holding": fund_holding,
        "big_deal": big_deal,
    }

result = _clean_obj(result)

coverage = []
def _add_cov(name, ok, detail=None):
    item = {"name": name, "ok": bool(ok)}
    if detail is not None:
        item["detail"] = detail
    coverage.append(item)

_add_cov("最近 20 日：日线（K 线）", _non_empty(_pick(result, "daily.last_20")), {"rows": len(_pick(result, "daily.last_20") or [])})
_add_cov("量能指标 (VOL/OBV/MFI/ADL)", _pick(result, "volume.latest.volume") is not None or _pick(result, "volume.latest.obv") is not None or _pick(result, "volume.latest.mfi_14") is not None)
_add_cov("次新股属性 (上市时长/流通盘/筹码)", _pick(result, "new_stock.days_since_listing") is not None or _pick(result, "new_stock.float_mv_yi") is not None)
_add_cov("交易规则约束 (涨跌停/T+1)", _pick(result, "trading_rules.limit_up") is not None or _pick(result, "trading_rules.limit_down") is not None)
_add_cov("基本面概览 (行业/股本/财务指标)", _non_empty(_pick(result, "share_structure.raw")) or _non_empty(_pick(result, "fundamental.financial_analysis_last_4")))
_add_cov("公司画像与题材 (主营/概念/互动)", _non_empty(_pick(result, "company.profile")) or _non_empty(_pick(result, "company.concepts")) or _non_empty(_pick(result, "disclosure.announcements_top10")))
_add_cov("资金流向（个股近 100 个交易日内）", _non_empty(_pick(result, "money_flow.history_last_100")), {"rows": len(_pick(result, "money_flow.history_last_100") or [])})
_add_cov("5 分钟数据（分时历史）", _non_empty(_pick(result, "intraday.bars_5m_last_48")), {"rows": len(_pick(result, "intraday.bars_5m_last_48") or [])})
_add_cov("近 10 天换手率", _non_empty(_pick(result, "daily.turnover_last_10")), {"rows": len(_pick(result, "daily.turnover_last_10") or [])})
_add_cov("实时估值 (PE/PB/量比)", _pick(result, "snapshot.pe_dynamic") is not None or _pick(result, "snapshot.pb") is not None or _pick(result, "snapshot.volume_ratio") is not None)
_add_cov("ATR （平均真实波幅）", _pick(result, "technicals.latest.atr_14") is not None)
_add_cov("盘口五档/涨停跌停/外内盘", _non_empty(_pick(result, "order_book.buy")) and _non_empty(_pick(result, "order_book.sell")))
_add_cov("实时快照（单股过滤版）", _non_empty(_pick(result, "snapshot")))
_add_cov("今日分时 VWAP/日内强弱", _pick(result, "intraday.vwap") is not None and _pick(result, "intraday.vwap_bias_pct") is not None)
_add_cov("新闻获取", _non_empty(_pick(result, "meta.news_top5")), {"rows": len(_pick(result, "meta.news_top5") or [])})
_add_cov("筹码集中度", _non_empty(_pick(result, "chip.latest")))
_add_cov("最近 3 次上榜龙虎榜主力动向分析（年度回溯）", _non_empty(_pick(result, "lhb.items")) or _pick(result, "lhb.hint") is not None)
_add_cov("融资融券", _pick(result, "margin.date") is not None or _pick(result, "margin.hint") is not None)
_add_cov("个股资金流向分析（主力/超大单/大单）", _pick(result, "money_flow.main_net_in_10k") is not None or _pick(result, "money_flow.huge_net_in_10k") is not None or _pick(result, "money_flow.big_net_in_10k") is not None)
_add_cov("基金持仓分析", _non_empty(_pick(result, "fund_holding.top30")) or _pick(result, "fund_holding.hint") is not None, {"error": _pick(result, "fund_holding.error")})
_add_cov("大单追踪", _non_empty(_pick(result, "big_deal.top30")) or _pick(result, "big_deal.hint") is not None, {"error": _pick(result, "big_deal.error")})
_add_cov("趋势系统 (MA/MACD/RSI)", _non_empty(_pick(result, "signals.trend_system")))
_add_cov("突破与风险 (ATR/通道)", _non_empty(_pick(result, "signals.breakout_risk")))
_add_cov("相对强弱 (RS vs HS300)", _pick(result, "relative.rs") is not None or _pick(result, "relative.status") is not None)
_add_cov("中短线-突破与波动（布林+唐奇安+ATR）", _pick(result, "technicals.latest.boll_upper") is not None and _pick(result, "technicals.latest.donchian_high_20") is not None and _pick(result, "technicals.latest.atr_14") is not None)
_add_cov("中短线-相对强弱（对沪深300）", _pick(result, "relative.rs") is not None)
_add_cov("中短线-震荡与反转 (KDJ/WR/BIAS)", _non_empty(_pick(result, "signals.short_term")))

result_meta = result.get("meta") if isinstance(result, dict) else None
if isinstance(result_meta, dict):
    result_meta["coverage_report"] = coverage
    result_meta["coverage_ok_count"] = sum(1 for x in coverage if x.get("ok"))
    result_meta["coverage_total"] = len(coverage)
