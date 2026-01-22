
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

meta = {
    "symbol": symbol,
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
    try:
        spot_df = ak.stock_zh_a_spot_em()
        row_df = spot_df[spot_df["代码"] == symbol]
        if not row_df.empty:
            r = row_df.iloc[0]
            snapshot = {
                "price": _round(r.get("最新价")),
                "change_pct": _round(r.get("涨跌幅")),
                "turnover_rate": _round(r.get("换手率")),
                "volume_ratio": _round(r.get("量比")),
                "pe_dynamic": _round(r.get("市盈率-动态")),
                "pb": _round(r.get("市净率")),
                "total_mv": _to_float(r.get("总市值")),
                "circ_mv": _to_float(r.get("流通市值")),
                "open": _round(r.get("今开")),
                "prev_close": _round(r.get("昨收")),
                "high": _round(r.get("最高")),
                "low": _round(r.get("最低")),
                "volume": _to_float(r.get("成交量")),
                "amount": _to_float(r.get("成交额")),
                "amp": _round(r.get("振幅")),
                "speed": _round(r.get("涨速")),
                "chg_5min": _round(r.get("5分钟涨跌")),
                "chg_60d": _round(r.get("60日涨跌幅")),
                "chg_ytd": _round(r.get("年初至今涨跌幅")),
            }
    except Exception as e:
        snapshot = {"error": str(e)}

    try:
        info_df = ak.stock_individual_info_em(symbol=symbol)
        if info_df is not None and not info_df.empty and "item" in info_df.columns and "value" in info_df.columns:
            info = info_df.set_index("item")["value"].to_dict()
            share_structure = {
                "industry": str(info.get("行业")).strip() if info.get("行业") is not None else None,
                "listing_date": info.get("上市时间") if info.get("上市时间") is not None else info.get("上市日期"),
                "total_shares": _to_float(info.get("总股本")),
                "float_shares": _to_float(info.get("流通股")),
                "free_float_mv": _to_float(snapshot.get("circ_mv")) if isinstance(snapshot, dict) else None,
                "raw": info,
            }

            listing_dt = pd.to_datetime(share_structure.get("listing_date"), errors="coerce")
            days_since_listing = None if pd.isna(listing_dt) else int((pd.Timestamp.now().normalize() - listing_dt.normalize()).days)
            new_stock = {
                "listing_date": share_structure.get("listing_date"),
                "days_since_listing": days_since_listing,
                "is_recent_ipo": bool(days_since_listing is not None and days_since_listing <= 365),
                "float_mv_yi": _round((_to_float(snapshot.get("circ_mv")) / 1e8) if isinstance(snapshot, dict) else None, 3),
            }

            profile = {
                "name": stock_name or _to_str(info.get("股票简称")),
                "industry": str(info.get("行业")).strip() if info.get("行业") is not None else None,
                "region": _to_str(info.get("地区")) or _to_str(info.get("所属地区")),
                "listing_date": share_structure.get("listing_date"),
                "main_business": _clip_str(info.get("主营业务") or info.get("主营业务描述") or info.get("主营")),
                "business_scope": _clip_str(info.get("经营范围") or info.get("经营范围描述")),
                "website": _to_str(info.get("公司网址") or info.get("网址")),
                "chairman": _to_str(info.get("董事长")),
                "gm": _to_str(info.get("总经理")),
            }
            concepts_raw = (
                _to_str(info.get("所属概念"))
                or _to_str(info.get("概念"))
                or _to_str(info.get("概念题材"))
                or _to_str(info.get("题材"))
            )
            company = {
                "profile": profile,
                "concepts": _split_terms(concepts_raw),
            }
    except Exception as e:
        share_structure = {"error": str(e)}

    try:
        fa_fn = getattr(ak, "stock_financial_analysis_indicator", None)
        if callable(fa_fn):
            fa_df = fa_fn(symbol=symbol)
        else:
            fa_df = pd.DataFrame()
        if fa_df is not None and not fa_df.empty:
            fa_df = fa_df.copy().tail(4)
            fundamental = {"financial_analysis_last_4": fa_df.to_dict("records")}
        else:
            fundamental = {"hint": "无财务分析指标数据"}
    except Exception as e:
        fundamental = {"error": str(e)}

    try:
        bidask_df = ak.stock_bid_ask_em(symbol=symbol)
        if bidask_df is not None and not bidask_df.empty and "item" in bidask_df.columns and "value" in bidask_df.columns:
            d = bidask_df.set_index("item")["value"].to_dict()
            order_book = {
                "latest": _round(d.get("最新")),
                "avg_price": _round(d.get("均价")),
                "high": _round(d.get("最高")),
                "low": _round(d.get("最低")),
                "open": _round(d.get("今开")),
                "prev_close": _round(d.get("昨收")),
                "limit_up": _round(d.get("涨停")),
                "limit_down": _round(d.get("跌停")),
                "outer_vol": _to_float(d.get("外盘")),
                "inner_vol": _to_float(d.get("内盘")),
                "buy": [
                    {"price": _round(d.get("buy_1")), "vol": _to_float(d.get("buy_1_vol"))},
                    {"price": _round(d.get("buy_2")), "vol": _to_float(d.get("buy_2_vol"))},
                    {"price": _round(d.get("buy_3")), "vol": _to_float(d.get("buy_3_vol"))},
                    {"price": _round(d.get("buy_4")), "vol": _to_float(d.get("buy_4_vol"))},
                    {"price": _round(d.get("buy_5")), "vol": _to_float(d.get("buy_5_vol"))},
                ],
                "sell": [
                    {"price": _round(d.get("sell_1")), "vol": _to_float(d.get("sell_1_vol"))},
                    {"price": _round(d.get("sell_2")), "vol": _to_float(d.get("sell_2_vol"))},
                    {"price": _round(d.get("sell_3")), "vol": _to_float(d.get("sell_3_vol"))},
                    {"price": _round(d.get("sell_4")), "vol": _to_float(d.get("sell_4_vol"))},
                    {"price": _round(d.get("sell_5")), "vol": _to_float(d.get("sell_5_vol"))},
                ],
            }
    except Exception as e:
        order_book = {"error": str(e)}

    try:
        prev_close = None
        if isinstance(snapshot, dict):
            prev_close = _to_float(snapshot.get("prev_close"))
        if prev_close is None and isinstance(order_book, dict):
            prev_close = _to_float(order_book.get("prev_close"))

        limit_up = _to_float(order_book.get("limit_up")) if isinstance(order_book, dict) else None
        limit_down = _to_float(order_book.get("limit_down")) if isinstance(order_book, dict) else None

        limit_up_pct = None
        limit_down_pct = None
        if prev_close and limit_up:
            limit_up_pct = _round((limit_up / prev_close - 1.0) * 100.0, 2)
        if prev_close and limit_down:
            limit_down_pct = _round((limit_down / prev_close - 1.0) * 100.0, 2)

        trading_rules = {
            "t_plus_1": True,
            "limit_up": _round(limit_up),
            "limit_down": _round(limit_down),
            "limit_up_pct": limit_up_pct,
            "limit_down_pct": limit_down_pct,
        }
    except Exception as e:
        trading_rules = {"error": str(e)}

    try:
        disclosure = {}
        irm_fn = getattr(ak, "stock_irm_cninfo", None)
        if callable(irm_fn):
            irm_df = irm_fn(symbol=symbol)
        else:
            irm_df = pd.DataFrame()
        if irm_df is not None and not irm_df.empty:
            irm_df = irm_df.copy()
            for c in ["提问时间", "更新时间"]:
                if c in irm_df.columns:
                    irm_df[c] = pd.to_datetime(irm_df[c], errors="coerce")
            sort_col = "更新时间" if ("更新时间" in irm_df.columns and irm_df["更新时间"].notna().any()) else ("提问时间" if "提问时间" in irm_df.columns else None)
            if sort_col:
                irm_df = irm_df.sort_values(sort_col, ascending=False)
            ans_col = "回答内容" if "回答内容" in irm_df.columns else None
            if ans_col:
                answered = irm_df[irm_df[ans_col].notna() & (irm_df[ans_col].astype(str).str.len() > 0)].copy()
            else:
                answered = pd.DataFrame()
            if answered is not None and not answered.empty:
                answered = answered.head(10)
                records = []
                for _, rr in answered.iterrows():
                    rec = {
                        "question": _clip_str(rr.get("问题"), 500),
                        "answer": _clip_str(rr.get("回答内容"), 800),
                        "asker": _to_str(rr.get("提问者")),
                        "source": _to_str(rr.get("来源")),
                        "ask_time": rr.get("提问时间"),
                        "update_time": rr.get("更新时间"),
                    }
                    rec = _clip_record(rec, max_len=800)
                    records.append(rec)
                disclosure["irm_qna_top10"] = records
            unanswered_count = None
            if ans_col:
                unanswered_count = int((irm_df[ans_col].isna() | (irm_df[ans_col].astype(str).str.len() == 0)).sum())
            disclosure["irm_total_rows"] = int(len(irm_df))
            if unanswered_count is not None:
                disclosure["irm_unanswered_count"] = unanswered_count
        else:
            disclosure["irm_hint"] = "无互动易问答数据"
    except Exception as e:
        disclosure = {"irm_error": str(e)}

    try:
        rep_fn = getattr(ak, "stock_zh_a_disclosure_report_cninfo", None)
        if callable(rep_fn):
            end_str = now_dt.strftime("%Y%m%d")
            start_str = (now_dt - pd.Timedelta(days=120)).strftime("%Y%m%d")
            rep_df = rep_fn(symbol=symbol, market="沪深京", keyword="", category="", start_date=start_str, end_date=end_str)
        else:
            rep_df = pd.DataFrame()
        if rep_df is not None and not rep_df.empty:
            rep_df = rep_df.copy()
            if "公告时间" in rep_df.columns:
                rep_df["公告时间"] = pd.to_datetime(rep_df["公告时间"], errors="coerce")
                rep_df = rep_df.sort_values("公告时间", ascending=False)
            rep_df = rep_df.head(10)
            recs = []
            for _, rr in rep_df.iterrows():
                rec = {
                    "title": _clip_str(rr.get("公告标题"), 500),
                    "time": rr.get("公告时间"),
                    "url": _to_str(rr.get("公告链接")),
                }
                rec = _clip_record(rec, max_len=600)
                recs.append(rec)
            if not isinstance(disclosure, dict) or "irm_error" in disclosure:
                disclosure = {}
            disclosure["announcements_top10"] = recs
        else:
            if not isinstance(disclosure, dict) or "irm_error" in disclosure:
                disclosure = {}
            disclosure["announcements_hint"] = "无公告数据"
    except Exception as e:
        if not isinstance(disclosure, dict) or "irm_error" in disclosure:
            disclosure = {}
        disclosure["announcements_error"] = str(e)

    if not isinstance(disclosure, dict) or len(disclosure) == 0:
        disclosure = {"hint": "无公告/互动数据"}

    try:
        end_dt = now_dt
        start_dt = end_dt - pd.Timedelta(days=420)
        start_str = start_dt.strftime("%Y%m%d")
        end_str = end_dt.strftime("%Y%m%d")
        hist_df = ak.stock_zh_a_hist(symbol=symbol, period="daily", start_date=start_str, end_date=end_str, adjust="qfq")
        if hist_df is not None and not hist_df.empty:
            hist_df = hist_df.sort_values(by="日期").reset_index(drop=True)
            hist_df["日期"] = pd.to_datetime(hist_df["日期"], errors="coerce")
            for c in ["开盘", "收盘", "最高", "最低", "成交量", "成交额", "换手率"]:
                if c in hist_df.columns:
                    hist_df[c] = pd.to_numeric(hist_df[c], errors="coerce")

            close = hist_df["收盘"].values
            high = hist_df["最高"].values
            low = hist_df["最低"].values
            vol = hist_df["成交量"].values if "成交量" in hist_df.columns else np.array([])
            amt = hist_df["成交额"].values if "成交额" in hist_df.columns else np.array([])

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
            if len(vol) and len(amt):
                vol_20 = pd.Series(vol).rolling(20).sum().values
                amt_20 = pd.Series(amt).rolling(20).sum().values
                denom = np.where(vol_20 > 0, vol_20 * 100.0, np.nan)
                vwap_20_arr = amt_20 / denom
                vwap_20 = _round(vwap_20_arr[-1])

            last_n = 20
            tail = hist_df.tail(last_n).copy()
            if not tail.empty:
                tail["pct_change"] = tail["收盘"].pct_change() * 100.0
                daily_items = []
                for _, rr in tail.iterrows():
                    amt_v = _to_float(rr.get("成交额"))
                    daily_items.append(
                        {
                            "date": rr["日期"].strftime("%Y-%m-%d") if not pd.isna(rr["日期"]) else None,
                            "open": _round(rr.get("开盘")),
                            "high": _round(rr.get("最高")),
                            "low": _round(rr.get("最低")),
                            "close": _round(rr.get("收盘")),
                            "volume": _to_float(rr.get("成交量")),
                            "amount": amt_v,
                            "amount_yi": _round((amt_v / 1e8) if amt_v is not None else None, 3),
                            "turnover_rate": _round(rr.get("换手率"), 2),
                            "pct_change": _round(rr.get("pct_change"), 2),
                        }
                    )
                turnover_last_10 = []
                if "换手率" in hist_df.columns:
                    t10 = hist_df.tail(10).copy()
                    for _, rr in t10.iterrows():
                        turnover_last_10.append(
                            {
                                "date": rr["日期"].strftime("%Y-%m-%d") if not pd.isna(rr["日期"]) else None,
                                "close": _round(rr.get("收盘")),
                                "turnover_rate": _round(rr.get("换手率"), 2),
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
        df_min = pd.DataFrame()
        used_date = None
        for i in range(10):
            d = (pd.Timestamp.now() - pd.Timedelta(days=i)).strftime("%Y-%m-%d")
            start_dt = f"{d} 09:30:00"
            end_dt = f"{d} 15:00:00"
            try:
                tmp = ak.stock_zh_a_hist_min_em(symbol=symbol, start_date=start_dt, end_date=end_dt, period="1", adjust="")
            except Exception:
                tmp = pd.DataFrame()
            if tmp is not None and not tmp.empty:
                df_min = tmp
                used_date = d
                break

        if df_min is not None and not df_min.empty:
            df_min = df_min.sort_values("时间").reset_index(drop=True)
            last_row = df_min.iloc[-1]
            total_amount = float(pd.to_numeric(df_min.get("成交额"), errors="coerce").fillna(0).sum()) if "成交额" in df_min.columns else 0.0
            total_vol_hands = float(pd.to_numeric(df_min.get("成交量"), errors="coerce").fillna(0).sum()) if "成交量" in df_min.columns else 0.0
            total_shares = max(total_vol_hands * 100.0, 1.0)
            vwap = float(total_amount / total_shares)
            current_price = _to_float(last_row.get("收盘"))
            day_high = _to_float(pd.to_numeric(df_min.get("最高"), errors="coerce").max()) if "最高" in df_min.columns else current_price
            day_low = _to_float(pd.to_numeric(df_min.get("最低"), errors="coerce").min()) if "最低" in df_min.columns else current_price
            vwap_bias_pct = None
            if current_price is not None and vwap:
                vwap_bias_pct = round((current_price / vwap - 1.0) * 100.0, 2)
            intraday = {
                "date": used_date,
                "last_time": pd.to_datetime(last_row.get("时间")).strftime("%Y-%m-%d %H:%M:%S") if last_row.get("时间") is not None else None,
                "price": _round(current_price),
                "vwap": _round(vwap),
                "vwap_bias_pct": vwap_bias_pct,
                "day_high": _round(day_high),
                "day_low": _round(day_low),
                "total_volume_hands": int(total_vol_hands) if total_vol_hands is not None else None,
                "total_amount": total_amount,
                "total_amount_yi": _round(total_amount / 1e8, 3),
            }
    except Exception as e:
        intraday = {"error": str(e)}

    try:
        start_date = (pd.Timestamp.now() - pd.Timedelta(days=5)).strftime("%Y-%m-%d 09:30:00")
        end_date = pd.Timestamp.now().strftime("%Y-%m-%d 15:00:00")
        df_5m = ak.stock_zh_a_hist_min_em(symbol=symbol, start_date=start_date, end_date=end_date, period="5", adjust="")
        if df_5m is not None and not df_5m.empty:
            df_5m = df_5m.copy()
            df_5m["时间"] = pd.to_datetime(df_5m["时间"]).dt.strftime("%Y-%m-%d %H:%M:%S")
            df_5m = df_5m.sort_values(by="时间").reset_index(drop=True)
            df_5m["收盘"] = pd.to_numeric(df_5m["收盘"], errors="coerce")
            df_5m["MA5"] = df_5m["收盘"].rolling(window=5).mean()
            df_5m["MA20"] = df_5m["收盘"].rolling(window=20).mean()
            df_5m = df_5m.tail(48)
            rows = []
            for _, rr in df_5m.iterrows():
                rows.append(
                    {
                        "time": rr.get("时间"),
                        "open": _round(rr.get("开盘")),
                        "high": _round(rr.get("最高")),
                        "low": _round(rr.get("最低")),
                        "close": _round(rr.get("收盘")),
                        "volume": _to_float(rr.get("成交量")),
                        "ma5": _round(rr.get("MA5")),
                        "ma20": _round(rr.get("MA20")),
                    }
                )
            intraday["bars_5m_last_48"] = rows
    except Exception as e:
        if isinstance(intraday, dict):
            intraday["bars_5m_error"] = str(e)

    try:
        end_dt = pd.Timestamp.now()
        start_dt = end_dt - pd.Timedelta(days=120)
        start_str = start_dt.strftime("%Y%m%d")
        end_str = end_dt.strftime("%Y%m%d")
        df_stock = ak.stock_zh_a_hist(symbol=symbol, period="daily", start_date=start_str, end_date=end_str, adjust="qfq")
        df_index = ak.index_zh_a_hist(symbol="000300", period="daily", start_date=start_str, end_date=end_str)
        if df_stock is not None and not df_stock.empty and df_index is not None and not df_index.empty:
            df_stock = df_stock.copy()
            df_index = df_index.copy()
            df_stock["日期"] = pd.to_datetime(df_stock["日期"], errors="coerce")
            df_index["日期"] = pd.to_datetime(df_index["日期"], errors="coerce")
            df_stock = df_stock[df_stock["日期"].notna()]
            df_index = df_index[df_index["日期"].notna()]
            df_stock["收盘"] = pd.to_numeric(df_stock["收盘"], errors="coerce")
            df_index["收盘"] = pd.to_numeric(df_index["收盘"], errors="coerce")
            m = pd.merge(df_stock[["日期", "收盘"]], df_index[["日期", "收盘"]], on="日期", how="inner", suffixes=("_stock", "_hs300"))
            m = m.dropna()
            if not m.empty:
                rs = m["收盘_stock"] / m["收盘_hs300"]
                rs_ma20 = rs.rolling(20).mean()
                relative = {
                    "rs": _round(rs.iloc[-1], 6),
                    "rs_ma20": _round(rs_ma20.iloc[-1], 6),
                    "status": "STRONG" if rs_ma20.iloc[-1] == rs_ma20.iloc[-1] and rs.iloc[-1] > rs_ma20.iloc[-1] else "WEAK",
                }
    except Exception as e:
        relative = {"error": str(e)}

    try:
        if symbol.startswith("6") or symbol.startswith("68"):
            market = "sh"
        else:
            market = "sz"
        flow_df = ak.stock_individual_fund_flow(stock=symbol, market=market)
        if flow_df is not None and not flow_df.empty and "日期" in flow_df.columns:
            flow_df = flow_df.copy()
            flow_df["date_dt"] = pd.to_datetime(flow_df["日期"], errors="coerce")
            flow_df = flow_df[flow_df["date_dt"].notna()].sort_values("date_dt", ascending=False).reset_index(drop=True)
            for c in ["主力净流入-净额", "主力净流入-净占比", "超大单净流入-净额", "大单净流入-净额", "收盘价", "涨跌幅"]:
                if c in flow_df.columns:
                    flow_df[c] = pd.to_numeric(flow_df[c], errors="coerce")
            latest = flow_df.iloc[0].to_dict()
            main_net_5d = None
            if "主力净流入-净额" in flow_df.columns:
                main_net_5d = _round((flow_df["主力净流入-净额"].head(5).sum() / 10000.0), 2)
            history_last_100 = []
            for _, rr in flow_df.head(100).iterrows():
                history_last_100.append(
                    {
                        "date": rr.get("日期"),
                        "close": _round(rr.get("收盘价")),
                        "chg_pct": _round(rr.get("涨跌幅"), 2),
                        "main_net_in_10k": _round(_to_float(rr.get("主力净流入-净额")) / 10000.0 if rr.get("主力净流入-净额") is not None else None, 2),
                        "main_ratio_pct": _round(rr.get("主力净流入-净占比"), 2),
                        "huge_net_in_10k": _round(_to_float(rr.get("超大单净流入-净额")) / 10000.0 if rr.get("超大单净流入-净额") is not None else None, 2),
                        "big_net_in_10k": _round(_to_float(rr.get("大单净流入-净额")) / 10000.0 if rr.get("大单净流入-净额") is not None else None, 2),
                    }
                )
            money_flow = {
                "latest_date": latest.get("日期"),
                "close": _round(latest.get("收盘价")),
                "chg_pct": _round(latest.get("涨跌幅"), 2),
                "main_net_in_10k": _round(_to_float(latest.get("主力净流入-净额")) / 10000.0 if latest.get("主力净流入-净额") is not None else None, 2),
                "main_ratio_pct": _round(latest.get("主力净流入-净占比"), 2),
                "huge_net_in_10k": _round(_to_float(latest.get("超大单净流入-净额")) / 10000.0 if latest.get("超大单净流入-净额") is not None else None, 2),
                "big_net_in_10k": _round(_to_float(latest.get("大单净流入-净额")) / 10000.0 if latest.get("大单净流入-净额") is not None else None, 2),
                "main_net_5d_10k": main_net_5d,
                "history_last_100": history_last_100,
            }
    except Exception as e:
        money_flow = {"error": str(e)}

    try:
        CONFIG = {"institution_ratio_threshold": 3.0, "sell_threshold_yi": 1.0, "back_days": 365, "top_n_days": 3}
        now = pd.Timestamp.now()
        end_date = now.strftime("%Y%m%d")
        start_date = (now - pd.Timedelta(days=CONFIG["back_days"])).strftime("%Y%m%d")
        raw_df = ak.stock_lhb_detail_em(start_date=start_date, end_date=end_date)
        if raw_df is not None and not raw_df.empty:
            code_col = "代码" if "代码" in raw_df.columns else ("证券代码" if "证券代码" in raw_df.columns else None)
            date_col = "上榜日" if "上榜日" in raw_df.columns else ("上榜日期" if "上榜日期" in raw_df.columns else None)
            if code_col and date_col:
                target_df = raw_df[raw_df[code_col].astype(str) == str(symbol)].copy()
                if not target_df.empty:
                    target_df[date_col] = pd.to_datetime(target_df[date_col], errors="coerce")
                    target_df = target_df[target_df[date_col].notna()].drop_duplicates(subset=[date_col], keep="first")
                    target_df = target_df.sort_values(date_col, ascending=False).head(CONFIG["top_n_days"]).reset_index(drop=True)
                    if "龙虎榜净买额" in target_df.columns:
                        target_df["net_buy_yi"] = pd.to_numeric(target_df["龙虎榜净买额"], errors="coerce") / 100000000.0
                    else:
                        target_df["net_buy_yi"] = np.nan
                    ratio_col = "净买额占总成交比" if "净买额占总成交比" in target_df.columns else ("净买额占比" if "净买额占比" in target_df.columns else None)
                    if ratio_col:
                        target_df[ratio_col] = pd.to_numeric(target_df[ratio_col], errors="coerce")

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
                    for _, rr in target_df.iterrows():
                        ratio_v = rr.get(ratio_col) if ratio_col else None
                        movement, suggestion = judge_main_force(rr.get("net_buy_yi"), ratio_v)
                        items.append(
                            {
                                "date": rr[date_col].strftime("%Y-%m-%d") if rr.get(date_col) is not None and rr[date_col] == rr[date_col] else None,
                                "close": _round(rr.get("收盘价")),
                                "chg_pct": _round(rr.get("涨跌幅"), 2),
                                "net_buy_yi": _round(rr.get("net_buy_yi"), 3),
                                "net_ratio_pct": _round(ratio_v, 2),
                                "turnover_rate": _round(rr.get("换手率"), 2),
                                "reason": rr.get("上榜原因"),
                                "movement": movement,
                                "suggestion": suggestion,
                            }
                        )
                    lhb = {"items": items}
                else:
                    lhb = {"hint": "近期未上榜"}
            else:
                lhb = {"hint": "龙虎榜字段变化，无法解析"}
        else:
            lhb = {"hint": "无龙虎榜数据"}
    except Exception as e:
        lhb = {"error": str(e)}

    try:
        fund_df = ak.stock_fund_stock_holder(symbol=symbol)
        if fund_df is not None and not fund_df.empty:
            fund_df = fund_df.copy()
            if "占流通股比例" in fund_df.columns:
                fund_df["占流通股比例"] = pd.to_numeric(fund_df["占流通股比例"], errors="coerce").fillna(0)
                fund_df = fund_df.sort_values("占流通股比例", ascending=False).reset_index(drop=True)

            total_ratio = float(fund_df["占流通股比例"].sum()) if "占流通股比例" in fund_df.columns else None
            fund_count = int(len(fund_df))
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

            keep = [c for c in ["基金名称", "基金代码", "持仓数量", "占流通股比例", "持股市值", "占净值比例", "截止日期"] if c in fund_df.columns]
            show = fund_df[keep].head(30).copy() if keep else fund_df.head(30).copy()
            if "持股市值" in show.columns:
                show["持股市值(亿)"] = pd.to_numeric(show["持股市值"], errors="coerce").fillna(0) / 100000000.0
                show.drop(columns=["持股市值"], inplace=True)
            items = show.to_dict("records")
            fund_holding = {"summary": {"total_ratio_pct": _round(total_ratio, 2) if total_ratio is not None else None, "fund_count": fund_count, "concentration": concentration}, "top30": items}
        else:
            fund_holding = {"hint": "无基金持仓数据"}
    except Exception as e:
        fund_holding = {"error": str(e)}

    try:
        detail_fn = ak.stock_margin_detail_sse if (symbol.startswith("6") or symbol.startswith("68")) else ak.stock_margin_detail_szse
        found_df = None
        found_date = None
        for i in range(15):
            d = (pd.Timestamp.now() - pd.Timedelta(days=i)).strftime("%Y%m%d")
            try:
                tmp = detail_fn(date=d)
            except Exception:
                tmp = pd.DataFrame()
            if tmp is not None and not tmp.empty:
                found_df = tmp
                found_date = d
                break

        if found_df is not None and not found_df.empty:
            found_df = found_df.copy()
            code_col = None
            for c in ["证券代码", "标的证券代码", "代码"]:
                if c in found_df.columns:
                    code_col = c
                    break
            if code_col:
                row_df = found_df[found_df[code_col].astype(str) == str(symbol)].copy()
                if not row_df.empty:
                    rr = row_df.iloc[0].to_dict()
                    margin = {"date": found_date}
                    for k in ["融资余额", "融券余量", "融券余额", "融资买入额", "融资偿还额", "融券卖出量", "融券偿还量"]:
                        if k in rr:
                            margin[k] = _to_float(rr.get(k))
                else:
                    margin = {"date": found_date, "hint": "当日无个股两融明细"}
            else:
                margin = {"date": found_date, "hint": "两融字段变化，无法解析"}
        else:
            margin = {"hint": "无两融数据"}
    except Exception as e:
        margin = {"error": str(e)}

    try:
        ticks_df = ak.stock_intraday_em(symbol=symbol)
        if ticks_df is not None and not ticks_df.empty:
            ticks_df = ticks_df.copy()
            for c in ["成交价", "手数"]:
                if c in ticks_df.columns:
                    ticks_df[c] = pd.to_numeric(ticks_df[c], errors="coerce")
            if "成交价" in ticks_df.columns and "手数" in ticks_df.columns:
                ticks_df["amount_wan"] = (ticks_df["成交价"] * ticks_df["手数"] * 100.0) / 10000.0
            else:
                ticks_df["amount_wan"] = np.nan

            top_df = ticks_df.sort_values("amount_wan", ascending=False).head(30).copy()
            items = []
            buy_wan = 0.0
            sell_wan = 0.0
            for _, rr in top_df.iterrows():
                nature = rr.get("买卖盘性质")
                amt_wan = _to_float(rr.get("amount_wan")) or 0.0
                if nature == "买盘":
                    buy_wan += amt_wan
                elif nature == "卖盘":
                    sell_wan += amt_wan
                items.append(
                    {
                        "time": rr.get("时间"),
                        "price": _round(rr.get("成交价"), 3),
                        "hands": _to_int(rr.get("手数")),
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
        chip_df = ak.stock_cyq_em(symbol=symbol)
        if chip_df is not None and not chip_df.empty:
            chip_df = chip_df.copy()
            if "日期" in chip_df.columns:
                chip_df["日期"] = pd.to_datetime(chip_df["日期"], errors="coerce")
                chip_df = chip_df.sort_values("日期").reset_index(drop=True)
            last = chip_df.iloc[-1].to_dict()
            keys = ["日期", "获利比例", "平均成本", "90%集中度", "90集中度", "集中度90", "成本均线", "成本"] 
            out = {}
            for k in keys:
                if k in last:
                    out[k] = last.get(k)
            chip = {"latest": out}
    except Exception as e:
        chip = {"error": str(e)}

    try:
        news_df = ak.stock_news_em(symbol=symbol)
        if news_df is not None and not news_df.empty:
            if "发布时间" in news_df.columns:
                news_df["发布时间_dt"] = pd.to_datetime(news_df["发布时间"], errors="coerce")
                news_df = news_df.sort_values("发布时间_dt", ascending=False)
            keep_cols = [c for c in ["发布时间", "新闻标题", "文章来源", "url"] if c in news_df.columns]
            top = news_df.head(5)[keep_cols].copy() if keep_cols else news_df.head(5).copy()
            items = []
            for _, rr in top.iterrows():
                items.append({k: (rr.get(k) if not pd.isna(rr.get(k)) else None) for k in top.columns})
            meta["news_top5"] = items
    except Exception as e:
        meta["news_error"] = str(e)

    def _attach_describe(obj, text):
        if obj is None or not isinstance(obj, dict):
            obj = {}
        if "describe" not in obj:
            obj["describe"] = text
        return obj

    snapshot = _attach_describe(snapshot, "实时快照：价格/涨跌幅/估值/成交等概览")
    order_book = _attach_describe(order_book, "盘口信息：五档买卖/涨跌停/外内盘等")
    daily = _attach_describe(daily, "日线数据：最近K线、涨跌幅、换手率等")
    volume_indicators = _attach_describe(volume_indicators, "量能指标：成交量均线/量能强弱/OBV/MFI/AD 等")
    share_structure = _attach_describe(share_structure, "股本结构：行业/上市日期/总股本/流通股等")
    fundamental = _attach_describe(fundamental, "基本面：财务分析指标（如可用）与概览")
    trading_rules = _attach_describe(trading_rules, "交易规则：涨跌停价/涨跌停幅度推算/T+1")
    new_stock = _attach_describe(new_stock, "次新股属性：上市时长/是否次新/流通市值等")
    company = _attach_describe(company, "公司画像：主营/题材/管理层等（如可用）")
    disclosure = _attach_describe(disclosure, "公告/互动：互动易问答摘要（如可用）")
    intraday = _attach_describe(intraday, "分时数据：1分钟/5分钟、VWAP、日内高低等")
    technicals = _attach_describe(technicals, "技术指标：MA/MACD/RSI/BOLL/ATR/唐奇安等")
    relative = _attach_describe(relative, "相对强弱：相对沪深300的RS与状态")
    money_flow = _attach_describe(money_flow, "资金流向：主力/超大单/大单净流入与历史")
    chip = _attach_describe(chip, "筹码分布：成本、集中度与获利比例等")
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
_add_cov("公司画像与题材 (主营/概念/互动)", _non_empty(_pick(result, "company.profile")) or _non_empty(_pick(result, "company.concepts")) or _non_empty(_pick(result, "disclosure.irm_qna_top10")) or _non_empty(_pick(result, "disclosure.announcements_top10")))
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
