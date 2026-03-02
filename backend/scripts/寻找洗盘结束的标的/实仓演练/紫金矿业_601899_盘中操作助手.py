import argparse
import os
import sys
import time
from dataclasses import dataclass
from datetime import datetime, time as dtime
from typing import Dict, List, Optional, Tuple
 
 
def _find_backend_dir() -> str:
    script_dir = os.path.dirname(os.path.abspath(__file__))
    probe_dir = script_dir
    for _ in range(10):
        if os.path.exists(os.path.join(probe_dir, "backend", "utils", "pytdx_client.py")):
            return os.path.join(probe_dir, "backend")
        if os.path.exists(os.path.join(probe_dir, "utils", "pytdx_client.py")):
            return probe_dir
        parent = os.path.dirname(probe_dir)
        if parent == probe_dir:
            break
        probe_dir = parent
    return os.path.join(os.path.dirname(os.path.dirname(script_dir)), "backend")
 
 
backend_dir = _find_backend_dir()
if backend_dir not in sys.path:
    sys.path.insert(0, backend_dir)
 
 
from utils.pytdx_client import tdx
 
 
def _to_market_code(ts_code_or_symbol: str) -> Optional[Tuple[int, str]]:
    s = str(ts_code_or_symbol or "").strip().upper()
    if not s:
        return None
    if "." in s:
        code, suf = s.split(".", 1)
        code = str(code).strip().zfill(6)
        suf = str(suf).strip()
        if suf == "SZ":
            return 0, code
        if suf == "SH":
            return 1, code
        return None
    code = s.zfill(6)
    if len(code) != 6 or not code.isdigit():
        return None
    if code.startswith("6"):
        return 1, code
    return 0, code
 
 
def _fmt_amount_yi(yuan: float) -> str:
    v = float(yuan or 0.0) / 1e8
    if abs(v) >= 1000:
        return f"{v:,.0f}亿"
    if abs(v) >= 100:
        return f"{v:,.1f}亿"
    return f"{v:,.2f}亿"
 
 
def _fmt_pct(v: float) -> str:
    return f"{float(v):+.2f}%"
 
 
def _in_time_range(now: dtime, start: dtime, end: dtime) -> bool:
    return start <= now <= end
 
 
@dataclass(frozen=True)
class Quote:
    ts_code: str
    price: float
    open: float
    high: float
    low: float
    last_close: float
    vol_hand: float
    amount_yuan: float
    b_vol_hand: float
    s_vol_hand: float
    bid1: float
    ask1: float
 
    @property
    def pct_chg(self) -> float:
        if self.last_close <= 0:
            return 0.0
        return (self.price / self.last_close - 1.0) * 100.0
 
    @property
    def gap_open_pct(self) -> float:
        if self.last_close <= 0:
            return 0.0
        if self.open <= 0:
            return 0.0
        return (self.open / self.last_close - 1.0) * 100.0
 
    @property
    def net_mf_est_yuan(self) -> float:
        price = float(self.price or 0.0)
        if price <= 0:
            return 0.0
        return (float(self.b_vol_hand or 0.0) - float(self.s_vol_hand or 0.0)) * price * 100.0
 
 
def _fetch_quotes(ts_codes: List[str]) -> Dict[str, Quote]:
    req: List[Tuple[int, str]] = []
    keep: List[str] = []
    for c in ts_codes:
        mc = _to_market_code(c)
        if mc is None:
            continue
        keep.append(str(c))
        req.append(mc)
 
    if not req:
        return {}
 
    try:
        ret = tdx.get_security_quotes(req)
    except Exception:
        return {}
    if not isinstance(ret, list):
        return {}
 
    out: Dict[str, Quote] = {}
    for ts_code, q in zip(keep, ret):
        if not isinstance(q, dict):
            continue
        out[str(ts_code)] = Quote(
            ts_code=str(ts_code),
            price=float(q.get("price") or 0.0),
            open=float(q.get("open") or 0.0),
            high=float(q.get("high") or 0.0),
            low=float(q.get("low") or 0.0),
            last_close=float(q.get("last_close") or 0.0),
            vol_hand=float(q.get("vol") or 0.0),
            amount_yuan=float(q.get("amount") or 0.0),
            b_vol_hand=float(q.get("b_vol") or 0.0),
            s_vol_hand=float(q.get("s_vol") or 0.0),
            bid1=float(q.get("bid1") or 0.0),
            ask1=float(q.get("ask1") or 0.0),
        )
    return out
 
 
def _sector_proxy_pct(quotes: Dict[str, Quote], members: List[str]) -> Optional[float]:
    vals: List[float] = []
    for c in members:
        q = quotes.get(c)
        if q is None:
            continue
        if q.last_close <= 0 or q.price <= 0:
            continue
        vals.append(float(q.pct_chg))
    if not vals:
        return None
    return sum(vals) / len(vals)
 
 
def _within(price: float, lo: float, hi: float) -> bool:
    p = float(price or 0.0)
    return (p >= float(lo)) and (p <= float(hi))
 
 
@dataclass
class PlanConfig:
    ts_code: str = "601899.SH"
    name: str = "紫金矿业"
    total_cash: float = 199360.40
    invest_cash: float = 150000.0
    batch_shares: int = 2600
    tail_shares: int = 1000
    buy1_max_price: float = 18.8
    buy2_max_price: float = 18.7
    buy3_max_price: float = 18.9
    open_lo: float = 18.4
    open_hi: float = 18.8
    buy1_price_lo: float = 18.4
    buy1_price_hi: float = 18.8
    buy2_price_lo: float = 18.5
    buy2_price_hi: float = 18.7
    buy3_price_lo: float = 18.6
    buy3_price_hi: float = 18.9
    stop_loss: float = 17.5
    pause3_below: float = 18.3
    hard_warn_below: float = 18.0
    tp1: float = 20.5
    tp2: float = 22.0
    auction_amount_min_yuan: float = 3e8
    buy1_amount_min_yuan: float = 4e9
    buy2_amount_min_yuan: float = 2e9
    sector_min_pct: float = 0.5
    tail_net_in_yuan: float = 5e8
    sector_proxy_members: Tuple[str, ...] = (
        "600362.SH",
        "603993.SH",
        "601600.SH",
        "000878.SZ",
        "600111.SH",
    )
 
 
def _stage_actions(now_t: dtime, q: Quote, sector_pct: Optional[float], cfg: PlanConfig) -> List[str]:
    actions: List[str] = []
 
    if q.price > 0 and q.price <= cfg.stop_loss:
        actions.append(f"止损触发价位({cfg.stop_loss:.2f})已跌破：建议立即评估全清")
        return actions
 
    if q.price > 0 and q.price >= cfg.tp2:
        actions.append(f"第二止盈触发({cfg.tp2:.2f}+): 建议卖出剩余50%仓位")
    elif q.price > 0 and q.price >= cfg.tp1:
        actions.append(f"第一止盈触发({cfg.tp1:.2f}+): 建议卖出50%仓位")
 
    if _in_time_range(now_t, dtime(9, 15), dtime(9, 25)):
        ok_open_range = _within(q.open if q.open > 0 else q.price, cfg.open_lo, cfg.open_hi)
        ok_gap = (q.gap_open_pct <= 5.0) if (q.open > 0 and q.last_close > 0) else True
        ok_amount = q.amount_yuan >= cfg.auction_amount_min_yuan
        if ok_open_range and ok_gap and ok_amount:
            actions.append(
                f"集合竞价健康：可准备第一笔(限价≤{cfg.buy1_max_price:.2f})，目标{cfg.batch_shares}股"
            )
        else:
            bads = []
            if not ok_open_range:
                bads.append(f"开盘不在{cfg.open_lo:.2f}-{cfg.open_hi:.2f}")
            if not ok_gap:
                bads.append("高开>5%")
            if not ok_amount:
                bads.append(f"竞价成交额<{_fmt_amount_yi(cfg.auction_amount_min_yuan)}")
            actions.append("集合竞价观望：" + "，".join(bads))
        return actions
 
    if _in_time_range(now_t, dtime(9, 30), dtime(10, 0)):
        ok_price = _within(q.price, cfg.buy1_price_lo, cfg.buy1_price_hi)
        ok_amount = q.amount_yuan >= cfg.buy1_amount_min_yuan
        if q.price > 19.0:
            actions.append("早盘冲高>19：暂停第一笔，等回落至≤18.8再考虑")
        elif ok_price and ok_amount and q.price <= cfg.buy1_max_price:
            actions.append(f"第一笔触发：建议买入{cfg.batch_shares}股(限价≤{cfg.buy1_max_price:.2f})")
        else:
            parts = []
            if not ok_price:
                parts.append(f"价格不在{cfg.buy1_price_lo:.2f}-{cfg.buy1_price_hi:.2f}")
            if not ok_amount:
                parts.append(f"量能不足<{_fmt_amount_yi(cfg.buy1_amount_min_yuan)}")
            if q.price > cfg.buy1_max_price:
                parts.append(f"价格>{cfg.buy1_max_price:.2f}")
            if parts:
                actions.append("第一笔暂不触发：" + "，".join(parts))
        return actions
 
    if _in_time_range(now_t, dtime(10, 30), dtime(11, 0)):
        if q.price > 19.0:
            actions.append("强势突破>19：暂缓第二笔，观察午盘")
            return actions
        ok_price = _within(q.price, cfg.buy2_price_lo, cfg.buy2_price_hi)
        ok_amount = q.amount_yuan >= cfg.buy2_amount_min_yuan
        if ok_price and ok_amount and q.price <= cfg.buy2_max_price:
            actions.append(f"第二笔触发：建议买入{cfg.batch_shares}股(限价≤{cfg.buy2_max_price:.2f})")
        else:
            parts = []
            if not ok_price:
                parts.append(f"价格不在{cfg.buy2_price_lo:.2f}-{cfg.buy2_price_hi:.2f}")
            if not ok_amount:
                parts.append(f"量能不足<{_fmt_amount_yi(cfg.buy2_amount_min_yuan)}")
            if q.price > cfg.buy2_max_price:
                parts.append(f"价格>{cfg.buy2_max_price:.2f}")
            if parts:
                actions.append("第二笔暂不触发：" + "，".join(parts))
        return actions
 
    if _in_time_range(now_t, dtime(13, 0), dtime(13, 30)):
        if q.price > 0 and q.price < cfg.hard_warn_below:
            actions.append(f"跌破{cfg.hard_warn_below:.2f}：进入止损评估区(第三笔暂停)")
            return actions
        if q.price > 0 and q.price < cfg.pause3_below:
            actions.append(f"跌破{cfg.pause3_below:.2f}(10日线附近)：第三笔暂停，转观望")
            return actions
        ok_price = _within(q.price, cfg.buy3_price_lo, cfg.buy3_price_hi)
        ok_sector = (sector_pct is not None) and (sector_pct >= cfg.sector_min_pct)
        if ok_price and ok_sector and q.price <= cfg.buy3_max_price:
            actions.append(f"第三笔触发：建议买入{cfg.batch_shares}股(限价≤{cfg.buy3_max_price:.2f})")
        else:
            parts = []
            if not ok_price:
                parts.append(f"价格不在{cfg.buy3_price_lo:.2f}-{cfg.buy3_price_hi:.2f}")
            if not ok_sector:
                if sector_pct is None:
                    parts.append("板块涨幅不可用")
                else:
                    parts.append(f"板块<{cfg.sector_min_pct:.2f}%")
            if q.price > cfg.buy3_max_price:
                parts.append(f"价格>{cfg.buy3_max_price:.2f}")
            if parts:
                actions.append("第三笔暂不触发：" + "，".join(parts))
        return actions
 
    if _in_time_range(now_t, dtime(14, 30), dtime(15, 0)):
        ok_price = _within(q.price, 18.4, 19.0)
        ok_net = q.net_mf_est_yuan >= cfg.tail_net_in_yuan
        if ok_price and ok_net:
            actions.append(f"尾盘加仓触发：建议加仓{cfg.tail_shares}股")
        else:
            parts = []
            if not ok_price:
                parts.append("价格不在18.4-19.0")
            if not ok_net:
                parts.append(f"净流入估算<{_fmt_amount_yi(cfg.tail_net_in_yuan)}")
            if parts:
                actions.append("尾盘不加仓：" + "，".join(parts))
        return actions
 
    actions.append("非计划操作窗口：只监控止盈/止损")
    return actions
 
 
def _format_status_line(q: Quote, sector_pct: Optional[float], cfg: PlanConfig) -> str:
    price = q.price
    pct = q.pct_chg
    amount = q.amount_yuan
    gap = q.gap_open_pct
    net_mf = q.net_mf_est_yuan
    parts = [
        f"{cfg.name}({cfg.ts_code})",
        f"价{price:.2f}({_fmt_pct(pct)})",
        f"开{q.open:.2f}" if q.open > 0 else "开-",
        f"昨收{q.last_close:.2f}" if q.last_close > 0 else "昨收-",
        f"缺口{gap:+.2f}%" if (q.open > 0 and q.last_close > 0) else "缺口-",
        f"额{_fmt_amount_yi(amount)}",
        f"净{_fmt_amount_yi(net_mf)}",
        f"板块{sector_pct:+.2f}%" if sector_pct is not None else "板块-",
        f"买一{q.bid1:.2f}/卖一{q.ask1:.2f}" if (q.bid1 > 0 or q.ask1 > 0) else "盘口-",
    ]
    return " | ".join(parts)
 
 
def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--interval", type=float, default=float(os.getenv("INTERVAL", "12")))
    p.add_argument("--once", action="store_true")
    p.add_argument("--ts-code", type=str, default=os.getenv("TS_CODE", "601899.SH"))
    p.add_argument("--name", type=str, default=os.getenv("NAME", "紫金矿业"))
    p.add_argument("--batch-shares", type=int, default=int(os.getenv("BATCH_SHARES", "2600")))
    p.add_argument("--tail-shares", type=int, default=int(os.getenv("TAIL_SHARES", "1000")))
    p.add_argument("--buy1-max", type=float, default=float(os.getenv("BUY1_MAX", "18.8")))
    p.add_argument("--buy2-max", type=float, default=float(os.getenv("BUY2_MAX", "18.7")))
    p.add_argument("--buy3-max", type=float, default=float(os.getenv("BUY3_MAX", "18.9")))
    p.add_argument("--auction-min-yi", type=float, default=float(os.getenv("AUCTION_MIN_YI", "3")))
    p.add_argument("--buy1-min-yi", type=float, default=float(os.getenv("BUY1_MIN_YI", "40")))
    p.add_argument("--buy2-min-yi", type=float, default=float(os.getenv("BUY2_MIN_YI", "20")))
    p.add_argument("--tail-net-min-yi", type=float, default=float(os.getenv("TAIL_NET_MIN_YI", "5")))
    p.add_argument("--sector-min-pct", type=float, default=float(os.getenv("SECTOR_MIN_PCT", "0.5")))
    p.add_argument("--sector-members", type=str, default=os.getenv("SECTOR_MEMBERS", ""))
    return p.parse_args()
 
 
def main() -> None:
    args = _parse_args()
    cfg = PlanConfig()
    cfg.ts_code = str(args.ts_code).strip().upper() or cfg.ts_code
    cfg.name = str(args.name).strip() or cfg.name
    cfg.batch_shares = int(args.batch_shares)
    cfg.tail_shares = int(args.tail_shares)
    cfg.buy1_max_price = float(args.buy1_max)
    cfg.buy2_max_price = float(args.buy2_max)
    cfg.buy3_max_price = float(args.buy3_max)
    cfg.auction_amount_min_yuan = float(args.auction_min_yi) * 1e8
    cfg.buy1_amount_min_yuan = float(args.buy1_min_yi) * 1e8
    cfg.buy2_amount_min_yuan = float(args.buy2_min_yi) * 1e8
    cfg.tail_net_in_yuan = float(args.tail_net_min_yi) * 1e8
    cfg.sector_min_pct = float(args.sector_min_pct)
    if str(args.sector_members).strip():
        cfg.sector_proxy_members = tuple([x.strip().upper() for x in str(args.sector_members).split(",") if x.strip()])
 
    watch = [cfg.ts_code] + list(cfg.sector_proxy_members)
    interval_s = max(2.0, float(args.interval))
 
    i = 0
    while True:
        i += 1
        now = datetime.now()
        now_t = now.time().replace(microsecond=0)
 
        with tdx:
            quotes = _fetch_quotes(watch)
 
        q = quotes.get(cfg.ts_code)
        if q is None:
            print(f"{now.strftime('%Y-%m-%d %H:%M:%S')} | 行情缺失：{cfg.ts_code}")
        else:
            sector_pct = _sector_proxy_pct(quotes, list(cfg.sector_proxy_members))
            print(f"{now.strftime('%Y-%m-%d %H:%M:%S')} | {_format_status_line(q, sector_pct, cfg)}")
            actions = _stage_actions(now_t, q, sector_pct, cfg)
            for a in actions:
                print("-> " + a)
 
        if bool(args.once):
            break
        time.sleep(interval_s)
 
 
if __name__ == "__main__":
    main()
