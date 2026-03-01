import argparse
import os
import random
import sys
import time
from datetime import datetime
from typing import Iterable, List, Tuple

import numpy as np


def _now() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _fmt_bool(v: bool) -> str:
    return "OK" if bool(v) else "FAIL"


def _backend_dir() -> str:
    here = os.path.dirname(os.path.abspath(__file__))
    return os.path.abspath(os.path.join(here, ".."))


def _is_a_share_stock(market: int, code: str) -> bool:
    code = str(code or "").zfill(6)
    if int(market) == 0:
        return code.startswith(("000", "001", "002", "003", "300", "301"))
    if int(market) == 1:
        return code.startswith(("600", "601", "603", "605", "688"))
    return False


def _parse_markets(s: str) -> List[int]:
    s = str(s or "").strip().lower()
    if not s or s in {"both", "all"}:
        return [0, 1]
    out: List[int] = []
    for part in s.replace(";", ",").split(","):
        part = part.strip()
        if not part:
            continue
        if part in {"sz", "0"}:
            out.append(0)
        elif part in {"sh", "1"}:
            out.append(1)
    out = sorted(set(out))
    return out if out else [0, 1]


def _sample_codes(tdx, markets: List[int], n: int, seed: int) -> List[Tuple[int, str]]:
    rng = random.Random(int(seed))
    out: List[Tuple[int, str]] = []
    for m in markets:
        try:
            total = int(tdx.get_security_count(int(m)) or 0)
        except Exception:
            total = 0
        if total <= 0:
            continue
        tries = 0
        while tries < max(30, int(n) * 3) and len([x for x in out if x[0] == int(m)]) < max(1, int(n) // len(markets)):
            tries += 1
            start = int(rng.randint(0, max(0, total - 1)))
            start = (start // 1000) * 1000
            try:
                rows = tdx.get_security_list(int(m), int(start)) or []
            except Exception:
                rows = []
            if not rows:
                continue
            rng.shuffle(rows)
            for r in rows:
                code = str((r or {}).get("code", "")).zfill(6)
                if not code:
                    continue
                if _is_a_share_stock(int(m), code):
                    out.append((int(m), code))
                if len([x for x in out if x[0] == int(m)]) >= max(1, int(n) // len(markets)):
                    break

    if len(out) < int(n):
        all_codes = out[:]
        while len(out) < int(n) and all_codes:
            out.append(rng.choice(all_codes))

    rng.shuffle(out)
    return out[: int(n)]


def _fetch_bars(tdx, market: int, code: str, bars: int) -> Tuple[bool, float, int, str]:
    t0 = time.time()
    err = ""
    rows = 0
    ok = False
    try:
        data = tdx.get_security_bars(9, int(market), str(code).zfill(6), 0, int(bars))
        rows = 0 if not data else int(len(data))
        ok = rows > 0
    except Exception as e:
        err = f"{type(e).__name__}:{e}"
        ok = False
        rows = 0
    elapsed = time.time() - t0
    return ok, float(elapsed), int(rows), err


def _quantile(xs: List[float], q: float) -> float:
    if not xs:
        return 0.0
    try:
        return float(np.quantile(np.asarray(xs, dtype=float), float(q)))
    except Exception:
        xs2 = sorted(float(x) for x in xs)
        idx = int(round((len(xs2) - 1) * float(q)))
        idx = max(0, min(len(xs2) - 1, idx))
        return float(xs2[idx])


def _run_once(tdx, markets: List[int], samples: int, repeats: int, bars: int, sleep_each: float, seed: int) -> dict:
    picked = _sample_codes(tdx, markets=markets, n=int(samples), seed=int(seed))
    latencies: List[float] = []
    ok_calls = 0
    empty_calls = 0
    err_calls = 0
    by_market = {int(m): {"ok": 0, "empty": 0, "err": 0} for m in markets}

    for market, code in picked:
        for _ in range(int(repeats)):
            ok, elapsed, rows, err = _fetch_bars(tdx, market=int(market), code=str(code), bars=int(bars))
            latencies.append(float(elapsed))
            if err:
                err_calls += 1
                by_market[int(market)]["err"] += 1
            elif ok:
                ok_calls += 1
                by_market[int(market)]["ok"] += 1
            else:
                empty_calls += 1
                by_market[int(market)]["empty"] += 1
            if float(sleep_each) > 0:
                time.sleep(float(sleep_each))

    total_calls = int(samples) * int(repeats)
    return {
        "samples": int(samples),
        "repeats": int(repeats),
        "bars": int(bars),
        "total_calls": int(total_calls),
        "ok": int(ok_calls),
        "empty": int(empty_calls),
        "err": int(err_calls),
        "p50": round(_quantile(latencies, 0.50), 3),
        "p90": round(_quantile(latencies, 0.90), 3),
        "p99": round(_quantile(latencies, 0.99), 3),
        "by_market": by_market,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="TDX 接口健康探测：批量请求日线 bars，统计空返回比例与延迟分位数")
    parser.add_argument("--interval-s", type=float, default=5.0)
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--markets", type=str, default="both")
    parser.add_argument("--samples", type=int, default=60)
    parser.add_argument("--repeats", type=int, default=1)
    parser.add_argument("--bars", type=int, default=120)
    parser.add_argument("--sleep-each", type=float, default=0.0)
    parser.add_argument("--seed", type=int, default=1)
    args = parser.parse_args()

    backend_dir = _backend_dir()
    if backend_dir not in sys.path:
        sys.path.insert(0, backend_dir)

    try:
        from utils.pytdx_client import tdx, connected_endpoint
    except Exception as e:
        raise SystemExit(f"导入 pytdx_client 失败: {type(e).__name__}:{e}")

    markets = _parse_markets(str(args.markets))
    interval_s = max(0.2, float(args.interval_s))
    i = 0
    while True:
        i += 1
        with tdx:
            ep = connected_endpoint()
            r = _run_once(
                tdx,
                markets=markets,
                samples=int(args.samples),
                repeats=int(args.repeats),
                bars=int(args.bars),
                sleep_each=float(args.sleep_each),
                seed=int(args.seed) + i,
            )
        ok = (int(r.get("err", 0)) == 0) and (int(r.get("empty", 0)) == 0)
        msg = (
            f"{_now()} {_fmt_bool(ok)} "
            f"endpoint={ep} markets={','.join(str(x) for x in markets)} "
            f"samples={r['samples']} repeats={r['repeats']} bars={r['bars']} "
            f"ok={r['ok']} empty={r['empty']} err={r['err']} "
            f"p50={r['p50']:.3f}s p90={r['p90']:.3f}s p99={r['p99']:.3f}s"
        )
        bm = r.get("by_market") or {}
        if isinstance(bm, dict) and bm:
            parts = []
            for m in markets:
                mm = bm.get(int(m)) or {}
                parts.append(f"m{int(m)}(ok={int(mm.get('ok',0))},empty={int(mm.get('empty',0))},err={int(mm.get('err',0))})")
            msg += " " + " ".join(parts)
        print(msg, flush=True)

        if bool(args.once):
            return
        time.sleep(interval_s)


if __name__ == "__main__":
    main()
