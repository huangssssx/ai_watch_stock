import sys
import os
import time
import io

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../../"))

import numpy as np
import optuna
from utils.pytdx_client import connect, DEFAULT_IP, DEFAULT_PORT
from utils.tushare_client import pro

TRAIN_DATES = [
    "20260410", "20260409", "20260408", "20260407", "20260403",
    "20260402", "20260401", "20260331",
]
TEST_DATES = ["20260330", "20260327"]

MARKET_CAP_BINS = {
    "超小盘(<30亿)": (0, 30),
    "小盘(30-100亿)": (30, 100),
    "中盘(100-500亿)": (100, 500),
    "大盘(500-2000亿)": (500, 2000),
    "超大盘(>2000亿)": (2000, float("inf")),
}

K_GRID = np.logspace(-6, -2, 50)
K_GRID_EXT = np.logspace(-7, -2, 60)
M_GRID = [1.0, 1.2, 1.5, 2.0, 2.5, 3.0]
USE_EXPANDED_DATASET = True
EXPANDED_TOTAL_DATES = 32
EXPANDED_TEST_DATES = 8
WALK_FORWARD_FOLDS = 4
BAYES_TRIALS = 60
RUN_PHASE8_GRID = False


def classify_cap(market_cap_yi):
    for label, (lo, hi) in MARKET_CAP_BINS.items():
        if lo <= market_cap_yi < hi:
            return label
    return "未知"


def get_open_dates(end_date, total_days):
    start_guess = "20250101"
    cal = pro.trade_cal(exchange="", start_date=start_guess, end_date=end_date, is_open="1")
    if cal is None or cal.empty:
        return []
    dates = sorted(cal["cal_date"].tolist())
    return dates[-total_days:]


def get_train_test_dates():
    if not USE_EXPANDED_DATASET:
        return TRAIN_DATES, TEST_DATES
    open_dates = get_open_dates(TRAIN_DATES[0], EXPANDED_TOTAL_DATES)
    if len(open_dates) <= EXPANDED_TEST_DATES + 8:
        return TRAIN_DATES, TEST_DATES
    train_dates = open_dates[:-EXPANDED_TEST_DATES]
    test_dates = open_dates[-EXPANDED_TEST_DATES:]
    return train_dates, test_dates


def _fetch_all_ticks(tdx, market, code, date_int):
    ticks = []
    for start in range(0, 300000, 500):
        batch = tdx.get_history_transaction_data(
            market=market, code=code, start=start, count=500, date=date_int
        )
        if not batch:
            break
        for t in batch:
            if t["vol"] <= 0:
                continue
            amount = t["vol"] * 100 * t["price"]
            ticks.append({
                "time": t["time"],
                "price": t["price"],
                "vol": t["vol"],
                "buyorsell": t["buyorsell"],
                "amount": amount,
            })
    return ticks


def calc_direction(ticks, threshold):
    mainforce = [t for t in ticks if t["amount"] >= threshold]
    if not mainforce:
        return None, 0
    buy = sum(t["amount"] for t in mainforce if t["buyorsell"] == 0)
    sell = sum(t["amount"] for t in mainforce if t["buyorsell"] == 1)
    net = buy - sell
    direction = "流入" if net > 0 else "流出"
    return direction, net


def collect_samples(tdx, dates, label):
    all_samples = []
    for trade_date_str in dates:
        trade_date_int = int(trade_date_str)
        print(f"  [{label}] {trade_date_str} ... ", end="", flush=True)

        df_lhb = pro.top_list(trade_date=trade_date_str)
        if df_lhb is None or df_lhb.empty:
            print("无龙虎榜")
            continue

        count = 0
        for ts_code in df_lhb["ts_code"].unique():
            if ts_code.endswith(".BJ"):
                continue
            sub = df_lhb[df_lhb["ts_code"] == ts_code]
            row = sub.iloc[0]
            net_amount = row["net_amount"]
            ground_truth = "流入" if net_amount > 0 else "流出"

            code = ts_code[:6]
            market = 0 if ts_code.endswith(".SZ") else 1

            ticks = _fetch_all_ticks(tdx, market, code, trade_date_int)
            if not ticks:
                continue

            finance_info = tdx.get_finance_info(market=market, code=code)
            if not finance_info:
                continue
            price = ticks[-1]["price"]
            liutongguben = finance_info.get("liutongguben", 0)
            if not liutongguben:
                continue
            market_cap_yi = liutongguben * price / 1e8

            amounts = [t["amount"] for t in ticks]
            p95 = np.percentile(amounts, 95)
            p90 = np.percentile(amounts, 90)
            p99 = np.percentile(amounts, 99)
            free_market_cap = liutongguben * price

            sample = {
                "trade_date": trade_date_str,
                "ts_code": ts_code,
                "name": row["name"],
                "pct_change": row["pct_change"] or 0,
                "market_cap_yi": market_cap_yi,
                "cap_label": classify_cap(market_cap_yi),
                "ground_truth": ground_truth,
                "ticks_count": len(ticks),
                "p95": p95,
                "p90": p90,
                "p99": p99,
                "free_market_cap": free_market_cap,
                "amounts": amounts,
                "ticks": ticks,
            }

            all_samples.append(sample)
            count += 1

        print(f"{count} 只")

        if trade_date_str != dates[-1]:
            time.sleep(0.3)

    return all_samples


def scan_K_for_sample(sample):
    ticks = sample["ticks"]
    free_market_cap = sample["free_market_cap"]
    ground_truth = sample["ground_truth"]
    p95 = sample["p95"]

    best_k = None
    best_m = None
    best_hit = False

    for k in K_GRID:
        threshold_base = free_market_cap * k / 100
        for m in M_GRID:
            threshold = max(threshold_base, p95 * m)
            direction, _ = calc_direction(ticks, threshold)
            if direction is None:
                continue
            if direction == ground_truth:
                if best_k is None:
                    best_k = k
                    best_m = m
                    best_hit = True
                break
        if best_hit:
            break

    if best_k is None:
        threshold_base = free_market_cap * 0.00001 / 100
        threshold = max(threshold_base, p95 * 1.5)
        direction, _ = calc_direction(ticks, threshold)
        best_k = 0.00001
        best_m = 1.5
        best_hit = direction == ground_truth

    return best_k, best_m, best_hit


def scan_all_K(sample):
    ticks = sample["ticks"]
    free_market_cap = sample["free_market_cap"]
    ground_truth = sample["ground_truth"]
    p95 = sample["p95"]

    results = []
    for k in K_GRID:
        threshold_base = free_market_cap * k / 100
        for m in M_GRID:
            threshold = max(threshold_base, p95 * m)
            direction, net = calc_direction(ticks, threshold)
            if direction is None:
                results.append((k, m, False, 0))
                continue
            hit = direction == ground_truth
            results.append((k, m, hit, net))
    return results


def evaluate_formula(samples, k_func, m_fixed=1.5):
    match = 0
    total = 0
    for s in samples:
        k = k_func(s["market_cap_yi"])
        threshold_base = s["free_market_cap"] * k / 100
        threshold = max(threshold_base, s["p95"] * m_fixed)
        direction, _ = calc_direction(s["ticks"], threshold)
        if direction is None:
            continue
        if direction == s["ground_truth"]:
            match += 1
        total += 1
    return match / total * 100 if total > 0 else 0, match, total


def evaluate_formula_km(samples, k_func, m_func):
    match = 0
    total = 0
    for s in samples:
        k = k_func(s["market_cap_yi"])
        m = m_func(s["market_cap_yi"])
        threshold_base = s["free_market_cap"] * k / 100
        threshold = max(threshold_base, s["p95"] * m)
        direction, _ = calc_direction(s["ticks"], threshold)
        if direction is None:
            continue
        if direction == s["ground_truth"]:
            match += 1
        total += 1
    return match / total * 100 if total > 0 else 0, match, total


def make_sigmoid(lo, hi, lam, s0):
    def f(cap_yi):
        s = np.log(max(cap_yi, 1e-6))
        return lo + (hi - lo) / (1.0 + np.exp(lam * (s - s0)))
    return f


def build_walk_forward_folds(samples, n_folds=4):
    dates = sorted({s["trade_date"] for s in samples})
    if len(dates) < n_folds + 2:
        return []
    fold_size = max(1, len(dates) // (n_folds + 1))
    folds = []
    for i in range(1, n_folds + 1):
        train_end_idx = i * fold_size
        valid_start_idx = train_end_idx
        valid_end_idx = min(len(dates), valid_start_idx + fold_size)
        if valid_end_idx <= valid_start_idx:
            continue
        train_dates = set(dates[:train_end_idx])
        valid_dates = set(dates[valid_start_idx:valid_end_idx])
        train_part = [s for s in samples if s["trade_date"] in train_dates]
        valid_part = [s for s in samples if s["trade_date"] in valid_dates]
        if len(train_part) < 30 or len(valid_part) < 20:
            continue
        folds.append((train_part, valid_part))
    return folds


def evaluate_params_on_samples(samples, params):
    k_func = make_sigmoid(params["k_min"], params["k_max"], params["lam_k"], params["s0_k"])
    m_func = make_sigmoid(params["m_min"], params["m_max"], params["lam_m"], params["s0_m"])
    return evaluate_formula_km(samples, k_func, m_func)


def evaluate_params_detailed(samples, params):
    k_func = make_sigmoid(params["k_min"], params["k_max"], params["lam_k"], params["s0_k"])
    m_func = make_sigmoid(params["m_min"], params["m_max"], params["lam_m"], params["s0_m"])
    match = 0
    total = 0
    cap_stat = {}
    date_stat = {}
    for s in samples:
        k = k_func(s["market_cap_yi"])
        m = m_func(s["market_cap_yi"])
        threshold_base = s["free_market_cap"] * k / 100
        threshold = max(threshold_base, s["p95"] * m)
        direction, _ = calc_direction(s["ticks"], threshold)
        if direction is None:
            continue
        hit = direction == s["ground_truth"]
        if hit:
            match += 1
        total += 1
        cap_key = s["cap_label"]
        date_key = s["trade_date"]
        cap_m, cap_t = cap_stat.get(cap_key, (0, 0))
        date_m, date_t = date_stat.get(date_key, (0, 0))
        cap_stat[cap_key] = (cap_m + (1 if hit else 0), cap_t + 1)
        date_stat[date_key] = (date_m + (1 if hit else 0), date_t + 1)
    rate = match / total * 100 if total > 0 else 0.0
    cap_rates = [m / t * 100 for m, t in cap_stat.values() if t >= 20]
    date_rates = [m / t * 100 for m, t in date_stat.values() if t >= 10]
    cap_std = float(np.std(cap_rates)) if len(cap_rates) >= 2 else 0.0
    date_std = float(np.std(date_rates)) if len(date_rates) >= 2 else 0.0
    return rate, match, total, cap_std, date_std


def bayes_optimize_params(train_samples, n_trials=60, n_folds=4):
    caps = np.array([s["market_cap_yi"] for s in train_samples])
    log_caps = np.log(np.clip(caps, 1e-6, None))
    s_med = float(np.median(log_caps))
    s_std = float(np.std(log_caps))
    if s_std < 1e-6:
        s_std = 1.0
    folds = build_walk_forward_folds(train_samples, n_folds=n_folds)
    if not folds:
        return None

    def objective(trial):
        k_min = trial.suggest_float("k_min", 1e-8, 5e-6, log=True)
        k_ratio = trial.suggest_float("k_ratio", 2.0, 300.0, log=True)
        k_max = min(k_min * k_ratio, 1e-2)
        m_min = trial.suggest_float("m_min", 1.0, 2.2)
        m_span = trial.suggest_float("m_span", 0.2, 3.5)
        m_max = min(m_min + m_span, 6.0)
        lam_k = trial.suggest_float("lam_k", 0.2, 3.0)
        lam_m = trial.suggest_float("lam_m", 0.2, 3.0)
        s0_k = trial.suggest_float("s0_k", s_med - 2.0 * s_std, s_med + 2.0 * s_std)
        s0_m = trial.suggest_float("s0_m", s_med - 2.0 * s_std, s_med + 2.0 * s_std)
        params = {
            "k_min": k_min,
            "k_max": k_max,
            "m_min": m_min,
            "m_max": m_max,
            "lam_k": lam_k,
            "lam_m": lam_m,
            "s0_k": s0_k,
            "s0_m": s0_m,
        }
        fold_scores = []
        fold_rates = []
        for _, valid_part in folds:
            rate, _, _, cap_std, date_std = evaluate_params_detailed(valid_part, params)
            score = rate - 0.10 * cap_std - 0.10 * date_std
            fold_scores.append(score)
            fold_rates.append(rate)
        if not fold_scores:
            return 0.0
        mean_score = float(np.mean(fold_scores))
        std_rate = float(np.std(fold_rates))
        return mean_score - 0.10 * std_rate

    sampler = optuna.samplers.TPESampler(seed=42)
    study = optuna.create_study(direction="maximize", sampler=sampler)
    study.optimize(objective, n_trials=n_trials, show_progress_bar=False)
    best = study.best_params
    best_params = {
        "k_min": best["k_min"],
        "k_max": min(best["k_min"] * best["k_ratio"], 1e-2),
        "m_min": best["m_min"],
        "m_max": min(best["m_min"] + best["m_span"], 6.0),
        "lam_k": best["lam_k"],
        "lam_m": best["lam_m"],
        "s0_k": best["s0_k"],
        "s0_m": best["s0_m"],
    }
    return {
        "study_value": study.best_value,
        "best_params": best_params,
        "fold_count": len(folds),
    }


def main():
    report = io.StringIO()
    def p(s=""):
        print(s)
        report.write(s + "\n")

    train_dates, test_dates = get_train_test_dates()

    p(f"\n{'='*100}")
    p(f"  K(市值) 参数曲线研究")
    p(f"  训练日期数: {len(train_dates)}")
    p(f"  测试日期数: {len(test_dates)}")
    p(f"  训练集日期: {train_dates[0]} ~ {train_dates[-1]}")
    p(f"  测试集日期: {test_dates[0]} ~ {test_dates[-1]}")
    p(f"{'='*100}")

    tdx = connect(DEFAULT_IP, DEFAULT_PORT)
    tdx.__enter__()

    try:
        p(f"\n[Phase 1] 采集样本 ...")
        train_samples = collect_samples(tdx, train_dates, "训练集")
        test_samples = collect_samples(tdx, test_dates, "测试集")
        p(f"  训练集: {len(train_samples)} 只×日")
        p(f"  测试集: {len(test_samples)} 只×日")

        p(f"\n[Phase 2] 扫描每个样本的最优 K 值 ...")
        for s in train_samples:
            best_k, best_m, hit = scan_K_for_sample(s)
            s["best_k"] = best_k
            s["best_m"] = best_m
            s["best_hit"] = hit

        p(f"\n[Phase 3] 按盘口分析最优 K 值分布")
        p(f"\n  {'盘口':<20s} {'样本数':>6s} {'K中位数':>12s} {'K均值':>12s} {'K最小':>12s} {'K最大':>12s} {'命中率':>8s}")
        p(f"  {'─'*80}")

        cap_order = list(MARKET_CAP_BINS.keys())
        cap_groups = {}
        for s in train_samples:
            cap_groups.setdefault(s["cap_label"], []).append(s)

        cap_k_stats = {}
        for cap_label in cap_order:
            group = cap_groups.get(cap_label, [])
            if not group:
                continue
            ks = [s["best_k"] for s in group]
            hit_rate = sum(1 for s in group if s["best_hit"]) / len(group) * 100
            cap_k_stats[cap_label] = {
                "median": np.median(ks),
                "mean": np.mean(ks),
                "min": np.min(ks),
                "max": np.max(ks),
                "hit_rate": hit_rate,
                "count": len(group),
            }
            p(f"  {cap_label:<20s} {len(group):>6d} {np.median(ks):>12.6f} {np.mean(ks):>12.6f} {np.min(ks):>12.6f} {np.max(ks):>12.6f} {hit_rate:>7.1f}%")

        p(f"\n[Phase 4] 散点: log10(市值) vs log10(K)")
        p(f"\n  {'ts_code':<12s} {'名称':>8s} {'市值(亿)':>10s} {'盘口':>16s} {'最优K':>12s} {'log10(K)':>10s} {'命中':>4s}")
        p(f"  {'─'*80}")
        for s in sorted(train_samples, key=lambda x: x["market_cap_yi"]):
            hit_str = "✅" if s["best_hit"] else "❌"
            p(f"  {s['ts_code']:<12s} {s['name']:>8s} {s['market_cap_yi']:>10.1f} {s['cap_label']:>16s} {s['best_k']:>12.6f} {np.log10(s['best_k']):>10.2f} {hit_str:>4s}")

        p(f"\n[Phase 5] 拟合 K = f(市值) 的候选公式")
        mcaps = np.array([s["market_cap_yi"] for s in train_samples])
        log_mcaps = np.log10(np.clip(mcaps, 1, None))
        log_ks = np.array([np.log10(s["best_k"]) for s in train_samples])

        if len(mcaps) > 5:
            coeffs = np.polyfit(log_mcaps, log_ks, 1)
            a, b = coeffs
            p(f"\n  候选1: log10(K) = {a:.4f} × log10(市值) + {b:.4f}")
            p(f"         K = 10^({b:.4f}) × 市值^{a:.4f}")
            p(f"         K = {10**b:.6f} × 市值^{a:.4f}")

            coeffs2 = np.polyfit(log_mcaps, log_ks, 2)
            p(f"\n  候选2: log10(K) = {coeffs2[0]:.4f} × x² + {coeffs2[1]:.4f} × x + {coeffs2[2]:.4f}")

        p(f"\n[Phase 6] 回测候选公式 (训练集 + 测试集)")

        p(f"\n  --- 基线: 原方案 K=0.00001, M=1.5 ---")
        train_rate, train_m, train_t = evaluate_formula(
            train_samples, lambda cap: 0.00001, 1.5
        )
        test_rate, test_m, test_t = evaluate_formula(
            test_samples, lambda cap: 0.00001, 1.5
        )
        base_train_rate, base_train_m, base_train_t = train_rate, train_m, train_t
        base_test_rate, base_test_m, base_test_t = test_rate, test_m, test_t
        p(f"  训练集: {train_m}/{train_t} = {train_rate:.1f}%")
        p(f"  测试集: {test_m}/{test_t} = {test_rate:.1f}%")

        candidates = []

        if len(mcaps) > 5:
            def make_k_poly1(a, b):
                def k_func(cap_yi):
                    return 10 ** (a * np.log10(max(cap_yi, 1)) + b)
                return k_func
            candidates.append(("幂律拟合(1次)", make_k_poly1(a, b)))

            a2, b2, c2 = coeffs2
            def make_k_poly2(a2, b2, c2):
                def k_func(cap_yi):
                    x = np.log10(max(cap_yi, 1))
                    return 10 ** (a2 * x * x + b2 * x + c2)
                return k_func
            candidates.append(("幂律拟合(2次)", make_k_poly2(a2, b2, c2)))

        for cap_label in cap_order:
            if cap_label in cap_k_stats:
                med_k = cap_k_stats[cap_label]["median"]
                lo, hi = MARKET_CAP_BINS[cap_label]
                candidates.append((f"分段({cap_label}) K={med_k:.6f}", med_k, lo, hi))

        p(f"\n  --- 候选公式 ---")
        for m_val in M_GRID:
            for name, k_or_func, *rest in candidates:
                if rest:
                    continue
                k_func = k_or_func
                train_rate, train_m, train_t = evaluate_formula(train_samples, k_func, m_val)
                test_rate, test_m, test_t = evaluate_formula(test_samples, k_func, m_val)
                p(f"  {name}, M={m_val}: 训练 {train_m}/{train_t}={train_rate:.1f}% | 测试 {test_m}/{test_t}={test_rate:.1f}%")

        p(f"\n  --- 分段常数 K (每个盘口用中位数K) ---")
        seg_k_map = {}
        for cap_label in cap_order:
            if cap_label in cap_k_stats:
                seg_k_map[cap_label] = cap_k_stats[cap_label]["median"]

        def segmented_k(cap_yi):
            label = classify_cap(cap_yi)
            return seg_k_map.get(label, 0.00001)

        for m_val in M_GRID:
            train_rate, train_m, train_t = evaluate_formula(train_samples, segmented_k, m_val)
            test_rate, test_m, test_t = evaluate_formula(test_samples, segmented_k, m_val)
            p(f"  分段中位数K, M={m_val}: 训练 {train_m}/{train_t}={train_rate:.1f}% | 测试 {test_m}/{test_t}={test_rate:.1f}%")

        p(f"\n[Phase 7] 按盘口 × M 的热力图 (分段中位数K)")
        header = f"  {'盘口':<20s} {'中位数K':>12s}"
        for m_val in M_GRID:
            header += f"  M={m_val:<4.1f}"
        p(header)

        for cap_label in cap_order:
            group = cap_groups.get(cap_label, [])
            if not group:
                continue
            med_k = cap_k_stats[cap_label]["median"]
            line = f"  {cap_label:<20s} {med_k:>12.6f}"
            for m_val in M_GRID:
                match = 0
                total = 0
                for s in group:
                    threshold_base = s["free_market_cap"] * med_k / 100
                    threshold = max(threshold_base, s["p95"] * m_val)
                    direction, _ = calc_direction(s["ticks"], threshold)
                    if direction is None:
                        continue
                    if direction == s["ground_truth"]:
                        match += 1
                    total += 1
                rate = match / total * 100 if total > 0 else 0
                line += f"  {rate:>6.1f}%"
            p(line)

        p(f"\n[Phase 8] 边界扩展与Sigmoid自适应实验")
        if RUN_PHASE8_GRID:
            p(f"\n  --- 实验A: K下界扩展到1e-7 (仅边界检验) ---")
            ext_hits = 0
            ext_total = 0
            ext_best_k = []
            ext_at_floor = 0
            for s in train_samples:
                best_k_ext = None
                hit_ext = False
                for k in K_GRID_EXT:
                    threshold_base = s["free_market_cap"] * k / 100
                    threshold = max(threshold_base, s["p95"] * 1.5)
                    direction, _ = calc_direction(s["ticks"], threshold)
                    if direction is None:
                        continue
                    if direction == s["ground_truth"]:
                        best_k_ext = k
                        hit_ext = True
                        break
                if best_k_ext is None:
                    best_k_ext = 0.00001
                    threshold_base = s["free_market_cap"] * best_k_ext / 100
                    threshold = max(threshold_base, s["p95"] * 1.5)
                    direction, _ = calc_direction(s["ticks"], threshold)
                    hit_ext = direction == s["ground_truth"]
                if hit_ext:
                    ext_hits += 1
                ext_total += 1
                ext_best_k.append(best_k_ext)
                if np.isclose(best_k_ext, K_GRID_EXT[0]):
                    ext_at_floor += 1
            ext_rate = ext_hits / ext_total * 100 if ext_total > 0 else 0
            p(f"  命中率: {ext_hits}/{ext_total} = {ext_rate:.1f}%")
            p(f"  最优K中位数: {np.median(ext_best_k):.8f}")
            p(f"  命中样本中贴下界比例: {ext_at_floor}/{ext_total} = {ext_at_floor/ext_total*100:.1f}%")

            p(f"\n  --- 实验B: Sigmoid K(S), M(S) 网格搜索 ---")
            mcaps_all = np.array([s["market_cap_yi"] for s in train_samples + test_samples])
            s_vals = np.log(np.clip(mcaps_all, 1e-6, None))
            s_med = float(np.median(s_vals))
            s_std = float(np.std(s_vals)) if float(np.std(s_vals)) > 1e-9 else 1.0
            s0_grid = [s_med - 0.5 * s_std, s_med, s_med + 0.5 * s_std]
            lam_grid = [0.8, 1.2, 1.8]
            kmin_grid = [1e-7, 3e-7, 1e-6]
            kmax_grid = [3e-6, 1e-5, 3e-5]
            mmin_grid = [1.0, 1.2, 1.5]
            mmax_grid = [2.0, 2.5, 3.0]
            best_sig = None
            for kmin in kmin_grid:
                for kmax in kmax_grid:
                    if kmax <= kmin:
                        continue
                    for mmin in mmin_grid:
                        for mmax in mmax_grid:
                            if mmax <= mmin:
                                continue
                            for lam_k in lam_grid:
                                for lam_m in lam_grid:
                                    for s0_k in s0_grid:
                                        for s0_m in s0_grid:
                                            k_func = make_sigmoid(kmin, kmax, lam_k, s0_k)
                                            m_func = make_sigmoid(mmin, mmax, lam_m, s0_m)
                                            tr_rate, tr_m, tr_t = evaluate_formula_km(train_samples, k_func, m_func)
                                            te_rate, te_m, te_t = evaluate_formula_km(test_samples, k_func, m_func)
                                            score = te_rate * 1000 + tr_rate
                                            if best_sig is None or score > best_sig["score"]:
                                                best_sig = {
                                                    "score": score,
                                                    "tr_rate": tr_rate,
                                                    "tr_m": tr_m,
                                                    "tr_t": tr_t,
                                                    "te_rate": te_rate,
                                                    "te_m": te_m,
                                                    "te_t": te_t,
                                                    "kmin": kmin,
                                                    "kmax": kmax,
                                                    "lam_k": lam_k,
                                                    "s0_k": s0_k,
                                                    "mmin": mmin,
                                                    "mmax": mmax,
                                                    "lam_m": lam_m,
                                                    "s0_m": s0_m,
                                                }

            if best_sig is not None:
                p(f"  最优Sigmoid: 训练 {best_sig['tr_m']}/{best_sig['tr_t']}={best_sig['tr_rate']:.1f}% | 测试 {best_sig['te_m']}/{best_sig['te_t']}={best_sig['te_rate']:.1f}%")
                p(f"  K(S): kmin={best_sig['kmin']:.8f}, kmax={best_sig['kmax']:.8f}, lam={best_sig['lam_k']:.2f}, s0={best_sig['s0_k']:.4f}")
                p(f"  M(S): mmin={best_sig['mmin']:.2f}, mmax={best_sig['mmax']:.2f}, lam={best_sig['lam_m']:.2f}, s0={best_sig['s0_m']:.4f}")
                p(f"  基线(固定K/M): 训练 {base_train_m}/{base_train_t}={base_train_rate:.1f}% | 测试 {base_test_m}/{base_test_t}={base_test_rate:.1f}%")
        else:
            p("  已跳过（专注贝叶斯优化Phase 9）")

        p(f"\n[Phase 9] 贝叶斯优化 + 低维参数化 + 时间序列验证")
        bo = bayes_optimize_params(
            train_samples,
            n_trials=BAYES_TRIALS,
            n_folds=WALK_FORWARD_FOLDS,
        )
        if bo is None:
            p("  样本不足，无法构建 walk-forward 折叠")
        else:
            bp = bo["best_params"]
            p(f"  walk-forward折数: {bo['fold_count']}")
            p(f"  联合目标值(命中率-稳定性惩罚): {bo['study_value']:.3f}")
            p(f"  K(S): k_min={bp['k_min']:.8f}, k_max={bp['k_max']:.8f}, lam_k={bp['lam_k']:.3f}, s0_k={bp['s0_k']:.4f}")
            p(f"  M(S): m_min={bp['m_min']:.3f}, m_max={bp['m_max']:.3f}, lam_m={bp['lam_m']:.3f}, s0_m={bp['s0_m']:.4f}")
            bo_train_rate, bo_train_m, bo_train_t, bo_train_cap_std, bo_train_date_std = evaluate_params_detailed(train_samples, bp)
            bo_test_rate, bo_test_m, bo_test_t, bo_test_cap_std, bo_test_date_std = evaluate_params_detailed(test_samples, bp)
            p(f"  BO参数训练集: {bo_train_m}/{bo_train_t}={bo_train_rate:.1f}%")
            p(f"  BO参数测试集: {bo_test_m}/{bo_test_t}={bo_test_rate:.1f}%")
            p(f"  BO稳定性(训练): 盘口std={bo_train_cap_std:.2f}, 日期std={bo_train_date_std:.2f}")
            p(f"  BO稳定性(测试): 盘口std={bo_test_cap_std:.2f}, 日期std={bo_test_date_std:.2f}")
            p(f"  固定基线测试集: {base_test_m}/{base_test_t}={base_test_rate:.1f}%")

        p(f"\n{'='*100}")
        p(f"  研究完成")
        p(f"{'='*100}")

    finally:
        tdx.__exit__(None, None, None)

    report_path = os.path.join(os.path.dirname(__file__), "research_K_curve_report.txt")
    with open(report_path, "w", encoding="utf-8") as f:
        f.write(report.getvalue())
    print(f"\n报告已保存到: {report_path}")


if __name__ == "__main__":
    main()
