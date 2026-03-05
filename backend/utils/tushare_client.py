import argparse
import os
import time

import tushare as ts


def _wrap_query_with_failover(pro, urls, failover_on_empty: bool):
    orig_query = pro.query

    def query(api_name, fields="", **kwargs):
        url = str(urls[0]) if urls else ""
        if not url:
            raise RuntimeError(f"tushare_request_failed api={api_name} url=EMPTY")

        try:
            max_retries = int(os.getenv("TUSHARE_RETRY_MAX", "3"))
        except Exception:
            max_retries = 3
        max_retries = max(1, max_retries)

        try:
            base_sleep_s = float(os.getenv("TUSHARE_RETRY_BASE_S", "0.8"))
        except Exception:
            base_sleep_s = 0.8
        base_sleep_s = max(0.0, base_sleep_s)

        last_err = None
        for i in range(max_retries):
            try:
                pro._DataApi__http_url = url
                res = orig_query(api_name, fields=fields, **kwargs)
                if bool(failover_on_empty) and getattr(res, "empty", False):
                    raise ValueError(f"empty_response url={url} api={api_name}")
                return res
            except Exception as e:
                last_err = e
                if i < max_retries - 1 and base_sleep_s > 0:
                    time.sleep(base_sleep_s * (2**i))
                continue
        raise RuntimeError(f"tushare_request_failed api={api_name} url={url} retries={max_retries} err={type(last_err).__name__}:{last_err}")

    pro.query = query


try:
    try:
        from dotenv import load_dotenv

        load_dotenv(override=False)
    except Exception:
        pass

    token = os.getenv("TUSHARE_TOKEN", "f5187841c7d5663c97cd3a4125214b8fa7f7866fa32fb2ea93e9bebfebba")
    ts.set_token(token)
    pro = ts.pro_api(token)
    pro._DataApi__token = token

    primary_url = os.getenv("TUSHARE_HTTP_URL", "http://lianghua.nanyangqiankun.top")
    backup_url = os.getenv("TUSHARE_OFFICIAL_HTTP_URL", "http://api.waditu.com")
    pro._DataApi__http_url = primary_url
    try:
        pro._DataApi__timeout = float(os.getenv("TUSHARE_TIMEOUT", "30"))
    except Exception:
        pass
    failover_on_empty = str(os.getenv("TUSHARE_FAILOVER_ON_EMPTY", "0")).strip() in ("1", "true", "True", "yes", "YES")
    _wrap_query_with_failover(pro, [primary_url], failover_on_empty=failover_on_empty)
    
    print("Tushare client initialized successfully with custom config.")
except Exception as e:
    print(f"Warning: Tushare initialization failed: {e}")
    pro = None


def test_tushare_client():
    if pro is None:
        raise ValueError("Tushare client is not initialized.")
    try:
        # Test a simple API call to verify the client is working
        df = pro.daily(ts_code="000001.SZ", start_date="20230101", end_date="20230103")
        if df.empty:
            raise ValueError("Tushare API call failed. Received empty DataFrame.")
        print("Tushare client test passed.",df)
    except Exception as e:
        raise ValueError(f"Tushare client test failed: {e}")

def test_tushare_rate_limit(max_requests: int = 200, sleep: float = 0.0):
    if pro is None:
        raise ValueError("Tushare client is not initialized.")
    ok = 0
    failed = 0
    first_error = ""
    t0 = time.perf_counter()
    for i in range(int(max_requests)):
        try:
            df = pro.daily(ts_code="000001.SZ", start_date="20230101", end_date="20230103")
            print(f"Request {i}: {df}")
            if df is None or df.empty:
                failed += 1
                first_error = "empty_response"
                break
            ok += 1
        except Exception as e:
            failed += 1
            first_error = str(e)
            break
        if sleep and sleep > 0:
            time.sleep(float(sleep))
    cost = time.perf_counter() - t0
    qps = ok / cost if cost > 0 else 0.0
    print(
        f"rate_test_done ok={ok} failed={failed} elapsed={cost:.3f}s qps={qps:.2f} first_error={first_error}"
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", type=str, default=os.getenv("MODE", "test"))
    parser.add_argument("--max-requests", type=int, default=int(os.getenv("MAX_REQUESTS", "200")))
    parser.add_argument("--sleep", type=float, default=float(os.getenv("SLEEP", "0.0")))
    args = parser.parse_args()

    if str(args.mode).lower() == "rate":
        test_tushare_rate_limit(max_requests=args.max_requests, sleep=args.sleep)
    else:
        test_tushare_client()
