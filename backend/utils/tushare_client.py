import argparse
import os
import time

import tushare as ts

try:
    # Initialize with a dummy token initially if needed, but we override it below
    ts.set_token('f5187841c7d5663c97cd3a4125214b8fa7f7866fa32fb2ea93e9bebfebba')
    pro = ts.pro_api('此处不用改')
    
    # Configure with the specific token and URL provided by the user
    pro._DataApi__token = 'f5187841c7d5663c97cd3a4125214b8fa7f7866fa32fb2ea93e9bebfebba'
    pro._DataApi__http_url = 'http://lianghua.nanyangqiankun.top'
    
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
