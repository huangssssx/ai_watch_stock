import os
import sys
from typing import Optional
import pandas as pd

backend_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if backend_dir not in sys.path:
    sys.path.insert(0, backend_dir)

from utils.pytdx_client import tdx

def is_a_share_stock(market: int, code: str) -> bool:
    code = str(code or "").zfill(6)
    if market == 0:
        return code.startswith(("000", "001", "002", "003", "300", "301"))
    if market == 1:
        return code.startswith(("600", "601", "603", "605", "688"))
    return False

# 获取全市场code
def iter_all_a_share_codes():
    for market in (0, 1):  # 0=深, 1=沪
        total = tdx.get_security_count(market)
        step = 1000  # 常见每页1000
        for start in range(0, total, step):
            rows = tdx.get_security_list(market, start) or []
            for r in rows:
                code = str(r.get("code", "")).zfill(6)
                if code and is_a_share_stock(market, code):
                    yield (market, code)


df_stock_codes = pd.DataFrame(iter_all_a_share_codes(), columns=["market", "code"])
df_cut = df_stock_codes.head(3)
# print(df)

# 保存到csv
# df_stock_codes.to_csv("all_a_share_codes.csv", index=False)


# ((close - open) / ((high - low) + .001))
# data = tdx.get_security_bars(9, 0, "000001", 0, 1)
# df = tdx.to_df(data)
# print(df)

# 获取股票实时报价
# data = tdx.get_security_quotes(df_cut[["market", "code"]].values.tolist())
# df = tdx.to_df(data)
# print(df)
# print(df.columns

# 将df_stock_codes划分为 80 一组的DataFrame
df_groups = [df_stock_codes[i:i+80] for i in range(0, len(df_stock_codes), 80)]
print(len(df_groups))

max_groups: Optional[int] = None
groups_to_fetch = df_groups if max_groups is None else df_groups[:max_groups]

def fetch_quotes(req):
    if not req:
        return ([], [])
    pending = [req]
    ok = []
    failed = []
    with tdx:
        while pending:
            sub = pending.pop()
            if len(sub) > 80:
                mid = len(sub) // 2
                pending.append(sub[mid:])
                pending.append(sub[:mid])
                continue
            ret = tdx.get_security_quotes(sub)
            if ret is None or len(ret) != len(sub):
                if len(sub) == 1:
                    failed.append(sub[0])
                    continue
                mid = len(sub) // 2
                pending.append(sub[mid:])
                pending.append(sub[:mid])
                continue
            ok.extend(ret)
    return (ok, failed)


all_quotes = []
failed_quotes = []
for g in groups_to_fetch:
    req = [
        (int(market), str(code).zfill(6))
        for market, code in g[["market", "code"]].itertuples(index=False, name=None)
    ]
    ok, failed = fetch_quotes(req)
    all_quotes.extend(ok)
    failed_quotes.extend(failed)

df_quotes = tdx.to_df(all_quotes) if all_quotes else pd.DataFrame()
print(len(df_quotes))
print(len(failed_quotes))
print(failed_quotes[:20])
