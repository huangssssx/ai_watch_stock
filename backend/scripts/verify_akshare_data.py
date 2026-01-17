import akshare as ak
import pandas as pd
import time
import datetime

def verify_data():
    print(f"=== 开始验证 Akshare 数据接口 (akshare={getattr(ak, '__version__', '?')}) ===")
    
    # 1. 核心指数
    print("\n[1/4] 获取核心指数 (stock_zh_index_spot_em)...")
    try:
        targets = ["上证指数", "深证成指", "创业板指", "科创50"]
        frames = []
        start_time = time.time()
        for sym in ["上证系列指数", "深证系列指数", "指数成份", "核心指数"]:
            try:
                df = ak.stock_zh_index_spot_em(symbol=sym)
                if isinstance(df, pd.DataFrame) and (not df.empty):
                    frames.append(df)
            except Exception:
                continue
        if not frames:
            indices = ak.stock_zh_index_spot_em()
        else:
            indices = pd.concat(frames, ignore_index=True, copy=False)
        indices = indices.drop_duplicates()
        print(f"耗时: {time.time() - start_time:.2f}s")
        if "名称" in indices.columns:
            filtered = indices[indices["名称"].isin(targets)]
            cols = [c for c in ["名称", "最新价", "涨跌幅"] if c in filtered.columns]
            print(filtered[cols] if cols else filtered.head(5))
        else:
            print(indices.head(5))
    except Exception as e:
        print(f"FAILED: {e}")

    # 2. 北向资金
    print("\n[2/4] 获取北向资金 (stock_hsgt_fund_flow_summary_em)...")
    try:
        start_time = time.time()
        north = ak.stock_hsgt_fund_flow_summary_em()
        print(f"耗时: {time.time() - start_time:.2f}s")
        print(north) 
    except Exception as e:
        print(f"FAILED: {e}")
        # Try min flow as fallback
        try:
            print("尝试 stock_hsgt_fund_min_em...")
            north_min = ak.stock_hsgt_fund_min_em(symbol="北向资金")
            print(north_min.tail())
        except Exception as e2:
            print(f"Fallback FAILED: {e2}")

    # 3. 行业板块
    print("\n[3/4] 获取行业板块 (stock_board_industry_name_em)...")
    try:
        start_time = time.time()
        sectors = ak.stock_board_industry_name_em()
        print(f"耗时: {time.time() - start_time:.2f}s")
        print(f"获取到 {len(sectors)} 个板块")
        print(sectors[['板块名称', '最新价', '涨跌幅']].head())
    except Exception as e:
        print(f"FAILED: {e}")

    # 4. 市场概况 (涨跌分布)
    print("\n[4/4] 获取市场概况 (stock_sse_summary / stock_szse_summary)...")
    try:
        start_time = time.time()
        sse = ak.stock_sse_summary()
        szse = ak.stock_szse_summary()
        print(f"耗时: {time.time() - start_time:.2f}s")
        print("上证概况:", sse.iloc[0]['股票'] if not sse.empty else "Empty")
        print("深证概况:", szse.iloc[0]['数量'] if not szse.empty else "Empty")
    except Exception as e:
        print(f"FAILED: {e}")
        
    # 5. 个股核心快照
    print("\n[5/8] 获取个股快照 (stock_zh_a_spot_em)...")
    try:
        start_time = time.time()
        spot = ak.stock_zh_a_spot_em()
        print(f"耗时: {time.time() - start_time:.2f}s")
        required = ["代码", "名称", "最新价", "涨跌幅", "换手率", "量比", "成交量", "成交额"]
        missing = [c for c in required if c not in spot.columns]
        print("列检查:", "OK" if not missing else f"缺失 {missing}")
        if not spot.empty:
            print(spot[["代码", "名称", "最新价", "涨跌幅"]].head(3).to_string(index=False))
    except Exception as e:
        print(f"FAILED: {e}")

    # 6. 龙虎榜明细（字段变更预警）
    print("\n[6/8] 获取龙虎榜明细 (stock_lhb_detail_em)...")
    try:
        now = datetime.datetime.now()
        end_date = now.strftime("%Y%m%d")
        start_date = (now - datetime.timedelta(days=60)).strftime("%Y%m%d")
        start_time = time.time()
        lhb = ak.stock_lhb_detail_em(start_date=start_date, end_date=end_date)
        print(f"耗时: {time.time() - start_time:.2f}s")
        if lhb is None or lhb.empty:
            print("返回为空：可能非交易期或接口异常")
        else:
            key_candidates = ["上榜日", "上榜日期", "代码", "证券代码", "龙虎榜净买额", "净买额占总成交比", "上榜原因"]
            exists = [c for c in key_candidates if c in lhb.columns]
            print("关键列存在:", exists)
            print("列数:", len(lhb.columns), "行数:", len(lhb))
    except Exception as e:
        print(f"FAILED: {e}")

    # 7. 两融明细（字段变更预警）
    print("\n[7/8] 获取两融明细 (stock_margin_detail_sse / stock_margin_detail_szse)...")
    try:
        found_any = False
        for fn_name in ["stock_margin_detail_sse", "stock_margin_detail_szse"]:
            fn = getattr(ak, fn_name, None)
            if fn is None:
                print(f"{fn_name}: 不存在")
                continue
            df = None
            used = None
            for i in range(10):
                d = (datetime.datetime.now() - datetime.timedelta(days=i)).strftime("%Y%m%d")
                try:
                    tmp = fn(date=d)
                except Exception:
                    tmp = pd.DataFrame()
                if tmp is not None and not tmp.empty:
                    df = tmp
                    used = d
                    break
            if df is None or df.empty:
                print(f"{fn_name}: 返回为空")
                continue
            found_any = True
            cols = df.columns.tolist()
            code_cols = [c for c in ["证券代码", "标的证券代码", "代码"] if c in cols]
            print(f"{fn_name}({used}) 列数={len(cols)}，代码列候选={code_cols}")
        if not found_any:
            print("两融明细：未取到任何有效数据")
    except Exception as e:
        print(f"FAILED: {e}")

    # 8. 分笔成交（大单追踪依赖）
    print("\n[8/8] 获取分笔成交 (stock_intraday_em)...")
    try:
        start_time = time.time()
        ticks = ak.stock_intraday_em(symbol="000001")
        print(f"耗时: {time.time() - start_time:.2f}s")
        required = ["时间", "成交价", "手数", "买卖盘性质"]
        missing = [c for c in required if c not in ticks.columns]
        print("列检查:", "OK" if not missing else f"缺失 {missing}")
        if ticks is not None and not ticks.empty:
            print(ticks.head(3).to_string(index=False))
    except Exception as e:
        print(f"FAILED: {e}")

    print("\n=== 验证结束 ===")

if __name__ == "__main__":
    verify_data()
