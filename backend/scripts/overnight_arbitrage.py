import pandas as pd
import argparse
import time
import datetime
import sys
import os

# Add project root to path to allow importing utils if needed
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.tushare_client import ts, pro

class MarketData:
    def __init__(self):
        self._stock_basic = None
        self._sector_map = None
        self._last_trade_date = None
        self._prev_trade_date = None

    def _ensure_pro(self):
        if pro is None:
            print("Tushare 未初始化，无法执行")
            return False
        return True

    def _get_trade_dates(self):
        if self._last_trade_date and self._prev_trade_date:
            return self._last_trade_date, self._prev_trade_date
        if not self._ensure_pro():
            return None, None
        end_date = datetime.datetime.now().strftime("%Y%m%d")
        start_date = (datetime.datetime.now() - datetime.timedelta(days=30)).strftime("%Y%m%d")
        cal = pro.trade_cal(start_date=start_date, end_date=end_date, is_open=1)
        if cal is None or cal.empty:
            return None, None
        dates = cal["cal_date"].tolist()
        dates.sort()
        if len(dates) == 1:
            self._last_trade_date = dates[-1]
            self._prev_trade_date = dates[-1]
        else:
            self._last_trade_date = dates[-1]
            self._prev_trade_date = dates[-2]
        print(f"交易日期已锁定: 最新[{self._last_trade_date}], 上一[{self._prev_trade_date}]")
        return self._last_trade_date, self._prev_trade_date

    def _load_stock_basic(self):
        if self._stock_basic is not None:
            return self._stock_basic
        if not self._ensure_pro():
            self._stock_basic = pd.DataFrame()
            return self._stock_basic
        df = pro.stock_basic(exchange="", list_status="L", fields="ts_code,name")
        if df is None:
            df = pd.DataFrame()
        self._stock_basic = df
        return self._stock_basic

    def _ts_code_from_symbol(self, symbol):
        df = self._load_stock_basic()
        if df.empty:
            return None
        code = str(symbol).zfill(6)
        row = df[df["ts_code"].str.startswith(code)]
        if row.empty:
            return None
        return row.iloc[0]["ts_code"]

    def get_spot_data(self):
        print("正在获取 Tushare 日行情 + 指标数据...")
        if not self._ensure_pro():
            return pd.DataFrame()
        trade_date, _ = self._get_trade_dates()
        if trade_date is None:
            return pd.DataFrame()
        daily = pro.daily(trade_date=trade_date, fields="ts_code,close,pct_chg,amount")
        basic = pro.daily_basic(trade_date=trade_date, fields="ts_code,turnover_rate,circ_mv")
        if daily is None or basic is None or daily.empty or basic.empty:
            return pd.DataFrame()
        df = pd.merge(daily, basic, on="ts_code", how="inner")
        basics = self._load_stock_basic()
        if basics is not None and not basics.empty:
            df = pd.merge(df, basics, on="ts_code", how="left")
        df["代码"] = df["ts_code"].str.slice(0, 6)
        df["名称"] = df.get("name", df["ts_code"])
        df["最新价"] = df["close"]
        df["涨跌幅"] = df["pct_chg"]
        df["成交额"] = df["amount"] * 1000
        df["换手率"] = df["turnover_rate"]
        df["流通市值"] = df["circ_mv"] * 10000
        return df[["代码", "名称", "最新价", "涨跌幅", "成交额", "换手率", "流通市值", "ts_code"]]

    def get_flow_data(self):
        print("正在获取 Tushare 资金流数据...")
        if not self._ensure_pro():
            return pd.DataFrame()
        trade_date, _ = self._get_trade_dates()
        if trade_date is None:
            return pd.DataFrame()
        df = pro.moneyflow(
            trade_date=trade_date,
            fields="ts_code,buy_sm_amount,buy_md_amount,buy_lg_amount,buy_elg_amount,sell_sm_amount,sell_md_amount,sell_lg_amount,sell_elg_amount"
        )
        if df is None or df.empty:
            print(f"警告: 无法获取 {trade_date} 的资金流数据 (可能尚未更新，通常在17:00-18:00后可用)")
            return pd.DataFrame()
        df["主力净流入-净额"] = (df["buy_lg_amount"] + df["buy_elg_amount"] - df["sell_lg_amount"] - df["sell_elg_amount"]) * 10000
        df["小单净流入-净额"] = (df["buy_sm_amount"] - df["sell_sm_amount"]) * 10000
        df["代码"] = df["ts_code"].str.slice(0, 6)
        return df[["代码", "ts_code", "主力净流入-净额", "小单净流入-净额"]]

    def get_sector_list(self):
        print("正在获取申万一级行业板块...")
        if not self._ensure_pro():
            return pd.DataFrame()
        trade_date, _ = self._get_trade_dates()
        if trade_date is None:
            return pd.DataFrame()
        sectors = pro.index_classify(level="L1", src="SW", fields="index_code,industry_name")
        if sectors is None or sectors.empty:
            return pd.DataFrame()
        results = []
        for _, row in sectors.iterrows():
            index_code = row["index_code"]
            name = row["industry_name"]
            time.sleep(0.1)
            df = pro.index_daily(ts_code=index_code, trade_date=trade_date, fields="ts_code,pct_chg")
            if df is None or df.empty:
                continue
            pct_chg = df.iloc[0]["pct_chg"]
            results.append({"板块名称": name, "涨跌幅": pct_chg, "index_code": index_code})
        df_res = pd.DataFrame(results)
        self._sector_map = {r["板块名称"]: r["index_code"] for r in results}
        return df_res

    def get_sector_cons(self, sector_name):
        print(f"正在获取板块[{sector_name}]成分股...")
        if not self._ensure_pro():
            return []
        if self._sector_map is None or sector_name not in self._sector_map:
            self.get_sector_list()
        index_code = None if self._sector_map is None else self._sector_map.get(sector_name)
        if not index_code:
            return []
        df = pro.index_member(index_code=index_code, fields="con_code")
        if df is None or df.empty:
            return []
        return df["con_code"].str.slice(0, 6).tolist()

    def get_history_flow_yesterday(self, symbol):
        if not self._ensure_pro():
            return 0
        _, prev_trade_date = self._get_trade_dates()
        if prev_trade_date is None:
            return 0
        ts_code = self._ts_code_from_symbol(symbol)
        if ts_code is None:
            return 0
        df = pro.moneyflow(
            trade_date=prev_trade_date,
            ts_code=ts_code,
            fields="ts_code,buy_sm_amount,buy_md_amount,buy_lg_amount,buy_elg_amount,sell_sm_amount,sell_md_amount,sell_lg_amount,sell_elg_amount"
        )
        if df is None or df.empty:
            return 0
        main_net = (df.iloc[0]["buy_lg_amount"] + df.iloc[0]["buy_elg_amount"] - df.iloc[0]["sell_lg_amount"] - df.iloc[0]["sell_elg_amount"]) * 10000
        return main_net

class Strategy:
    """核心筛选逻辑"""
    
    def __init__(self, target_sectors=None):
        self.target_sectors = target_sectors
        self.data_api = MarketData()

    def prepare_data(self):
        """准备基础数据（合并行情与资金流）"""
        df_spot = self.data_api.get_spot_data()
        df_flow = self.data_api.get_flow_data()
        
        self.flow_data_available = True
        
        if df_spot.empty:
             print(f"错误: 无法获取行情数据 (Date: {self.data_api._last_trade_date})")
             return None, None, None

        # Handle missing flow data (Intraday mode)
        if df_flow.empty:
            print(f"\n{'!'*50}")
            print("警告: 资金流数据缺失，进入【盘中估算模式】")
            print("筛选将仅基于 [量价] 指标，结果精确度可能下降，请结合实盘判断。")
            print(f"{'!'*50}\n")
            self.flow_data_available = False
            # Create dummy flow columns to prevent merge errors
            df_flow = pd.DataFrame({
                "代码": df_spot["代码"],
                "主力净流入-净额": 0,
                "小单净流入-净额": 0
            })
        
        # Merge
        # df_spot: 代码, ...
        # df_flow: 代码, ...
        # Ensure code type consistency
        df_spot['代码'] = df_spot['代码'].astype(str)
        df_flow['代码'] = df_flow['代码'].astype(str)
        
        df_merged = pd.merge(df_spot, df_flow[['代码', '主力净流入-净额', '小单净流入-净额']], on='代码', how='left') # Use left join
        
        if self.flow_data_available:
            df_merged["今日主力净流入-净额"] = df_merged["主力净流入-净额"]
            df_merged["今日小单净流入-净额"] = df_merged["小单净流入-净额"]
            df_merged["今日主力净流入-净占比"] = df_merged["今日主力净流入-净额"] / df_merged["成交额"].replace(0, pd.NA) * 100
            df_merged["今日小单净流入-净占比"] = df_merged["今日小单净流入-净额"] / df_merged["成交额"].replace(0, pd.NA) * 100
            df_merged = df_merged.dropna(subset=["今日主力净流入-净占比", "今日小单净流入-净占比"])
        else:
            # Dummy values
            df_merged["今日主力净流入-净额"] = 0
            df_merged["今日小单净流入-净额"] = 0
            df_merged["今日主力净流入-净占比"] = 0
            df_merged["今日小单净流入-净占比"] = 0
        
        # Base Filter
        # 1. Non-ST
        name_series = df_merged['名称'].fillna("").astype(str)
        df_merged = df_merged[~name_series.str.contains('ST')]
        df_merged = df_merged[~name_series.str.contains('退')]
        
        # 2. Liquidity
        # 成交额 > 5000万
        df_merged = df_merged[df_merged['成交额'] > 50000000]
        # 换手率 > 2%
        df_merged = df_merged[df_merged['换手率'] > 2]
        
        # 3. Sector Filter (if applied)
        if self.target_sectors:
            valid_codes = set()
            print(f"正在筛选指定板块: {self.target_sectors}")
            for sector in self.target_sectors:
                codes = self.data_api.get_sector_cons(sector)
                valid_codes.update(codes)
            
            df_merged = df_merged[df_merged['代码'].isin(valid_codes)]
            print(f"板块筛选后剩余标的数: {len(df_merged)}")

        return df_merged, df_spot, df_flow

    def run_dimension_a(self, df):
        """维度 A：机构强力锁仓"""
        print("\n正在执行 [维度 A：机构强力锁仓] 筛选...")
        candidates = []
        
        for _, row in df.iterrows():
            mkt_cap = row['流通市值'] # Unit: 元
            main_ratio = row['今日主力净流入-净占比'] # Unit: % (e.g. 5.0)
            small_ratio = row['今日小单净流入-净占比'] # Unit: %
            
            is_match = False
            reason = ""
            
            # 大盘 (>500亿)
            if mkt_cap > 500 * 10000 * 10000:
                if main_ratio > 3.5 and small_ratio < -2:
                    is_match = True
                    reason = f"大盘锁仓(市值{mkt_cap/1e8:.1f}亿): 主力{main_ratio}%, 小单{small_ratio}%"
            # 中盘 (50-500亿)
            elif mkt_cap > 50 * 10000 * 10000:
                if main_ratio > 5 and small_ratio < -3:
                    is_match = True
                    reason = f"中盘锁仓(市值{mkt_cap/1e8:.1f}亿): 主力{main_ratio}%, 小单{small_ratio}%"
            # 小盘 (<50亿)
            else:
                if main_ratio > 6.5 and small_ratio < -4:
                    is_match = True
                    reason = f"小盘锁仓(市值{mkt_cap/1e8:.1f}亿): 主力{main_ratio}%, 小单{small_ratio}%"
            
            if is_match:
                row_dict = row.to_dict()
                row_dict['筛选维度'] = 'A:机构锁仓'
                row_dict['入选理由'] = reason
                candidates.append(row_dict)
                
        return pd.DataFrame(candidates)

    def run_dimension_b(self, df):
        """维度 B：低位弹性起爆"""
        print("\n正在执行 [维度 B：低位弹性起爆] 筛选...")
        candidates = []
        
        # Filter Today
        if self.flow_data_available:
            mask = (df['涨跌幅'] < 3) & (df['今日主力净流入-净额'] > 5000000)
        else:
            # Intraday: Price < 4%, Turnover > 3% (Active)
            mask = (df['涨跌幅'] > 1) & (df['涨跌幅'] < 4) & (df['换手率'] > 3)
            
        potential = df[mask]
        
        print(f"维度B初筛符合条件: {len(potential)} 只，正在进行历史校验...")
        
        count = 0
        for _, row in potential.iterrows():
            # Check Yesterday
            time.sleep(0.1) 
            yesterday_flow = self.data_api.get_history_flow_yesterday(row['代码'])
            
            is_match = False
            reason = ""
            
            if yesterday_flow < -5000000:
                if self.flow_data_available:
                     is_match = True
                     reason = f"涨幅{row['涨跌幅']}%, 主力流入{row['今日主力净流入-净额']/1e4:.0f}万, 昨日流出{yesterday_flow/1e4:.0f}万"
                else:
                     # Intraday fallback: Only check yesterday flow (which is available) + Today's price action
                     is_match = True
                     reason = f"[盘中估算] 涨幅{row['涨跌幅']}%, 换手{row['换手率']}%, 昨日流出{yesterday_flow/1e4:.0f}万 (低位承接)"

            if is_match:
                row_dict = row.to_dict()
                row_dict['筛选维度'] = 'B:低位起爆'
                row_dict['入选理由'] = reason
                candidates.append(row_dict)
            
            count += 1
            if count % 10 == 0:
                print(f"已校验 {count}/{len(potential)} 只...")

        return pd.DataFrame(candidates)

    def run_dimension_c(self, df_merged):
        """维度 C：分歧中的领头羊"""
        print("\n正在执行 [维度 C：分歧领头羊] 筛选...")
        candidates = []
        
        # 1. Get Sectors
        df_sectors = self.data_api.get_sector_list()
        if df_sectors.empty:
            return pd.DataFrame()
            
        # 2. Filter Sectors: Change in -3% to 0%
        target_sectors_df = df_sectors[(df_sectors['涨跌幅'] >= -3) & (df_sectors['涨跌幅'] <= 0)]
        
        # If user specified target_sectors, filter this list further
        if self.target_sectors:
            target_sectors_df = target_sectors_df[target_sectors_df['板块名称'].isin(self.target_sectors)]
            
        print(f"符合分歧条件的板块数: {len(target_sectors_df)}")
        
        for _, sec_row in target_sectors_df.iterrows():
            sec_name = sec_row['板块名称']
            sec_change = sec_row['涨跌幅']
            
            # Get components
            cons_codes = self.data_api.get_sector_cons(sec_name)
            if not cons_codes:
                continue
                
            # Filter stocks in this sector that are in our merged df (which already has base filters)
            sec_stocks = df_merged[df_merged['代码'].isin(cons_codes)].copy()
            
            if sec_stocks.empty:
                continue
                
            # Sort by Net Inflow
            sec_stocks = sec_stocks.sort_values(by='今日主力净流入-净额', ascending=False)
            
            # Take Top 3
            top_3 = sec_stocks.head(3)
            
            for rank, (idx, stock_row) in enumerate(top_3.iterrows(), 1):
                inflow = stock_row['今日主力净流入-净额']
                
                # Check threshold: > 1000万
                if inflow > 10000000:
                    row_dict = stock_row.to_dict()
                    row_dict['筛选维度'] = 'C:分歧龙头'
                    row_dict['入选理由'] = f"板块[{sec_name}]({sec_change}%)排名第{rank}, 主力流入{inflow/1e4:.0f}万"
                    candidates.append(row_dict)
        
        return pd.DataFrame(candidates)

def main():
    parser = argparse.ArgumentParser(description="A股隔夜套利选股脚本")
    parser.add_argument("--sectors", type=str, help="指定板块，逗号分隔，例如 '半导体,银行'")
    args = parser.parse_args()
    
    target_sectors = None
    if args.sectors:
        target_sectors = args.sectors.split(",")
        print(f"已启用指定板块筛选: {target_sectors}")
    
    strategy = Strategy(target_sectors)
    df_merged, df_spot, df_flow = strategy.prepare_data()
    
    if df_merged is None:
        return

    results = []
    
    # Run Dimensions
    res_a = strategy.run_dimension_a(df_merged)
    if not res_a.empty:
        results.append(res_a)
        
    res_b = strategy.run_dimension_b(df_merged)
    if not res_b.empty:
        results.append(res_b)
        
    res_c = strategy.run_dimension_c(df_merged) # Note: Dimension C logic iterates sectors, but uses merged data
    if not res_c.empty:
        results.append(res_c)
        
    print("\n" + "="*50)
    print(f"选股完成，共发现 {sum([len(r) for r in results])} 个信号")
    print("="*50)
    
    if results:
        final_df = pd.concat(results)
        # Deduplicate (a stock might hit multiple dimensions)
        # Aggregating reasons
        final_df = final_df.groupby('代码').agg({
            '名称': 'first',
            '最新价': 'first',
            '涨跌幅': 'first',
            '今日主力净流入-净额': 'first',
            '今日主力净流入-净占比': 'first',
            '今日小单净流入-净占比': 'first',
            '筛选维度': lambda x: ','.join(set(x)),
            '入选理由': lambda x: ' | '.join(set(x))
        }).reset_index()
        
        # Sort by Inflow Ratio
        final_df = final_df.sort_values(by='今日主力净流入-净占比', ascending=False)
        
        print(final_df[['代码', '名称', '最新价', '涨跌幅', '筛选维度', '入选理由']].to_markdown(index=False))
        
        # Save to CSV
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"overnight_picks_{timestamp}.csv"
        final_df.to_csv(filename, index=False)
        print(f"\n结果已保存至: {filename}")
    else:
        print("今日无符合条件的标的。")

if __name__ == "__main__":
    main()
