#!/usr/bin/env python3
"""
验证单只股票的策略逻辑
"""
import os
import sys

backend_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if backend_dir not in sys.path:
    sys.path.insert(0, backend_dir)

import pandas as pd
from utils.pytdx_client import tdx

PYTDX_VOL_MULTIPLIER = 100


def verify_stock(code: str, market: int, expected_breakout_date: str):
    """验证单只股票的策略逻辑"""
    print(f"\n{'='*60}")
    print(f"验证股票: {code}")
    print(f"预期突破日: {expected_breakout_date}")
    print("="*60)
    
    with tdx:
        data = tdx.get_security_bars(9, market, code, 0, 150)
        df = pd.DataFrame(data)
        df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce")
        df = df.dropna(subset=["datetime"]).sort_values("datetime", ascending=True)
        for c in ("open", "close", "high", "low", "vol", "amount"):
            if c in df.columns:
                df[c] = pd.to_numeric(df[c], errors="coerce")
        df = df.dropna(subset=["open", "close", "high", "low", "vol"]).reset_index(drop=True)
        df["vol"] = df["vol"] * PYTDX_VOL_MULTIPLIER
        
        df["ma20"] = df["close"].rolling(20, min_periods=20).mean()
        df["ma60"] = df["close"].rolling(60, min_periods=60).mean()
        df["ma120"] = df["close"].rolling(120, min_periods=120).mean()
        df["ma20_vol"] = df["vol"].rolling(20, min_periods=20).mean()
        df["high60"] = df["high"].rolling(60, min_periods=60).max()
        
        breakout_date_dt = pd.to_datetime(expected_breakout_date)
        breakout_rows = df[df["datetime"].dt.strftime("%Y-%m-%d") == expected_breakout_date]
        
        if breakout_rows.empty:
            print(f"❌ 未找到突破日 {expected_breakout_date} 的数据")
            return
        
        breakout_row = breakout_rows.iloc[0]
        print(f"\n【突破日数据】{expected_breakout_date}")
        print(f"  收盘价: {breakout_row['close']:.4f}")
        print(f"  成交量: {breakout_row['vol']:,.0f} 股")
        print(f"  MA20成交量: {breakout_row['ma20_vol']:,.0f} 股")
        print(f"  量比: {breakout_row['vol'] / breakout_row['ma20_vol']:.3f}")
        print(f"  MA60: {breakout_row['ma60']:.4f}")
        print(f"  MA120: {breakout_row['ma120']:.4f}")
        print(f"  High60: {breakout_row['high60']:.4f}")
        
        print(f"\n【突破条件检查】")
        close = breakout_row["close"]
        ma60 = breakout_row["ma60"]
        ma120 = breakout_row["ma120"]
        high60 = breakout_row["high60"]
        vol_ratio = breakout_row["vol"] / breakout_row["ma20_vol"]
        
        print(f"  量比 {vol_ratio:.3f} >= 1.5? {vol_ratio >= 1.5}")
        print(f"  量比 {vol_ratio:.3f} <= 4.0? {vol_ratio <= 4.0}")
        
        ma60_threshold = ma60 * 1.015
        print(f"  收盘 {close:.4f} >= MA60*1.015 ({ma60_threshold:.4f})? {close >= ma60_threshold}")
        
        ma120_threshold = ma120 * 1.015
        print(f"  收盘 {close:.4f} >= MA120*1.015 ({ma120_threshold:.4f})? {close >= ma120_threshold}")
        
        high60_threshold = high60 * 1.005
        print(f"  收盘 {close:.4f} >= High60*1.005 ({high60_threshold:.4f})? {close >= high60_threshold}")
        
        breakout_idx = breakout_rows.index[0]
        print(f"\n【站稳检查】突破日后3天")
        
        for i in range(1, 4):
            if breakout_idx + i >= len(df):
                print(f"  Day{i}: 数据不足")
                continue
            row = df.iloc[breakout_idx + i]
            date_str = row["datetime"].strftime("%Y-%m-%d")
            close_i = row["close"]
            vol_i = row["vol"]
            ma20_vol_i = row["ma20_vol"]
            
            above_ma60 = close_i >= ma60
            above_99 = close_i >= ma60 * 0.99
            vol_ok = vol_i >= ma20_vol_i * 0.5
            
            print(f"  Day{i} ({date_str}):")
            print(f"    收盘: {close_i:.4f}, >= MA60? {above_ma60}, >= MA60*0.99? {above_99}")
            print(f"    成交量: {vol_i:,.0f}, >= MA20_Vol*0.5? {vol_ok}")
        
        print(f"\n【最近5日数据】")
        recent = df.tail(5)[["datetime", "open", "close", "high", "low", "vol", "ma60", "high60"]]
        recent["datetime"] = recent["datetime"].dt.strftime("%Y-%m-%d")
        print(recent.to_string(index=False))


if __name__ == "__main__":
    verify_stock("000158", 0, "2025-10-17")
    verify_stock("000999", 0, "2026-02-09")
    verify_stock("000839", 0, "2026-02-24")
