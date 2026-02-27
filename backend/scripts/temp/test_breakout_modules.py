#!/usr/bin/env python3
"""
突破后站稳3日策略 - 模块测试脚本
用于验证各模块功能和数据接口
"""
import os
import sys
from datetime import datetime, timedelta

backend_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if backend_dir not in sys.path:
    sys.path.insert(0, backend_dir)

import pandas as pd

from utils.pytdx_client import tdx

try:
    from utils.tushare_client import pro
except Exception:
    pro = None


def test_pytdx_data():
    """测试pytdx数据接口"""
    print("=" * 60)
    print("【测试1】pytdx数据接口检查")
    print("=" * 60)
    
    results = {
        "test_name": "pytdx数据接口",
        "passed": False,
        "details": []
    }
    
    try:
        with tdx:
            data = tdx.get_security_bars(9, 0, "000001", 0, 10)
            
            if data is None or len(data) == 0:
                results["details"].append("❌ 无法获取数据")
                return results
            
            df = pd.DataFrame(data)
            results["details"].append(f"✓ 获取到 {len(df)} 条K线数据")
            
            results["details"].append(f"✓ 数据列: {list(df.columns)}")
            
            if "datetime" in df.columns:
                latest_date = df.iloc[-1]["datetime"]
                results["details"].append(f"✓ 最新日期: {latest_date}")
                
                today = datetime.now()
                latest_dt = pd.to_datetime(latest_date)
                days_diff = (today - latest_dt).days
                if days_diff <= 3:
                    results["details"].append(f"✓ 数据较新（距今天{days_diff}天）")
                else:
                    results["details"].append(f"⚠ 数据可能过旧（距今天{days_diff}天）")
            
            if "vol" in df.columns:
                vol_sample = df.iloc[-1]["vol"]
                results["details"].append(f"✓ 成交量示例: {vol_sample:,.0f}")
                
                if "amount" in df.columns and "close" in df.columns:
                    amount = df.iloc[-1]["amount"]
                    close = df.iloc[-1]["close"]
                    if vol_sample > 0:
                        vwap = amount / vol_sample
                        ratio = vwap / close if close > 0 else 0
                        results["details"].append(f"✓ VWAP估算: {vwap:.2f}")
                        results["details"].append(f"✓ VWAP/Close比值: {ratio:.2f}")
                        
                        if ratio > 50:
                            results["details"].append("⚠ 成交量单位可能是【手】，需要×100转换为股")
                            results["volume_unit"] = "手"
                        else:
                            results["details"].append("✓ 成交量单位可能是【股】")
                            results["volume_unit"] = "股"
            
            sample_row = df.iloc[-1]
            results["details"].append(f"✓ 示例数据: open={sample_row.get('open')}, close={sample_row.get('close')}, high={sample_row.get('high')}, low={sample_row.get('low')}")
            
            results["passed"] = True
            
    except Exception as e:
        results["details"].append(f"❌ 异常: {str(e)}")
    
    return results


def test_tushare_data():
    """测试tushare数据接口"""
    print("\n" + "=" * 60)
    print("【测试2】tushare数据接口检查")
    print("=" * 60)
    
    results = {
        "test_name": "tushare数据接口",
        "passed": False,
        "details": []
    }
    
    if pro is None:
        results["details"].append("❌ tushare pro 未初始化")
        return results
    
    try:
        df = pro.stock_basic(exchange="", list_status="L", fields="ts_code,name,list_status,list_date")
        
        if df is None or df.empty:
            results["details"].append("❌ 无法获取股票列表")
            return results
        
        results["details"].append(f"✓ 获取到 {len(df)} 只股票")
        results["details"].append(f"✓ 数据列: {list(df.columns)}")
        
        st_stocks = df[df["name"].str.contains("ST", na=False)]
        results["details"].append(f"✓ ST股票数量: {len(st_stocks)}")
        
        if "list_date" in df.columns:
            sample = df[df["list_date"].notna()].head(5)
            results["details"].append(f"✓ 上市日期示例: {sample['list_date'].tolist()}")
        
        results["passed"] = True
        
    except Exception as e:
        results["details"].append(f"❌ 异常: {str(e)}")
    
    return results


def test_data_fetch_module():
    """测试数据获取模块"""
    print("\n" + "=" * 60)
    print("【测试3】数据获取模块")
    print("=" * 60)
    
    results = {
        "test_name": "数据获取模块",
        "passed": False,
        "details": []
    }
    
    try:
        with tdx:
            data = tdx.get_security_bars(9, 0, "000001", 0, 150)
            
            if data is None or len(data) == 0:
                results["details"].append("❌ 无法获取K线数据")
                return results
            
            df = pd.DataFrame(data)
            df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce")
            df = df.dropna(subset=["datetime"]).sort_values("datetime", ascending=True)
            
            for c in ("open", "close", "high", "low", "vol", "amount"):
                if c in df.columns:
                    df[c] = pd.to_numeric(df[c], errors="coerce")
            
            df = df.dropna(subset=["open", "close", "high", "low", "vol"])
            df = df.reset_index(drop=True)
            
            results["details"].append(f"✓ 清洗后K线数量: {len(df)}")
            results["details"].append(f"✓ 日期范围: {df['datetime'].min()} ~ {df['datetime'].max()}")
            results["details"].append(f"✓ 数据列: {list(df.columns)}")
            
            results["passed"] = True
            
    except Exception as e:
        results["details"].append(f"❌ 异常: {str(e)}")
    
    return results


def test_indicator_calc():
    """测试技术指标计算模块"""
    print("\n" + "=" * 60)
    print("【测试4】技术指标计算模块")
    print("=" * 60)
    
    results = {
        "test_name": "技术指标计算",
        "passed": False,
        "details": []
    }
    
    try:
        with tdx:
            data = tdx.get_security_bars(9, 0, "000001", 0, 150)
            df = pd.DataFrame(data)
            df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce")
            df = df.dropna(subset=["datetime"]).sort_values("datetime", ascending=True)
            for c in ("open", "close", "high", "low", "vol", "amount"):
                if c in df.columns:
                    df[c] = pd.to_numeric(df[c], errors="coerce")
            df = df.dropna(subset=["open", "close", "high", "low", "vol"]).reset_index(drop=True)
            
            df["ma20"] = df["close"].rolling(20, min_periods=20).mean()
            df["ma60"] = df["close"].rolling(60, min_periods=60).mean()
            df["ma120"] = df["close"].rolling(120, min_periods=120).mean()
            df["ma20_vol"] = df["vol"].rolling(20, min_periods=20).mean()
            df["high60"] = df["high"].rolling(60, min_periods=60).max()
            
            last = df.iloc[-1]
            results["details"].append(f"✓ 最新收盘价: {last['close']:.2f}")
            results["details"].append(f"✓ MA20: {last['ma20']:.2f}" if pd.notna(last["ma20"]) else "⚠ MA20: NaN")
            results["details"].append(f"✓ MA60: {last['ma60']:.2f}" if pd.notna(last["ma60"]) else "⚠ MA60: NaN")
            results["details"].append(f"✓ MA120: {last['ma120']:.2f}" if pd.notna(last["ma120"]) else "⚠ MA120: NaN")
            results["details"].append(f"✓ High60: {last['high60']:.2f}" if pd.notna(last["high60"]) else "⚠ High60: NaN")
            results["details"].append(f"✓ MA20_Vol: {last['ma20_vol']:,.0f}" if pd.notna(last["ma20_vol"]) else "⚠ MA20_Vol: NaN")
            
            results["passed"] = True
            
    except Exception as e:
        results["details"].append(f"❌ 异常: {str(e)}")
    
    return results


def run_all_tests():
    """运行所有测试"""
    print("\n" + "=" * 60)
    print("开始运行模块测试")
    print("=" * 60)
    
    all_results = []
    
    all_results.append(test_pytdx_data())
    all_results.append(test_tushare_data())
    all_results.append(test_data_fetch_module())
    all_results.append(test_indicator_calc())
    
    print("\n" + "=" * 60)
    print("测试结果汇总")
    print("=" * 60)
    
    passed = 0
    failed = 0
    
    for r in all_results:
        status = "✓ 通过" if r["passed"] else "❌ 失败"
        print(f"\n{r['test_name']}: {status}")
        for detail in r["details"]:
            print(f"  {detail}")
        
        if r["passed"]:
            passed += 1
        else:
            failed += 1
    
    print("\n" + "=" * 60)
    print(f"总计: {passed} 通过, {failed} 失败")
    print("=" * 60)
    
    return all_results


if __name__ == "__main__":
    run_all_tests()
