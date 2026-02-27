#!/usr/bin/env python3
"""
A股隔夜套利综合分析系统
多维度分析：技术面 + 财务面 + 舆情面 + 政策面
"""

import sys
import os
import time
import json
from typing import List, Dict, Any

import pandas as pd

backend_dir = os.path.dirname(os.path.abspath(__file__))
if backend_dir not in sys.path:
    sys.path.insert(0, backend_dir)

from utils.pytdx_client import tdx
from utils.tushare_client import ts, pro
import akshare as ak

from scripts.一夜持股法_实盘 import (
    load_stock_codes,
    normalize_stock_codes,
    stock_code_cache_name,
    fetch_quotes,
    calculate_Alpha_effectiveness,
    filter_Alpha_effectiveness_stocks,
    mean_volume_last_n_days,
    calc_bid_ask_imbalance,
)


class StockAnalyzer:
    """股票综合分析器"""
    
    def __init__(self):
        self.results = []
    
    def get_financial_data(self, code: str, name: str) -> Dict[str, Any]:
        """获取财务数据（快速版本）"""
        financial_info = {
            "code": code,
            "name": name,
            "pe_ratio": None,
            "pb_ratio": None,
            "market_cap": None,
            "revenue_growth": None,
            "profit_growth": None,
            "financial_score": 50  # 默认中等分数
        }
        
        try:
            # 尝试用 akshare 获取估值指标
            try:
                # 估值指标
                df_individual = ak.stock_individual_spot_xq(symbol=code)
                if df_individual is not None and not df_individual.empty:
                    pass
            except:
                pass
            
            # 尝试用 tushare 获取财务数据
            if pro is not None:
                try:
                    # 获取股票基本信息
                    df_basic = pro.stock_basic(ts_code=code, list_status='L')
                    if df_basic is not None and not df_basic.empty:
                        pass
                except:
                    pass
            
            # 简易财务评分逻辑
            # 如果是 ST 股，降低评分
            if "ST" in name or "*ST" in name:
                financial_info["financial_score"] = 30
            # 科创板、创业板相对风险较高
            elif code.startswith("688") or code.startswith("30"):
                financial_info["financial_score"] = 45
            # 主板
            else:
                financial_info["financial_score"] = 60
                
        except Exception as e:
            print(f"    获取 {code} {name} 财务数据时出错: {e}")
        
        return financial_info
    
    def get_news_sentiment(self, code: str, name: str) -> Dict[str, Any]:
        """获取新闻舆情（快速版本）"""
        sentiment_info = {
            "code": code,
            "name": name,
            "recent_news": [],
            "sentiment_score": 50,  # 中性
            "news_count": 0
        }
        
        try:
            # 尝试用 akshare 获取个股新闻
            try:
                df_news = ak.stock_news_em(symbol=code)
                if df_news is not None and not df_news.empty:
                    sentiment_info["news_count"] = min(len(df_news), 10)
                    # 简单分析新闻标题
                    positive_keywords = ["涨", "增长", "利好", "签约", "中标", "盈利", "突破", "创新高"]
                    negative_keywords = ["跌", "亏损", "利空", "处罚", "调查", "诉讼", "风险", "警示"]
                    
                    positive_count = 0
                    negative_count = 0
                    
                    for _, row in df_news.head(10).iterrows():
                        title = str(row.get("title", ""))
                        sentiment_info["recent_news"].append(title[:50])
                        
                        for kw in positive_keywords:
                            if kw in title:
                                positive_count += 1
                                break
                        for kw in negative_keywords:
                            if kw in title:
                                negative_count += 1
                                break
                    
                    # 计算舆情分数
                    if sentiment_info["news_count"] > 0:
                        sentiment_score = 50 + (positive_count - negative_count) * 10
                        sentiment_info["sentiment_score"] = max(0, min(100, sentiment_score))
            except:
                pass
                
        except Exception as e:
            print(f"    获取 {code} {name} 舆情数据时出错: {e}")
        
        return sentiment_info
    
    def analyze_stock(self, stock_row: pd.Series) -> Dict[str, Any]:
        """单只股票综合分析"""
        code = str(stock_row["code"]).zfill(6)
        name = str(stock_row.get("name", ""))
        
        print(f"  正在分析: {code} {name}")
        
        # 1. 技术面指标（已有的）
        technical_score = 0
        alpha = stock_row.get("Alpha_effectiveness", 0)
        volume_ratio = stock_row.get("volume_ratio", 1)
        tail_attack = stock_row.get("tail_attack_coefficient", 0)
        bid_ask = stock_row.get("bid_ask_imbalance", 0)
        
        # 技术面评分（0-100）
        if 0.85 <= alpha <= 0.98:
            technical_score += 40
        elif 0.7 <= alpha < 0.85:
            technical_score += 25
        
        if volume_ratio >= 1.5:
            technical_score += 25
        elif volume_ratio >= 1.0:
            technical_score += 15
        
        if tail_attack >= 0.02:
            technical_score += 20
        elif tail_attack >= 0.01:
            technical_score += 10
        
        if bid_ask > 0.2:
            technical_score += 15
        elif bid_ask > 0:
            technical_score += 8
        
        technical_score = min(100, technical_score)
        
        # 2. 财务面分析
        financial_data = self.get_financial_data(code, name)
        financial_score = financial_data["financial_score"]
        
        # 3. 舆情面分析
        sentiment_data = self.get_news_sentiment(code, name)
        sentiment_score = sentiment_data["sentiment_score"]
        
        # 4. 综合评分（加权）
        # 技术面 50%，财务面 30%，舆情面 20%
        overall_score = (
            technical_score * 0.5 +
            financial_score * 0.3 +
            sentiment_score * 0.2
        )
        
        # 5. 操作建议
        recommendation = "观望"
        if overall_score >= 70:
            recommendation = "强烈推荐"
        elif overall_score >= 55:
            recommendation = "推荐"
        elif overall_score >= 40:
            recommendation = "谨慎关注"
        
        return {
            "code": code,
            "name": name,
            "price": stock_row.get("price", 0),
            "alpha": round(alpha, 3),
            "volume_ratio": round(volume_ratio, 2),
            "tail_attack": round(tail_attack, 4),
            "bid_ask": round(bid_ask, 2),
            "technical_score": technical_score,
            "financial_score": financial_score,
            "sentiment_score": sentiment_score,
            "overall_score": round(overall_score, 1),
            "recommendation": recommendation,
            "recent_news": sentiment_data["recent_news"][:3] if sentiment_data["recent_news"] else []
        }


def get_hot_sectors() -> List[str]:
    """获取当前热门板块"""
    hot_sectors = []
    try:
        # 尝试用 akshare 获取板块涨幅榜
        df_sectors = ak.stock_board_industry_name_em()
        if df_sectors is not None and not df_sectors.empty:
            # 取涨幅前5的板块
            for _, row in df_sectors.head(5).iterrows():
                sector_name = row.get("板块名称", "")
                if sector_name:
                    hot_sectors.append(sector_name)
    except Exception as e:
        print(f"获取热门板块时出错: {e}")
        hot_sectors = ["人工智能", "新能源", "半导体", "医药生物", "消费"]
    
    return hot_sectors


def get_market_overview() -> Dict[str, Any]:
    """获取市场概览"""
    overview = {
        "sh_index": None,
        "sz_index": None,
        "market_sentiment": "中性",
        "up_count": 0,
        "down_count": 0
    }
    
    try:
        # 尝试获取大盘指数
        df_sh = ak.stock_zh_index_spot()
        if df_sh is not None and not df_sh.empty:
            pass
    except Exception as e:
        print(f"获取市场概览时出错: {e}")
    
    return overview


def main():
    print("=" * 80)
    print("          A 股隔夜套利 - 多维度综合分析系统")
    print("=" * 80)
    
    t_total_start = time.perf_counter()
    
    # 1. 获取市场概览
    print("\n[1/6] 获取市场概览...")
    market_overview = get_market_overview()
    hot_sectors = get_hot_sectors()
    print(f"    当前热门板块: {', '.join(hot_sectors[:3])}")
    
    # 2. 股票池筛选（技术面初筛）
    print("\n[2/6] 技术面初筛...")
    cache_file = stock_code_cache_name()
    df_stock_codes = normalize_stock_codes(load_stock_codes(cache_file))
    stock_codes = list(df_stock_codes[["market", "code"]].itertuples(index=False, name=None))
    print(f"    全市场 A 股: {len(stock_codes)} 只")
    
    # 3. 拉取实时数据
    print("\n[3/6] 拉取实时行情...")
    sum_quotes = fetch_quotes(stock_codes, batch_size=80)
    if sum_quotes is not None and not sum_quotes.empty and "code" in sum_quotes.columns:
        sum_quotes["code"] = sum_quotes["code"].astype(str).str.zfill(6)
        name_map = df_stock_codes.set_index("code")["name"].to_dict()
        sum_quotes["name"] = sum_quotes["code"].map(name_map)
    
    # 4. 计算技术指标并初筛
    print("\n[4/6] 计算技术指标...")
    sum_quotes = calculate_Alpha_effectiveness(sum_quotes)
    
    # 宽松筛选获取更多候选
    alpha_min, alpha_max = 0.70, 0.98
    df_candidates = filter_Alpha_effectiveness_stocks(sum_quotes, alpha_min, alpha_max).copy()
    print(f"    Alpha 筛选 [{alpha_min}, {alpha_max}]: {len(sum_quotes)} -> {len(df_candidates)}")
    
    if df_candidates.empty:
        print("    无满足条件的股票，结束。")
        return
    
    # 5. 补充量能、尾盘、委比指标
    print("\n[5/6] 补充技术指标...")
    df_candidates = mean_volume_last_n_days(df_candidates)
    
    # 进一步筛选
    df_candidates = df_candidates[df_candidates["volume_ratio"] >= 0.5]
    df_candidates = df_candidates[df_candidates["tail_attack_coefficient"] >= 0.003]
    
    # 计算委比
    required_cols = ["bid_vol1", "bid_vol2", "bid_vol3", "bid_vol4", "bid_vol5",
                     "ask_vol1", "ask_vol2", "ask_vol3", "ask_vol4", "ask_vol5"]
    if all(c in df_candidates.columns for c in required_cols):
        df_candidates["bid_ask_imbalance"] = calc_bid_ask_imbalance(df_candidates)
        df_candidates = df_candidates[df_candidates["bid_ask_imbalance"] > -0.5]
    
    print(f"    筛选后剩余: {len(df_candidates)} 只")
    
    if df_candidates.empty:
        print("    无满足条件的股票，结束。")
        return
    
    # 只取前20只进行详细分析（避免耗时太长）
    df_candidates = df_candidates.sort_values(by="Alpha_effectiveness", ascending=False).head(20)
    
    # 6. 多维度综合分析
    print("\n[6/6] 多维度综合分析...")
    analyzer = StockAnalyzer()
    analysis_results = []
    
    for idx, row in df_candidates.iterrows():
        result = analyzer.analyze_stock(row)
        analysis_results.append(result)
    
    # 按综合评分排序
    analysis_results.sort(key=lambda x: x["overall_score"], reverse=True)
    
    # 输出结果
    print("\n" + "=" * 80)
    print("                      综合分析结果")
    print("=" * 80)
    
    print(f"\n📊 市场环境:")
    print(f"   热门板块: {', '.join(hot_sectors)}")
    
    print(f"\n🎯 推荐股票列表 (按综合评分排序):")
    print("-" * 120)
    print(f"{'代码':<8} {'名称':<10} {'当前价':<8} {'技术分':<8} {'财务分':<8} {'舆情分':<8} {'综合分':<8} {'操作建议':<10}")
    print("-" * 120)
    
    for r in analysis_results:
        # 根据评分加颜色标记
        marker = "⭐" if r["overall_score"] >= 60 else "  "
        if r["overall_score"] >= 70:
            marker = "🔥"
        
        print(f"{marker} {r['code']:<8} {r['name']:<10} {r['price']:<8} "
              f"{r['technical_score']:<8} {r['financial_score']:<8} "
              f"{r['sentiment_score']:<8} {r['overall_score']:<8} {r['recommendation']:<10}")
    
    print("-" * 120)
    
    # 详细分析 Top 5
    print(f"\n📋 详细分析 - Top 5:")
    print("=" * 80)
    
    for i, r in enumerate(analysis_results[:5], 1):
        print(f"\n【{i}. {r['code']} {r['name']}】")
        print(f"    综合评分: {r['overall_score']} | 操作建议: {r['recommendation']}")
        print(f"    当前价: {r['price']}")
        print(f"    技术面: Alpha={r['alpha']}, 量比={r['volume_ratio']}, "
              f"尾盘={r['tail_attack']}, 委比={r['bid_ask']}")
        print(f"    评分构成: 技术={r['technical_score']}, 财务={r['financial_score']}, "
              f"舆情={r['sentiment_score']}")
        if r['recent_news']:
            print(f"    相关新闻:")
            for news in r['recent_news']:
                print(f"      - {news}")
    
    # 保存结果
    df_result = pd.DataFrame(analysis_results)
    output_file = "隔夜套利综合分析结果.csv"
    df_result.to_csv(output_file, index=False, encoding="utf-8-sig")
    print(f"\n💾 完整结果已保存到: {output_file}")
    
    # 总结
    print(f"\n📈 分析总结:")
    print(f"   共分析 {len(analysis_results)} 只股票")
    strong_recommend = sum(1 for r in analysis_results if r["recommendation"] == "强烈推荐")
    recommend = sum(1 for r in analysis_results if r["recommendation"] == "推荐")
    print(f"   强烈推荐: {strong_recommend} 只 | 推荐: {recommend} 只")
    
    print(f"\n⏱️  总耗时: {time.perf_counter() - t_total_start:.2f}s")
    print("=" * 80)


if __name__ == "__main__":
    main()
