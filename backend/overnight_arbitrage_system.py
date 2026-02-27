#!/usr/bin/env python3
"""
A股隔夜套利完整决策系统
整合：技术面 + 财务面 + 舆情面 + 资金面 + 政策面
输出：可操作的股票推荐名单 + 详细分析报告
"""

import sys
import os
import time
from typing import List, Dict, Any

import pandas as pd

backend_dir = os.path.dirname(os.path.abspath(__file__))
if backend_dir not in sys.path:
    sys.path.insert(0, backend_dir)

from utils.pytdx_client import tdx
from utils.tushare_client import pro
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

from enhanced_sentiment import EnhancedSentimentAnalyzer


class OvernightArbitrageSystem:
    """隔夜套利完整决策系统"""
    
    def __init__(self):
        self.sentiment_analyzer = EnhancedSentimentAnalyzer()
    
    def get_financial_score(self, code: str, name: str) -> Dict[str, Any]:
        """获取财务评分（简化但实用版）"""
        financial_info = {
            "code": code,
            "name": name,
            "score": 50,
            "risk_level": "中等",
            "notes": []
        }
        
        # 风险判断
        if "ST" in name or "*ST" in name:
            financial_info["score"] = 25
            financial_info["risk_level"] = "高风险 ⚠️"
            financial_info["notes"].append("ST股票，风险较高")
        elif code.startswith("688"):
            financial_info["score"] = 40
            financial_info["risk_level"] = "中高风险"
            financial_info["notes"].append("科创板，波动较大")
        elif code.startswith("30"):
            financial_info["score"] = 45
            financial_info["risk_level"] = "中等风险"
            financial_info["notes"].append("创业板，需注意")
        else:
            financial_info["score"] = 60
            financial_info["risk_level"] = "低风险 ✅"
            financial_info["notes"].append("主板，相对稳健")
        
        return financial_info
    
    def analyze_single_stock(self, stock_row: pd.Series, market_context: Dict) -> Dict[str, Any]:
        """单只股票完整分析"""
        code = str(stock_row["code"]).zfill(6)
        name = str(stock_row.get("name", ""))
        
        # 1. 技术面分析
        alpha = stock_row.get("Alpha_effectiveness", 0)
        volume_ratio = stock_row.get("volume_ratio", 1)
        tail_attack = stock_row.get("tail_attack_coefficient", 0)
        bid_ask = stock_row.get("bid_ask_imbalance", 0)
        
        # 技术面评分
        technical_score = 0
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
        financial_data = self.get_financial_score(code, name)
        financial_score = financial_data["score"]
        
        # 3. 舆情面分析
        stock_sentiment = self.sentiment_analyzer.analyze_stock_sentiment(code, name)
        sentiment_score = stock_sentiment["sentiment_score"]
        
        # 4. 板块匹配度
        sector_match_score = 0
        related_sectors = stock_sentiment.get("related_sectors", [])
        hot_sectors = market_context.get("hot_sectors", [])
        hot_sector_names = [s["name"] for s in hot_sectors] if hot_sectors else []
        
        for sector in related_sectors:
            if sector in hot_sector_names:
                sector_match_score += 20
        
        # 5. 综合评分（加权）
        overall_score = (
            technical_score * 0.45 +      # 技术面 45%
            financial_score * 0.25 +      # 财务面 25%
            sentiment_score * 0.20 +      # 舆情面 20%
            sector_match_score * 0.10      # 板块热度 10%
        )
        
        # 6. 操作建议
        recommendation = "观望"
        confidence = "低"
        if overall_score >= 70:
            recommendation = "强烈推荐 🔥"
            confidence = "高"
        elif overall_score >= 58:
            recommendation = "推荐 ✅"
            confidence = "中高"
        elif overall_score >= 45:
            recommendation = "谨慎关注 ⚠️"
            confidence = "中等"
        else:
            recommendation = "观望"
            confidence = "低"
        
        return {
            "code": code,
            "name": name,
            "price": round(stock_row.get("price", 0), 2),
            "alpha": round(alpha, 3),
            "volume_ratio": round(volume_ratio, 2),
            "tail_attack": round(tail_attack, 4),
            "bid_ask": round(bid_ask, 2),
            "technical_score": technical_score,
            "financial_score": financial_score,
            "sentiment_score": sentiment_score,
            "sector_match_score": sector_match_score,
            "overall_score": round(overall_score, 1),
            "recommendation": recommendation,
            "confidence": confidence,
            "risk_level": financial_data["risk_level"],
            "related_sectors": related_sectors,
            "recent_news": stock_sentiment.get("recent_news", [])[:3]
        }
    
    def generate_report(self, results: List[Dict], market_context: Dict):
        """生成分析报告"""
        print("\n" + "=" * 100)
        print("                             A 股隔夜套利 - 完整决策报告")
        print("=" * 100)
        
        # 市场环境
        print(f"\n📊 【市场环境】")
        print(f"   整体情绪: {market_context.get('sentiment_label', '中性')}")
        print(f"   情绪分数: {market_context.get('sentiment_score', 50)}/100")
        
        print(f"\n🔥 【热门板块】")
        hot_sectors = market_context.get("hot_sectors", [])
        if hot_sectors:
            for i, sector in enumerate(hot_sectors[:6], 1):
                change = sector.get('change_pct', 0)
                marker = "📈" if change > 0 else "📉"
                print(f"   {i}. {sector['name']}: {change:+.2f}% {marker}")
        
        # 推荐股票列表
        print(f"\n🎯 【推荐股票列表】 (按综合评分排序)")
        print("-" * 100)
        print(f"{'优先级':<6} {'代码':<8} {'名称':<10} {'当前价':<8} {'技术分':<8} {'财务分':<8} {'舆情分':<8} {'综合分':<8} {'操作建议':<12} {'风险等级':<10}")
        print("-" * 100)
        
        strong_recommend = []
        recommend = []
        watch = []
        
        for r in results:
            if "强烈推荐" in r["recommendation"]:
                strong_recommend.append(r)
            elif "推荐" in r["recommendation"]:
                recommend.append(r)
            else:
                watch.append(r)
        
        priority = 1
        for r in strong_recommend + recommend + watch:
            marker = "🔥" if "强烈推荐" in r["recommendation"] else "✅" if "推荐" in r["recommendation"] else "⚠️"
            print(f"{marker} {priority:<4} {r['code']:<8} {r['name']:<10} {r['price']:<8} "
                  f"{r['technical_score']:<8} {r['financial_score']:<8} "
                  f"{r['sentiment_score']:<8} {r['overall_score']:<8} {r['recommendation']:<12} {r['risk_level']:<10}")
            priority += 1
        
        print("-" * 100)
        
        # Top 3 详细分析
        top_3 = strong_recommend[:3] + recommend[:3 - len(strong_recommend)]
        if top_3:
            print(f"\n📋 【详细分析 - Top {len(top_3)}】")
            print("=" * 100)
            
            for i, r in enumerate(top_3, 1):
                print(f"\n【{i}. {r['code']} {r['name']}】")
                print(f"    {'=' * 50}")
                print(f"    综合评分: {r['overall_score']} | 操作建议: {r['recommendation']} | 信心: {r['confidence']}")
                print(f"    当前价: {r['price']} | 风险等级: {r['risk_level']}")
                print(f"    技术面: Alpha={r['alpha']}, 量比={r['volume_ratio']}, 尾盘={r['tail_attack']}, 委比={r['bid_ask']}")
                print(f"    评分构成: 技术={r['technical_score']} (45%), 财务={r['financial_score']} (25%), "
                      f"舆情={r['sentiment_score']} (20%), 板块={r['sector_match_score']} (10%)")
                
                if r['related_sectors']:
                    print(f"    相关板块: {', '.join(r['related_sectors'])}")
                
                if r['recent_news']:
                    print(f"    相关新闻:")
                    for news in r['recent_news']:
                        sentiment = "😊" if news.get('sentiment_score', 50) >= 60 else "😐"
                        print(f"      {sentiment} {news.get('title', '')[:60]}...")
        
        # 操作建议
        print(f"\n💡 【操作建议】")
        print("=" * 100)
        
        if strong_recommend:
            print(f"   ✅ 强烈推荐 ({len(strong_recommend)}只): 可考虑重点关注，建议仓位 3-5%/只")
            for r in strong_recommend:
                print(f"      - {r['code']} {r['name']} (综合分: {r['overall_score']})")
        
        if recommend:
            print(f"   ⚠️ 推荐 ({len(recommend)}只): 可适度关注，建议仓位 2-3%/只")
            for r in recommend[:5]:
                print(f"      - {r['code']} {r['name']} (综合分: {r['overall_score']})")
        
        print(f"\n📌 风险提示:")
        print(f"   1. 单只股票仓位不超过总资金的 5%")
        print(f"   2. 建议设置止损线 -3%，止盈线 +5~8%")
        print(f"   3. 尾盘14:45-14:55为最佳买入时间窗口")
        print(f"   4. 次日开盘后根据盘面情况决定卖出时机")
        
        print(f"\n" + "=" * 100)


def main():
    print("=" * 100)
    print("                        A 股隔夜套利 - 完整决策系统")
    print("=" * 100)
    
    system = OvernightArbitrageSystem()
    t_total_start = time.perf_counter()
    
    # 1. 获取市场舆情环境
    print("\n[1/6] 分析市场舆情环境...")
    df_news = system.sentiment_analyzer.fetch_news_cailian(limit=30)
    market_sentiment = system.sentiment_analyzer.analyze_market_sentiment(df_news)
    hot_sectors = system.sentiment_analyzer.fetch_hot_sectors()
    
    market_context = {
        "sentiment_score": market_sentiment["overall_sentiment_score"],
        "sentiment_label": market_sentiment["sentiment_label"],
        "hot_sectors": hot_sectors
    }
    print(f"    市场情绪: {market_context['sentiment_label']} ({market_context['sentiment_score']}/100)")
    
    # 2. 获取股票池
    print("\n[2/6] 获取股票池...")
    cache_file = stock_code_cache_name()
    df_stock_codes = normalize_stock_codes(load_stock_codes(cache_file))
    stock_codes = list(df_stock_codes[["market", "code"]].itertuples(index=False, name=None))
    print(f"    全市场 A 股: {len(stock_codes)} 只")
    
    # 3. 拉取实时行情
    print("\n[3/6] 拉取实时行情...")
    sum_quotes = fetch_quotes(stock_codes, batch_size=80)
    if sum_quotes is not None and not sum_quotes.empty and "code" in sum_quotes.columns:
        sum_quotes["code"] = sum_quotes["code"].astype(str).str.zfill(6)
        name_map = df_stock_codes.set_index("code")["name"].to_dict()
        sum_quotes["name"] = sum_quotes["code"].map(name_map)
    print(f"    快照数据: {len(sum_quotes)} 条")
    
    # 4. 计算技术指标并初筛
    print("\n[4/6] 计算技术指标...")
    sum_quotes = calculate_Alpha_effectiveness(sum_quotes)
    
    # 宽松筛选
    alpha_min, alpha_max = 0.70, 0.98
    df_candidates = filter_Alpha_effectiveness_stocks(sum_quotes, alpha_min, alpha_max).copy()
    print(f"    Alpha 筛选 [{alpha_min}, {alpha_max}]: {len(sum_quotes)} -> {len(df_candidates)}")
    
    if df_candidates.empty:
        print("    无满足条件的股票，结束。")
        return
    
    # 5. 补充技术指标
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
    
    # 只取前25只进行详细分析
    df_candidates = df_candidates.sort_values(by="Alpha_effectiveness", ascending=False).head(25)
    
    # 6. 多维度综合分析
    print("\n[6/6] 多维度综合分析...")
    results = []
    for idx, row in df_candidates.iterrows():
        result = system.analyze_single_stock(row, market_context)
        results.append(result)
    
    # 按综合评分排序
    results.sort(key=lambda x: x["overall_score"], reverse=True)
    
    # 生成报告
    system.generate_report(results, market_context)
    
    # 保存结果
    df_result = pd.DataFrame(results)
    output_file = "隔夜套利完整决策报告.csv"
    df_result.to_csv(output_file, index=False, encoding="utf-8-sig")
    print(f"\n💾 完整结果已保存到: {output_file}")
    
    print(f"\n⏱️  总耗时: {time.perf_counter() - t_total_start:.2f}s")
    print("=" * 100)


if __name__ == "__main__":
    main()
