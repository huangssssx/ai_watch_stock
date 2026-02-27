#!/usr/bin/env python3
"""
增强版舆情分析系统
- 多新闻源整合
- 深度情感分析
- 热点追踪
- 北向资金监控
"""

import sys
import os
import time
import re
from typing import List, Dict, Any, Tuple
from datetime import datetime, timedelta

import pandas as pd

backend_dir = os.path.dirname(os.path.abspath(__file__))
if backend_dir not in sys.path:
    sys.path.insert(0, backend_dir)

import akshare as ak
from utils.tushare_client import pro


class EnhancedSentimentAnalyzer:
    """增强版舆情分析器"""
    
    def __init__(self):
        # 情感关键词库
        self.positive_keywords = [
            "涨", "上涨", "大涨", "暴涨", "创新高", "突破", "利好", "超预期",
            "盈利", "业绩大增", "扭亏为盈", "签约", "中标", "收购", "并购",
            "战略合作", "政策支持", "补贴", "减税", "行业景气", "需求旺盛",
            "供不应求", "涨价", "提价", "机构买入", "北向资金", "增持", "回购"
        ]
        
        self.negative_keywords = [
            "跌", "下跌", "大跌", "暴跌", "创新低", "破位", "利空", "低于预期",
            "亏损", "业绩下滑", "大幅亏损", "违约", "诉讼", "调查", "处罚",
            "立案", "减持", "解禁", "质押", "平仓", "退市风险", "监管收紧",
            "行业不景气", "需求疲软", "供过于求", "降价", "机构卖出", "资金流出"
        ]
        
        # 热点板块关键词
        self.sector_keywords = {
            "人工智能": ["AI", "人工智能", "大模型", "ChatGPT", "算力", "芯片", "GPU", "半导体"],
            "新能源": ["新能源", "光伏", "风电", "储能", "动力电池", "特斯拉", "比亚迪"],
            "汽车": ["汽车", "整车", "零部件", "自动驾驶", "新能源车"],
            "医药": ["医药", "医疗", "生物", "疫苗", "创新药", "CXO"],
            "消费": ["消费", "白酒", "食品", "饮料", "零售", "电商"],
            "房地产": ["房地产", "地产", "保利", "万科", "金地"],
            "金融": ["金融", "银行", "证券", "保险", "基金"],
            "数字经济": ["数字经济", "数据要素", "东数西算", "信创"],
            "军工": ["军工", "航天", "航空", "防务", "兵器"],
            "农业": ["农业", "种业", "粮食", "生猪", "农药"]
        }
    
    def clean_text(self, text: str) -> str:
        """清洗文本"""
        if not text:
            return ""
        text = str(text)
        text = re.sub(r'\s+', ' ', text)
        text = text.strip()
        return text
    
    def analyze_sentiment_score(self, text: str) -> Tuple[int, List[str], List[str]]:
        """
        分析单条新闻的情感
        返回: (情感分数, 正面关键词, 负面关键词)
        """
        text = self.clean_text(text)
        if not text:
            return 50, [], []
        
        positive_hits = []
        negative_hits = []
        
        for kw in self.positive_keywords:
            if kw in text:
                positive_hits.append(kw)
        
        for kw in self.negative_keywords:
            if kw in text:
                negative_hits.append(kw)
        
        # 计算情感分数
        base_score = 50
        score = base_score + (len(positive_hits) * 8) - (len(negative_hits) * 10)
        score = max(0, min(100, score))
        
        return score, positive_hits, negative_hits
    
    def identify_sectors(self, text: str) -> List[str]:
        """识别新闻涉及的板块"""
        text = self.clean_text(text)
        sectors = []
        
        for sector, keywords in self.sector_keywords.items():
            for kw in keywords:
                if kw in text:
                    sectors.append(sector)
                    break
        
        return list(set(sectors))
    
    def fetch_news_cailian(self, limit: int = 30) -> pd.DataFrame:
        """获取财联社新闻"""
        news_list = []
        try:
            print("  正在获取财联社新闻...")
            df = ak.stock_info_global_cls()
            if df is not None and not df.empty:
                for _, row in df.head(limit).iterrows():
                    title = str(row.get("标题", ""))
                    content = str(row.get("内容", ""))
                    pub_time = row.get("发布时间", datetime.now())
                    
                    if not content and title:
                        content = title
                    
                    if content:
                        news_list.append({
                            "source": "财联社",
                            "title": title,
                            "content": content,
                            "publish_time": pub_time
                        })
            print(f"    财联社: 获取 {len(news_list)} 条")
        except Exception as e:
            print(f"    财联社获取失败: {e}")
        
        return pd.DataFrame(news_list) if news_list else pd.DataFrame()
    
    def fetch_news_eastmoney(self, limit: int = 30) -> pd.DataFrame:
        """获取东方财富新闻"""
        news_list = []
        try:
            print("  正在获取东方财富新闻...")
            df = ak.stock_news_em(symbol="000001")  # 用平安银行获取市场新闻
            if df is not None and not df.empty:
                for _, row in df.head(limit).iterrows():
                    title = str(row.get("title", ""))
                    content = str(row.get("content", ""))
                    pub_time = row.get("published time", datetime.now())
                    
                    if content:
                        news_list.append({
                            "source": "东方财富",
                            "title": title,
                            "content": content,
                            "publish_time": pub_time
                        })
            print(f"    东方财富: 获取 {len(news_list)} 条")
        except Exception as e:
            print(f"    东方财富获取失败: {e}")
        
        return pd.DataFrame(news_list) if news_list else pd.DataFrame()
    
    def fetch_stock_news(self, code: str, name: str, limit: int = 15) -> List[Dict]:
        """获取个股新闻"""
        news_list = []
        try:
            # 尝试用 akshare 获取个股新闻
            df = ak.stock_news_em(symbol=code)
            if df is not None and not df.empty:
                for _, row in df.head(limit).iterrows():
                    title = str(row.get("title", ""))
                    content = str(row.get("content", ""))
                    pub_time = row.get("published time", datetime.now())
                    
                    if content:
                        score, pos_kw, neg_kw = self.analyze_sentiment_score(content)
                        sectors = self.identify_sectors(content)
                        
                        news_list.append({
                            "title": title[:80] if title else content[:80],
                            "content": content[:200],
                            "sentiment_score": score,
                            "positive_keywords": pos_kw[:3],
                            "negative_keywords": neg_kw[:3],
                            "sectors": sectors,
                            "publish_time": pub_time
                        })
        except Exception as e:
            pass
        
        return news_list
    
    def fetch_northbound_flow(self) -> Dict[str, Any]:
        """获取北向资金数据"""
        flow_info = {
            "northbound_net_inflow": None,
            "sh_connect_inflow": None,
            "sz_connect_inflow": None,
            "trend": "未知"
        }
        
        try:
            print("  正在获取北向资金数据...")
            df = ak.stock_em_hsgt_north_net_flow_in()
            if df is not None and not df.empty:
                latest = df.iloc[0]
                flow_info["northbound_net_inflow"] = latest.get("净买入额", None)
                flow_info["sh_connect_inflow"] = latest.get("沪股通净买入", None)
                flow_info["sz_connect_inflow"] = latest.get("深股通净买入", None)
                
                # 判断趋势
                if flow_info["northbound_net_inflow"] is not None:
                    inflow = float(flow_info["northbound_net_inflow"])
                    if inflow > 50:
                        flow_info["trend"] = "大幅流入 📈"
                    elif inflow > 0:
                        flow_info["trend"] = "小幅流入 📊"
                    elif inflow > -50:
                        flow_info["trend"] = "小幅流出 📉"
                    else:
                        flow_info["trend"] = "大幅流出 ⚠️"
            
            print(f"    北向资金: {flow_info['trend']}")
        except Exception as e:
            print(f"    北向资金获取失败: {e}")
        
        return flow_info
    
    def fetch_hot_sectors(self) -> List[Dict]:
        """获取热门板块涨幅榜"""
        sectors = []
        try:
            print("  正在获取热门板块...")
            df = ak.stock_board_industry_name_em()
            if df is not None and not df.empty:
                for _, row in df.head(10).iterrows():
                    name = row.get("板块名称", "")
                    change = row.get("涨跌幅", 0)
                    if name:
                        sectors.append({
                            "name": name,
                            "change_pct": float(change) if change else 0
                        })
            print(f"    热门板块: 获取 {len(sectors)} 个")
        except Exception as e:
            print(f"    热门板块获取失败: {e}")
        
        return sectors
    
    def analyze_market_sentiment(self, news_df: pd.DataFrame) -> Dict[str, Any]:
        """分析市场整体情绪"""
        if news_df.empty:
            return {
                "overall_sentiment_score": 50,
                "sentiment_label": "中性",
                "positive_ratio": 0.5,
                "negative_ratio": 0.5,
                "hot_sectors_mentioned": []
            }
        
        all_scores = []
        all_sectors = []
        positive_count = 0
        negative_count = 0
        
        for _, row in news_df.iterrows():
            content = str(row.get("content", ""))
            score, _, _ = self.analyze_sentiment_score(content)
            all_scores.append(score)
            
            sectors = self.identify_sectors(content)
            all_sectors.extend(sectors)
            
            if score > 55:
                positive_count += 1
            elif score < 45:
                negative_count += 1
        
        avg_score = sum(all_scores) / len(all_scores) if all_scores else 50
        
        # 确定情绪标签
        if avg_score >= 65:
            label = "乐观 📈"
        elif avg_score >= 55:
            label = "偏多 📊"
        elif avg_score >= 45:
            label = "中性 ➖"
        elif avg_score >= 35:
            label = "偏空 📉"
        else:
            label = "悲观 ⚠️"
        
        # 统计热门板块
        sector_counts = {}
        for sector in all_sectors:
            sector_counts[sector] = sector_counts.get(sector, 0) + 1
        
        hot_sectors = sorted(sector_counts.items(), key=lambda x: x[1], reverse=True)[:5]
        
        total = positive_count + negative_count or 1
        
        return {
            "overall_sentiment_score": round(avg_score, 1),
            "sentiment_label": label,
            "positive_ratio": round(positive_count / total, 2),
            "negative_ratio": round(negative_count / total, 2),
            "hot_sectors_mentioned": [s[0] for s in hot_sectors]
        }
    
    def analyze_stock_sentiment(self, code: str, name: str) -> Dict[str, Any]:
        """分析个股舆情"""
        news_list = self.fetch_stock_news(code, name)
        
        if not news_list:
            return {
                "code": code,
                "name": name,
                "sentiment_score": 50,
                "news_count": 0,
                "recent_news": [],
                "related_sectors": []
            }
        
        scores = [n["sentiment_score"] for n in news_list]
        avg_score = sum(scores) / len(scores) if scores else 50
        
        all_sectors = []
        for news in news_list:
            all_sectors.extend(news["sectors"])
        
        return {
            "code": code,
            "name": name,
            "sentiment_score": round(avg_score, 1),
            "news_count": len(news_list),
            "recent_news": news_list[:5],
            "related_sectors": list(set(all_sectors))[:3]
        }


def main():
    print("=" * 80)
    print("                增强版舆情分析系统")
    print("=" * 80)
    
    analyzer = EnhancedSentimentAnalyzer()
    t_total_start = time.perf_counter()
    
    # 1. 获取多源新闻
    print("\n[1/5] 获取市场新闻...")
    df_cailian = analyzer.fetch_news_cailian(limit=30)
    df_eastmoney = analyzer.fetch_news_eastmoney(limit=20)
    
    all_news = []
    if not df_cailian.empty:
        all_news.append(df_cailian)
    if not df_eastmoney.empty:
        all_news.append(df_eastmoney)
    
    df_all_news = pd.concat(all_news, axis=0, ignore_index=True) if all_news else pd.DataFrame()
    print(f"    共获取 {len(df_all_news)} 条新闻")
    
    # 2. 获取北向资金
    print("\n[2/5] 获取资金流向...")
    northbound = analyzer.fetch_northbound_flow()
    
    # 3. 获取热门板块
    print("\n[3/5] 获取热门板块...")
    hot_sectors = analyzer.fetch_hot_sectors()
    
    # 4. 分析市场情绪
    print("\n[4/5] 分析市场情绪...")
    market_sentiment = analyzer.analyze_market_sentiment(df_all_news)
    
    # 5. 输出结果
    print("\n" + "=" * 80)
    print("                     舆情分析报告")
    print("=" * 80)
    
    print(f"\n📊 市场情绪概览:")
    print(f"   整体情绪: {market_sentiment['sentiment_label']}")
    print(f"   情绪分数: {market_sentiment['overall_sentiment_score']}/100")
    print(f"   正面新闻占比: {int(market_sentiment['positive_ratio'] * 100)}%")
    print(f"   负面新闻占比: {int(market_sentiment['negative_ratio'] * 100)}%")
    
    print(f"\n💵 北向资金动向:")
    print(f"   趋势: {northbound['trend']}")
    if northbound['northbound_net_inflow'] is not None:
        print(f"   净买入: {northbound['northbound_net_inflow']} 亿")
        if northbound['sh_connect_inflow'] is not None:
            print(f"   沪股通: {northbound['sh_connect_inflow']} 亿")
        if northbound['sz_connect_inflow'] is not None:
            print(f"   深股通: {northbound['sz_connect_inflow']} 亿")
    
    print(f"\n🔥 热门板块涨幅榜:")
    if hot_sectors:
        for i, sector in enumerate(hot_sectors[:8], 1):
            change = sector['change_pct']
            marker = "📈" if change > 0 else "📉" if change < 0 else "➖"
            print(f"   {i}. {sector['name']}: {change:+.2f}% {marker}")
    
    print(f"\n📰 新闻中提及的热门板块:")
    if market_sentiment['hot_sectors_mentioned']:
        print(f"   {', '.join(market_sentiment['hot_sectors_mentioned'])}")
    
    if not df_all_news.empty:
        print(f"\n📋 最新重要新闻 (情感分析):")
        print("-" * 80)
        
        recent_news = df_all_news.head(10)
        for i, (_, row) in enumerate(recent_news.iterrows(), 1):
            content = str(row.get("content", ""))
            score, pos_kw, neg_kw = analyzer.analyze_sentiment_score(content)
            
            sentiment_marker = "😊" if score >= 60 else "😐" if score >= 40 else "😟"
            
            title = str(row.get("title", ""))[:50] or content[:50]
            source = row.get("source", "未知")
            
            kw_str = ""
            if pos_kw:
                kw_str += f" [+:{','.join(pos_kw[:2])}]"
            if neg_kw:
                kw_str += f" [-:{','.join(neg_kw[:2])}]"
            
            print(f"{i}. [{source}] {title}... {sentiment_marker} (情感分:{score}){kw_str}")
    
    print("\n" + "=" * 80)
    print(f"⏱️  总耗时: {time.perf_counter() - t_total_start:.2f}s")
    print("=" * 80)


if __name__ == "__main__":
    main()
