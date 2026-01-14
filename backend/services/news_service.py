import akshare as ak
import pandas as pd
import json
import datetime
from sqlalchemy.orm import Session
from models import StockNews, SentimentAnalysis, AIConfig
from services.ai_service import ai_service
import traceback

class NewsService:
    def fetch_market_news(self, db: Session, limit: int = 50):
        """
        Fetch latest market news using akshare.
        Prioritizes Cailian Press (CLS) telegraphs for real-time sentiment.
        """
        try:
            print("Fetching news from akshare (stock_info_global_cls)...")
            # 财联社电报 - 全球/A股
            try:
                df = ak.stock_info_global_cls()
            except AttributeError:
                # Fallback or different API name depending on akshare version
                try:
                    df = ak.stock_zh_a_alert()
                except:
                    print("Failed to fetch news from stock_info_global_cls and stock_zh_a_alert")
                    return 0
            
            if df is None or df.empty:
                print("No news fetched.")
                return 0
            
            # Normalize columns
            # stock_info_global_cls usually has: '发布时间', '内容', '标题'
            # stock_zh_a_alert usually has: '时间', '标题', '内容'
            
            # Map to standard names
            col_map = {
                '时间': 'publish_time',
                '发布时间': 'publish_time',
                '标题': 'title',
                '内容': 'content'
            }
            df = df.rename(columns=col_map)
            
            count = 0
            # Ensure sorting
            if 'publish_time' in df.columns:
                df['publish_time'] = pd.to_datetime(df['publish_time'], errors='coerce')
                df = df.sort_values(by='publish_time', ascending=False)
            
            for _, row in df.head(limit).iterrows():
                title = row.get('title', '')
                content = row.get('content', '')
                
                # Some APIs don't have title, use start of content
                if not title and content:
                    title = content[:30] + "..."
                
                pub_time = row.get('publish_time', datetime.datetime.now())
                if pd.isna(pub_time):
                    pub_time = datetime.datetime.now()
                
                if not content:
                    continue
                    
                # Check duplication
                exists = db.query(StockNews).filter(
                    StockNews.content.like(f"{content[:20]}%")
                ).first()
                
                if not exists:
                    news = StockNews(
                        title=title,
                        content=content,
                        source="CLS/EastMoney",
                        publish_time=pub_time,
                        created_at=datetime.datetime.now()
                    )
                    db.add(news)
                    count += 1
            
            db.commit()
            print(f"Saved {count} new news items.")
            return count
        except Exception as e:
            print(f"Error fetching news: {e}")
            traceback.print_exc()
            return 0

    def analyze_sentiment(self, db: Session, ai_config_id: int = None, custom_prompt: str = None):
        """
        Analyze recent news for sentiment and policy trends.
        """
        # 1. Get AI Config
        if ai_config_id:
            ai_config = db.query(AIConfig).filter(AIConfig.id == ai_config_id).first()
        else:
            ai_config = db.query(AIConfig).filter(AIConfig.is_active == True).first()
        
        if not ai_config:
            return {"ok": False, "error": "No active AI config found"}

        config_dict = {
            "api_key": ai_config.api_key,
            "base_url": ai_config.base_url,
            "model_name": ai_config.model_name,
            "temperature": ai_config.temperature
        }

        # 2. Get Recent News (last 24 hours)
        yesterday = datetime.datetime.now() - datetime.timedelta(hours=24)
        recent_news = db.query(StockNews).filter(
            StockNews.created_at >= yesterday
        ).order_by(StockNews.publish_time.desc()).limit(30).all()
        
        if not recent_news:
            # Fallback: if no news in last 24h (maybe dev env), take last 10 regardless of time
            recent_news = db.query(StockNews).order_by(StockNews.publish_time.desc()).limit(10).all()
            if not recent_news:
                return {"ok": False, "error": "No recent news to analyze"}

        # 3. Construct Prompt
        news_text_list = []
        for n in recent_news:
            t_str = n.publish_time.strftime("%H:%M") if n.publish_time else ""
            content_snippet = n.content[:150].replace("\n", " ") # Truncate long news
            news_text_list.append(f"- [{t_str}] {content_snippet}")
        
        news_block = "\n".join(news_text_list)
        
        extra_instructions = (custom_prompt or "").strip()
        extra_block = f"\n\nUser Custom Requirements:\n{extra_instructions}\n" if extra_instructions else ""

        prompt = f"""
        You are a financial news analyst. Analyze the following latest market news:
        
        {news_block}
        {extra_block}
        
        Analysis Requirements:
        1. Market Sentiment Score: -1.0 (Panic) to 1.0 (Euphoria).
        2. Policy Trend: Summarize key policy directions or regulatory attitudes.
        3. Trading Signal: BUY / SELL / WAIT based on news sentiment.
        4. Hot Sectors: List promising sectors.
        5. Summary: Brief market outlook.
        
        Output strictly in JSON format:
        {{
            "sentiment_score": 0.5,
            "policy_orientation": "...",
            "trading_signal": "WAIT",
            "summary": "...",
            "hot_sectors": ["sector1", "sector2"]
        }}
        """

        # 4. Call AI
        print(f"Calling AI ({ai_config.model_name}) for sentiment analysis...")
        raw_response = ai_service.analyze_raw("", prompt, config_dict)
        
        # 5. Parse and Save
        try:
            # Clean markdown
            clean_json = raw_response
            if "```json" in clean_json:
                clean_json = clean_json.split("```json")[1].split("```")[0].strip()
            elif "```" in clean_json:
                clean_json = clean_json.split("```")[1].split("```")[0].strip()
            
            data = json.loads(clean_json)
            
            analysis = SentimentAnalysis(
                target_type="market",
                target_value="global",
                sentiment_score=float(data.get("sentiment_score", 0.0)),
                policy_orientation=data.get("policy_orientation", ""),
                trading_signal=data.get("trading_signal", "WAIT"),
                summary=data.get("summary", ""),
                raw_response=raw_response,
                ai_provider_id=ai_config.id,
                timestamp=datetime.datetime.now()
            )
            db.add(analysis)
            db.commit()
            db.refresh(analysis)
            
            return {
                "ok": True,
                "data": data,
                "analysis_id": analysis.id
            }
        except Exception as e:
            print(f"AI Parse Error: {e}")
            return {"ok": False, "error": f"Failed to parse AI response: {e}", "raw": raw_response}

    def analyze_news_raw(self, db: Session, ai_config_id: int = None, custom_prompt: str = None, limit: int = 30):
        if ai_config_id:
            ai_config = db.query(AIConfig).filter(AIConfig.id == ai_config_id).first()
        else:
            ai_config = db.query(AIConfig).filter(AIConfig.is_active == True).first()
        if not ai_config:
            return {"ok": False, "error": "No active AI config found"}

        config_dict = {
            "api_key": ai_config.api_key,
            "base_url": ai_config.base_url,
            "model_name": ai_config.model_name,
            "temperature": getattr(ai_config, "temperature", 0.7),
        }

        recent_news = db.query(StockNews).order_by(StockNews.publish_time.desc()).limit(limit).all()
        if not recent_news:
            return {"ok": False, "error": "No news available"}

        parts = []
        for n in recent_news:
            t = n.publish_time.strftime("%Y-%m-%d %H:%M") if n.publish_time else ""
            title = (n.title or "").strip()
            content = (n.content or "").strip().replace("\n", " ")
            src = (n.source or "").strip()
            snippet = content[:300]
            line = f"- [{t}] {title} ({src})\n  {snippet}"
            parts.append(line)
        news_block = "新闻列表：\n" + "\n".join(parts)
        raw_response = ai_service.analyze_raw(news_block, (custom_prompt or "").strip(), config_dict)
        return {"ok": True, "raw": raw_response}

news_service = NewsService()
