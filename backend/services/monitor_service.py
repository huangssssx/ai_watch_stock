from apscheduler.schedulers.background import BackgroundScheduler
from sqlalchemy.orm import Session
from database import SessionLocal
from models import Stock, Log, AIConfig, SystemConfig
from services.data_fetcher import data_fetcher
from services.ai_service import ai_service
from services.alert_service import alert_service
import datetime
import json
import time

scheduler = BackgroundScheduler()

def process_stock(stock_id: int):
    start_time_perf = time.time()
    db: Session = SessionLocal()
    try:
        stock = db.query(Stock).filter(Stock.id == stock_id).first()
        if not stock or not stock.is_monitoring:
            return

        # Check schedule
        schedule_str = stock.monitoring_schedule
        if not schedule_str:
             # Default schedule
             schedule_str = json.dumps([
                 {"start": "09:30", "end": "11:30"},
                 {"start": "13:00", "end": "15:00"}
             ])

        if schedule_str:
            try:
                schedule = json.loads(schedule_str)
                if isinstance(schedule, list) and len(schedule) > 0:
                    now = datetime.datetime.now().time()
                    is_in_schedule = False
                    for period in schedule:
                        start_str = period.get("start")
                        end_str = period.get("end")
                        if start_str and end_str:
                            try:
                                start_time = datetime.datetime.strptime(start_str, "%H:%M").time()
                                end_time = datetime.datetime.strptime(end_str, "%H:%M").time()
                                if start_time <= now <= end_time:
                                    is_in_schedule = True
                                    break
                            except ValueError:
                                continue
                    
                    if not is_in_schedule:
                        # print(f"Stock {stock.symbol} is outside monitoring schedule. Skipping.")
                        return
            except Exception as e:
                print(f"Error checking schedule for {stock.symbol}: {e}")

        print(f"Processing stock: {stock.symbol} ({stock.name})")

        # 1. Fetch Data
        context = {"symbol": stock.symbol}
        data_parts = []
        for indicator in stock.indicators:
            data = data_fetcher.fetch(indicator.akshare_api, indicator.params_json, context)
            data_parts.append(f"--- Indicator: {indicator.name} ---\n{data}\n")
        
        full_data = "\n".join(data_parts)

        # 2. AI Analysis
        if not stock.ai_provider_id:
            print(f"No AI provider configured for {stock.symbol}")
            return
            
        ai_config = db.query(AIConfig).filter(AIConfig.id == stock.ai_provider_id).first()
        if not ai_config:
            print(f"AI Config not found")
            return

        config_dict = {
            "api_key": ai_config.api_key,
            "base_url": ai_config.base_url,
            "model_name": ai_config.model_name
        }
        
        # Truncate data based on max_tokens config (approx chars)
        max_chars = ai_config.max_tokens if ai_config.max_tokens else 100000
        data_for_ai = full_data[:max_chars]

        # Use stock specific prompt or default
        prompt = stock.prompt_template
        if not prompt:
            global_prompt_config = db.query(SystemConfig).filter(SystemConfig.key == "global_prompt").first()
            if global_prompt_config and global_prompt_config.value:
                try:
                    data = json.loads(global_prompt_config.value)
                    prompt = data.get("prompt_template")
                except:
                    pass
        
        if not prompt:
            prompt = "Analyze the trend."
        
        analysis_json, raw_response = ai_service.analyze(data_for_ai, prompt, config_dict)
        
        # 3. Log Result
        is_alert = analysis_json.get("type") == "warning"
        
        log_entry = Log(
            stock_id=stock.id,
            raw_data=f"Prompt Template: {prompt}\n\nData Context:\n{data_for_ai}", # Store the actual data sent
            ai_response=raw_response,
            ai_analysis=analysis_json,
            is_alert=is_alert
        )
        db.add(log_entry)
        db.commit()

        # 4. Alert
        if is_alert:
            msg = analysis_json.get("message", "No message")
            alert_service.send_email(
                subject=f"Stock Warning: {stock.symbol}",
                body=f"Stock: {stock.symbol} ({stock.name})\nMessage: {msg}\n\nTime: {datetime.datetime.now()}"
            )
            print(f"Alert sent for {stock.symbol}")
        
        duration = time.time() - start_time_perf
        print(f"Finished processing {stock.symbol} in {duration:.2f}s")

    except Exception as e:
        print(f"Error processing stock {stock_id}: {e}")
    finally:
        db.close()

def start_scheduler():
    scheduler.start()
    print("Scheduler started")
    
    # Restore jobs from DB
    db: Session = SessionLocal()
    try:
        stocks = db.query(Stock).filter(Stock.is_monitoring == True).all()
        for stock in stocks:
            update_stock_job(stock.id, stock.interval_seconds, True)
        print(f"Restored {len(stocks)} monitoring jobs")
    except Exception as e:
        print(f"Error restoring jobs: {e}")
    finally:
        db.close()

def update_stock_job(stock_id: int, interval: int, is_monitoring: bool):
    job_id = f"stock_{stock_id}"
    
    # Remove existing job
    if scheduler.get_job(job_id):
        scheduler.remove_job(job_id)
    
    # Add new job if monitoring is on
    if is_monitoring:
        scheduler.add_job(
            process_stock, 
            'interval', 
            seconds=interval, 
            args=[stock_id], 
            id=job_id,
            replace_existing=True,
            max_instances=3,
            misfire_grace_time=120
        )
        print(f"Job added/updated for stock {stock_id} with interval {interval}s")
    else:
        print(f"Job removed for stock {stock_id}")
