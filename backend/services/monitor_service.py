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
import os

scheduler = BackgroundScheduler()
_alert_history_by_stock_id = {}

def _emit(event: str, payload: dict):
    try:
        print(json.dumps({"event": event, **payload}, ensure_ascii=False, default=str))
    except Exception:
        print(f"{event} {payload}")

def _get_alert_rate_limit(db: Session):
    config = db.query(SystemConfig).filter(SystemConfig.key == "alert_rate_limit").first()
    if not config or not config.value:
        return None
    try:
        data = json.loads(config.value)
        enabled = bool(data.get("enabled", False))
        max_per_hour_per_stock = int(data.get("max_per_hour_per_stock", 0) or 0)
        return {"enabled": enabled, "max_per_hour_per_stock": max_per_hour_per_stock}
    except Exception:
        return None

def process_stock(stock_id: int):
    start_time_perf = time.time()
    db: Session = SessionLocal()
    run_id = f"{stock_id}-{time.time_ns()}"
    try:
        stock = db.query(Stock).filter(Stock.id == stock_id).first()
        if not stock:
            _emit("monitor_skip", {"run_id": run_id, "stock_id": stock_id, "reason": "stock_not_found"})
            return
        if not stock.is_monitoring:
            _emit("monitor_skip", {"run_id": run_id, "stock_id": stock_id, "symbol": stock.symbol, "reason": "monitoring_disabled"})
            return

        # Check schedule
        schedule_str = stock.monitoring_schedule
        if not schedule_str:
             # Default schedule
             schedule_str = json.dumps([
                 {"start": "09:30", "end": "11:30"},
                 {"start": "13:00", "end": "15:00"}
             ])

        is_in_schedule = True
        parsed_schedule = None
        if schedule_str:
            try:
                schedule = json.loads(schedule_str)
                parsed_schedule = schedule
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
                        _emit(
                            "monitor_skip",
                            {
                                "run_id": run_id,
                                "stock_id": stock.id,
                                "symbol": stock.symbol,
                                "reason": "outside_schedule",
                                "now": now.strftime("%H:%M"),
                                "schedule": schedule,
                            },
                        )
                        return
            except Exception as e:
                print(f"Error checking schedule for {stock.symbol}: {e}")

        _emit(
            "monitor_start",
            {
                "run_id": run_id,
                "stock_id": stock.id,
                "symbol": stock.symbol,
                "name": stock.name,
                "interval_seconds": stock.interval_seconds,
                "in_schedule": is_in_schedule,
                "schedule": parsed_schedule,
                "indicators_count": len(stock.indicators or []),
            },
        )

        print(f"Processing stock: {stock.symbol} ({stock.name})")

        # 1. Fetch Data
        context = {"symbol": stock.symbol}
        data_parts = []
        fetch_errors = []
        fetch_ok = 0
        fetch_error = 0
        for indicator in stock.indicators:
            fetch_start = time.time()
            data = data_fetcher.fetch(indicator.akshare_api, indicator.params_json, context, indicator.post_process_json)
            fetch_duration_ms = int((time.time() - fetch_start) * 1000)
            if isinstance(data, str) and data.startswith("Error"):
                fetch_error += 1
                fetch_errors.append({"indicator": indicator.name, "api": indicator.akshare_api, "duration_ms": fetch_duration_ms, "error": data[:200]})
            else:
                fetch_ok += 1
            data_parts.append(f"--- Indicator: {indicator.name} ---\n{data}\n")
        
        full_data = "\n".join(data_parts)

        # 2. AI Analysis
        if not stock.ai_provider_id:
            print(f"No AI provider configured for {stock.symbol}")
            _emit(
                "monitor_finish",
                {
                    "run_id": run_id,
                    "stock_id": stock.id,
                    "symbol": stock.symbol,
                    "duration_ms": int((time.time() - start_time_perf) * 1000),
                    "ai_called": False,
                    "reason": "no_ai_provider",
                    "fetch_ok": fetch_ok,
                    "fetch_error": fetch_error,
                    "fetch_errors": fetch_errors,
                    "data_chars": len(full_data),
                },
            )
            return
            
        ai_config = db.query(AIConfig).filter(AIConfig.id == stock.ai_provider_id).first()
        if not ai_config:
            print(f"AI Config not found")
            _emit(
                "monitor_finish",
                {
                    "run_id": run_id,
                    "stock_id": stock.id,
                    "symbol": stock.symbol,
                    "duration_ms": int((time.time() - start_time_perf) * 1000),
                    "ai_called": False,
                    "reason": "ai_config_not_found",
                    "fetch_ok": fetch_ok,
                    "fetch_error": fetch_error,
                    "fetch_errors": fetch_errors,
                    "data_chars": len(full_data),
                },
            )
            return

        config_dict = {
            "api_key": ai_config.api_key,
            "base_url": ai_config.base_url,
            "model_name": ai_config.model_name
        }
        
        # Truncate data based on max_tokens config (approx chars)
        max_chars = ai_config.max_tokens if ai_config.max_tokens else 100000
        data_for_ai = full_data[:max_chars]
        data_truncated = len(full_data) > max_chars

        # Use stock specific prompt or default
        global_prompt = ""
        global_prompt_config = db.query(SystemConfig).filter(SystemConfig.key == "global_prompt").first()
        if global_prompt_config and global_prompt_config.value:
            try:
                data = json.loads(global_prompt_config.value)
                global_prompt = data.get("prompt_template", "")
            except:
                pass
        
        stock_prompt = stock.prompt_template
        
        prompt = ""
        prompt_source = "system_default"
        
        if global_prompt and stock_prompt:
            prompt = f"{global_prompt}\n\n【个股特别设定/持仓信息】\n{stock_prompt}"
            prompt_source = "global_plus_stock"
        elif stock_prompt:
            prompt = stock_prompt
            prompt_source = "stock_specific"
        elif global_prompt:
            prompt = global_prompt
            prompt_source = "global_setting"
        else:
            prompt = "Analyze the trend."
            prompt_source = "system_default"
        
        ai_start = time.time()
        analysis_json, raw_response = ai_service.analyze(data_for_ai, prompt, config_dict)
        ai_duration_ms = int((time.time() - ai_start) * 1000)
        
        # 3. Log Result
        signal = analysis_json.get("signal", "WAIT")
        # Alert if type is warning OR if signal is not WAIT (i.e. actionable advice)
        is_alert = (analysis_json.get("type") == "warning") or (signal not in ["WAIT", "wait"])
        
        log_entry = Log(
            stock_id=stock.id,
            raw_data=f"Prompt Source: {prompt_source}\nPrompt Template: {prompt}\n\nData Context:\n{data_for_ai}", # Store the actual data sent
            ai_response=raw_response,
            ai_analysis=analysis_json,
            is_alert=is_alert
        )
        db.add(log_entry)
        db.commit()

        # 4. Alert
        alert_attempted = False
        alert_result = None
        alert_suppressed = False
        if is_alert:
            alert_attempted = True
            msg = analysis_json.get("message", "No message")
            action_advice = analysis_json.get("action_advice", "No advice")
            suggested_position = analysis_json.get("suggested_position", "-")
            stop_loss = analysis_json.get("stop_loss_price", "-")
            
            email_body = (
                f"Stock: {stock.symbol} ({stock.name})\n"
                f"Signal: {signal}\n"
                f"Advice: {action_advice}\n"
                f"Position: {suggested_position}\n"
                f"Stop Loss: {stop_loss}\n\n"
                f"Analysis: {msg}\n\n"
                f"Time: {datetime.datetime.now()}"
            )

            alert_rate_limit_config = _get_alert_rate_limit(db)
            max_per_hour_str = os.getenv("ALERT_MAX_PER_HOUR_PER_STOCK", "").strip() if not alert_rate_limit_config else ""
            max_per_hour_cfg = alert_rate_limit_config.get("max_per_hour_per_stock", 0) if alert_rate_limit_config else 0
            enabled_cfg = alert_rate_limit_config.get("enabled", False) if alert_rate_limit_config else False
            bypass_rate_limit = (analysis_json.get("type") == "warning") or (signal in ["STRONG_SELL", "STRONG_BUY"])
            if enabled_cfg and max_per_hour_cfg > 0 and not bypass_rate_limit:
                now_ts = time.time()
                history = _alert_history_by_stock_id.get(stock.id, [])
                history = [t for t in history if now_ts - t < 3600]
                if len(history) >= max_per_hour_cfg:
                    alert_suppressed = True
                    _alert_history_by_stock_id[stock.id] = history
                    _emit(
                        "alert_suppressed",
                        {
                            "run_id": run_id,
                            "stock_id": stock.id,
                            "symbol": stock.symbol,
                            "signal": signal,
                            "max_per_hour": max_per_hour_cfg,
                            "sent_last_hour": len(history),
                        },
                    )
                else:
                    subject = f"Stock Signal [{signal}]: {stock.symbol} {stock.name}"
                    alert_result = alert_service.send_email(subject=subject, body=email_body)
                    history.append(now_ts)
                    _alert_history_by_stock_id[stock.id] = history
                    print(f"Alert sent for {stock.symbol}")
            elif max_per_hour_str:
                try:
                    max_per_hour = int(max_per_hour_str)
                except ValueError:
                    max_per_hour = 0
                if max_per_hour > 0 and not bypass_rate_limit:
                    now_ts = time.time()
                    history = _alert_history_by_stock_id.get(stock.id, [])
                    history = [t for t in history if now_ts - t < 3600]
                    if len(history) >= max_per_hour:
                        alert_suppressed = True
                        _alert_history_by_stock_id[stock.id] = history
                        _emit(
                            "alert_suppressed",
                            {
                                "run_id": run_id,
                                "stock_id": stock.id,
                                "symbol": stock.symbol,
                                "signal": signal,
                                "max_per_hour": max_per_hour,
                                "sent_last_hour": len(history),
                            },
                        )
                    else:
                        subject = f"Stock Signal [{signal}]: {stock.symbol} {stock.name}"
                        alert_result = alert_service.send_email(subject=subject, body=email_body)
                        history.append(now_ts)
                        _alert_history_by_stock_id[stock.id] = history
                        print(f"Alert sent for {stock.symbol}")
                else:
                    subject = f"Stock Signal [{signal}]: {stock.symbol} {stock.name}"
                    alert_result = alert_service.send_email(subject=subject, body=email_body)
                    history = _alert_history_by_stock_id.get(stock.id, [])
                    history.append(time.time())
                    _alert_history_by_stock_id[stock.id] = history
                    print(f"Alert sent for {stock.symbol}")
            else:
                subject = f"Stock Signal [{signal}]: {stock.symbol} {stock.name}"
                alert_result = alert_service.send_email(subject=subject, body=email_body)
                history = _alert_history_by_stock_id.get(stock.id, [])
                history.append(time.time())
                _alert_history_by_stock_id[stock.id] = history
                print(f"Alert sent for {stock.symbol}")
        
        duration = time.time() - start_time_perf
        print(f"Finished processing {stock.symbol} in {duration:.2f}s")
        _emit(
            "monitor_finish",
            {
                "run_id": run_id,
                "stock_id": stock.id,
                "symbol": stock.symbol,
                "duration_ms": int(duration * 1000),
                "fetch_ok": fetch_ok,
                "fetch_error": fetch_error,
                "fetch_errors": fetch_errors,
                "data_chars": len(full_data),
                "data_truncated": data_truncated,
                "ai_called": True,
                "ai_model": ai_config.model_name,
                "ai_duration_ms": ai_duration_ms,
                "signal": signal,
                "type": analysis_json.get("type"),
                "is_alert": is_alert,
                "alert_attempted": alert_attempted,
                "alert_suppressed": alert_suppressed,
                "alert_result": alert_result,
            },
        )

    except Exception as e:
        print(f"Error processing stock {stock_id}: {e}")
        _emit(
            "monitor_error",
            {
                "run_id": run_id,
                "stock_id": stock_id,
                "duration_ms": int((time.time() - start_time_perf) * 1000),
                "error": str(e),
            },
        )
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
