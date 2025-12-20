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
import html
import akshare as ak

scheduler = BackgroundScheduler()
_alert_history_by_stock_id = {}
_last_alert_content_by_stock_id = {}

# Cache for trade day check
_is_trade_day_cache = {"date": None, "is_trade": True}

def _check_is_trade_day():
    today = datetime.date.today()
    if _is_trade_day_cache["date"] == today:
        return _is_trade_day_cache["is_trade"]

    try:
        # Check if it's weekend first (simple heuristic)
        if today.weekday() >= 5: # Saturday=5, Sunday=6
            # Still check akshare in case of special workdays, but akshare is authoritative
            pass

        # Use akshare tool_trade_date_hist_sina or similar
        # tool_trade_date_hist_sina returns a dataframe with trade_date column
        # This API might be heavy, so caching is crucial.
        # Alternatively, use a simpler API if available. 
        # A common way is to check if today's date is in the list of recent trading dates.
        
        # Let's try fetching recent trade dates.
        # This returns all history, might be big.
        # Optimisation: Check basic weekend first? No, holidays exist.
        
        # Using tool_trade_date_hist_sina() returns all dates.
        # Let's verify with a lightweight call if possible.
        # For now, we will rely on akshare.tool_trade_date_hist_sina() 
        # but filter for current year to speed up if possible? No param.
        
        # Fallback to simple weekday check if API fails
        try:
            trade_dates_df = ak.tool_trade_date_hist_sina()
            trade_dates_list = trade_dates_df['trade_date'].astype(str).tolist()
            today_str = today.strftime("%Y-%m-%d")
            is_trade = today_str in trade_dates_list
            _is_trade_day_cache["date"] = today
            _is_trade_day_cache["is_trade"] = is_trade
            print(f"Trade day check for {today_str}: {is_trade}")
            return is_trade
        except Exception as e:
            print(f"Akshare trade date check failed: {e}. Fallback to weekday check.")
            is_weekday = today.weekday() < 5
            _is_trade_day_cache["date"] = today
            _is_trade_day_cache["is_trade"] = is_weekday
            return is_weekday

    except Exception as e:
        print(f"Error checking trade day: {e}")
        return True # Default to True on error to not miss monitoring

def _emit(event: str, payload: dict):
    try:
        print(json.dumps({"event": event, **payload}, ensure_ascii=False, default=str))
    except Exception:
        print(f"{event} {payload}")

def _get_alert_config(db: Session):
    config = db.query(SystemConfig).filter(SystemConfig.key == "alert_rate_limit").first()
    
    # Default config
    result = {
        "enabled": False,
        "max_per_hour_per_stock": 0,
        "allowed_signals": ["STRONG_BUY", "BUY", "SELL", "STRONG_SELL"],
        "allowed_urgencies": ["紧急", "一般", "不紧急"],
        "suppress_duplicates": True,
        "bypass_rate_limit_for_strong_signals": True
    }

    if config and config.value:
        try:
            data = json.loads(config.value)
            # Merge with defaults
            if "enabled" in data: result["enabled"] = bool(data["enabled"])
            if "max_per_hour_per_stock" in data: result["max_per_hour_per_stock"] = int(data["max_per_hour_per_stock"] or 0)
            if "allowed_signals" in data: result["allowed_signals"] = data["allowed_signals"]
            if "allowed_urgencies" in data: result["allowed_urgencies"] = data["allowed_urgencies"]
            if "suppress_duplicates" in data: result["suppress_duplicates"] = bool(data["suppress_duplicates"])
            if "bypass_rate_limit_for_strong_signals" in data: result["bypass_rate_limit_for_strong_signals"] = bool(data["bypass_rate_limit_for_strong_signals"])
        except Exception as e:
            print(f"Error parsing alert config: {e}")
            
    return result

def _canonicalize_signal(signal_value) -> str:
    if signal_value is None:
        return "WAIT"
    text = str(signal_value).strip()
    if not text:
        return "WAIT"

    upper = text.upper()
    if upper in ["STRONG_BUY", "BUY", "WAIT", "SELL", "STRONG_SELL"]:
        return upper

    if text in {"观望", "空仓观望", "持币观望", "等待", "继续观望", "谨慎观望"}:
        return "WAIT"
    if text in {"强烈买入", "强买", "重仓买入"}:
        return "STRONG_BUY"
    if text in {"买入", "建议买入", "做多", "开多"}:
        return "BUY"
    if text in {"强烈卖出", "强卖", "清仓", "全部卖出"}:
        return "STRONG_SELL"
    if text in {"卖出", "建议卖出", "减仓", "做空", "平仓"}:
        return "SELL"

    return upper

def _signal_display(canonical_signal: str):
    mapping = {
        "STRONG_BUY": ("强烈买入", "#16A34A"),
        "BUY": ("买入", "#16A34A"),
        "WAIT": ("观望", "#6B7280"),
        "SELL": ("卖出", "#DC2626"),
        "STRONG_SELL": ("强烈卖出", "#DC2626"),
    }
    return mapping.get(canonical_signal, (canonical_signal, "#111827"))

def _duration_severity(duration_text: str):
    text = (duration_text or "").strip()
    if not text or text == "-":
        return ("一般", "#D97706")
    urgent_keywords = ["立即", "马上", "立刻", "分钟", "分", "当日", "今天", "T+0", "T0", "T+1", "T1", "短线"]
    medium_keywords = ["1天", "2天", "3天", "几天", "日内", "本周", "3-5天", "一周内"]
    low_keywords = ["两周", "2周", "几周", "周", "月", "季度", "中线", "长线"]
    if any(k in text for k in urgent_keywords):
        return ("紧急", "#DC2626")
    if any(k in text for k in low_keywords):
        return ("不紧急", "#6B7280")
    if any(k in text for k in medium_keywords):
        return ("一般", "#D97706")
    return ("一般", "#D97706")

def _to_html(text_value):
    text = "" if text_value is None else str(text_value)
    return html.escape(text).replace("\n", "<br/>")

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

        # Check trading day
        if getattr(stock, 'only_trade_days', True):
            if not _check_is_trade_day():
                 _emit("monitor_skip", {"run_id": run_id, "stock_id": stock_id, "symbol": stock.symbol, "reason": "not_trade_day"})
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
        context = {"symbol": stock.symbol, "name": stock.name}
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
            "model_name": ai_config.model_name,
            "temperature": getattr(ai_config, "temperature", 0.1)
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
        canonical_signal = _canonicalize_signal(signal)
        analysis_json["signal"] = canonical_signal
        # Alert if type is warning OR if signal is not WAIT (i.e. actionable advice)
        is_alert = (analysis_json.get("type") == "warning") or (canonical_signal != "WAIT")
        
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

        # Load Alert Config
        alert_config = _get_alert_config(db)
        
        # Prepare variables for filtering and email
        msg = analysis_json.get("message", "No message")
        action_advice = analysis_json.get("action_advice", "No advice")
        suggested_position = analysis_json.get("suggested_position", "-")
        duration_text = analysis_json.get("duration", "-")
        stop_loss = analysis_json.get("stop_loss_price", "-")
        now_dt = datetime.datetime.now()
        signal_cn, signal_color = _signal_display(canonical_signal)
        duration_level, duration_color = _duration_severity(duration_text)

        # Filter 1: Allowed Signals
        allowed_signals = alert_config.get("allowed_signals", ["BUY", "SELL", "STRONG_BUY", "STRONG_SELL"])
        should_send_email = canonical_signal in allowed_signals
        
        alert_content_key = f"{canonical_signal}-{(action_advice or '').strip()}"
        
        # Filter 2: Urgency
        if should_send_email:
            allowed_urgencies = alert_config.get("allowed_urgencies", ["紧急", "一般", "不紧急"])
            if duration_level not in allowed_urgencies:
                should_send_email = False
                # print(f"Alert suppressed due to urgency level '{duration_level}'")

        # Filter 3: Deduplication
        if should_send_email and alert_config.get("suppress_duplicates", True):
            last_content = _last_alert_content_by_stock_id.get(stock.id)
            if last_content == alert_content_key:
                should_send_email = False
                print(f"Duplicate alert suppressed for {stock.symbol}")

        if should_send_email:
            alert_attempted = True
            
            email_body = f"""
<div style="font-family:-apple-system,BlinkMacSystemFont,Segoe UI,Roboto,Helvetica,Arial; font-size:14px; color:#111827; line-height:1.6;">
  <div style="font-size:18px; font-weight:700; margin-bottom:12px;">AI 盯盘提醒</div>
  <div style="margin-bottom:12px;">
    <span style="color:#6B7280;">股票</span>：<span style="font-weight:700;">{_to_html(stock.symbol)}</span> {_to_html(stock.name)}
  </div>
  <table style="border-collapse:collapse; width:100%; max-width:720px;">
    <tr>
      <td style="padding:8px 10px; border:1px solid #E5E7EB; width:140px; color:#374151;">信号</td>
      <td style="padding:8px 10px; border:1px solid #E5E7EB;">
        <span style="color:{signal_color}; font-weight:800;">{_to_html(signal_cn)}</span>
        <span style="color:#6B7280;">（{_to_html(canonical_signal)}）</span>
      </td>
    </tr>
    <tr>
      <td style="padding:8px 10px; border:1px solid #E5E7EB; color:#374151;">操作建议</td>
      <td style="padding:8px 10px; border:1px solid #E5E7EB;">{_to_html(action_advice)}</td>
    </tr>
    <tr>
      <td style="padding:8px 10px; border:1px solid #E5E7EB; color:#374151;">建议仓位</td>
      <td style="padding:8px 10px; border:1px solid #E5E7EB;">{_to_html(suggested_position)}</td>
    </tr>
    <tr>
      <td style="padding:8px 10px; border:1px solid #E5E7EB; color:#374151;">建议持仓时间</td>
      <td style="padding:8px 10px; border:1px solid #E5E7EB;">
        <span style="color:{duration_color}; font-weight:800;">{_to_html(duration_text)}</span>
        <span style="color:{duration_color};">（{_to_html(duration_level)}）</span>
      </td>
    </tr>
    <tr>
      <td style="padding:8px 10px; border:1px solid #E5E7EB; color:#374151;">止损价</td>
      <td style="padding:8px 10px; border:1px solid #E5E7EB;">{_to_html(stop_loss)}</td>
    </tr>
    <tr>
      <td style="padding:8px 10px; border:1px solid #E5E7EB; color:#374151;">分析摘要</td>
      <td style="padding:8px 10px; border:1px solid #E5E7EB;">{_to_html(msg)}</td>
    </tr>
  </table>
  <div style="margin-top:12px; color:#6B7280;">触发时间：{_to_html(now_dt.strftime("%Y-%m-%d %H:%M:%S"))}</div>
</div>
""".strip()

            # Use loaded alert_config
            max_per_hour_str = os.getenv("ALERT_MAX_PER_HOUR_PER_STOCK", "").strip() if not alert_config.get("enabled") else ""
            max_per_hour_cfg = alert_config.get("max_per_hour_per_stock", 0)
            enabled_cfg = alert_config.get("enabled", False)
            
            bypass_strong = alert_config.get("bypass_rate_limit_for_strong_signals", True)
            bypass_rate_limit = bypass_strong and canonical_signal in ["STRONG_SELL", "STRONG_BUY"]
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
                            "signal": canonical_signal,
                            "max_per_hour": max_per_hour_cfg,
                            "sent_last_hour": len(history),
                        },
                    )
                else:
                    subject = f"【AI盯盘】{stock.symbol} {stock.name} - {signal_cn}"
                    alert_result = alert_service.send_email(subject=subject, body=email_body, is_html=True)
                    history.append(now_ts)
                    _alert_history_by_stock_id[stock.id] = history
                    _last_alert_content_by_stock_id[stock.id] = alert_content_key
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
                            "signal": canonical_signal,
                            "max_per_hour": max_per_hour,
                            "sent_last_hour": len(history),
                        },
                    )
                    else:
                        subject = f"【AI盯盘】{stock.symbol} {stock.name} - {signal_cn}"
                        alert_result = alert_service.send_email(subject=subject, body=email_body, is_html=True)
                        history.append(now_ts)
                        _alert_history_by_stock_id[stock.id] = history
                        _last_alert_content_by_stock_id[stock.id] = alert_content_key
                        print(f"Alert sent for {stock.symbol}")
                else:
                    subject = f"【AI盯盘】{stock.symbol} {stock.name} - {signal_cn}"
                    alert_result = alert_service.send_email(subject=subject, body=email_body, is_html=True)
                    history = _alert_history_by_stock_id.get(stock.id, [])
                    history.append(time.time())
                    _alert_history_by_stock_id[stock.id] = history
                    _last_alert_content_by_stock_id[stock.id] = alert_content_key
                    print(f"Alert sent for {stock.symbol}")
            else:
                subject = f"【AI盯盘】{stock.symbol} {stock.name} - {signal_cn}"
                alert_result = alert_service.send_email(subject=subject, body=email_body, is_html=True)
                history = _alert_history_by_stock_id.get(stock.id, [])
                history.append(time.time())
                _alert_history_by_stock_id[stock.id] = history
                _last_alert_content_by_stock_id[stock.id] = alert_content_key
                print(f"Alert sent for {stock.symbol}")
        
        # 5. Cleanup Logs
        try:
            # Keep only last 3 days of logs for this stock
            cutoff_date = datetime.datetime.now() - datetime.timedelta(days=3)
            deleted_count = db.query(Log).filter(Log.stock_id == stock.id, Log.timestamp < cutoff_date).delete()
            if deleted_count > 0:
                print(f"Cleaned up {deleted_count} old logs for {stock.symbol}")
                db.commit()
        except Exception as e:
            print(f"Error cleaning up logs: {e}")

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
                "signal": canonical_signal,
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
