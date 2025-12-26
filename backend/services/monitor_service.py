from apscheduler.schedulers.background import BackgroundScheduler
from sqlalchemy.orm import Session
from typing import Optional
from database import SessionLocal
from models import Stock, Log, AIConfig, SystemConfig, RuleScript
from services.data_fetcher import data_fetcher
from services.ai_service import ai_service
from services.alert_service import alert_service
import datetime
import json
import time
import os
import html
import io
import sys
import akshare as ak
import pandas as pd
import numpy as np

scheduler = BackgroundScheduler()
_alert_history_by_stock_id = {}

# Cache for trade day check
_is_trade_day_cache = {"date": None, "is_trade": True}

def _check_is_trade_day():
    today = datetime.date.today()
    if _is_trade_day_cache["date"] == today:
        return _is_trade_day_cache["is_trade"]

    try:
        if today.weekday() >= 5: 
            pass

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
        return True 

def _emit(event: str, payload: dict):
    try:
        print(json.dumps({"event": event, **payload}, ensure_ascii=False, default=str))
    except Exception:
        print(f"{event} {payload}")

def _get_alert_config(db: Session):
    config = db.query(SystemConfig).filter(SystemConfig.key == "alert_rate_limit").first()
    
    result = {
        "enabled": False,
        "max_per_hour_per_stock": 0,
        "allowed_signals": ["STRONG_BUY", "BUY", "SELL", "STRONG_SELL"],
        "allowed_urgencies": ["紧急", "一般", "不紧急"],
        "bypass_rate_limit_for_strong_signals": True
    }

    if config and config.value:
        try:
            data = json.loads(config.value)
            if "enabled" in data: result["enabled"] = bool(data["enabled"])
            if "max_per_hour_per_stock" in data: result["max_per_hour_per_stock"] = int(data["max_per_hour_per_stock"] or 0)
            if "allowed_signals" in data: result["allowed_signals"] = data["allowed_signals"]
            if "allowed_urgencies" in data: result["allowed_urgencies"] = data["allowed_urgencies"]
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

def _infer_signal_from_text(text_value) -> Optional[str]:
    text = str(text_value or "").strip()
    if not text:
        return None
    if any(k in text for k in ["强烈卖出", "强卖", "清仓", "全部卖出"]):
        return "STRONG_SELL"
    if any(k in text for k in ["强烈买入", "强买", "重仓买入"]):
        return "STRONG_BUY"
    if any(k in text for k in ["卖出", "建议卖出", "减仓", "做空", "平仓"]):
        return "SELL"
    if any(k in text for k in ["买入", "建议买入", "做多", "开多"]):
        return "BUY"
    if any(k in text for k in ["观望", "空仓观望", "持币观望", "等待", "继续观望", "谨慎观望"]):
        return "WAIT"
    return None

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

def _execute_rule_script(stock, rule_script):
    if not rule_script or not rule_script.code:
        return False, "No script code", ""
    
    try:
        old_stdout = sys.stdout
        new_stdout = io.StringIO()
        sys.stdout = new_stdout

        local_scope = {
            "ak": ak,
            "pd": pd,
            "np": np,
            "datetime": datetime,
            "time": time,
            "symbol": stock.symbol,
            "triggered": False,
            "message": ""
        }
        
        try:
            exec(rule_script.code, {}, local_scope)
            triggered = bool(local_scope.get("triggered", False))
            message = str(local_scope.get("message", ""))
            signal = local_scope.get("signal", None)
        except Exception as e:
            print(f"Error executing script: {e}")
            triggered = False
            message = f"Error: {e}"
            signal = None
        output_log = new_stdout.getvalue()
        sys.stdout = old_stdout
        return triggered, message, output_log, signal
        
    except Exception as e:
        try:
            sys.stdout = old_stdout
        except Exception:
            pass
        print(f"Error executing rule script for {stock.symbol}: {e}")
        return False, f"Error: {e}", "", None

def _get_last_signal_from_db(stock_id: int, db: Session) -> str:
    """
    Get the last signal from the DB logs for a specific stock.
    Returns "WAIT" if no logs found or signal is missing.
    """
    try:
        last_log = db.query(Log).filter(Log.stock_id == stock_id).order_by(Log.timestamp.desc()).first()
        if last_log and last_log.ai_analysis:
            # ai_analysis is stored as JSON
            analysis = last_log.ai_analysis
            if isinstance(analysis, str):
                 try:
                     analysis = json.loads(analysis)
                 except:
                     pass
            
            if isinstance(analysis, dict):
                signal = analysis.get("signal")
                return _canonicalize_signal(signal)
    except Exception as e:
        print(f"Error fetching last signal for stock {stock_id}: {e}")
    
    return "WAIT"

def process_stock(
    stock_id: int,
    bypass_checks: bool = False,
    send_alerts: bool = True,
    is_test: bool = False,
    return_result: bool = False,
    db: Optional[Session] = None,
):
    start_time_perf = time.time()
    owns_db = db is None
    if db is None:
        db = SessionLocal()
    run_id = f"{stock_id}-{time.time_ns()}"
    try:
        stock = db.query(Stock).filter(Stock.id == stock_id).first()
        if not stock:
            _emit("monitor_skip", {"run_id": run_id, "stock_id": stock_id, "reason": "stock_not_found"})
            if return_result:
                return {
                    "ok": False,
                    "run_id": run_id,
                    "stock_id": stock_id,
                    "stock_symbol": "",
                    "error": "stock_not_found",
                }
            return
        if (not bypass_checks) and (not stock.is_monitoring):
            _emit("monitor_skip", {"run_id": run_id, "stock_id": stock_id, "symbol": stock.symbol, "reason": "monitoring_disabled"})
            if return_result:
                return {
                    "ok": True,
                    "run_id": run_id,
                    "stock_id": stock.id,
                    "stock_symbol": stock.symbol,
                    "monitoring_mode": getattr(stock, "monitoring_mode", "ai_only") or "ai_only",
                    "skipped_reason": "monitoring_disabled",
                }
            return

        # Check trading day
        if (not bypass_checks) and getattr(stock, "only_trade_days", True):
            if not _check_is_trade_day():
                 _emit("monitor_skip", {"run_id": run_id, "stock_id": stock_id, "symbol": stock.symbol, "reason": "not_trade_day"})
                 if return_result:
                     return {
                         "ok": True,
                         "run_id": run_id,
                         "stock_id": stock.id,
                         "stock_symbol": stock.symbol,
                         "monitoring_mode": getattr(stock, "monitoring_mode", "ai_only") or "ai_only",
                         "skipped_reason": "not_trade_day",
                     }
                 return

        # Check schedule
        schedule_str = stock.monitoring_schedule
        if (not bypass_checks) and (not schedule_str):
            schedule_str = json.dumps(
                [
                    {"start": "09:30", "end": "11:30"},
                    {"start": "13:00", "end": "15:00"},
                ]
            )

        is_in_schedule = True
        parsed_schedule = None
        if (not bypass_checks) and schedule_str:
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
                        if return_result:
                            return {
                                "ok": True,
                                "run_id": run_id,
                                "stock_id": stock.id,
                                "stock_symbol": stock.symbol,
                                "monitoring_mode": getattr(stock, "monitoring_mode", "ai_only") or "ai_only",
                                "skipped_reason": "outside_schedule",
                            }
                        return
            except Exception as e:
                print(f"Error checking schedule for {stock.symbol}: {e}")

        # Determine Mode
        monitoring_mode = getattr(stock, "monitoring_mode", "ai_only") or "ai_only"
        
        # Get Last Signal from DB
        last_signal = _get_last_signal_from_db(stock.id, db)
        
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
                "mode": monitoring_mode,
                "last_signal": last_signal
            },
        )

        print(f"Processing stock: {stock.symbol} ({stock.name}) Mode: {monitoring_mode} Last Signal: {last_signal}")

        # Rule Execution (for script_only and hybrid)
        script_triggered = False
        script_msg = ""
        script_log = ""
        script_signal = None
        
        needs_rule = monitoring_mode in ["script_only", "hybrid"]
        needs_ai = monitoring_mode in ["ai_only", "hybrid"]
        
        # In hybrid mode, we might skip AI if rule result matches last signal
        skip_ai_in_hybrid = False 

        if needs_rule:
            if not stock.rule_script_id:
                msg = f"No rule script configured for {monitoring_mode} mode"
                _emit("monitor_skip", {"run_id": run_id, "stock_id": stock_id, "reason": "rule_missing"})
                if is_test:
                    analysis_json = {
                        "type": "error",
                        "signal": "WAIT",
                        "action_advice": "-",
                        "message": msg,
                        "duration": "-",
                        "suggested_position": "-",
                        "stop_loss_price": "-",
                    }
                    log_entry = Log(
                        stock_id=stock.id,
                        raw_data=f"Mode: {monitoring_mode}\nSkip Reason: rule_missing\nRule Script ID: -\nScript Triggered: -\nScript Msg: {msg}\nScript Log:\n",
                        ai_response="",
                        ai_analysis=analysis_json,
                        is_alert=False,
                    )
                    db.add(log_entry)
                    db.commit()
                if return_result:
                    return {
                        "ok": False,
                        "run_id": run_id,
                        "stock_id": stock.id,
                        "stock_symbol": stock.symbol,
                        "monitoring_mode": monitoring_mode,
                        "error": "rule_missing",
                        "script_triggered": False,
                        "script_message": msg,
                        "script_log": "",
                    }
                return

            rule = db.query(RuleScript).filter(RuleScript.id == stock.rule_script_id).first()
            if not rule:
                msg = f"Rule script {stock.rule_script_id} not found"
                _emit("monitor_skip", {"run_id": run_id, "stock_id": stock_id, "reason": "rule_not_found"})
                if is_test:
                    analysis_json = {
                        "type": "error",
                        "signal": "WAIT",
                        "action_advice": "-",
                        "message": msg,
                        "duration": "-",
                        "suggested_position": "-",
                        "stop_loss_price": "-",
                    }
                    log_entry = Log(
                        stock_id=stock.id,
                        raw_data=f"Mode: {monitoring_mode}\nSkip Reason: rule_not_found\nRule Script ID: {stock.rule_script_id}\nScript Triggered: -\nScript Msg: {msg}\nScript Log:\n",
                        ai_response="",
                        ai_analysis=analysis_json,
                        is_alert=False,
                    )
                    db.add(log_entry)
                    db.commit()
                if return_result:
                    return {
                        "ok": False,
                        "run_id": run_id,
                        "stock_id": stock.id,
                        "stock_symbol": stock.symbol,
                        "monitoring_mode": monitoring_mode,
                        "error": "rule_not_found",
                        "script_triggered": False,
                        "script_message": msg,
                        "script_log": "",
                    }
                return

            script_triggered, script_msg, script_log, script_signal = _execute_rule_script(stock, rule)
            
            # Hybrid Mode Logic: Check if we should call AI
            if monitoring_mode == "hybrid":
                # Derive rule signal
                rule_derived_signal = "WAIT"
                if script_signal is not None:
                    rule_derived_signal = _canonicalize_signal(script_signal)
                else:
                    inferred = _infer_signal_from_text(script_msg)
                    if inferred is not None:
                        rule_derived_signal = inferred
                    elif script_triggered:
                        rule_derived_signal = "BUY"
                
                # Compare with last DB signal
                if rule_derived_signal == last_signal:
                    skip_ai_in_hybrid = True
                    # Log skip
                    analysis_json = {
                        "type": "info",
                        "signal": rule_derived_signal,
                        "action_advice": "Rule Consistent with Last Signal",
                        "message": f"Rule signal ({rule_derived_signal}) consistent with last DB signal ({last_signal}). AI Skipped.",
                        "duration": "-",
                        "suggested_position": "-",
                        "stop_loss_price": "-",
                    }
                    log_entry = Log(
                        stock_id=stock.id,
                        raw_data=(
                            f"Mode: {monitoring_mode}\nSkip Reason: signal_consistent\nRule Script ID: {stock.rule_script_id}\n"
                            f"Script Triggered: {script_triggered}\nScript Msg: {script_msg}\nScript Log:\n{(script_log or '')}\n"
                            f"Last DB Signal: {last_signal}\nRule Derived Signal: {rule_derived_signal}"
                        ),
                        ai_response="",
                        ai_analysis=analysis_json,
                        is_alert=False,
                    )
                    db.add(log_entry)
                    db.commit()
                    
                    _emit("monitor_skip", {"run_id": run_id, "stock_id": stock_id, "reason": "signal_consistent", "signal": rule_derived_signal})
                    
                    if return_result:
                        return {
                            "ok": True,
                            "run_id": run_id,
                            "stock_id": stock.id,
                            "stock_symbol": stock.symbol,
                            "monitoring_mode": monitoring_mode,
                            "skipped_reason": "signal_consistent",
                            "script_triggered": script_triggered,
                            "script_message": script_msg,
                            "script_log": script_log,
                            "ai_reply": analysis_json,
                        }
                    return

            # Note: For script_only, we don't return early here if not triggered, 
            # because we might need to log the "WAIT" signal if last signal was "BUY".
            # The original logic skipped logging if not triggered unless error, 
            # but now we need to track signal changes.
            
            # However, if script failed, we still log error
            if not script_triggered and not monitoring_mode == "hybrid":
                 # Check if we need to update signal to WAIT from something else
                 pass 

        # Prepare for Alert/AI
        analysis_json = {}
        raw_response = ""
        full_data = ""
        fetch_ok = 0
        fetch_error = 0
        fetch_errors = []
        ai_duration_ms = 0
        is_alert = False
        prompt_source = ""
        prompt = ""
        data_truncated = False
        max_chars = 0
        data_for_ai = ""
        ai_request_payload_text = ""
        ai_model_name = "-"
        prompt_debug = {"system_prompt": "", "user_prompt": ""}
        ai_base_url = ""

        # SCRIPT ONLY: Bypass AI
        if monitoring_mode == "script_only":
            final_signal = None
            if script_signal is not None:
                final_signal = _canonicalize_signal(script_signal)
            else:
                inferred = _infer_signal_from_text(script_msg)
                if inferred is not None:
                    final_signal = inferred
                elif script_triggered:
                    final_signal = "BUY"
                else:
                    final_signal = "WAIT"

            # Check for signal change
            is_signal_changed = final_signal != last_signal
            
            analysis_json = {
                "type": "warning" if is_signal_changed else "info",
                "signal": final_signal, 
                "action_advice": "Hard Rule Triggered" if script_triggered else "Rule Not Triggered",
                "message": script_msg or ("Rule triggered" if script_triggered else "No trigger"),
                "duration": "Immediate",
                "suggested_position": "Check Manually",
                "stop_loss_price": "-"
            }
            is_alert = is_signal_changed # Alert if signal changed
            raw_response = f"Script Triggered: {script_msg} Signal: {final_signal} Last: {last_signal}"

        # AI ONLY or HYBRID (if triggered and not skipped)
        else:
            if skip_ai_in_hybrid:
                # Should not reach here due to early return above, but safe guard
                pass
            elif needs_ai and (not stock.ai_provider_id):
                msg = f"No AI provider configured for {stock.symbol}"
                print(msg)
                if is_test:
                    analysis_json = {
                        "type": "error",
                        "signal": "WAIT",
                        "action_advice": "-",
                        "message": msg,
                        "duration": "-",
                        "suggested_position": "-",
                        "stop_loss_price": "-",
                    }
                    log_entry = Log(
                        stock_id=stock.id,
                        raw_data=(
                            f"Mode: {monitoring_mode}\nSkip Reason: ai_provider_missing\nRule Script ID: {stock.rule_script_id}\n"
                            f"Script Triggered: {script_triggered if needs_rule else '-'}\nScript Msg: {script_msg}\nScript Log:\n{(script_log or '')}"
                        ),
                        ai_response="",
                        ai_analysis=analysis_json,
                        is_alert=False,
                    )
                    db.add(log_entry)
                    db.commit()
                if return_result:
                    return {
                        "ok": False,
                        "run_id": run_id,
                        "stock_id": stock.id,
                        "stock_symbol": stock.symbol,
                        "monitoring_mode": monitoring_mode,
                        "error": "ai_provider_missing",
                        "script_triggered": script_triggered,
                        "script_message": script_msg,
                        "script_log": script_log,
                    }
                return

            # 1. Fetch Data
            context = {"symbol": stock.symbol, "name": stock.name}
            data_parts = []
            
            for indicator in stock.indicators:
                fetch_start = time.time()
                data = data_fetcher.fetch(indicator.akshare_api, indicator.params_json, context, indicator.post_process_json, indicator.python_code)
                fetch_duration_ms = int((time.time() - fetch_start) * 1000)
                if isinstance(data, str) and data.startswith("Error"):
                    fetch_error += 1
                    fetch_errors.append({"indicator": indicator.name, "api": indicator.akshare_api, "duration_ms": fetch_duration_ms, "error": data[:200]})
                else:
                    fetch_ok += 1
                data_parts.append(f"--- Indicator: {indicator.name} ---\n{data}\n")
            
            full_data = "\n".join(data_parts)

            # 2. AI Analysis
            ai_config = db.query(AIConfig).filter(AIConfig.id == stock.ai_provider_id).first()
            if not ai_config:
                print("AI Config not found")
                if is_test:
                    analysis_json = {
                        "type": "error",
                        "signal": "WAIT",
                        "action_advice": "-",
                        "message": "AI Config not found",
                        "duration": "-",
                        "suggested_position": "-",
                        "stop_loss_price": "-",
                    }
                    log_entry = Log(
                        stock_id=stock.id,
                        raw_data=(
                            f"Mode: {monitoring_mode}\nSkip Reason: ai_config_not_found\nRule Script ID: {stock.rule_script_id}\n"
                            f"Script Triggered: {script_triggered if needs_rule else '-'}\nScript Msg: {script_msg}\nScript Log:\n{(script_log or '')}"
                            f"\nData:\n{(full_data or '')}"
                        ),
                        ai_response="",
                        ai_analysis=analysis_json,
                        is_alert=False,
                    )
                    db.add(log_entry)
                    db.commit()
                if return_result:
                    return {
                        "ok": False,
                        "run_id": run_id,
                        "stock_id": stock.id,
                        "stock_symbol": stock.symbol,
                        "monitoring_mode": monitoring_mode,
                        "error": "ai_config_not_found",
                        "script_triggered": script_triggered,
                        "script_message": script_msg,
                        "script_log": script_log,
                    }
                return

            ai_model_name = ai_config.model_name
            ai_base_url = ai_config.base_url
            config_dict = {
                "api_key": ai_config.api_key,
                "base_url": ai_config.base_url,
                "model_name": ai_config.model_name,
                "temperature": getattr(ai_config, "temperature", 0.1),
            }

            max_chars = ai_config.max_tokens if ai_config.max_tokens else 100000
            data_for_ai = full_data[:max_chars]
            data_truncated = len(full_data) > max_chars

            # Load Prompts
            global_prompt = ""
            global_prompt_config = db.query(SystemConfig).filter(SystemConfig.key == "global_prompt").first()
            if global_prompt_config and global_prompt_config.value:
                try:
                    raw_value = global_prompt_config.value
                    prompt_text = raw_value
                    try:
                        parsed = json.loads(raw_value)
                        if isinstance(parsed, dict):
                            prompt_text = str(parsed.get("prompt_template") or "")
                    except Exception:
                        pass

                    from jinja2 import Template

                    global_prompt = Template(prompt_text).render(symbol=stock.symbol, name=stock.name)
                except Exception:
                    global_prompt = ""

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

            if monitoring_mode == "hybrid":
                prompt += f"\n\n【硬规则触发信息】\n{script_msg}"

            ai_request_time_str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            ai_request_payload = {
                "model": ai_config.model_name,
                "temperature": config_dict.get("temperature", 0.1),
                "response_format": {"type": "json_object"},
                "messages": [
                    {"role": "system", "content": ai_service._build_system_prompt()},
                    {"role": "user", "content": ai_service._build_user_content(data_for_ai, prompt, current_time_str=ai_request_time_str)},
                ],
            }
            ai_request_payload_text = json.dumps(ai_request_payload, ensure_ascii=False, indent=2)

            ai_start = time.time()
            if return_result:
                analysis_json, raw_response, prompt_debug = ai_service.analyze_debug(data_for_ai, prompt, config_dict, current_time_str=ai_request_time_str)
            else:
                analysis_json, raw_response = ai_service.analyze(data_for_ai, prompt, config_dict, current_time_str=ai_request_time_str)
            ai_duration_ms = int((time.time() - ai_start) * 1000)

            signal = analysis_json.get("signal", "WAIT")
            canonical_signal = _canonicalize_signal(signal)
            analysis_json["signal"] = canonical_signal
            is_alert = (analysis_json.get("type") == "warning") or (canonical_signal != "WAIT")

        # 3. Log Result
        signal = analysis_json.get("signal", "WAIT")
        canonical_signal = _canonicalize_signal(signal)
        
        log_entry = Log(
            stock_id=stock.id,
            raw_data=(
                f"Mode: {monitoring_mode}\nRule Script ID: {stock.rule_script_id if needs_rule else '-'}\n"
                f"Script Triggered: {script_triggered if needs_rule else '-'}\nScript Msg: {script_msg}\n"
                f"Script Log:\n{(script_log or '')}\n\n"
                f"AI Request Payload:\n{ai_request_payload_text}"
            ),
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
        
        # Filter 2: Signal Change (Primary Trigger)
        # For script_only, we already calculated is_signal_changed.
        # For ai_only or hybrid, we need to compare canonical_signal with last_signal
        if monitoring_mode == "script_only":
             # Already set in analysis_json construction
             is_signal_changed = is_alert 
        else:
             is_signal_changed = canonical_signal != last_signal
        
        if not is_signal_changed:
            should_send_email = False

        # Filter 3: Urgency
        if should_send_email:
            allowed_urgencies = alert_config.get("allowed_urgencies", ["紧急", "一般", "不紧急"])
            if duration_level not in allowed_urgencies:
                should_send_email = False

        if should_send_email and send_alerts:
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

            max_per_hour_str = os.getenv("ALERT_MAX_PER_HOUR_PER_STOCK", "").strip() if not alert_config.get("enabled") else ""
            max_per_hour_cfg = alert_config.get("max_per_hour_per_stock", 0)
            enabled_cfg = alert_config.get("enabled", False)
            
            bypass_strong = alert_config.get("bypass_rate_limit_for_strong_signals", True)
            bypass_rate_limit = bypass_strong and canonical_signal in ["STRONG_SELL", "STRONG_BUY"]
            
            should_limit = False
            limit_val = 0
            
            if enabled_cfg and max_per_hour_cfg > 0 and not bypass_rate_limit:
                should_limit = True
                limit_val = max_per_hour_cfg
            elif max_per_hour_str and not bypass_rate_limit:
                 try:
                     limit_val = int(max_per_hour_str)
                     if limit_val > 0: should_limit = True
                 except: pass

            if should_limit:
                now_ts = time.time()
                history = _alert_history_by_stock_id.get(stock.id, [])
                history = [t for t in history if now_ts - t < 3600]
                if len(history) >= limit_val:
                    alert_suppressed = True
                    _alert_history_by_stock_id[stock.id] = history
                    _emit(
                        "alert_suppressed",
                        {
                            "run_id": run_id,
                            "stock_id": stock.id,
                            "symbol": stock.symbol,
                            "signal": canonical_signal,
                            "max_per_hour": limit_val,
                            "sent_last_hour": len(history),
                        },
                    )
                else:
                    subject = f"【AI盯盘】{stock.symbol} {stock.name} - {signal_cn}"
                    alert_result = alert_service.send_email(subject=subject, body=email_body, is_html=True)
                    history.append(now_ts)
                    _alert_history_by_stock_id[stock.id] = history
                    print(f"Alert sent for {stock.symbol}")
            else:
                subject = f"【AI盯盘】{stock.symbol} {stock.name} - {signal_cn}"
                alert_result = alert_service.send_email(subject=subject, body=email_body, is_html=True)
                history = _alert_history_by_stock_id.get(stock.id, [])
                history.append(time.time())
                _alert_history_by_stock_id[stock.id] = history
                print(f"Alert sent for {stock.symbol}")
        
        # 5. Cleanup Logs
        if not is_test:
            try:
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
                "ai_called": (monitoring_mode != "script_only") or (monitoring_mode == "hybrid" and script_triggered),
                "ai_model": ai_model_name,
                "ai_duration_ms": ai_duration_ms,
                "signal": canonical_signal,
                "type": analysis_json.get("type"),
                "is_alert": is_alert,
                "alert_attempted": alert_attempted,
                "alert_suppressed": alert_suppressed,
                "alert_result": alert_result,
            },
        )
        if return_result:
            return {
                "ok": True,
                "run_id": run_id,
                "stock_id": stock.id,
                "stock_symbol": stock.symbol,
                "monitoring_mode": monitoring_mode,
                "script_triggered": script_triggered if needs_rule else None,
                "script_message": script_msg if needs_rule else None,
                "script_log": script_log if needs_rule else None,
                "model_name": ai_model_name if monitoring_mode != "script_only" else None,
                "base_url": ai_base_url if monitoring_mode != "script_only" else None,
                "system_prompt": (prompt_debug or {}).get("system_prompt", "") if monitoring_mode != "script_only" else None,
                "user_prompt": (prompt_debug or {}).get("user_prompt", "") if monitoring_mode != "script_only" else None,
                "ai_reply": analysis_json,
                "raw_response": raw_response,
                "data_truncated": data_truncated,
                "data_char_limit": (max_chars if data_truncated else None) if monitoring_mode != "script_only" else None,
                "fetch_ok": fetch_ok,
                "fetch_error": fetch_error,
                "fetch_errors": fetch_errors,
                "ai_duration_ms": ai_duration_ms,
                "is_alert": is_alert,
                "alert_attempted": alert_attempted,
                "alert_suppressed": alert_suppressed,
                "alert_result": alert_result,
                "prompt_source": prompt_source,
            }

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
        if return_result:
            return {
                "ok": False,
                "run_id": run_id,
                "stock_id": stock_id,
                "stock_symbol": "",
                "error": str(e),
            }
    finally:
        if owns_db:
            db.close()

def start_scheduler():
    scheduler.start()
    print("Scheduler started")
    
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
    
    if scheduler.get_job(job_id):
        scheduler.remove_job(job_id)
    
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
