from pymr_compat import ensure_py_mini_racer
ensure_py_mini_racer()
from tushare_client import get_pro_client, get_ts_module
import pandas as pd
import numpy as np
import json
import datetime
import traceback
import io
from contextlib import redirect_stdout, redirect_stderr
from sqlalchemy.orm import Session
from database import SessionLocal
from models import StockScreener, ScreenerResult
from services.monitor_service import scheduler

def execute_screener_script(script_content: str):
    """
    Executes the python script.
    The script must define a variable 'df' or return a list of dicts.
    """
    local_scope = {
        "ts": get_ts_module(),
        "pro": get_pro_client(),
        "pd": pd, 
        "datetime": datetime, 
        "np": np, 
        "__name__": "__screener__"
    }
    
    # Helper for simple printing to log
    log_buffer = []
    def log(*args, **kwargs):
        sep = kwargs.get('sep', ' ')
        msg = sep.join(map(str, args))
        # Ignore 'end' and 'file' kwargs as we just append to list
        log_buffer.append(msg)
    
    local_scope["print"] = log
    
    try:
        stdout_buffer = io.StringIO()
        with redirect_stdout(stdout_buffer), redirect_stderr(stdout_buffer):
            exec(script_content, local_scope, local_scope)
        stdout_value = stdout_buffer.getvalue()
        if stdout_value:
            log_buffer.append(stdout_value.rstrip("\n"))
        
        result_data = None
        if "df" in local_scope:
            df = local_scope["df"]
            if isinstance(df, pd.DataFrame):
                # Ensure date handling
                result_data = json.loads(df.to_json(orient="records", force_ascii=False, date_format="iso"))
            elif isinstance(df, list):
                result_data = df
        elif "result" in local_scope:
             result_data = local_scope["result"]
        
        if result_data is None:
             log("Warning: Script did not define 'df' or 'result' variable.")
             result_data = []
             
        return True, result_data, "\n".join(log_buffer)
    except Exception as e:
        error_msg = f"Error: {str(e)}\n{traceback.format_exc()}"
        log_buffer.append(error_msg)
        return False, None, "\n".join(log_buffer)

def run_screener_task(screener_id: int):
    print(f"Running screener {screener_id}")
    db: Session = SessionLocal()
    try:
        screener = db.query(StockScreener).filter(StockScreener.id == screener_id).first()
        if not screener:
            return

        success, data, log = execute_screener_script(screener.script_content)
        
        screener.last_run_at = datetime.datetime.now()
        screener.last_run_status = "success" if success else "failed"
        screener.last_run_log = log
        
        if success:
            data_to_save = data if data is not None else []
            result_entry = ScreenerResult(
                screener_id=screener.id,
                result_json=json.dumps(data_to_save, ensure_ascii=False),
                count=len(data_to_save),
            )
            db.add(result_entry)

            subq = (
                db.query(ScreenerResult.id)
                .filter(ScreenerResult.screener_id == screener_id)
                .order_by(ScreenerResult.run_at.desc())
                .offset(10)
                .all()
            )
            if subq:
                ids_to_delete = [r[0] for r in subq]
                db.query(ScreenerResult).filter(ScreenerResult.id.in_(ids_to_delete)).delete(synchronize_session=False)

        db.commit()
    except Exception as e:
        print(f"Error running screener task {screener_id}: {e}")
    finally:
        db.close()

def update_screener_job(screener_id: int, cron_expression: str, is_active: bool):
    job_id = f"screener_{screener_id}"
    
    if scheduler.get_job(job_id):
        scheduler.remove_job(job_id)
        
    if is_active and cron_expression:
        try:
            # Parse cron expression roughly
            # Expected format: "min hour day month day_of_week"
            parts = cron_expression.strip().split()
            if len(parts) != 5:
                print(f"Invalid cron expression for {screener_id}: {cron_expression}")
                return

            minute, hour, day, month, day_of_week = parts
            
            scheduler.add_job(
                run_screener_task,
                'cron',
                minute=minute,
                hour=hour,
                day=day,
                month=month,
                day_of_week=day_of_week,
                args=[screener_id],
                id=job_id,
                replace_existing=True
            )
            print(f"Scheduled screener {screener_id} with cron {cron_expression}")
        except Exception as e:
            print(f"Failed to schedule screener {screener_id}: {e}")

def restore_screener_jobs():
    print("Restoring screener jobs...")
    db: Session = SessionLocal()
    try:
        screeners = db.query(StockScreener).filter(StockScreener.is_active == True).all()
        for s in screeners:
            update_screener_job(s.id, s.cron_expression, True)
        print(f"Restored {len(screeners)} screener jobs")
    except Exception as e:
        print(f"Error restoring screener jobs: {e}")
    finally:
        db.close()
