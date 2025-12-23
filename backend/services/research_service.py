import akshare as ak
import pandas as pd
import json
import datetime
import traceback
import os

def execute_research_script(script_content: str):
    """
    Executes the research script.
    Expects 'df' (DataFrame) or 'result' (List[Dict]) for table data.
    Expects 'chart' (Dict) for chart options.
    """
    scope = {"ak": ak, "pd": pd, "datetime": datetime}
    scope["today"] = datetime.date.today()
    scope["now"] = datetime.datetime.now()
    
    log_buffer = []
    def log(*args):
        msg = " ".join(map(str, args))
        log_buffer.append(msg)
    
    scope["print"] = log

    os.environ.setdefault("MPLBACKEND", "Agg")
    try:
        import matplotlib
        matplotlib.use("Agg", force=True)
    except Exception:
        pass
    
    try:
        exec(script_content, scope, scope)
        
        result_data = []
        chart_data = None
        
        # 1. Extract Table Data
        if "df" in scope:
            df = scope["df"]
            if isinstance(df, pd.DataFrame):
                # Handle NaN and Dates
                result_data = json.loads(df.to_json(orient="records", force_ascii=False, date_format="iso"))
            elif isinstance(df, list):
                result_data = df
        elif "result" in scope:
            result_data = scope["result"]
            
        # 2. Extract Chart Data
        if "chart" in scope:
            chart_data = scope["chart"]
            
        return True, result_data, chart_data, "\n".join(log_buffer), None

    except Exception as e:
        error_msg = f"{str(e)}\n{traceback.format_exc()}"
        log_buffer.append(error_msg)
        return False, None, None, "\n".join(log_buffer), error_msg
