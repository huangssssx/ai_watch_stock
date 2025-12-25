import akshare as ak
import json
import datetime
from typing import Dict, Any, Optional

class DataFetcher:
    def __init__(self):
        pass

    def _replace_placeholders_in_string(self, text: str, context: Dict[str, Any], today: datetime.date, now: datetime.datetime) -> str:
        import re

        out = text

        if "{symbol}" in out:
            out = out.replace("{symbol}", context.get("symbol", ""))

        if "{now}" in out:
            out = out.replace("{now}", now.strftime("%Y-%m-%d %H:%M:%S"))

        if "{now_time}" in out:
            out = out.replace("{now_time}", now.strftime("%H:%M:%S"))

        if "{today_iso}" in out:
            out = out.replace("{today_iso}", today.strftime("%Y-%m-%d"))

        if "{today_iso-" in out:
            matches = re.findall(r"\{today_iso-(\d+)\}", out)
            for days_str in matches:
                days = int(days_str)
                target_date = today - datetime.timedelta(days=days)
                out = out.replace(f"{{today_iso-{days}}}", target_date.strftime("%Y-%m-%d"))

        if "{today}" in out:
            out = out.replace("{today}", today.strftime("%Y%m%d"))

        if "{today-" in out:
            matches = re.findall(r"\{today-(\d+)\}", out)
            for days_str in matches:
                days = int(days_str)
                target_date = today - datetime.timedelta(days=days)
                out = out.replace(f"{{today-{days}}}", target_date.strftime("%Y%m%d"))

        return out

    def _resolve_placeholders_in_obj(self, obj: Any, context: Dict[str, Any], today: datetime.date, now: datetime.datetime) -> Any:
        if isinstance(obj, str):
            return self._replace_placeholders_in_string(obj, context, today, now)
        if isinstance(obj, list):
            return [self._resolve_placeholders_in_obj(x, context, today, now) for x in obj]
        if isinstance(obj, dict):
            return {k: self._resolve_placeholders_in_obj(v, context, today, now) for k, v in obj.items()}
        return obj

    def _apply_post_process(self, df, python_code: str = None, context: Dict[str, Any] = None):
        """
        Apply Python script to the DataFrame.
        """
        if df is None or df.empty:
            return df
        
        if not python_code or not python_code.strip():
            return df

        try:
            import pandas as pd
            import numpy as np
            
            local_scope = {"df": df, "pd": pd, "np": np, "context": context or {}}
            if context:
                local_scope.update(context)
            
            # Use local_scope as globals to avoid scope issues in list comprehensions
            exec(python_code, local_scope)
            
            # Update df if it was modified in the scope
            if "df" in local_scope:
                new_df = local_scope["df"]
                if isinstance(new_df, pd.DataFrame):
                    df = new_df
            
            return df
        except Exception as e:
            raise ValueError(f"Error executing python_code: {str(e)}")

    def _resolve_params(self, params: Dict[str, Any], context: Dict[str, Any]) -> Dict[str, Any]:
        """
        Replace placeholders in params with context values.
        Context usually contains: 'symbol', 'today'
        Supported placeholders: 
            {symbol}
            {today}, {today-N} -> YYYYMMDD
            {today_iso}, {today_iso-N} -> YYYY-MM-DD
            {now} -> YYYY-MM-DD HH:MM:SS
            {now_time} -> HH:MM:SS
        """
        resolved = {}
        today = datetime.date.today()
        now = datetime.datetime.now()
        
        for k, v in params.items():
            if isinstance(v, str):
                v = self._replace_placeholders_in_string(v, context, today, now)
            
            resolved[k] = v
        return resolved
    
    def _execute_pure_script(self, python_code: str, context: Dict[str, Any]) -> str:
        if not python_code or not python_code.strip():
            return "Error: No script provided for pure script mode."

        try:
            import pandas as pd
            import numpy as np
            import requests
            import json
            import time
            
            local_scope = {
                "ak": ak,
                "pd": pd,
                "np": np,
                "requests": requests,
                "json": json,
                "datetime": datetime,
                "time": time,
                "context": context,
                "df": None,
                "result": None
            }
            if context:
                local_scope.update(context)
            
            exec(python_code, local_scope)
            
            if "df" in local_scope and local_scope["df"] is not None:
                df = local_scope["df"]
                if isinstance(df, pd.DataFrame):
                    if df.empty:
                        return "No data returned (empty DataFrame)."
                    return df.to_json(orient="records", force_ascii=False)
            
            if "result" in local_scope and local_scope["result"] is not None:
                result = local_scope["result"]
                if isinstance(result, (dict, list)):
                    return json.dumps(result, ensure_ascii=False)
                return str(result)
                
            return "Error: Script did not assign 'df' or 'result'."
            
        except Exception as e:
            return f"Error executing script: {str(e)}"

    def fetch(self, api_name: Optional[str], params_json: Optional[str], context: Dict[str, Any], post_process_json: str = None, python_code: str = None) -> str:
        """
        Fetch data from akshare.
        Returns a string representation (JSON) of the data.
        """
        if not api_name:
            return self._execute_pure_script(python_code, context)

        try:
            params = json.loads(params_json or "{}")
            resolved_params = self._resolve_params(params, context)
            
            # Legacy support: if python_code is not provided but post_process_json has python_exec, use it.
            # But we are deprecating post_process_json logic.
            code_to_run = python_code
            if (not code_to_run or not code_to_run.strip()) and post_process_json:
                try:
                    spec = json.loads(post_process_json)
                    if isinstance(spec, dict) and "python_exec" in spec:
                         code_to_run = spec["python_exec"]
                except:
                    pass

            if not hasattr(ak, api_name):
                return f"Error: API {api_name} not found in akshare."
            
            func = getattr(ak, api_name)
            # Call the function with resolved params
            # Note: akshare functions usually take positional args or kwargs. 
            # We assume kwargs here for simplicity as per user requirement "params json".
            
            df = func(**resolved_params)
            df = self._apply_post_process(df, code_to_run, context)
            
            if df is None or df.empty:
                return f"No data returned for {api_name}."
            
            # Return as JSON string for AI to consume
            return df.to_json(orient="records", force_ascii=False)
            
        except Exception as e:
            return f"Error fetching {api_name}: {str(e)}"

data_fetcher = DataFetcher()
