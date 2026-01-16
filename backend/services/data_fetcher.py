from pymr_compat import ensure_py_mini_racer
ensure_py_mini_racer()
import akshare as ak
import json
import datetime
from typing import Dict, Any, Optional

class DataFetcher:
    def __init__(self):
        pass

    def execute_script(self, python_code: str, context: Dict[str, Any]) -> str:
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
        Fetch data using python script.
        Legacy parameters (api_name, params_json, post_process_json) are ignored.
        """
        return self.execute_script(python_code, context)

data_fetcher = DataFetcher()
