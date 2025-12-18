import akshare as ak
import json
import datetime
from typing import Dict, Any

class DataFetcher:
    def __init__(self):
        pass

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
        import re
        
        for k, v in params.items():
            if isinstance(v, str):
                # Handle {symbol}
                if "{symbol}" in v:
                    v = v.replace("{symbol}", context.get("symbol", ""))
                
                # Handle {now} (YYYY-MM-DD HH:MM:SS)
                if "{now}" in v:
                    v = v.replace("{now}", now.strftime("%Y-%m-%d %H:%M:%S"))
                
                # Handle {now_time} (HH:MM:SS)
                if "{now_time}" in v:
                    v = v.replace("{now_time}", now.strftime("%H:%M:%S"))

                # Handle {today_iso} and {today_iso-N}
                # Must handle ISO first to avoid {today} conflict if names overlap
                if "{today_iso}" in v:
                    v = v.replace("{today_iso}", today.strftime("%Y-%m-%d"))
                
                if "{today_iso-" in v:
                    matches = re.findall(r"\{today_iso-(\d+)\}", v)
                    for days_str in matches:
                        days = int(days_str)
                        target_date = today - datetime.timedelta(days=days)
                        v = v.replace(f"{{today_iso-{days}}}", target_date.strftime("%Y-%m-%d"))

                # Handle {today} and {today-N}
                if "{today}" in v:
                    v = v.replace("{today}", today.strftime("%Y%m%d"))
                
                if "{today-" in v:
                    matches = re.findall(r"\{today-(\d+)\}", v)
                    for days_str in matches:
                        days = int(days_str)
                        target_date = today - datetime.timedelta(days=days)
                        v = v.replace(f"{{today-{days}}}", target_date.strftime("%Y%m%d"))
            
            resolved[k] = v
        return resolved

    def fetch(self, api_name: str, params_json: str, context: Dict[str, Any]) -> str:
        """
        Fetch data from akshare.
        Returns a string representation (CSV or Markdown) of the data.
        """
        try:
            params = json.loads(params_json)
            resolved_params = self._resolve_params(params, context)
            
            if not hasattr(ak, api_name):
                return f"Error: API {api_name} not found in akshare."
            
            func = getattr(ak, api_name)
            # Call the function with resolved params
            # Note: akshare functions usually take positional args or kwargs. 
            # We assume kwargs here for simplicity as per user requirement "params json".
            # Some akshare APIs might strictly require positional args, but most new ones support kwargs or have named args.
            
            df = func(**resolved_params)
            
            if df is None or df.empty:
                return f"No data returned for {api_name}."
            
            # Return as CSV string for AI to consume
            return df.to_csv(index=False)
            
        except Exception as e:
            return f"Error fetching {api_name}: {str(e)}"

data_fetcher = DataFetcher()
