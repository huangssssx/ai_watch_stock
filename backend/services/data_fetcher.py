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
        Supported placeholders: {symbol}, {today}, {today-N}
        """
        resolved = {}
        today = datetime.date.today()
        
        for k, v in params.items():
            if isinstance(v, str):
                # Handle {symbol}
                if "{symbol}" in v:
                    v = v.replace("{symbol}", context.get("symbol", ""))
                
                # Handle {today} and {today-N}
                # Simple parsing logic
                if "{today}" in v:
                    v = v.replace("{today}", today.strftime("%Y%m%d"))
                
                if "{today-" in v:
                    # simplistic parser for {today-20}
                    import re
                    match = re.search(r"\{today-(\d+)\}", v)
                    if match:
                        days = int(match.group(1))
                        target_date = today - datetime.timedelta(days=days)
                        v = v.replace(match.group(0), target_date.strftime("%Y%m%d"))
            
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
