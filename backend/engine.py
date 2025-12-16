import requests
import pandas as pd
import numpy as np
import logging
import time
import os
from datetime import datetime

logger = logging.getLogger("StrategyEngine")

class StrategyEngine:
    def __init__(self, strategy_data, akshare_url=None):
        self.strategy = strategy_data
        self.akshare_url = akshare_url or os.getenv("AKSHARE_URL", "http://alaya.zone:3001")
        self.variables = strategy_data.get("variables", {})
        self.symbol = strategy_data.get("symbol")
        self.data_cache = {}
        self.last_run_time = 0
        self._last_error_log_ts = 0
        self._error_suppress_seconds = 20

    def fetch_data(self):
        """Fetch data defined in strategy['data']"""
        data_defs = self.strategy.get("data", [])
        for d in data_defs:
            api_name = d.get("api")
            args = d.get("args", {}).copy()
            # Inject symbol if needed and not present
            if "symbol" not in args and self.symbol:
                args["symbol"] = self.symbol
            
            try:
                # Use GET for simplicity based on user's proxy
                url = f"{self.akshare_url}/akshare/{api_name}"
                # logger.info(f"Fetching {url} with {args}")
                resp = requests.get(url, params=args, timeout=10)
                if resp.status_code == 200:
                    res_json = resp.json()
                    if res_json.get("code") == 200:
                        data = res_json["data"]
                        if isinstance(data, list) and len(data) > 0:
                            df = pd.DataFrame(data)
                            # Basic type conversion
                            if "date" in df.columns:
                                df["date"] = pd.to_datetime(df["date"])
                            # Convert numeric columns automatically
                            for col in df.columns:
                                try:
                                    df[col] = pd.to_numeric(df[col])
                                except:
                                    pass
                            
                            self.data_cache[d["id"]] = df
                        elif isinstance(data, dict):
                             self.data_cache[d["id"]] = data
                    else:
                        now = time.time()
                        if now - self._last_error_log_ts > self._error_suppress_seconds:
                            logger.error(f"API Error {api_name}: {res_json.get('message')}")
                            self._last_error_log_ts = now
                else:
                    now = time.time()
                    if now - self._last_error_log_ts > self._error_suppress_seconds:
                        logger.error(f"HTTP Error {api_name}: {resp.status_code}")
                        self._last_error_log_ts = now
            except Exception as e:
                now = time.time()
                if now - self._last_error_log_ts > self._error_suppress_seconds:
                    logger.error(f"Fetch Error {api_name}: {e}")
                    self._last_error_log_ts = now

    def calculate_indicators(self):
        """Calculate indicators using pandas"""
        indicators = self.strategy.get("indicators", {})
        # Create a local context for eval
        context = {k: v for k, v in self.data_cache.items()}
        
        computed = {}
        for name, expr in indicators.items():
            try:
                # We need to be careful with eval security, but for a personal tool it's acceptable
                # Allow pandas access
                val = eval(expr, {"__builtins__": None}, {"pd": pd, "np": np, **context})
                computed[name] = val
            except Exception as e:
                logger.error(f"Indicator Error {name}: {e}")
                computed[name] = None
        
        return computed

    def run_once(self):
        """Execute one cycle of monitoring"""
        self.fetch_data()
        indicators = self.calculate_indicators()
        
        scenarios = self.strategy.get("scenarios", [])
        alerts = []
        
        # Context for rules: variables + computed indicators + data
        # Flatten data_cache if it holds single row dicts? 
        # For now, just expose them as objects
        context = {**self.variables, **indicators, **self.data_cache}
        
        def set_variable(name, value):
            self.variables[name] = value
            # Note: This only updates the in-memory variables for the current session loop.
            # To persist across restarts, we'd need to save back to DB.
        
        def alert(msg):
            alerts.append({
                "scenario": scenario.get("name"),
                "message": msg,
                "timestamp": datetime.utcnow()
            })
            
        def buy(amount, price):
            # Placeholder for buy action
            pass
            
        def sell(amount):
            # Placeholder for sell action
            pass
            
        def sell_all(msg):
             # Placeholder
             pass

        context["set_variable"] = set_variable
        context["alert"] = alert
        context["buy"] = buy
        context["sell"] = sell
        context["sell_all"] = sell_all
        
        for scenario in scenarios:
            cond = scenario.get("condition")
            if not cond: continue
            
            try:
                # Check condition
                if eval(cond, {"__builtins__": None}, context):
                    action_code = scenario.get("action")
                    if action_code:
                        # Execute action code
                        exec(action_code, {"__builtins__": None}, context)
            except Exception as e:
                # Don't log every eval error to avoid spam if data is missing momentarily
                # logger.error(f"Rule Error {scenario.get('name')}: {e}")
                pass
        
        return alerts
