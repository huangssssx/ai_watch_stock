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
        logger.info(f"fetch_data symbol={self.symbol}")
        data_defs = self.strategy.get("data", [])
        for d in data_defs:
            api_name = d.get("api")
            args = d.get("args", {}).copy()
            if "symbol" not in args and self.symbol:
                args["symbol"] = self.symbol
            try:
                logger.info(f"request api={api_name} args={args}")
                url = f"{self.akshare_url}/akshare/{api_name}"
                resp = requests.get(url, params=args, timeout=10)
                if resp.status_code == 200:
                    res_json = resp.json()
                    if res_json.get("code") == 200:
                        data = res_json["data"]
                        if isinstance(data, list) and len(data) > 0:
                            df = pd.DataFrame(data)
                            if "date" in df.columns:
                                df["date"] = pd.to_datetime(df["date"])
                            for col in df.columns:
                                try:
                                    df[col] = pd.to_numeric(df[col])
                                except:
                                    pass
                            self.data_cache[d["id"]] = df
                            logger.info(f"data_cached id={d['id']} rows={len(df)} cols={list(df.columns)[:6]}")
                        elif isinstance(data, dict):
                             self.data_cache[d["id"]] = data
                             logger.info(f"data_cached id={d['id']} keys={list(data.keys())[:6]}")
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
        logger.info("calculate_indicators")
        indicators = self.strategy.get("indicators", {})
        context = {k: v for k, v in self.data_cache.items()}
        computed = {}
        for name, expr in indicators.items():
            try:
                val = eval(expr, {"__builtins__": None}, {"pd": pd, "np": np, **context})
                computed[name] = val
                try:
                    if isinstance(val, (pd.Series, pd.DataFrame)):
                        logger.info(f"indicator {name} type={type(val).__name__}")
                    else:
                        logger.info(f"indicator {name} value={val}")
                except Exception:
                    pass
            except Exception as e:
                logger.error(f"Indicator Error {name}: {e}")
                computed[name] = None
        return computed

    def run_once(self):
        logger.info(f"run_once symbol={self.symbol}")
        self.fetch_data()
        indicators = self.calculate_indicators()
        scenarios = self.strategy.get("scenarios", [])
        alerts = []
        context = {**self.variables, **indicators, **self.data_cache}
        def set_variable(name, value):
            old = self.variables.get(name)
            self.variables[name] = value
            logger.info(f"set_variable {name} {old} -> {value}")
        def alert(msg):
            alerts.append({
                "scenario": scenario.get("name"),
                "message": msg,
                "timestamp": datetime.utcnow()
            })
            logger.info(f"alert scenario={scenario.get('name')} msg={msg}")
        def buy(amount, price):
            pass
        def sell(amount):
            pass
        def sell_all(msg):
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
                logger.info(f"scenario {scenario.get('name')} cond={cond}")
                if eval(cond, {"__builtins__": None}, context):
                    logger.info(f"scenario {scenario.get('name')} matched")
                    action_code = scenario.get("action")
                    if action_code:
                        logger.info(f"action exec for {scenario.get('name')}")
                        safe_globals = {
                            "__builtins__": None,
                            "str": str,
                            "float": float,
                            "int": int,
                            "abs": abs,
                            "max": max,
                            "min": min,
                        }
                        exec(action_code, safe_globals, context)
                else:
                    logger.info(f"scenario {scenario.get('name')} not_matched")
            except Exception as e:
                logger.error(f"scenario error {scenario.get('name')}: {e}")
        return alerts
