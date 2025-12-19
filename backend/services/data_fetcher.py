import akshare as ak
import json
import datetime
from typing import Dict, Any

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

    def _apply_post_process(self, df, post_process_json: str):
        def _to_number_series(series):
            import pandas as pd
            s = series
            if getattr(s, "dtype", None) == "object":
                s = s.astype(str).str.replace("%", "", regex=False)
                s = s.astype(str).str.replace(",", "", regex=False)
                s = s.astype(str).str.replace("--", "", regex=False)
                s = s.replace("", pd.NA)
            return pd.to_numeric(s, errors="coerce")

        def _as_number(value):
            if isinstance(value, (int, float)):
                return float(value)
            if isinstance(value, str):
                try:
                    return float(value.replace("%", "").replace(",", "").strip())
                except Exception:
                    return None
            return None

        def _eval_filter_mask(df, spec_value):
            import pandas as pd

            if spec_value is None:
                return pd.Series([True] * len(df), index=df.index)

            if isinstance(spec_value, list):
                mask = pd.Series([True] * len(df), index=df.index)
                for item in spec_value:
                    mask = mask & _eval_filter_mask(df, item)
                return mask

            if not isinstance(spec_value, dict):
                return pd.Series([True] * len(df), index=df.index)

            if "and" in spec_value and isinstance(spec_value["and"], list):
                mask = pd.Series([True] * len(df), index=df.index)
                for item in spec_value["and"]:
                    mask = mask & _eval_filter_mask(df, item)
                return mask

            if "or" in spec_value and isinstance(spec_value["or"], list):
                mask = pd.Series([False] * len(df), index=df.index)
                for item in spec_value["or"]:
                    mask = mask | _eval_filter_mask(df, item)
                return mask

            column = spec_value.get("column")
            op = spec_value.get("op")
            value = spec_value.get("value")
            if not isinstance(column, str) or column not in df.columns:
                return pd.Series([True] * len(df), index=df.index)
            if not isinstance(op, str) or not op.strip():
                return pd.Series([True] * len(df), index=df.index)

            series = df[column]
            op = op.strip()

            if op in ["==", "!=", ">", ">=", "<", "<=", "between"]:
                value_num = _as_number(value)
                series_num = _to_number_series(series)
                if op == "between" and isinstance(value, list) and len(value) == 2:
                    low = _as_number(value[0])
                    high = _as_number(value[1])
                    if low is None or high is None:
                        return pd.Series([True] * len(df), index=df.index)
                    return series_num.between(low, high, inclusive="both")
                if value_num is None:
                    return pd.Series([True] * len(df), index=df.index)
                if op == "==":
                    return series_num == value_num
                if op == "!=":
                    return series_num != value_num
                if op == ">":
                    return series_num > value_num
                if op == ">=":
                    return series_num >= value_num
                if op == "<":
                    return series_num < value_num
                if op == "<=":
                    return series_num <= value_num

            if op == "in":
                if isinstance(value, list):
                    return series.isin(value)
                return pd.Series([True] * len(df), index=df.index)

            if op == "not_in":
                if isinstance(value, list):
                    return ~series.isin(value)
                return pd.Series([True] * len(df), index=df.index)

            if op in ["contains", "not_contains"]:
                needle = "" if value is None else str(value)
                case_sensitive = spec_value.get("case_sensitive", False)
                mask = series.astype(str).str.contains(needle, na=False, case=bool(case_sensitive))
                return ~mask if op == "not_contains" else mask

            if op == "isna":
                return series.isna()

            if op == "notna":
                return series.notna()

            return pd.Series([True] * len(df), index=df.index)

        if post_process_json is None:
            return df
        if isinstance(post_process_json, str) and post_process_json.strip() == "":
            return df
        try:
            spec = json.loads(post_process_json)
        except Exception as e:
            raise ValueError(f"Invalid post_process_json: {str(e)}")

        if spec is None:
            return df
        if not isinstance(spec, dict):
            raise ValueError("post_process_json must be a JSON object")

        rename_columns = spec.get("rename_columns")
        if isinstance(rename_columns, dict) and rename_columns:
            df = df.rename(columns=rename_columns)

        numeric_columns = spec.get("numeric_columns")
        if isinstance(numeric_columns, list) and numeric_columns:
            import pandas as pd
            for col in numeric_columns:
                if col in df.columns:
                    df[col] = df[col].astype(str).str.replace("%", "", regex=False)
                    df[col] = df[col].astype(str).str.replace(",", "", regex=False)
                    df[col] = df[col].astype(str).str.replace("--", "", regex=False)
                    df[col] = df[col].replace("", pd.NA)
                    df[col] = pd.to_numeric(df[col], errors="coerce")

        compute_diff = spec.get("compute_diff")
        if isinstance(compute_diff, list) and compute_diff:
            for item in compute_diff:
                if not isinstance(item, dict):
                    continue
                column = item.get("column")
                new_column = item.get("new_column") or (f"{column}_diff" if column else None)
                periods = item.get("periods", 1)
                if not column or column not in df.columns or not new_column:
                    continue
                try:
                    periods_int = int(periods)
                except Exception:
                    periods_int = 1
                base_series = df[column]
                if base_series.dtype == "object":
                    import pandas as pd
                    base_series = base_series.astype(str).str.replace("%", "", regex=False)
                    base_series = base_series.astype(str).str.replace(",", "", regex=False)
                    base_series = base_series.astype(str).str.replace("--", "", regex=False)
                    base_series = base_series.replace("", pd.NA)
                    base_series = pd.to_numeric(base_series, errors="coerce")
                df[new_column] = base_series.diff(periods=periods_int)

        filter_rows = spec.get("filter_rows")
        if filter_rows is None:
            filter_rows = spec.get("row_filter")
        if filter_rows is not None:
            try:
                mask = _eval_filter_mask(df, filter_rows)
                df = df[mask]
            except Exception as e:
                raise ValueError(f"Invalid filter_rows: {str(e)}")

        sort_by = spec.get("sort_by")
        if isinstance(sort_by, str) and sort_by.strip():
            sort_asc = spec.get("sort_asc", True)
            df = df.sort_values(by=sort_by, ascending=bool(sort_asc))
        elif isinstance(sort_by, list) and sort_by:
            sort_asc = spec.get("sort_asc", True)
            df = df.sort_values(by=sort_by, ascending=bool(sort_asc))

        dropna = spec.get("dropna")
        if dropna is True:
            df = df.dropna()

        select_columns = spec.get("select_columns")
        if isinstance(select_columns, list) and select_columns:
            cols = [c for c in select_columns if isinstance(c, str) and c in df.columns]
            if cols:
                df = df[cols]

        head = spec.get("head")
        if head is not None:
            try:
                n = int(head)
                if n >= 0:
                    df = df.head(n)
            except Exception:
                pass

        tail = spec.get("tail")
        if tail is not None:
            try:
                n = int(tail)
                if n >= 0:
                    df = df.tail(n)
            except Exception:
                pass

        return df

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
    
    def fetch(self, api_name: str, params_json: str, context: Dict[str, Any], post_process_json: str = None) -> str:
        """
        Fetch data from akshare.
        Returns a string representation (JSON) of the data.
        """
        try:
            params = json.loads(params_json)
            resolved_params = self._resolve_params(params, context)
            resolved_post_process_json = post_process_json
            if isinstance(post_process_json, str) and post_process_json.strip():
                today = datetime.date.today()
                now = datetime.datetime.now()
                post_spec = json.loads(post_process_json)
                post_spec = self._resolve_placeholders_in_obj(post_spec, context, today, now)
                resolved_post_process_json = json.dumps(post_spec, ensure_ascii=False)
            
            if not hasattr(ak, api_name):
                return f"Error: API {api_name} not found in akshare."
            
            func = getattr(ak, api_name)
            # Call the function with resolved params
            # Note: akshare functions usually take positional args or kwargs. 
            # We assume kwargs here for simplicity as per user requirement "params json".
            # Some akshare APIs might strictly require positional args, but most new ones support kwargs or have named args.
            
            df = func(**resolved_params)
            df = self._apply_post_process(df, resolved_post_process_json)
            
            if df is None or df.empty:
                return f"No data returned for {api_name}."
            
            # Return as JSON string for AI to consume
            return df.to_json(orient="records", force_ascii=False)
            
        except Exception as e:
            return f"Error fetching {api_name}: {str(e)}"

data_fetcher = DataFetcher()
