from pymr_compat import ensure_py_mini_racer
ensure_py_mini_racer()
import akshare as ak
import json
import datetime
from typing import Dict, Any, Optional

class DataFetcher:
    def __init__(self):
        pass

    def execute_script(self, python_code: str, context: Dict[str, Any], df=None) -> str:
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
                "df": df,
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

    def _resolve_placeholders(self, text: str, context: Dict[str, Any]) -> str:
        if text is None:
            return ""
        out = str(text)
        for k, v in (context or {}).items():
            out = out.replace("{" + str(k) + "}", str(v))
        return out

    def _parse_params(self, params_json: Optional[str], context: Dict[str, Any]) -> Dict[str, Any]:
        raw = (params_json or "").strip()
        if not raw:
            return {}
        resolved = self._resolve_placeholders(raw, context)
        try:
            data = json.loads(resolved)
            return data if isinstance(data, dict) else {}
        except Exception:
            return {}

    def _coerce_numeric_series(self, s):
        import pandas as pd
        x = s.astype(str).str.replace(",", "", regex=False)
        x = x.replace({"--": None, "nan": None, "None": None})
        return pd.to_numeric(x, errors="coerce")

    def _build_filter_mask(self, df, spec, context: Dict[str, Any]):
        import pandas as pd
        if spec is None:
            return pd.Series(True, index=df.index)

        if isinstance(spec, dict) and ("and" in spec or "or" in spec):
            if "and" in spec:
                masks = [self._build_filter_mask(df, item, context) for item in spec.get("and") or []]
                m = pd.Series(True, index=df.index)
                for mm in masks:
                    m &= mm
                return m
            masks = [self._build_filter_mask(df, item, context) for item in spec.get("or") or []]
            m = pd.Series(False, index=df.index)
            for mm in masks:
                m |= mm
            return m

        if not isinstance(spec, dict):
            return pd.Series(True, index=df.index)

        col = spec.get("column")
        op = spec.get("op")
        val = spec.get("value")
        if col not in df.columns:
            return pd.Series(False, index=df.index)

        if isinstance(val, str):
            val = self._resolve_placeholders(val, context)
        if isinstance(val, list):
            val = [self._resolve_placeholders(x, context) if isinstance(x, str) else x for x in val]

        series = df[col]
        if op in {">", ">=", "<", "<="}:
            s_num = self._coerce_numeric_series(series)
            try:
                v_num = float(val)
            except Exception:
                v_num = float("nan")
            if op == ">":
                return s_num > v_num
            if op == ">=":
                return s_num >= v_num
            if op == "<":
                return s_num < v_num
            return s_num <= v_num

        if op == "==":
            return series.astype(str) == str(val)
        if op == "!=":
            return series.astype(str) != str(val)
        if op == "contains":
            return series.astype(str).str.contains(str(val), na=False)
        if op == "in":
            values = val if isinstance(val, list) else [val]
            values = [str(x) for x in values]
            return series.astype(str).isin(values)
        return pd.Series(True, index=df.index)

    def _apply_post_process(self, df, post_process_json: Optional[str], context: Optional[Dict[str, Any]] = None):
        import pandas as pd
        if df is None or not isinstance(df, pd.DataFrame):
            return df
        raw = (post_process_json or "").strip()
        if not raw:
            return df
        try:
            spec = json.loads(raw)
        except Exception:
            return df

        out = df.copy()

        filter_spec = spec.get("filter_rows")
        if filter_spec is not None:
            if isinstance(filter_spec, list):
                mask = pd.Series(True, index=out.index)
                for item in filter_spec:
                    mask &= self._build_filter_mask(out, item, context or {})
                out = out[mask]
            else:
                mask = self._build_filter_mask(out, filter_spec, context or {})
                out = out[mask]

        select_cols = spec.get("select_columns")
        if isinstance(select_cols, list) and select_cols:
            keep = [c for c in select_cols if c in out.columns]
            out = out[keep]

        rename_cols = spec.get("rename_columns")
        if isinstance(rename_cols, dict) and rename_cols:
            out = out.rename(columns=rename_cols)

        sort_by = spec.get("sort_by")
        if isinstance(sort_by, dict) and sort_by.get("column") in out.columns:
            ascending = bool(sort_by.get("ascending", True))
            out = out.sort_values(by=sort_by["column"], ascending=ascending)

        head_n = spec.get("head")
        if head_n is not None:
            try:
                out = out.head(int(head_n))
            except Exception:
                pass

        return out

    def fetch(
        self,
        api_name: Optional[str],
        params_json: Optional[str],
        context: Dict[str, Any],
        post_process_json: str = None,
        python_code: str = None,
    ) -> str:
        import pandas as pd

        if api_name:
            fn = getattr(ak, str(api_name), None)
            if fn is None:
                return f"Error fetching {api_name}: api_not_found"

            params = self._parse_params(params_json, context)
            try:
                df = fn(**params)
            except Exception as e:
                return f"Error fetching {api_name}: {str(e)}"

            if df is None or (isinstance(df, pd.DataFrame) and df.empty):
                return "No data returned (empty DataFrame)."

            if isinstance(df, pd.DataFrame) and post_process_json:
                df = self._apply_post_process(df, post_process_json, context=context)

            if python_code and python_code.strip():
                return self.execute_script(python_code, context, df=df)

            if isinstance(df, pd.DataFrame):
                return df.to_json(orient="records", force_ascii=False)
            return json.dumps(df, ensure_ascii=False)

        return self.execute_script(python_code, context, df=None)

data_fetcher = DataFetcher()
