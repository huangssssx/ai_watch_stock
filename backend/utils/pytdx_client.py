from __future__ import annotations

import atexit
import os
import socket
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional, Tuple, List, Dict, Any

from pytdx.hq import TdxHq_API
# [{'rank': 1, 'ip': '180.153.18.170', 'port': 7709, 'tcp_elapsed_s': 0.027932791000000012, 'confirm_ok': True, 'confirm_elapsed_s': 0.16914895799999985}, {'rank': 2, 'ip': '115.238.56.198', 'port': 7709, 'tcp_elapsed_s': 0.028577917000000064, 'confirm_ok': True, 'confirm_elapsed_s': 0.16183091699999985}, {'rank': 3, 'ip': '115.238.90.165', 'port': 7709, 'tcp_elapsed_s': 0.029971000000000025, 'confirm_ok': True, 'confirm_elapsed_s': 0.191138708}]
DEFAULT_IP = "180.153.18.170"
DEFAULT_PORT = 7709

AUTO_SELECT_IP_ON_FAIL = os.getenv("PYTDX_AUTO_SELECT_IP_ON_FAIL", "1") not in ("0", "false", "False")
AUTO_SELECT_CACHE_TTL_SECONDS = int(os.getenv("PYTDX_AUTO_SELECT_CACHE_TTL_SECONDS", "600"))
AUTO_SELECT_TCP_TIMEOUT_SECONDS = float(os.getenv("PYTDX_AUTO_SELECT_TCP_TIMEOUT_SECONDS", "0.6"))
AUTO_SELECT_MAX_CANDIDATES = int(os.getenv("PYTDX_AUTO_SELECT_MAX_CANDIDATES", "40"))
AUTO_SELECT_WORKERS = int(os.getenv("PYTDX_AUTO_SELECT_WORKERS", "20"))
AUTO_SELECT_CONFIRM_TOP_N = int(os.getenv("PYTDX_AUTO_SELECT_CONFIRM_TOP_N", "3"))

_lock = threading.Lock()
_api_instance: Optional[TdxHq_API] = None
_connected_endpoint: Optional[Tuple[str, int]] = None
_usage_count = 0
_auto_selected_endpoint: Optional[Tuple[str, int]] = None
_auto_selected_at: float = 0.0


def get_api() -> TdxHq_API:
    global _api_instance
    if _api_instance is not None:
        return _api_instance
    with _lock:
        if _api_instance is None:
            _api_instance = TdxHq_API()
        return _api_instance


def _now_ts() -> float:
    return time.monotonic()


def _tcp_probe(ip: str, port: int, timeout: float) -> Optional[float]:
    start = _now_ts()
    try:
        with socket.create_connection((ip, port), timeout=timeout):
            return _now_ts() - start
    except Exception:
        return None


def _iter_builtin_stock_ips(limit: int) -> List[Tuple[str, int]]:
    try:
        from pytdx.util import best_ip as _best_ip
    except Exception:
        return []
    items = getattr(_best_ip, "stock_ip", None)
    if not isinstance(items, list):
        return []
    out: List[Tuple[str, int]] = []
    seen: set[Tuple[str, int]] = set()
    for item in items:
        if not isinstance(item, dict):
            continue
        ip = item.get("ip")
        port = item.get("port")
        if not isinstance(ip, str) or not isinstance(port, int):
            continue
        key = (ip, port)
        if key in seen:
            continue
        seen.add(key)
        out.append(key)
        if len(out) >= max(0, limit):
            break
    return out


def _candidate_endpoints(primary: Tuple[str, int]) -> List[Tuple[str, int]]:
    candidates: List[Tuple[str, int]] = []
    seen: set[Tuple[str, int]] = set()

    def add(ep: Tuple[str, int]):
        if ep in seen:
            return
        seen.add(ep)
        candidates.append(ep)

    add(primary)
    add((DEFAULT_IP, DEFAULT_PORT))
    with _lock:
        if _auto_selected_endpoint is not None:
            add(_auto_selected_endpoint)

    for ep in _iter_builtin_stock_ips(AUTO_SELECT_MAX_CANDIDATES):
        add(ep)
        if len(candidates) >= AUTO_SELECT_MAX_CANDIDATES:
            break
    return candidates


def _try_connect_once(ip: str, port: int) -> Optional[TdxHq_API]:
    api = TdxHq_API()
    ok = False
    try:
        ok = bool(api.connect(ip, port))
        if not ok:
            return None
        data = api.get_security_bars(8, 0, "000001", 0, 1)
        if not data:
            try:
                api.disconnect()
            except Exception:
                pass
            return None
        return api
    except Exception:
        try:
            if ok:
                api.disconnect()
        except Exception:
            pass
        return None


def _select_best_endpoint_fast(primary: Tuple[str, int]) -> Optional[Tuple[str, int]]:
    global _auto_selected_endpoint, _auto_selected_at
    with _lock:
        if _auto_selected_endpoint is not None and (_now_ts() - _auto_selected_at) < AUTO_SELECT_CACHE_TTL_SECONDS:
            return _auto_selected_endpoint

    candidates = _candidate_endpoints(primary)
    if not candidates:
        return None

    results: List[Tuple[float, Tuple[str, int]]] = []
    with ThreadPoolExecutor(max_workers=max(1, AUTO_SELECT_WORKERS)) as pool:
        fut_map = {
            pool.submit(_tcp_probe, ip, port, AUTO_SELECT_TCP_TIMEOUT_SECONDS): (ip, port)
            for ip, port in candidates
        }
        for fut in as_completed(fut_map):
            ep = fut_map[fut]
            try:
                dt = fut.result()
            except Exception:
                dt = None
            if dt is not None:
                results.append((dt, ep))

    if not results:
        return None
    results.sort(key=lambda x: x[0])
    top_eps = [ep for _, ep in results[: max(1, AUTO_SELECT_CONFIRM_TOP_N)]]

    for ip, port in top_eps:
        api = _try_connect_once(ip, port)
        if api is not None:
            with _lock:
                _auto_selected_endpoint = (ip, port)
                _auto_selected_at = _now_ts()
            try:
                api.disconnect()
            except Exception:
                pass
            return (ip, port)
    return None


def connect(ip: str = DEFAULT_IP, port: int = DEFAULT_PORT) -> TdxHq_API:
    global _connected_endpoint
    global _usage_count
    api = get_api()
    target = (ip, port)
    with _lock:
        if _connected_endpoint == target:
            return api
        if (
            AUTO_SELECT_IP_ON_FAIL
            and target == (DEFAULT_IP, DEFAULT_PORT)
            and _connected_endpoint is not None
            and _connected_endpoint == _auto_selected_endpoint
        ):
            return api
        if _connected_endpoint is not None and _connected_endpoint != target:
            if _usage_count > 0:
                raise RuntimeError(
                    f"TdxHq_API 正被使用，禁止切换端点: current={_connected_endpoint}, target={target}"
                )
            try:
                api.disconnect()
            except Exception:
                pass
            _connected_endpoint = None

    try:
        if api.connect(ip, port):
            with _lock:
                _connected_endpoint = target
            return api
    except Exception:
        pass

    tried = [target]
    if AUTO_SELECT_IP_ON_FAIL:
        best = _select_best_endpoint_fast(target)
        if best is not None and best != target:
            tried.append(best)
            try:
                if api.connect(best[0], best[1]):
                    with _lock:
                        _connected_endpoint = best
                    return api
            except Exception:
                pass

    raise RuntimeError(f"TdxHq_API 连接失败: {ip}:{port}, tried={tried}")


def _force_reconnect(ip: str, port: int) -> bool:
    global _connected_endpoint
    api = get_api()
    with _lock:
        try:
            api.disconnect()
        except Exception:
            pass
        finally:
            _connected_endpoint = None

    ok = False
    try:
        ok = bool(api.connect(str(ip), int(port)))
    except Exception:
        ok = False

    if ok:
        with _lock:
            _connected_endpoint = (str(ip), int(port))
    return bool(ok)


def _should_failover_empty(method_name: str, args: tuple, result: Any) -> bool:
    if method_name == "get_security_count":
        try:
            return int(result or 0) <= 0
        except Exception:
            return True

    if method_name == "get_security_bars":
        if result:
            return False
        start = 0
        try:
            start = int(args[3])
        except Exception:
            start = 0
        return start == 0

    if method_name == "get_security_list":
        if result:
            return False
        start = 0
        try:
            start = int(args[1])
        except Exception:
            start = 0
        return start == 0

    return False


def test_connectivity(
    ip: str = DEFAULT_IP,
    port: int = DEFAULT_PORT,
    tcp_timeout_s: float = AUTO_SELECT_TCP_TIMEOUT_SECONDS,
    samples: Optional[List[Tuple[int, str]]] = None,
) -> Dict[str, Any]:
    tcp_elapsed_s = _tcp_probe(str(ip), int(port), float(tcp_timeout_s))
    if tcp_elapsed_s is None:
        return {
            "ok": False,
            "ip": str(ip),
            "port": int(port),
            "tcp_ok": False,
            "tcp_elapsed_s": None,
            "reason": "tcp_failed",
            "samples": [],
        }

    samples = samples or [(0, "000001"), (1, "600000")]
    api = TdxHq_API()
    connected = False
    sample_results: List[Dict[str, Any]] = []
    try:
        connected = bool(api.connect(str(ip), int(port)))
        if not connected:
            return {
                "ok": False,
                "ip": str(ip),
                "port": int(port),
                "tcp_ok": True,
                "tcp_elapsed_s": float(tcp_elapsed_s),
                "reason": "connect_failed",
                "samples": [],
            }

        ok = False
        for market, code in samples:
            err = ""
            rows = 0
            try:
                data = api.get_security_bars(9, int(market), str(code).zfill(6), 0, 1)
                rows = 0 if not data else int(len(data))
                ok = ok or (rows > 0)
            except Exception as e:
                err = f"{type(e).__name__}:{e}"
            sample_results.append({"market": int(market), "code": str(code).zfill(6), "rows": int(rows), "err": str(err)})

        reason = "ok" if ok else "data_empty"
        return {
            "ok": bool(ok),
            "ip": str(ip),
            "port": int(port),
            "tcp_ok": True,
            "tcp_elapsed_s": float(tcp_elapsed_s),
            "reason": str(reason),
            "samples": sample_results,
        }
    finally:
        try:
            if connected:
                api.disconnect()
        except Exception:
            pass


def best_endpoints_top_n(
    ip: str = DEFAULT_IP,
    port: int = DEFAULT_PORT,
    top_n: int = 3,
    tcp_timeout_s: float = AUTO_SELECT_TCP_TIMEOUT_SECONDS,
    confirm: bool = True,
) -> List[Dict[str, Any]]:
    primary = (str(ip), int(port))
    candidates = _candidate_endpoints(primary)
    if not candidates:
        return []

    results: List[Tuple[float, Tuple[str, int]]] = []
    with ThreadPoolExecutor(max_workers=max(1, AUTO_SELECT_WORKERS)) as pool:
        fut_map = {pool.submit(_tcp_probe, ep[0], ep[1], float(tcp_timeout_s)): ep for ep in candidates}
        for fut in as_completed(fut_map):
            ep = fut_map[fut]
            try:
                dt = fut.result()
            except Exception:
                dt = None
            if dt is not None:
                results.append((float(dt), (str(ep[0]), int(ep[1]))))

    if not results:
        return []

    results.sort(key=lambda x: x[0])
    picked = results[: max(1, int(top_n))]
    out: List[Dict[str, Any]] = []
    for rank, (tcp_elapsed_s, (ip_, port_)) in enumerate(picked, start=1):
        row: Dict[str, Any] = {"rank": int(rank), "ip": str(ip_), "port": int(port_), "tcp_elapsed_s": float(tcp_elapsed_s)}
        if bool(confirm):
            t0 = _now_ts()
            api = _try_connect_once(str(ip_), int(port_))
            ok = api is not None
            elapsed_s = _now_ts() - t0
            if api is not None:
                try:
                    api.disconnect()
                except Exception:
                    pass
            row["confirm_ok"] = bool(ok)
            row["confirm_elapsed_s"] = float(elapsed_s)
        out.append(row)
    return out


def disconnect() -> None:
    global _connected_endpoint
    api = get_api()
    with _lock:
        if _connected_endpoint is None:
            return
        try:
            api.disconnect()
        except Exception:
            pass
        finally:
            _connected_endpoint = None


def is_connected() -> bool:
    with _lock:
        return _connected_endpoint is not None


def connected_endpoint() -> Optional[Tuple[str, int]]:
    with _lock:
        return _connected_endpoint


def reset_api() -> None:
    global _api_instance
    global _connected_endpoint
    global _usage_count
    with _lock:
        if _usage_count > 0:
            raise RuntimeError("TdxHq_API 正在使用中，禁止 reset")
        if _connected_endpoint is not None and _api_instance is not None:
            try:
                _api_instance.disconnect()
            except Exception:
                pass
        _connected_endpoint = None
        _api_instance = None


atexit.register(disconnect)

api = get_api()


class _AutoTdxHq:
    def __init__(self, ip: str = DEFAULT_IP, port: int = DEFAULT_PORT):
        self._ip = ip
        self._port = port
        self._entered = False

    def configure(self, ip: str = DEFAULT_IP, port: int = DEFAULT_PORT) -> "_AutoTdxHq":
        self._ip = ip
        self._port = port
        return self

    def _switch_to_best_endpoint(self) -> Optional[Tuple[str, int]]:
        primary = (str(self._ip), int(self._port))
        best = _select_best_endpoint_fast(primary) if AUTO_SELECT_IP_ON_FAIL else None
        if best is None:
            return None
        if _force_reconnect(best[0], best[1]):
            self._ip = str(best[0])
            self._port = int(best[1])
            return (str(best[0]), int(best[1]))
        return None

    def _call_with_failover(self, method_name: str, *args, **kwargs):
        last_exc: Optional[BaseException] = None
        last_result: Any = None
        for attempt in range(2):
            api = connect(self._ip, self._port)
            fn = getattr(api, method_name)
            try:
                res = fn(*args, **kwargs)
                if _should_failover_empty(str(method_name), args, res):
                    last_result = res
                    if attempt == 0 and AUTO_SELECT_IP_ON_FAIL and (self._switch_to_best_endpoint() is not None):
                        continue
                return res
            except Exception as e:
                last_exc = e
                if attempt == 0 and AUTO_SELECT_IP_ON_FAIL and (self._switch_to_best_endpoint() is not None):
                    continue
                raise
        if last_exc is not None:
            raise last_exc
        return last_result

    def __getattr__(self, name: str):
        api = connect(self._ip, self._port)
        attr = getattr(api, name)
        if callable(attr) and str(name) in {"get_security_bars", "get_security_list", "get_security_count"}:
            return lambda *args, **kwargs: self._call_with_failover(str(name), *args, **kwargs)
        return attr

    def __enter__(self) -> "_AutoTdxHq":
        global _usage_count
        connect(self._ip, self._port)
        with _lock:
            _usage_count += 1
        self._entered = True
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        global _usage_count
        if not self._entered:
            return
        self._entered = False
        should_disconnect = False
        with _lock:
            if _usage_count > 0:
                _usage_count -= 1
            if _usage_count == 0:
                should_disconnect = True
        if should_disconnect:
            disconnect()


tdx = _AutoTdxHq()

__all__ = [
    "DEFAULT_IP",
    "DEFAULT_PORT",
    "api",
    "connect",
    "test_connectivity",
    "best_endpoints_top_n",
    "disconnect",
    "is_connected",
    "connected_endpoint",
    "reset_api",
    "get_api",
    "tdx",
]

if __name__ == "__main__":
    print(best_endpoints_top_n())
    print(test_connectivity())
