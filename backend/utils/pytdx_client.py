from __future__ import annotations

import atexit
import os
import socket
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional, Tuple, List

from pytdx.hq import TdxHq_API

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

    def __getattr__(self, name: str):
        api = connect(self._ip, self._port)
        return getattr(api, name)

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
    "disconnect",
    "is_connected",
    "connected_endpoint",
    "reset_api",
    "get_api",
    "tdx",
]
