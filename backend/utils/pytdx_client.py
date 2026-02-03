from __future__ import annotations

import atexit
import threading
from typing import Optional, Tuple

from pytdx.hq import TdxHq_API

DEFAULT_IP = "180.153.18.170"
DEFAULT_PORT = 7709

_lock = threading.Lock()
_api_instance: Optional[TdxHq_API] = None
_connected_endpoint: Optional[Tuple[str, int]] = None
_usage_count = 0


def get_api() -> TdxHq_API:
    global _api_instance
    if _api_instance is not None:
        return _api_instance
    with _lock:
        if _api_instance is None:
            _api_instance = TdxHq_API()
        return _api_instance


def connect(ip: str = DEFAULT_IP, port: int = DEFAULT_PORT) -> TdxHq_API:
    global _connected_endpoint
    global _usage_count
    api = get_api()
    with _lock:
        if _connected_endpoint is not None and _connected_endpoint != (ip, port):
            if _usage_count > 0:
                raise RuntimeError(
                    f"TdxHq_API 正被使用，禁止切换端点: current={_connected_endpoint}, target={(ip, port)}"
                )
            try:
                api.disconnect()
            except Exception:
                pass
            _connected_endpoint = None

        if _connected_endpoint == (ip, port):
            return api

        if not api.connect(ip, port):
            raise RuntimeError(f"TdxHq_API 连接失败: {ip}:{port}")

        _connected_endpoint = (ip, port)
        return api


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
