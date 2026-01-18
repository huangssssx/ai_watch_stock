"""
AI服务重试机制
使用 tenacity 库实现智能重试逻辑
"""

from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception,
    before_sleep_log,
)
import logging
from typing import Callable, TypeVar, Any
from functools import wraps
import time

logger = logging.getLogger(__name__)

T = TypeVar('T')


class AIServiceUnavailableError(Exception):
    """AI服务不可用错误"""
    pass


class AIRateLimitError(Exception):
    """AI服务限流错误"""
    pass


def should_retry_ai_error(exception: BaseException) -> bool:
    """
    判断是否应该重试的错误
    """
    # 网络相关错误
    if isinstance(exception, (AIServiceUnavailableError, ConnectionError)):
        return True

    # 限流错误
    if isinstance(exception, AIRateLimitError):
        return True

    # 检查错误消息中的关键字
    error_msg = str(exception).lower()
    retry_keywords = [
        'timeout',
        'connection',
        'network',
        'rate limit',
        'too many requests',
        'service unavailable',
        'temporary',
        'gateway',
    ]
    return any(keyword in error_msg for keyword in retry_keywords)


def ai_retry(
    max_attempts: int = 3,
    min_wait: float = 1.0,
    max_wait: float = 10.0,
    exponential_base: float = 2,
):
    """
    AI服务重试装饰器

    Args:
        max_attempts: 最大重试次数
        min_wait: 最小等待时间（秒）
        max_wait: 最大等待时间（秒）
        exponential_base: 指数退避基数
    """
    return retry(
        stop=stop_after_attempt(max_attempts),
        wait=wait_exponential(multiplier=1, min=min_wait, max=max_wait, exp_base=exponential_base),
        retry=retry_if_exception(should_retry_ai_error),
        before_sleep=before_sleep_log(logger, logging.WARNING),
        reraise=True,
    )


def ai_retry_with_fallback(
    fallback_value: Any = None,
    max_attempts: int = 3,
):
    """
    带降级值的AI服务重试装饰器

    Args:
        fallback_value: 重试失败后返回的降级值
        max_attempts: 最大重试次数
    """
    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        def _retry_error_callback(retry_state: Any) -> Any:
            exc = None
            try:
                exc = retry_state.outcome.exception()
            except Exception:
                exc = None

            if fallback_value is not None:
                logger.error(f"AI service failed after {max_attempts} attempts: {exc}")
                logger.warning(f"Using fallback value for {func.__name__}")
                return fallback_value

            if exc is not None:
                raise exc
            raise RuntimeError("AI service failed after retries, but no exception captured")

        decorated = retry(
            stop=stop_after_attempt(max_attempts),
            wait=wait_exponential(multiplier=1, min=1.0, max=10.0),
            retry=retry_if_exception(should_retry_ai_error),
            before_sleep=before_sleep_log(logger, logging.WARNING),
            retry_error_callback=_retry_error_callback,
            reraise=True,
        )(func)

        return wraps(func)(decorated)
    return decorator


class RetryableAIService:
    """
    可重试的AI服务包装器
    """

    def __init__(self, service_name: str = "AI"):
        self.service_name = service_name
        self._call_count = 0
        self._retry_count = 0
        self._failure_count = 0

    @ai_retry(max_attempts=3)
    def call_with_retry(
        self,
        func: Callable[..., T],
        *args: Any,
        **kwargs: Any
    ) -> T:
        """
        执行带重试的AI服务调用

        Args:
            func: 要执行的函数
            *args: 位置参数
            **kwargs: 关键字参数

        Returns:
            函数执行结果
        """
        self._call_count += 1
        logger.debug(f"Calling {func.__name__} (attempt #{self._call_count})")

        try:
            result = func(*args, **kwargs)
            return result
        except Exception as e:
            self._failure_count += 1
            logger.warning(f"Call to {func.__name__} failed: {e}")
            # 重试由装饰器处理
            raise

    def call_with_timeout(
        self,
        func: Callable[..., T],
        timeout: float,
        *args: Any,
        **kwargs: Any
    ) -> T:
        """
        执行带超时的AI服务调用

        Args:
            func: 要执行的函数
            timeout: 超时时间（秒）
            *args: 位置参数
            **kwargs: 关键字参数

        Returns:
            函数执行结果

        Raises:
            TimeoutError: 超时异常
        """
        import signal

        def timeout_handler(signum, frame):
            raise TimeoutError(f"Call to {func.__name__} timed out after {timeout}s")

        # 设置超时信号
        old_handler = signal.signal(signal.SIGALRM, timeout_handler)
        signal.alarm(int(timeout))

        try:
            result = self.call_with_retry(func, *args, **kwargs)
            return result
        finally:
            signal.alarm(0)
            signal.signal(signal.SIGALRM, old_handler)

    def get_stats(self) -> dict[str, int]:
        """
        获取调用统计信息

        Returns:
            统计信息字典
        """
        return {
            "total_calls": self._call_count,
            "retries": self._retry_count,
            "failures": self._failure_count,
            "success_rate": (
                (self._call_count - self._failure_count) / self._call_count
                if self._call_count > 0
                else 0
            ),
        }

    def reset_stats(self) -> None:
        """重置统计信息"""
        self._call_count = 0
        self._retry_count = 0
        self._failure_count = 0


# 全局AI服务实例
ai_service = RetryableAIService("AI")


def ai_service_call(func: Callable[..., T], *args: Any, **kwargs: Any) -> T:
    """
    便捷函数：使用全局AI服务进行调用

    Args:
        func: 要执行的函数
        *args: 位置参数
        **kwargs: 关键字参数

    Returns:
        函数执行结果
    """
    return ai_service.call_with_retry(func, *args, **kwargs)
