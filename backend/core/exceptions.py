"""
自定义异常类
用于统一错误处理和响应格式
"""

from typing import Any, Optional
from fastapi import HTTPException, status


class AppException(Exception):
    """应用基础异常类"""

    def __init__(
        self,
        message: str,
        code: str = "APP_ERROR",
        status_code: int = status.HTTP_500_INTERNAL_SERVER_ERROR,
        details: Optional[dict[str, Any]] = None,
    ):
        self.message = message
        self.code = code
        self.status_code = status_code
        self.details = details or {}
        super().__init__(self.message)


class ValidationError(AppException):
    """数据验证错误"""

    def __init__(self, message: str, details: Optional[dict[str, Any]] = None):
        super().__init__(
            message=message,
            code="VALIDATION_ERROR",
            status_code=status.HTTP_400_BAD_REQUEST,
            details=details,
        )


class NotFoundError(AppException):
    """资源未找到错误"""

    def __init__(self, message: str = "Resource not found", details: Optional[dict[str, Any]] = None):
        super().__init__(
            message=message,
            code="NOT_FOUND",
            status_code=status.HTTP_404_NOT_FOUND,
            details=details,
        )


class ConflictError(AppException):
    """资源冲突错误"""

    def __init__(self, message: str, details: Optional[dict[str, Any]] = None):
        super().__init__(
            message=message,
            code="CONFLICT",
            status_code=status.HTTP_409_CONFLICT,
            details=details,
        )


class ExternalServiceError(AppException):
    """外部服务错误"""

    def __init__(self, message: str, service_name: str, details: Optional[dict[str, Any]] = None):
        details = details or {}
        details["service"] = service_name
        super().__init__(
            message=message,
            code="EXTERNAL_SERVICE_ERROR",
            status_code=status.HTTP_502_BAD_GATEWAY,
            details=details,
        )


class AIServiceError(ExternalServiceError):
    """AI服务错误"""

    def __init__(self, message: str, details: Optional[dict[str, Any]] = None):
        super().__init__(message=message, service_name="AI", details=details)


class DataFetchError(ExternalServiceError):
    """数据获取错误"""

    def __init__(self, message: str, details: Optional[dict[str, Any]] = None):
        super().__init__(message=message, service_name="DataFetch", details=details)


class BusinessRuleError(AppException):
    """业务规则错误"""

    def __init__(self, message: str, details: Optional[dict[str, Any]] = None):
        super().__init__(
            message=message,
            code="BUSINESS_RULE_ERROR",
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            details=details,
        )
