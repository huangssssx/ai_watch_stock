/**
 * 统一API错误处理
 */

import { message, notification } from 'antd';
import type { AxiosError } from 'axios';

export interface ApiErrorResponse {
  detail?: string;
  message?: string;
  error?: string;
  code?: string;
  [key: string]: unknown;
}

/**
 * 错误类型枚举
 */
export const ErrorType = {
  NETWORK: 'NETWORK',
  VALIDATION: 'VALIDATION',
  NOT_FOUND: 'NOT_FOUND',
  SERVER: 'SERVER',
  PERMISSION: 'PERMISSION',
  UNKNOWN: 'UNKNOWN',
} as const;

export type ErrorType = (typeof ErrorType)[keyof typeof ErrorType];

/**
 * 解析错误响应
 */
export function parseApiError(error: unknown): { type: ErrorType; message: string } {
  // Axios错误
  if (error && typeof error === 'object' && 'response' in error) {
    const axiosError = error as AxiosError<ApiErrorResponse>;
    const status = axiosError.response?.status;
    const data = axiosError.response?.data;

    if (status === 400) {
      const msg = data?.detail || data?.message || '请求参数错误';
      return { type: ErrorType.VALIDATION, message: msg };
    }
    if (status === 401) {
      return { type: ErrorType.PERMISSION, message: '未授权，请检查登录状态' };
    }
    if (status === 403) {
      return { type: ErrorType.PERMISSION, message: '无权限访问' };
    }
    if (status === 404) {
      return { type: ErrorType.NOT_FOUND, message: '请求的资源不存在' };
    }
    if (status === 422) {
      const msg = data?.detail || '数据验证失败';
      return { type: ErrorType.VALIDATION, message: msg };
    }
    if (status && status >= 500) {
      return { type: ErrorType.SERVER, message: '服务器错误，请稍后重试' };
    }
  }

  // 网络错误
  if (error && typeof error === 'object' && 'code' in error) {
    if ((error as { code: string }).code === 'ERR_NETWORK') {
      return { type: ErrorType.NETWORK, message: '网络连接失败，请检查网络' };
    }
  }

  // 其他错误
  if (error instanceof Error) {
    return { type: ErrorType.UNKNOWN, message: error.message };
  }

  return { type: ErrorType.UNKNOWN, message: '未知错误' };
}

/**
 * 显示错误消息
 */
export function showApiError(error: unknown, options?: { silent?: boolean; fallback?: string }): void {
  if (options?.silent) return;

  const { type, message: msg } = parseApiError(error);
  const displayMsg = options?.fallback || msg;

  // 根据错误类型选择显示方式
  switch (type) {
    case ErrorType.VALIDATION:
      message.warning(displayMsg);
      break;
    case ErrorType.NETWORK:
    case ErrorType.SERVER:
      notification.error({
        message: '操作失败',
        description: displayMsg,
        duration: 5,
      });
      break;
    default:
      message.error(displayMsg);
  }
}

/**
 * 处理表单验证错误
 */
export function handleFieldErrors(data: unknown, setErrorFn: (field: string, message: string) => void): boolean {
  if (!data || typeof data !== 'object') return false;

  // FastAPI 验证错误格式: { detail: [{ loc: ['field'], msg: 'error', type: 'type' }] }
  if ('detail' in data && Array.isArray(data.detail)) {
    for (const item of data.detail) {
      if (typeof item === 'object' && 'loc' in item && 'msg' in item) {
        const loc = item.loc as unknown[];
        // loc 数组格式: ['body', 'field_name'] 或 ['query', 'param_name']
        const fieldName = loc.length > 1 ? String(loc[loc.length - 1]) : String(loc[0]);
        const errorMsg = String(item.msg);
        setErrorFn(fieldName, errorMsg);
      }
    }
    return true;
  }

  return false;
}

/**
 * 创建错误处理装饰器（用于异步操作）
 */
export function withErrorHandler<T extends (...args: unknown[]) => Promise<unknown>>(
  fn: T,
  options?: { silent?: boolean; fallback?: string }
): T {
  return (async (...args: Parameters<T>) => {
    try {
      return await fn(...args);
    } catch (error) {
      showApiError(error, options);
      throw error; // 重新抛出以便调用方处理
    }
  }) as T;
}
