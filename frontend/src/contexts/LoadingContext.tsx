/**
 * 全局Loading状态管理Context
 * 用于统一管理页面级和组件级的加载状态
 */

import { createContext, useContext, useState, useCallback } from 'react';
import type { ReactNode } from 'react';

interface LoadingState {
  // 全局loading状态
  globalLoading: boolean;
  // 全局loading消息
  globalMessage?: string;
  // 局部loading状态集合（key-value）
  localLoadings: Record<string, boolean>;
}

interface LoadingContextValue extends LoadingState {
  // 设置全局loading
  setGlobalLoading: (loading: boolean, message?: string) => void;
  // 设置局部loading
  setLocalLoading: (key: string, loading: boolean) => void;
  // 执行异步操作（自动管理loading）
  runAsync: <T>(
    fn: () => Promise<T>,
    options?: {
      global?: boolean;
      key?: string;
      message?: string;
      onError?: (error: unknown) => void;
    }
  ) => Promise<T>;
  // 批量设置局部loading
  setMultipleLocalLoadings: (loadings: Record<string, boolean>) => void;
}

const LoadingContext = createContext<LoadingContextValue | undefined>(undefined);

export interface LoadingProviderProps {
  children: ReactNode;
}

/**
 * Loading Provider组件
 */
export function LoadingProvider({ children }: LoadingProviderProps) {
  const [state, setState] = useState<LoadingState>({
    globalLoading: false,
    globalMessage: undefined,
    localLoadings: {},
  });

  const setGlobalLoading = useCallback((loading: boolean, message?: string) => {
    setState(prev => ({
      ...prev,
      globalLoading: loading,
      globalMessage: message,
    }));
  }, []);

  const setLocalLoading = useCallback((key: string, loading: boolean) => {
    setState(prev => ({
      ...prev,
      localLoadings: {
        ...prev.localLoadings,
        [key]: loading,
      },
    }));
  }, []);

  const setMultipleLocalLoadings = useCallback((loadings: Record<string, boolean>) => {
    setState(prev => ({
      ...prev,
      localLoadings: {
        ...prev.localLoadings,
        ...loadings,
      },
    }));
  }, []);

  const runAsync = useCallback(async <T,>(
    fn: () => Promise<T>,
    options?: {
      global?: boolean;
      key?: string;
      message?: string;
      onError?: (error: unknown) => void;
    }
  ): Promise<T> => {
    const { global = false, key, message: msg, onError } = options || {};

    if (global) {
      setGlobalLoading(true, msg);
    } else if (key) {
      setLocalLoading(key, true);
    }

    try {
      return await fn();
    } catch (error) {
      if (onError) {
        onError(error);
      }
      throw error;
    } finally {
      if (global) {
        setGlobalLoading(false);
      } else if (key) {
        setLocalLoading(key, false);
      }
    }
  }, [setGlobalLoading, setLocalLoading]);

  const value: LoadingContextValue = {
    ...state,
    setGlobalLoading,
    setLocalLoading,
    runAsync,
    setMultipleLocalLoadings,
  };

  return (
    <LoadingContext.Provider value={value}>
      {children}
    </LoadingContext.Provider>
  );
}

/**
 * 使用Loading Context的Hook
 */
export function useLoading(): LoadingContextValue {
  const context = useContext(LoadingContext);
  if (!context) {
    throw new Error('useLoading must be used within LoadingProvider');
  }
  return context;
}

/**
 * 便捷Hook：仅使用全局loading
 */
export function useGlobalLoading() {
  const { globalLoading, globalMessage, setGlobalLoading, runAsync } = useLoading();

  const withLoading = useCallback(<T,>(
    fn: () => Promise<T>,
    message?: string
  ): Promise<T> => {
    return runAsync(fn, { global: true, message });
  }, [runAsync]);

  return {
    loading: globalLoading,
    message: globalMessage,
    setLoading: setGlobalLoading,
    withLoading,
  };
}

/**
 * 便捷Hook：仅使用局部loading
 */
export function useLocalLoading(key: string) {
  const { localLoadings, setLocalLoading, runAsync } = useLoading();
  const loading = localLoadings[key] || false;

  const withLoading = useCallback(<T,>(
    fn: () => Promise<T>
  ): Promise<T> => {
    return runAsync(fn, { key });
  }, [runAsync, key]);

  return {
    loading,
    setLoading: (l: boolean) => setLocalLoading(key, l),
    withLoading,
  };
}
