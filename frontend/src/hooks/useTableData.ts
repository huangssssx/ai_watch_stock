/**
 * 通用表格数据管理Hook
 * 用于简化表格数据的加载、刷新、删除等操作
 */

import { useState, useCallback, useEffect } from 'react';
import { message } from 'antd';

export interface UseTableDataOptions<T> {
  // 数据获取函数
  fetchFn: () => Promise<T[]>;
  // 删除函数（可选）
  deleteFn?: (id: number) => Promise<{ ok: boolean }>;
  // 是否在挂载时自动加载
  autoFetch?: boolean;
  // 轮询间隔（毫秒），0表示不轮询
  pollInterval?: number;
  // 数据项名称
  itemName?: string;
}

export interface UseTableDataReturn<T> {
  // 数据列表
  data: T[];
  // 加载状态
  loading: boolean;
  // 刷新数据
  refresh: () => Promise<void>;
  // 删除项
  deleteItem: (id: number) => Promise<void>;
  // 设置数据
  setData: React.Dispatch<React.SetStateAction<T[]>>;
}

/**
 * 通用表格数据Hook
 */
export function useTableData<T extends { id: number }>(
  options: UseTableDataOptions<T>
): UseTableDataReturn<T> {
  const { fetchFn, deleteFn, autoFetch = true, pollInterval = 0, itemName = '项目' } = options;

  const [data, setData] = useState<T[]>([]);
  const [loading, setLoading] = useState(false);

  // 刷新数据
  const refresh = useCallback(async () => {
    setLoading(true);
    try {
      const items = await fetchFn();
      setData(items);
    } catch {
      message.error(`加载${itemName}列表失败`);
    } finally {
      setLoading(false);
    }
  }, [fetchFn, itemName]);

  // 删除项
  const deleteItem = useCallback(async (id: number) => {
    if (!deleteFn) {
      message.warning('删除功能未配置');
      return;
    }

    try {
      await deleteFn(id);
      message.success(`${itemName}已删除`);
      // 从列表中移除
      setData(prev => prev.filter(item => item.id !== id));
    } catch {
      message.error(`删除${itemName}失败`);
    }
  }, [deleteFn, itemName]);

  // 自动加载
  useEffect(() => {
    if (autoFetch) {
      const timer = setTimeout(() => {
        void refresh();
      }, 0);
      return () => clearTimeout(timer);
    }
  }, [autoFetch, refresh]);

  // 轮询
  useEffect(() => {
    if (pollInterval > 0) {
      const interval = setInterval(() => {
        void refresh();
      }, pollInterval);
      return () => clearInterval(interval);
    }
  }, [pollInterval, refresh]);

  return {
    data,
    loading,
    refresh,
    deleteItem,
    setData,
  };
}

/**
 * 带分页的表格数据Hook
 */
export interface UseTableDataWithPaginationOptions<T> extends UseTableDataOptions<T> {
  // 每页数量
  pageSize?: number;
}

export interface UseTableDataWithPaginationReturn<T> extends UseTableDataReturn<T> {
  // 当前页
  currentPage: number;
  // 每页数量
  pageSize: number;
  // 总数
  total: number;
  // 切换页码
  onPageChange: (page: number, size?: number) => void;
}

export function useTableDataWithPagination<T extends { id: number }>(
  options: UseTableDataWithPaginationOptions<T>
): UseTableDataWithPaginationReturn<T> {
  const { pageSize: pageSizeOption = 20, ...baseOptions } = options;
  const [currentPage, setCurrentPage] = useState(1);
  const [pageSize, setPageSize] = useState(pageSizeOption);

  // 这里简化处理，实际应该支持服务端分页
  const base = useTableData(baseOptions);

  const total = base.data.length;
  const paginatedData = base.data.slice(
    (currentPage - 1) * pageSize,
    currentPage * pageSize
  );

  const onPageChange = useCallback((page: number, size?: number) => {
    setCurrentPage(page);
    if (size && size !== pageSize) {
      setPageSize(size);
    }
  }, [pageSize]);

  return {
    ...base,
    data: paginatedData,
    currentPage,
    pageSize,
    total,
    onPageChange,
  };
}
