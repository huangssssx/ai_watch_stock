/**
 * 增强的API客户端，带有拦截器和统一错误处理
 */

import axios, { type AxiosInstance, type AxiosError } from 'axios';
import { message } from 'antd';

const API_URL = import.meta.env.VITE_API_URL || 'http://localhost:8000';

/**
 * 创建API客户端实例
 */
export function createApiClient(baseURL: string = API_URL): AxiosInstance {
  const client = axios.create({
    baseURL,
    timeout: 30000, // 30秒超时
    headers: {
      'Content-Type': 'application/json',
    },
  });

  // 请求拦截器
  client.interceptors.request.use(
    (config) => {
      // 可以在这里添加token等认证信息
      // const token = localStorage.getItem('token');
      // if (token) {
      //   config.headers.Authorization = `Bearer ${token}`;
      // }
      return config;
    },
    (error) => {
      return Promise.reject(error);
    }
  );

  // 响应拦截器
  client.interceptors.response.use(
    (response) => {
      return response;
    },
    (error: AxiosError) => {
      // 对于某些特定错误码，不做全局提示
      if (error.response?.status === 401) {
        // 认证错误可能需要特殊处理
        message.error('请先登录');
        return Promise.reject(error);
      }

      // 其他错误可以在这里做全局处理
      // 或者让各个组件自行处理
      return Promise.reject(error);
    }
  );

  return client;
}

/**
 * 默认API客户端
 */
export const apiClient = createApiClient();

/**
 * API辅助函数 - 用于处理常见的CRUD操作
 */
export const apiHelper = {
  /**
   * 安全的GET请求，带错误处理
   */
  async get<T>(url: string, params?: unknown): Promise<T> {
    const response = await apiClient.get<T>(url, { params });
    return response.data;
  },

  /**
   * 安全的POST请求，带错误处理
   */
  async post<T>(url: string, data?: unknown): Promise<T> {
    const response = await apiClient.post<T>(url, data);
    return response.data;
  },

  /**
   * 安全的PUT请求，带错误处理
   */
  async put<T>(url: string, data?: unknown): Promise<T> {
    const response = await apiClient.put<T>(url, data);
    return response.data;
  },

  /**
   * 安全的DELETE请求，带错误处理
   */
  async delete<T>(url: string): Promise<T> {
    const response = await apiClient.delete<T>(url);
    return response.data;
  },

  /**
   * 批量请求（并行）
   */
  async all<T>(requests: Array<Promise<T>>): Promise<T[]> {
    return Promise.all(requests);
  },
};
