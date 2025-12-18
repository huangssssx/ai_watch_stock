import axios from 'axios';
import type { Stock, AIConfig, Log, IndicatorDefinition, AIConfigTestRequest, AIConfigTestResponse, StockTestRunResponse } from './types';

const API_URL = 'http://localhost:8000';

export const api = axios.create({
  baseURL: API_URL,
});

export const getStocks = () => api.get<Stock[]>('/stocks/');
export const createStock = (stock: Partial<Stock> & { indicator_ids?: number[] }) => api.post<Stock>('/stocks/', stock);
export const updateStock = (id: number, stock: Partial<Stock> & { indicator_ids?: number[] }) =>
  api.put<Stock>(`/stocks/${id}`, stock);
export const deleteStock = (id: number) => api.delete(`/stocks/${id}`);
export const testRunStock = (id: number) => api.post<StockTestRunResponse>(`/stocks/${id}/test-run`);

export const getIndicators = () => api.get<IndicatorDefinition[]>('/indicators/');
export const createIndicator = (indicator: Pick<IndicatorDefinition, 'name' | 'akshare_api' | 'params_json'>) =>
  api.post<IndicatorDefinition>('/indicators/', indicator);
export const deleteIndicator = (id: number) => api.delete(`/indicators/${id}`);

export const getAIConfigs = () => api.get<AIConfig[]>('/ai-configs/');
export const createAIConfig = (config: Partial<AIConfig>) => api.post<AIConfig>('/ai-configs/', config);
export const deleteAIConfig = (id: number) => api.delete(`/ai-configs/${id}`);
export const testAIConfig = (id: number, payload: AIConfigTestRequest) =>
  api.post<AIConfigTestResponse>(`/ai-configs/${id}/test`, payload);

export const getLogs = (stockId?: number) => api.get<Log[]>('/logs/', { params: { stock_id: stockId } });
