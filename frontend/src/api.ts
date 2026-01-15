import axios from 'axios';
import type { Stock, AIConfig, Log, IndicatorDefinition, AIConfigTestRequest, AIConfigTestResponse, StockTestRunResponse, EmailConfig, GlobalPromptConfig, AlertRateLimitConfig, IndicatorTestRequest, IndicatorTestResponse, ResearchScript, ResearchRunResponse, RuleScript, RuleTestPayload, RuleTestResponse, StockAIWatchConfig, AIWatchAnalyzeRequest, AIWatchAnalyzeResponse, IndicatorPreviewResponse, StockNews, SentimentAnalysis } from './types';

const API_URL = 'http://localhost:8000';

export const api = axios.create({
  baseURL: API_URL,
});

export const getRules = () => api.get<RuleScript[]>('/rules/');
export const createRule = (rule: Partial<RuleScript>) => api.post<RuleScript>('/rules/', rule);
export const updateRule = (id: number, rule: Partial<RuleScript>) => api.put<RuleScript>(`/rules/${id}`, rule);
export const deleteRule = (id: number) => api.delete(`/rules/${id}`);
export const testRule = (id: number, payload: RuleTestPayload) => api.post<RuleTestResponse>(`/rules/${id}/test`, payload);

export const getResearchScripts = () => api.get<ResearchScript[]>('/research/');
export const createResearchScript = (script: Partial<ResearchScript>) => api.post<ResearchScript>('/research/', script);
export const updateResearchScript = (id: number, script: Partial<ResearchScript>) => api.put<ResearchScript>(`/research/${id}`, script);
export const deleteResearchScript = (id: number) => api.delete(`/research/${id}`);
export const runResearchScript = (script_content: string) => api.post<ResearchRunResponse>('/research/run', { script_content });
export const runStreamlitScript = (script_content: string) => api.post<{ url: string }>('/research/streamlit/run', { script_content });

export const getStocks = () => api.get<Stock[]>('/stocks/');
export const createStock = (stock: Partial<Stock> & { indicator_ids?: number[] }) => api.post<Stock>('/stocks/', stock);
export const updateStock = (id: number, stock: Partial<Stock> & { indicator_ids?: number[] }) =>
  api.put<Stock>(`/stocks/${id}`, stock);
export const deleteStock = (id: number) => api.delete(`/stocks/${id}`);
export const testRunStock = (id: number, options?: { send_alerts?: boolean; bypass_checks?: boolean }) =>
  api.post<StockTestRunResponse>(`/stocks/${id}/test-run`, undefined, { params: options });

export const getStockDaily = (symbol: string) => api.get<{ ok: boolean; data?: any[]; error?: string }>(`/stocks/${symbol}/daily`);
export const getStockHistory = (symbol: string, period: string = 'daily') => api.get<{ ok: boolean; data?: any[]; error?: string }>(`/stocks/${symbol}/history`, { params: { period } });

export const getIndicators = () => api.get<IndicatorDefinition[]>('/indicators/');
export const createIndicator = (
  indicator: Pick<IndicatorDefinition, 'name'> &
    Partial<Pick<IndicatorDefinition, 'akshare_api' | 'params_json' | 'post_process_json' | 'python_code'>>,
) =>
  api.post<IndicatorDefinition>('/indicators/', indicator);
export const updateIndicator = (
  id: number,
  indicator: Partial<Pick<IndicatorDefinition, 'name' | 'akshare_api' | 'params_json' | 'post_process_json' | 'python_code'>>,
) =>
  api.put<IndicatorDefinition>(`/indicators/${id}`, indicator);
export const deleteIndicator = (id: number) => api.delete(`/indicators/${id}`);
export const testIndicator = (id: number, payload: IndicatorTestRequest) =>
  api.post<IndicatorTestResponse>(`/indicators/${id}/test`, payload);

export const getAIConfigs = () => api.get<AIConfig[]>('/ai-configs/');
export const createAIConfig = (config: Partial<AIConfig>) => api.post<AIConfig>('/ai-configs/', config);
export const updateAIConfig = (id: number, config: Partial<AIConfig>) => api.put<AIConfig>(`/ai-configs/${id}`, config);
export const deleteAIConfig = (id: number) => api.delete(`/ai-configs/${id}`);
export const testAIConfig = (id: number, payload: AIConfigTestRequest) =>
  api.post<AIConfigTestResponse>(`/ai-configs/${id}/test`, payload);

export const getLogs = (stockId?: number) => api.get<Log[]>('/logs/', { params: { stock_id: stockId } });
export const clearLogs = (logIds?: number[]) => api.delete('/logs/', { data: logIds });

export const getEmailConfig = () => api.get<EmailConfig>('/settings/email');
export const updateEmailConfig = (config: EmailConfig) => api.put<EmailConfig>('/settings/email', config);
export const testEmailConfig = (config: EmailConfig) => api.post<{ ok: boolean; message: string }>('/settings/email/test', config);

export const getGlobalPrompt = () => api.get<GlobalPromptConfig>('/settings/global-prompt');
export const updateGlobalPrompt = (config: GlobalPromptConfig) => api.put<GlobalPromptConfig>('/settings/global-prompt', config);

export const getAlertRateLimitConfig = () => api.get<AlertRateLimitConfig>('/settings/alert-rate-limit');
export const updateAlertRateLimitConfig = (config: AlertRateLimitConfig) =>
  api.put<AlertRateLimitConfig>('/settings/alert-rate-limit', config);

export const getAIWatchConfig = (stockId: number) => api.get<StockAIWatchConfig>(`/stocks/${stockId}/ai-watch-config`);
export const saveAIWatchConfig = (stockId: number, config: { indicator_ids: string; custom_prompt: string }) => api.post<StockAIWatchConfig>(`/stocks/${stockId}/ai-watch-config`, config);
export const runAIWatchAnalyze = (stockId: number, request: AIWatchAnalyzeRequest) => api.post<AIWatchAnalyzeResponse>(`/stocks/${stockId}/ai-watch-analyze`, request);
export const previewStockIndicators = (stockId: number, request: AIWatchAnalyzeRequest) => api.post<IndicatorPreviewResponse>(`/stocks/${stockId}/preview-indicators`, request);

export const fetchMarketNews = (limit: number = 50) => api.post<{ ok: boolean; count: number }>('/news/fetch', undefined, { params: { limit } });
export const getLatestNews = (limit: number = 50) => api.get<StockNews[]>('/news/latest', { params: { limit } });
export const analyzeMarketNews = (ai_config_id?: number, custom_prompt?: string, limit?: number) =>
  api.post<{ ok: boolean; raw: string }>(
    '/news/analyze',
    { ai_config_id, custom_prompt, limit },
  );
export const getLatestSentimentAnalysis = (limit: number = 10) =>
  api.get<SentimentAnalysis[]>('/news/analysis/latest', { params: { limit } });
