import axios from 'axios';
const apiBase = import.meta.env.VITE_API_BASE || 'http://127.0.0.1:8000';
const api = axios.create({ baseURL: apiBase });

export const getStrategies = () => api.get('/strategies/');
export const getStrategy = (id) => api.get(`/strategies/${id}`);
export const createStrategy = (data) => api.post('/strategies/', data);
export const updateStrategy = (id, data) => api.put(`/strategies/${id}`, data);
export const startMonitor = (id) => api.post(`/monitor/start/${id}`);
export const stopMonitor = (id) => api.post(`/monitor/stop/${id}`);
export const getAlerts = (id) => api.get(`/alerts/${id}`);
export const updateAlert = (id, data) => api.put(`/alerts/${id}`, data);
export const getMinuteData = (symbol, period = '1', options = {}) =>
  api.get(`/data/minute`, { params: { symbol, period, provider: 'em', ...options } });

// Alert Rules APIs
export const getAlertRules = (params = {}) => api.get('/alert_rules/', { params });
export const createAlertRule = (data) => api.post('/alert_rules/', data);
export const updateAlertRule = (id, data) => api.put(`/alert_rules/${id}`, data);
export const deleteAlertRule = (id) => api.delete(`/alert_rules/${id}`);
export const batchCreateAlertRules = (payload) => api.post('/alert_rules/batch', payload);
export const batchDeleteAlertRules = (payload) => api.delete('/alert_rules/batch', { data: payload });

export default api;
