import axios from 'axios';
const apiBase = import.meta.env.VITE_API_BASE || 'http://127.0.0.1:8000';
const api = axios.create({ baseURL: apiBase });

// Alert Rules APIs
export const getAlertRules = (params = {}) => api.get('/alert_rules/', { params });
export const createAlertRule = (data) => api.post('/alert_rules/', data);
export const updateAlertRule = (id, data) => api.put(`/alert_rules/${id}`, data);
export const deleteAlertRule = (id) => api.delete(`/alert_rules/${id}`);
export const batchCreateAlertRules = (payload) => api.post('/alert_rules/batch', payload);
export const batchDeleteAlertRules = (payload) => api.delete('/alert_rules/batch', { data: payload });

// Alert Notifications APIs
export const getAlertNotifications = (params = {}) => api.get('/alert_notifications/', { params });
export const updateAlertNotification = (id, data) => api.put(`/alert_notifications/${id}`, data);
export const clearAllAlertNotifications = () => api.post('/alert_notifications/clear_all');

// Indicator Configs APIs
export const getIndicatorConfigs = () => api.get('/indicator_configs/');
export const createIndicatorConfig = (data) => api.post('/indicator_configs/', data);
export const updateIndicatorConfig = (id, data) => api.put(`/indicator_configs/${id}`, data);
export const deleteIndicatorConfig = (id) => api.delete(`/indicator_configs/${id}`);

// Proxy API
export const proxyAkshareGet = (apiName, params) => api.get(`/proxy/akshare/${apiName}`, { params });
export const proxyAksharePost = (data) => api.post('/proxy/akshare/proxy', data);

// Indicator Collections APIs
export const getIndicatorCollections = () => api.get('/indicator_collections/');
export const createIndicatorCollection = (data) => api.post('/indicator_collections/', data);
export const updateIndicatorCollection = (id, data) => api.put(`/indicator_collections/${id}`, data);
export const deleteIndicatorCollection = (id) => api.delete(`/indicator_collections/${id}`);
export const runIndicatorCollection = (id, params) => api.post(`/indicator_collections/${id}/run`, params);

export default api;
