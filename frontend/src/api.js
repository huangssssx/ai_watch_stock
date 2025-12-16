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

export default api;
