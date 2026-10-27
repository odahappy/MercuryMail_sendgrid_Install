import api from './index.js';

export async function getWarmupPlans(page = 1, pageSize = 20) {
  const response = await api.get('/warmup/plans', { params: { page, page_size: pageSize } });
  return response.data;
}

export async function getWarmupPlan(id) {
  const response = await api.get(`/warmup/plans/${id}`);
  return response.data;
}

export async function previewWarmupContent(payload) {
  const response = await api.post('/warmup/content-preview', payload, { timeout: 120000 });
  return response.data;
}

export async function createWarmupPlan(payload) {
  const response = await api.post('/warmup/plans', payload, { timeout: 120000 });
  return response.data;
}

export async function updatePlan(id, payload) {
  const response = await api.put(`/warmup/plans/${id}`, payload, { timeout: 120000 });
  return response.data;
}

export async function startWarmupPlan(id) {
  const response = await api.post(`/warmup/plans/${id}/start`);
  return response.data;
}

export async function pauseWarmupPlan(id) {
  const response = await api.post(`/warmup/plans/${id}/pause`);
  return response.data;
}

export async function addWarmupRecipients(id, emails) {
  const response = await api.post(`/warmup/plans/${id}/recipients`, { emails });
  return response.data;
}

export async function regenerateWarmupContent(id, payload = {}) {
  const response = await api.post(`/warmup/plans/${id}/regenerate-content`, payload, { timeout: 120000 });
  return response.data;
}

export async function updatePlanContent(planId, data) {
  const response = await api.put(`/warmup/plans/${planId}/content`, data);
  return response.data;
}

export async function deleteWarmupPlan(id) {
  const response = await api.delete(`/warmup/plans/${id}`);
  return response.data;
}
