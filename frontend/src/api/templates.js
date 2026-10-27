import api from './index.js';

export async function getTemplates(page = 1, pageSize = 20) {
  const response = await api.get('/templates', { params: { page, page_size: pageSize } });
  return response.data;
}

export async function createTemplate(payload) {
  const response = await api.post('/templates', payload);
  return response.data;
}

export async function updateTemplate(id, payload) {
  const response = await api.put(`/templates/${id}`, payload);
  return response.data;
}

export async function deleteTemplate(id) {
  const response = await api.delete(`/templates/${id}`);
  return response.data;
}

export async function previewTemplate(id, variables = {}, urlPoolId = null, taskId = null) {
  const payload = { variables };
  if (urlPoolId) {
    payload.url_pool_id = Number(urlPoolId);
  }
  if (taskId) {
    payload.task_id = Number(taskId);
  }
  const response = await api.post(`/templates/${id}/preview`, payload);
  return response.data;
}

