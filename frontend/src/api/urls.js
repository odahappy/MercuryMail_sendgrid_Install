import api, { uploadApi } from './index.js';

export async function getPools(page = 1, pageSize = 20) {
  const response = await api.get('/urls/pools', { params: { page, page_size: pageSize } });
  return response.data;
}

export async function createPool(payload) {
  const response = await api.post('/urls/pools', payload);
  return response.data;
}

export async function updatePool(id, payload) {
  const response = await api.put(`/urls/pools/${id}`, payload);
  return response.data;
}

export async function deletePool(id) {
  const response = await api.delete(`/urls/pools/${id}`);
  return response.data;
}

export async function uploadUrls(id, file, text) {
  return startUrlImport(id, { file, text });
}

export function resolveUrlImportFile(rawFile) {
  const candidate = rawFile?.originFileObj || rawFile;
  if (candidate instanceof Blob) {
    return candidate;
  }
  return null;
}

export async function startUrlImport(poolId, { file = null, text = '' } = {}) {
  const formData = new FormData();
  const trimmedText = String(text || '').trim();
  if (trimmedText) {
    formData.append('urls_text', trimmedText);
  } else {
    const uploadFile = resolveUrlImportFile(file);
    if (uploadFile) {
      formData.append('file', uploadFile, file?.name || uploadFile.name || 'urls.txt');
    }
  }
  if (!formData.has('urls_text') && !formData.has('file')) {
    throw new Error('请输入网址或上传文件');
  }
  const response = await uploadApi.post(`/urls/pools/${poolId}/import`, formData, { timeout: 300000 });
  return response.data;
}

export async function getUrlImportTasks({ poolId = null, page = 1, pageSize = 20 } = {}) {
  const params = { page, page_size: pageSize };
  if (poolId) {
    params.pool_id = poolId;
  }
  const response = await api.get('/urls/import-tasks', { params });
  return response.data;
}

export async function cancelUrlImportTask(taskId) {
  const response = await api.post(`/urls/import-tasks/${taskId}/cancel`);
  return response.data;
}

export async function getUrlImportProgress(taskId) {
  const response = await api.get(`/urls/import-tasks/${taskId}/progress`);
  return response.data;
}

export async function getNext(id) {
  const response = await api.post(`/urls/pools/${id}/next`);
  return response.data;
}

export async function getPoolItems(poolId, page = 1, pageSize = 20) {
  const response = await api.get(`/urls/pools/${poolId}/items`, {
    params: { page, page_size: pageSize }
  });
  return response.data;
}

export async function addPoolItems(poolId, payload) {
  const response = await api.post(`/urls/pools/${poolId}/items`, payload);
  return response.data;
}

export async function updatePoolItem(poolId, itemId, url) {
  const response = await api.put(`/urls/pools/${poolId}/items/${itemId}`, { url });
  return response.data;
}

export async function deletePoolItem(poolId, itemId) {
  const response = await api.delete(`/urls/pools/${poolId}/items/${itemId}`);
  return response.data;
}

export async function batchDeletePoolItems(poolId, itemIds) {
  const response = await api.post(`/urls/pools/${poolId}/items/batch-delete`, { item_ids: itemIds });
  return response.data;
}

export async function clearAllPoolItems(poolId) {
  const response = await api.delete(`/urls/pools/${poolId}/items`);
  return response.data;
}
