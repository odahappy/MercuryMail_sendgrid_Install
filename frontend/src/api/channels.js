import api from './index.js';
import { notifyDashboardRefresh } from '../utils/dashboardRefresh.js';

export async function listChannels(params = {}) {
  const response = await api.get('/channels', { params });
  return response.data;
}

export async function createChannel(data) {
  const response = await api.post('/channels', data);
  return response.data;
}

export async function updateChannel(id, data) {
  const response = await api.put(`/channels/${id}`, data);
  return response.data;
}

export async function deleteChannel(id) {
  const response = await api.delete(`/channels/${id}`);
  notifyDashboardRefresh();
  return response.data;
}

export async function testChannelConnection(id) {
  const response = await api.post(`/channels/${id}/test-connection`);
  return response.data;
}

export async function measureChannelProxyLatency(channelId, rateLimit, sendHttpBatchSize) {
  const params = {};
  const normalizedRate = Number(rateLimit);
  if (Number.isFinite(normalizedRate) && normalizedRate > 0) {
    params.rate_limit = normalizedRate;
  }
  const normalizedBatch = Number(sendHttpBatchSize);
  if (Number.isFinite(normalizedBatch) && normalizedBatch > 0) {
    params.send_http_batch_size = normalizedBatch;
  }
  const response = await api.post(`/channels/${channelId}/proxy-latency`, null, { params, timeout: 30000 });
  return response.data;
}
