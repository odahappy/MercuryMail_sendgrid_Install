import api from './index.js';

export async function getAIConfig() {
  const response = await api.get('/ai-config');
  return response.data;
}

export async function testAIConfig(payload) {
  const response = await api.post('/ai-config/test', payload, { timeout: 45000 });
  return response.data;
}

export async function getAvailableAIModels(payload) {
  if (payload) {
    const response = await api.post('/ai-config/models', payload, { timeout: 45000 });
    return response.data;
  }
  const response = await api.get('/ai-config/models', { timeout: 45000 });
  return response.data;
}

export async function updateAIConfig(payload) {
  const response = await api.put('/ai-config', payload);
  return response.data;
}
