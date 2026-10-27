import { requestApi } from './index.js';

export async function getDashboardStats(config = {}) {
  const response = await requestApi({
    method: 'get',
    url: '/dashboard/stats',
    params: { _t: Date.now() },
    headers: { 'Cache-Control': 'no-cache' },
    ...config,
  });
  return response.data;
}
