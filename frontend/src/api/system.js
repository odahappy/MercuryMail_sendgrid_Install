import api from './index.js';

// 宪法要求的前端 API 封装；当前页面未引用，保留供系统信息展示扩展
export async function getSystemInfo() {
  const response = await api.get('/system/info');
  return response.data;
}
