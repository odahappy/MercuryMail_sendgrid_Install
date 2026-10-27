import api from './index.js';

export async function login(password) {
  const response = await api.post('/auth/login', { password }, { skipAuthRedirect: true });
  return response.data;
}

export async function changePassword(oldPassword, newPassword) {
  const response = await api.put('/auth/password', { old_password: oldPassword, new_password: newPassword });
  return response.data;
}

export function saveAuthToken(accessToken, expiresIn) {
  const expireAt = Date.now() + expiresIn * 1000;
  window.localStorage.setItem('mercury_token', accessToken);
  window.localStorage.setItem('mercury_token_expire_at', String(expireAt));
}

export function clearAuthToken() {
  window.localStorage.removeItem('mercury_token');
  window.localStorage.removeItem('mercury_token_expire_at');
}

export function isAuthenticated() {
  const token = window.localStorage.getItem('mercury_token');
  const expireAt = Number(window.localStorage.getItem('mercury_token_expire_at') || '0');
  return Boolean(token) && expireAt > Date.now();
}
