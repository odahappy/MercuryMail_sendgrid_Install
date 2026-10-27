export const DASHBOARD_REFRESH_EVENT = 'mercury:dashboard-refresh';

export function notifyDashboardRefresh() {
  if (typeof window === 'undefined') return;
  window.dispatchEvent(new CustomEvent(DASHBOARD_REFRESH_EVENT));
}

export function subscribeDashboardRefresh(handler) {
  if (typeof window === 'undefined') {
    return () => {};
  }
  window.addEventListener(DASHBOARD_REFRESH_EVENT, handler);
  return () => window.removeEventListener(DASHBOARD_REFRESH_EVENT, handler);
}
