import React from 'react';
import { createRoot } from 'react-dom/client';
import { BrowserRouter } from 'react-router-dom';
import { App as AntApp, ConfigProvider, message } from 'antd';
import zhCN from 'antd/locale/zh_CN';
import 'antd/dist/reset.css';
import './styles/global.css';
import App from './App.jsx';
import BackendReadyGate from './components/BackendReadyGate.jsx';

function formatError(error, errorInfo) {
  const message = error instanceof Error ? error.message : String(error || '未知错误');
  const stack = error instanceof Error && error.stack ? error.stack : '';
  const componentStack = errorInfo?.componentStack || '';
  return [message, stack, componentStack].filter(Boolean).join('\n\n');
}

function escapeHtml(value) {
  return String(value).replace(/[<>&]/g, (char) => ({ '<': '&lt;', '>': '&gt;', '&': '&amp;' })[char]);
}

function renderBootError(error, errorInfo) {
  const root = document.getElementById('root');
  const message = formatError(error, errorInfo);
  if (root) {
    root.innerHTML = `<div style="min-height:100vh;display:flex;align-items:center;justify-content:center;background:#f8fafc;color:#0f172a;font-family:system-ui,-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;padding:24px;"><div style="max-width:760px;background:#fff;border:1px solid #e2e8f0;border-radius:18px;padding:28px;box-shadow:0 20px 60px rgba(15,23,42,.12);"><h1 style="margin:0 0 12px;font-size:24px;">MercuryMail 前端启动失败</h1><p style="margin:0 0 12px;color:#475569;line-height:1.7;">页面资源已经加载，但 React 应用渲染时发生错误。请复制下方错误信息用于定位。</p><pre style="white-space:pre-wrap;background:#0f172a;color:#e2e8f0;border-radius:12px;padding:16px;overflow:auto;max-height:56vh;">${escapeHtml(message)}</pre></div></div>`;
  }
}

window.addEventListener('error', (event) => {
  renderBootError(event.error || event.message || '未知脚本错误');
});

window.addEventListener('unhandledrejection', (event) => {
  console.error('未处理的 Promise 拒绝：', event.reason);
  if (!window.__MERCURY_REACT_MOUNTED__) {
    renderBootError(event.reason || '未处理的 Promise 拒绝');
    return;
  }
  if (typeof event.preventDefault === 'function') {
    event.preventDefault();
  }
  const detail = event.reason instanceof Error ? event.reason.message : String(event.reason || '未知错误');
  message.error(detail.length > 120 ? '操作失败，请稍后重试' : detail);
});

class RootErrorBoundary extends React.Component {
  constructor(props) {
    super(props);
    this.state = { error: null, errorInfo: null };
  }

  static getDerivedStateFromError(error) {
    return { error };
  }

  componentDidCatch(error, errorInfo) {
    this.setState({ error, errorInfo });
  }

  render() {
    if (this.state.error) {
      const message = formatError(this.state.error, this.state.errorInfo);
      return (
        <div style={{ minHeight: '100vh', display: 'flex', alignItems: 'center', justifyContent: 'center', background: '#f8fafc', color: '#0f172a', fontFamily: 'system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif', padding: 24 }}>
          <div style={{ maxWidth: 760, background: '#fff', border: '1px solid #e2e8f0', borderRadius: 18, padding: 28, boxShadow: '0 20px 60px rgba(15,23,42,.12)' }}>
            <h1 style={{ margin: '0 0 12px', fontSize: 24 }}>MercuryMail 前端渲染失败</h1>
            <p style={{ margin: '0 0 12px', color: '#475569', lineHeight: 1.7 }}>页面资源加载成功，但 React 组件渲染时发生错误。请复制下方错误信息用于定位。</p>
            <pre style={{ whiteSpace: 'pre-wrap', background: '#0f172a', color: '#e2e8f0', borderRadius: 12, padding: 16, overflow: 'auto', maxHeight: '56vh' }}>{message}</pre>
          </div>
        </div>
      );
    }
    return this.props.children;
  }
}

try {
  const rootElement = document.getElementById('root');
  if (!rootElement) {
    throw new Error('在 index.html 中未找到根元素 #root');
  }

  rootElement.setAttribute('data-react-mounted', 'true');
  window.__MERCURY_REACT_MOUNTED__ = true;
  window.clearTimeout(window.__MERCURY_BOOT_TIMER__);

  createRoot(rootElement).render(
    <React.StrictMode>
      <RootErrorBoundary>
        <ConfigProvider
          locale={zhCN}
          theme={{
            token: {
              colorPrimary: '#0c8ee6',
              colorSuccess: '#10b981',
              colorWarning: '#f59e0b',
              colorError: '#ef4444',
              colorInfo: '#06b6d4',
              borderRadius: 10,
              borderRadiusLG: 14,
              fontFamily: "'Segoe UI', 'PingFang SC', 'Microsoft YaHei', system-ui, -apple-system, sans-serif",
              colorBgContainer: '#ffffff',
              colorBgLayout: '#f0f4f8',
              colorBorderSecondary: 'rgba(15, 23, 42, 0.08)',
              boxShadow: '0 1px 3px rgba(15, 23, 42, 0.04), 0 8px 24px rgba(15, 23, 42, 0.06)',
              boxShadowSecondary: '0 4px 12px rgba(15, 23, 42, 0.06)',
            },
            components: {
              Layout: {
                siderBg: '#0b1120',
                headerBg: '#ffffff',
                bodyBg: '#f0f4f8',
              },
              Menu: {
                darkItemBg: 'transparent',
                darkSubMenuItemBg: 'transparent',
                darkItemSelectedBg: 'rgba(12, 142, 230, 0.18)',
                darkItemHoverBg: 'rgba(255, 255, 255, 0.06)',
              },
              Card: {
                borderRadiusLG: 14,
              },
              Table: {
                headerBg: '#f8fafc',
                borderRadius: 10,
              },
              Button: {
                primaryShadow: '0 2px 8px rgba(12, 142, 230, 0.3)',
              },
            },
          }}
        >
          <AntApp>
            <BrowserRouter>
              <BackendReadyGate>
                <App />
              </BackendReadyGate>
            </BrowserRouter>
          </AntApp>
        </ConfigProvider>
      </RootErrorBoundary>
    </React.StrictMode>
  );
} catch (error) {
  renderBootError(error);
}
