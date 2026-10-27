import { useCallback, useEffect, useRef, useState } from 'react';
import { Alert, Button, Card, Spin, Typography } from 'antd';
import { ApiOutlined, ReloadOutlined } from '@ant-design/icons';
import api from '../api/index.js';

const { Text, Title } = Typography;

const HEALTH_LIVE_PATH = '/health/live';
const HEALTH_TIMEOUT_MS = 8000;
const HEALTH_RETRY_MS = 2000;

function describeHealthError(err) {
  const status = err?.response?.status;
  if (status === 503) {
    const payload = err?.response?.data || {};
    if (payload?.db_ready === false || payload?.redis_ready === false) {
      return '系统正在初始化数据库或 Redis，请稍候，就绪后会自动进入。';
    }
    return '服务尚未完全就绪，请稍候，系统会自动重试。';
  }
  if (err?.code === 'ECONNABORTED' || /timeout/i.test(String(err?.message || ''))) {
    return '健康检查超时。后端可能正忙于发送任务，正在自动重试；若持续失败请检查服务器负载。';
  }
  if (!err?.response) {
    return '无法连接到后端服务，正在自动重试。请确认后端进程已启动且网络可达。';
  }
  return `后端健康检查失败（HTTP ${status || '未知'}），正在自动重试。`;
}

export default function BackendReadyGate({ children }) {
  const timerRef = useRef(0);
  const mountedRef = useRef(false);
  const [ready, setReady] = useState(false);
  const [checking, setChecking] = useState(true);
  const [error, setError] = useState('');

  const checkHealth = useCallback(async () => {
    window.clearTimeout(timerRef.current);
    setChecking(true);
    try {
      await api.get(HEALTH_LIVE_PATH, {
        timeout: HEALTH_TIMEOUT_MS,
        skipAuthRedirect: true,
        skipNetworkRetry: true,
      });
      if (!mountedRef.current) return;
      setReady(true);
      setError('');
    } catch (err) {
      if (!mountedRef.current) return;
      setReady(false);
      setError(describeHealthError(err));
      timerRef.current = window.setTimeout(checkHealth, HEALTH_RETRY_MS);
    } finally {
      if (mountedRef.current) setChecking(false);
    }
  }, []);

  useEffect(() => {
    mountedRef.current = true;
    checkHealth();
    return () => {
      mountedRef.current = false;
      window.clearTimeout(timerRef.current);
    };
  }, [checkHealth]);

  if (ready) return children;

  return (
    <div className="mm-boot-gate">
      <Card className="mm-boot-gate__card" bordered={false}>
        <Spin spinning={checking}>
          <div style={{ display: 'flex', alignItems: 'center', gap: 14, marginBottom: 12 }}>
            <div className="mm-login__logo" style={{ width: 40, height: 40, borderRadius: 12 }}>
              <ApiOutlined style={{ fontSize: 20, color: '#fff' }} />
            </div>
            <Title level={3} style={{ margin: 0 }}>服务启动中</Title>
          </div>
          <Text type="secondary">正在确认 MercuryMail 后端服务状态，服务就绪后会自动进入系统。</Text>
          {error ? <Alert type="warning" showIcon message={error} style={{ marginTop: 16 }} /> : null}
          <Button icon={<ReloadOutlined />} onClick={checkHealth} loading={checking} style={{ marginTop: 16 }}>
            立即重试
          </Button>
        </Spin>
      </Card>
    </div>
  );
}
