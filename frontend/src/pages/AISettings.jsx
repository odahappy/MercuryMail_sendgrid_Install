import { getErrorMessage } from '../api/index.js';
import { useEffect, useRef, useState } from 'react';
import { Alert, Button, Card, Col, Form, Input, InputNumber, Row, Select, Space, Switch, Tag, Typography, message } from 'antd';
import { ApiOutlined, SaveOutlined } from '@ant-design/icons';
import { getAIConfig, getAvailableAIModels, testAIConfig, updateAIConfig } from '../api/aiConfig.js';
import PageHeader from '../components/PageHeader.jsx';

const { Text } = Typography;

function buildProxyPayload(form) {
  const useProxy = Boolean(form.getFieldValue('ai_use_proxy'));
  const proxyType = String(form.getFieldValue('proxy_type') || '').trim().toLowerCase();
  const proxyHost = String(form.getFieldValue('proxy_host') || '').trim();
  const proxyPortRaw = form.getFieldValue('proxy_port');
  const proxyPort = proxyPortRaw === undefined || proxyPortRaw === null || proxyPortRaw === '' ? null : Number(proxyPortRaw);
  const proxyUsername = String(form.getFieldValue('proxy_username') || '').trim();
  const proxyPassword = String(form.getFieldValue('proxy_password') || '');
  return {
    ai_use_proxy: useProxy,
    proxy_type: useProxy ? proxyType : '',
    proxy_host: useProxy ? proxyHost : '',
    proxy_port: useProxy ? proxyPort : null,
    proxy_username: useProxy ? proxyUsername : '',
    proxy_password: useProxy ? proxyPassword : '',
    ai_proxy_url: '',
  };
}

function parseLegacyProxyUrl(proxyUrl) {
  const normalized = String(proxyUrl || '').trim();
  if (!normalized) return null;
  try {
    const parsed = new URL(normalized);
    const protocol = parsed.protocol.replace(':', '').toLowerCase();
    const proxyType = protocol === 'http' || protocol === 'https' ? 'http' : protocol === 'socks5' || protocol === 'socks5h' ? 'socks5' : '';
    const portValue = Number(parsed.port || 0);
    if (!proxyType || !parsed.hostname || !Number.isInteger(portValue) || portValue < 1 || portValue > 65535) return null;
    return {
      proxy_type: proxyType,
      proxy_host: parsed.hostname,
      proxy_port: portValue,
      proxy_username: decodeURIComponent(parsed.username || ''),
      proxy_password: decodeURIComponent(parsed.password || ''),
    };
  } catch (_) {
    return null;
  }
}

function formatProxySummary(data) {
  if (!data?.ai_use_proxy) {
    return '；AI 出站代理：未启用（直连）';
  }
  const proxyType = String(data.proxy_type || '').toUpperCase();
  const proxyHost = String(data.proxy_host || '').trim();
  const proxyPort = data.proxy_port;
  if (proxyType && proxyHost && proxyPort) return `；AI 出站代理：已启用（${proxyType}://${proxyHost}:${proxyPort}）`;
  const label = data.ai_proxy_url_masked || data.ai_proxy_url || '已配置';
  return `；AI 出站代理：已启用（${label}）`;
}

export default function AISettings() {
  const [form] = Form.useForm();
  const [loading, setLoading] = useState(false);
  const [saving, setSaving] = useState(false);
  const [testing, setTesting] = useState(false);
  const [loadingModels, setLoadingModels] = useState(false);
  const [maskedKey, setMaskedKey] = useState('');
  const [keyFingerprint, setKeyFingerprint] = useState('');
  const [modelOptions, setModelOptions] = useState(['gpt-3.5-turbo', 'gpt-4', 'gpt-4-turbo', 'gpt-4o', 'gpt-4o-mini']);
  const [connection, setConnection] = useState(null);
  const modelRequestSeq = useRef(0);

  const loadConfig = async () => {
    setLoading(true);
    try {
      const data = await getAIConfig();
      setMaskedKey(data.openai_api_key_masked || '');
      setKeyFingerprint(data.api_key_fingerprint || '');
      setConnection(data.has_api_key ? { connected: null, message: `已保存 API Key，指纹 ${data.api_key_fingerprint || '未知'}；可点击测试连接确认当前 Key 和模型是否可用。` } : null);
      const parsedLegacy = parseLegacyProxyUrl(data.ai_proxy_url);
      form.setFieldsValue({
        openai_api_key: '',
        model: data.model || 'gpt-3.5-turbo',
        max_tokens: data.max_tokens || 1200,
        temperature: data.temperature ?? 0.7,
        ai_use_proxy: Boolean(data.ai_use_proxy),
        proxy_type: data.proxy_type || parsedLegacy?.proxy_type || 'http',
        proxy_host: data.proxy_host || parsedLegacy?.proxy_host || '',
        proxy_port: data.proxy_port || parsedLegacy?.proxy_port || null,
        proxy_username: data.proxy_username || parsedLegacy?.proxy_username || '',
        proxy_password: data.proxy_password || parsedLegacy?.proxy_password || '',
      });
      await refreshModels(false);
    } catch (error) {
      message.error(getErrorMessage(error, '加载 AI 配置失败'));
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => { loadConfig(); }, []);

  const testConnection = async () => {
    try {
      const values = await form.validateFields(['openai_api_key', 'model', 'ai_use_proxy', 'proxy_type', 'proxy_host', 'proxy_port', 'proxy_username', 'proxy_password']);
      setTesting(true);
      const proxyPayload = buildProxyPayload(form);
      const data = await testAIConfig({
        openai_api_key: values.openai_api_key || '',
        model_name: values.model,
        ...proxyPayload,
      });
      setConnection({ connected: true, message: `${data.message}；本次测试 Key 指纹 ${data.api_key_fingerprint || '未知'}`, checked_at: data.checked_at, model: data.model });
      message.success('AI API Key 连接成功');
    } catch (error) {
      setConnection({ connected: false, message: getErrorMessage(error, 'AI 连接测试失败') });
      if (!error.errorFields) message.error(getErrorMessage(error, 'AI 连接测试失败'));
    } finally {
      setTesting(false);
    }
  };

  const refreshModels = async (showMessage = true) => {
    const requestId = modelRequestSeq.current + 1;
    modelRequestSeq.current = requestId;
    const isLatestRequest = () => modelRequestSeq.current === requestId;
    const currentKey = (form.getFieldValue('openai_api_key') || '').trim();
    const currentModel = form.getFieldValue('model') || 'gpt-3.5-turbo';
    const isOpenRouterInput = currentKey.startsWith('sk-or-');
    try {
      setLoadingModels(true);
      const payload = {
        openai_api_key: currentKey,
        model_name: currentModel,
        allow_fallback: false,
        force_refresh: showMessage,
        ...buildProxyPayload(form),
      };
      const data = currentKey
        ? await getAvailableAIModels(payload)
        : await getAvailableAIModels();
      if (!isLatestRequest()) return;
      const nextOptions = data.models?.length ? data.models : data.recommended_models;
      if (!nextOptions?.length) throw new Error('模型接口返回为空，未获取到可用模型');
      setModelOptions(nextOptions);
      if (!nextOptions.includes(currentModel)) form.setFieldValue('model', nextOptions[0]);
      const providerText = data.provider === 'openrouter' ? 'OpenRouter' : 'OpenAI';
      if (data.warning) {
        setConnection({ connected: false, message: data.warning });
        if (showMessage) message.warning(data.warning);
      } else if (showMessage) {
        const keySourceText = currentKey ? '当前输入的 API Key' : '已保存的 API Key';
        setConnection({ connected: null, message: `已根据${keySourceText}加载 ${data.models?.length || 0} 个 ${providerText} 模型${data.from_cache ? '（缓存）' : ''}。` });
        message.success(`已加载 ${providerText} 模型列表`);
      }
    } catch (error) {
      if (!isLatestRequest()) return;
      const errorMessage = getErrorMessage(error, '刷新模型列表失败');
      if (isOpenRouterInput) {
        setModelOptions([]);
        form.setFieldValue('model', undefined);
      }
      setConnection({ connected: false, message: errorMessage });
      if (showMessage && !error.errorFields) message.error(errorMessage);
    } finally {
      if (isLatestRequest()) setLoadingModels(false);
    }
  };

  const submit = async () => {
    try {
      const values = await form.validateFields();
      setSaving(true);
      const data = await updateAIConfig({
        openai_api_key: values.openai_api_key || '',
        model: values.model,
        model_name: values.model,
        max_tokens: Number(values.max_tokens),
        temperature: Number(values.temperature),
        ...buildProxyPayload(form),
      });
      setMaskedKey(data.openai_api_key_masked || '');
      setKeyFingerprint(data.api_key_fingerprint || '');
      form.setFieldValue('openai_api_key', '');
      setConnection({
        connected: null,
        message: `配置已保存并会持续生效；已保存 Key 指纹 ${data.api_key_fingerprint || '未知'}${formatProxySummary(data)}。点击测试连接可确认当前 API Key 和模型是否可用。`,
      });
      message.success('AI 配置已保存，后续 AI 文案生成会持续使用该连接配置');
    } catch (error) {
      if (!error.errorFields) message.error(getErrorMessage(error, '保存 AI 配置失败'));
    } finally {
      setSaving(false);
    }
  };

  return <Space direction="vertical" size="large" className="mm-page">
    <PageHeader title="AI 配置" description="配置 API Key 与模型即可；AI 将直接生成 HTML 邮件内容，不生成纯文本或 Markdown。OpenRouter Key 会自动识别并添加必要请求头。" />
    <Alert type="info" showIcon message="AI 将直接生成 HTML 邮件内容" description="生成结果会作为 text/html 邮件正文发送，不附加纯文本或 Markdown 版本。" />
    <Card loading={loading} bordered={false}>
      <Form form={form} layout="vertical" initialValues={{ ai_use_proxy: false, proxy_type: 'http', proxy_host: '', proxy_port: null, proxy_username: '', proxy_password: '' }}>
        <Form.Item label="当前 API Key"><Space wrap><Text>{maskedKey || '未配置；后端会尝试使用 OPENAI_API_KEY 环境变量作为后备'}</Text>{maskedKey && <Tag color="blue">已保存，持续生效</Tag>}{keyFingerprint && <Tag color="purple">指纹 {keyFingerprint}</Tag>}</Space></Form.Item>
        {connection && <Alert style={{ marginBottom: 16 }} type={connection.connected === false ? 'error' : connection.connected === true ? 'success' : 'info'} showIcon message={connection.connected === true ? 'AI 已连接' : connection.connected === false ? 'AI 未连接' : 'AI 连接状态'} description={connection.message} />}
        <Form.Item name="openai_api_key" label="API Key" extra="若使用 OpenRouter 密钥（sk-or-开头），将自动加载模型列表；保存后密钥会加密存储，留空保存时不会覆盖已有密钥。"><Input.Password placeholder="OpenAI: sk-... / OpenRouter: sk-or-..." autoComplete="new-password" /></Form.Item>
        <Form.Item name="ai_use_proxy" label="启用 AI 出站代理" valuePropName="checked" extra="启用后，AI 文案生成、模型列表拉取与连接测试均经下方代理配置发出（trust_env 已禁用，不会走系统环境代理）。">
          <Switch checkedChildren="启用" unCheckedChildren="直连" />
        </Form.Item>
        <Form.Item noStyle shouldUpdate={(prev, next) => prev.ai_use_proxy !== next.ai_use_proxy}>
          {({ getFieldValue }) => getFieldValue('ai_use_proxy') ? (
            <Row gutter={16}>
              <Col xs={24} md={8}>
                <Form.Item name="proxy_type" label="代理类型" rules={[{ required: true, message: '启用代理后请选择代理类型' }]}>
                  <Select
                    placeholder="请选择代理类型"
                    options={[
                      { value: 'http', label: 'HTTP' },
                      { value: 'socks5', label: 'SOCKS5' },
                    ]}
                  />
                </Form.Item>
              </Col>
              <Col xs={24} md={8}>
                <Form.Item name="proxy_host" label="代理主机" rules={[{ required: true, message: '启用代理后请填写代理主机' }]}>
                  <Input placeholder="例如 127.0.0.1 或 proxy.example.com" />
                </Form.Item>
              </Col>
              <Col xs={24} md={8}>
                <Form.Item
                  name="proxy_port"
                  label="代理端口"
                  rules={[
                    { required: true, message: '启用代理后请填写代理端口' },
                    {
                      validator: (_, value) => {
                        const port = Number(value);
                        if (!Number.isInteger(port) || port < 1 || port > 65535) {
                          return Promise.reject(new Error('代理端口必须是 1-65535 的整数'));
                        }
                        return Promise.resolve();
                      },
                    },
                  ]}
                >
                  <InputNumber placeholder="例如 7890" min={1} max={65535} precision={0} style={{ width: '100%' }} />
                </Form.Item>
              </Col>
              <Col xs={24} md={12}>
                <Form.Item name="proxy_username" label="代理账号（可选）">
                  <Input placeholder="未鉴权代理可留空" />
                </Form.Item>
              </Col>
              <Col xs={24} md={12}>
                <Form.Item name="proxy_password" label="代理密码（可选）">
                  <Input.Password placeholder="未鉴权代理可留空" autoComplete="new-password" />
                </Form.Item>
              </Col>
            </Row>
          ) : null}
        </Form.Item>
        <Form.Item name="model" label="模型" rules={[{ required: true, message: '请选择模型' }]} extra="刷新时会优先使用当前输入框中的 API Key；未输入新 Key 时使用已保存 Key。支持输入关键词过滤。"><Select showSearch loading={loadingModels} optionFilterProp="label" options={modelOptions.map((item) => ({ value: item, label: item }))} /></Form.Item>
        <Form.Item name="max_tokens" label="最大令牌数" rules={[{ required: true, message: '请输入最大令牌数' }]}><InputNumber min={100} max={8000} style={{ width: '100%' }} /></Form.Item>
        <Form.Item name="temperature" label="温度参数" rules={[{ required: true, message: '请输入温度参数' }]}><InputNumber min={0} max={2} step={0.1} style={{ width: '100%' }} /></Form.Item>
        <Space wrap><Button loading={loadingModels} onClick={() => refreshModels(true)}>刷新可用模型</Button><Button icon={<ApiOutlined />} loading={testing} onClick={testConnection}>测试 API 密钥连接</Button><Button type="primary" icon={<SaveOutlined />} loading={saving} onClick={submit}>保存并持续使用</Button></Space>
      </Form>
    </Card>
  </Space>;
}
