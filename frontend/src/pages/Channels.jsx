import { useCallback, useEffect, useRef, useState } from 'react';
import { Button, Card, Checkbox, Form, Input, InputNumber, Modal, Popconfirm, Select, Space, Switch, Table, Tag, Tooltip, Typography, message } from 'antd';
import { PlusOutlined, ReloadOutlined } from '@ant-design/icons';
import { createChannel, deleteChannel, listChannels, testChannelConnection, updateChannel } from '../api/channels.js';
import { getErrorMessage } from '../api/index.js';

const { Title, Text } = Typography;
const limitFields = [
  { name: 'daily_limit', unlimitedName: 'daily_limit_unlimited', label: '每日限额' },
  { name: 'hourly_limit', unlimitedName: 'hourly_limit_unlimited', label: '每小时限额' },
  { name: 'per_minute_limit', unlimitedName: 'per_minute_limit_unlimited', label: '每分钟限额' },
  { name: 'second_limit', unlimitedName: 'second_limit_unlimited', label: '每秒限额' }
];
const limitInputStyle = { width: 160 };
const CHANNEL_POLL_MS = 30000;
const emptyProxy = { name: '', host: '', port: 8080, type: 'HTTP', username: '', password: undefined, is_active: true };
const emptyChannel = { name: '', api_key: '', sender_emails: [{ name: '', email: '', enabled: true }], daily_limit: null, hourly_limit: null, per_minute_limit: null, second_limit: null, daily_limit_unlimited: true, hourly_limit_unlimited: true, per_minute_limit_unlimited: true, second_limit_unlimited: true, use_proxy: false, proxy: emptyProxy, is_active: true };

function isUnlimitedLimit(value) {
  return value === null || value === undefined || value === '' || Number(value) === 0;
}

function normalizeLimitFormValues(values) {
  return limitFields.reduce((acc, field) => {
    const unlimited = isUnlimitedLimit(values[field.name]);
    acc[field.name] = unlimited ? null : Number(values[field.name]);
    acc[field.unlimitedName] = unlimited;
    return acc;
  }, {});
}

function formatLimit(value) {
  return isUnlimitedLimit(value) ? '无限制' : value;
}

function maskProxy(proxy) {
  if (!proxy) return '直连';
  const scheme = String(proxy.type || 'HTTP').toLowerCase() === 'socks5' ? 'socks5' : 'http';
  const auth = proxy.username || proxy.password ? '***@' : '';
  return `${scheme}://${auth}${proxy.host}:${proxy.port}`;
}

function formatHealthCheckTime(value) {
  if (!value) return '尚未检测';
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return String(value);
  return date.toLocaleString('zh-CN', { hour12: false });
}

const HEALTH_STATUS_LABELS = {
  healthy: '健康',
  unauthorized: '未授权',
  error: '异常',
  unknown: '未知',
};

function resolveHealthStatusLabel(record) {
  if (record?.health_status_label) return record.health_status_label;
  const status = String(record?.health_status || 'healthy').toLowerCase();
  return HEALTH_STATUS_LABELS[status] || '未知';
}

function renderStatusDot(className) {
  return <span className={`mm-health-dot ${className}`} />;
}

function renderAccountHealth(record) {
  const status = String(record.health_status || 'healthy').toLowerCase();
  const label = resolveHealthStatusLabel(record);
  const checkedAt = formatHealthCheckTime(record.last_health_check);
  const probeDetail = record.health_error || record.last_error || '';
  if (status === 'healthy') {
    return (
      <Tooltip title={`检测时间：${checkedAt}`}>
        <Space size={4}>{renderStatusDot('mm-health-dot--online')}<Text>{label}</Text></Space>
      </Tooltip>
    );
  }
  const errorText = probeDetail || label;
  return (
    <Tooltip title={`检测时间：${checkedAt}；${errorText}`}>
      <Space size={4}>
        {renderStatusDot(status === 'unauthorized' ? 'mm-health-dot--offline' : 'mm-health-dot--warning')}
        <Text type={status === 'unauthorized' ? 'danger' : 'warning'}>{label}</Text>
      </Space>
    </Tooltip>
  );
}

function isChannelUnauthorized(record) {
  return String(record?.health_status || '').toLowerCase() === 'unauthorized';
}

function isMaskedApiKey(value) {
  const normalized = String(value || '').trim();
  return !normalized || normalized === '***' || (normalized.includes('...') && normalized.length >= 14);
}

function normalizeProxyForForm(proxy) {
  if (!proxy) return { ...emptyProxy };
  return {
    name: proxy.name || '',
    host: proxy.host || '',
    port: proxy.port || 8080,
    type: proxy.type || 'HTTP',
    username: proxy.username || '',
    password: undefined,
    is_active: proxy.is_active !== false
  };
}

export default function Channels() {
  const [channels, setChannels] = useState([]);
  const [loading, setLoading] = useState(false);
  const [saving, setSaving] = useState(false);
  const [testingId, setTestingId] = useState(0);
  const [channelModalOpen, setChannelModalOpen] = useState(false);
  const [editingChannel, setEditingChannel] = useState(null);
  const [isPageVisible, setIsPageVisible] = useState(
    typeof document === 'undefined' ? true : document.visibilityState !== 'hidden',
  );
  const [channelForm] = Form.useForm();
  const channelsRequestInFlightRef = useRef(false);
  const useProxy = Form.useWatch('use_proxy', channelForm);
  const limitUnlimitedValues = Form.useWatch((values) => limitFields.reduce((acc, field) => {
    acc[field.unlimitedName] = values?.[field.unlimitedName];
    return acc;
  }, {}), channelForm) || {};

  const loadData = useCallback(async () => {
    if (channelsRequestInFlightRef.current) return;
    channelsRequestInFlightRef.current = true;
    setLoading(true);
    try {
      const channelData = await listChannels({ page: 1, page_size: 100 });
      setChannels(channelData.items || []);
    } catch (error) {
      message.error(getErrorMessage(error, '加载通道管理数据失败'));
    } finally {
      channelsRequestInFlightRef.current = false;
      setLoading(false);
    }
  }, []);

  useEffect(() => { loadData(); }, [loadData]);

  useEffect(() => {
    const handleVisibility = () => {
      const visible = document.visibilityState !== 'hidden';
      setIsPageVisible(visible);
      if (visible) loadData();
    };
    document.addEventListener('visibilitychange', handleVisibility);
    return () => document.removeEventListener('visibilitychange', handleVisibility);
  }, [loadData]);

  useEffect(() => {
    if (!isPageVisible) return undefined;
    const timer = window.setInterval(() => {
      if (!channelModalOpen && !saving && !testingId) loadData();
    }, CHANNEL_POLL_MS);
    return () => window.clearInterval(timer);
  }, [isPageVisible, channelModalOpen, saving, testingId, loadData]);

  const openCreateChannel = () => {
    setEditingChannel(null);
    channelForm.setFieldsValue({ ...emptyChannel, proxy: { ...emptyProxy } });
    setChannelModalOpen(true);
  };

  const openEditChannel = (record) => {
    if (isChannelUnauthorized(record)) {
      message.warning('该账号 API 密钥已失效，请更新 API 密钥后重新测试连接');
    }
    setEditingChannel(record);
    channelForm.resetFields();
    channelForm.setFieldsValue({
      ...record,
      api_key: '',
      ...normalizeLimitFormValues(record),
      proxy: normalizeProxyForForm(record.proxy),
      sender_emails: record.sender_emails?.length ? record.sender_emails.map((item) => ({ ...item, enabled: item.enabled !== false })) : [{ name: record.sender_name || '', email: record.sender_email || '', enabled: true }]
    });
    setChannelModalOpen(true);
  };

  const saveChannel = async () => {
    try {
      const values = await channelForm.validateFields();
      setSaving(true);
      const limitPayload = limitFields.reduce((acc, field) => {
        acc[field.name] = values[field.unlimitedName] ? null : Number(values[field.name]);
        return acc;
      }, {});
      const payload = { ...values, ...limitPayload, enabled: values.is_active };
      const trimmedApiKey = String(payload.api_key || '').trim();
      if (editingChannel && isMaskedApiKey(trimmedApiKey)) {
        delete payload.api_key;
      } else {
        payload.api_key = trimmedApiKey;
      }
      if (values.use_proxy) {
        const proxy = values.proxy || {};
        const proxyPayload = {
          name: String(proxy.name || '').trim(),
          host: String(proxy.host || '').trim(),
          port: Number(proxy.port),
          type: proxy.type,
          is_active: proxy.is_active !== false
        };
        const proxyUsername = String(proxy.username || '').trim();
        if (proxyUsername) {
          proxyPayload.username = proxyUsername;
        } else if (!editingChannel || !editingChannel.proxy || String(editingChannel.proxy.username || '').trim()) {
          proxyPayload.username = '';
        }
        const proxyPassword = typeof proxy.password === 'string' ? proxy.password.trim() : '';
        if (proxyPassword) {
          proxyPayload.password = proxyPassword;
        } else if (!editingChannel || !editingChannel.proxy) {
          proxyPayload.password = '';
        } else if (editingChannel.proxy.has_password) {
          proxyPayload.keep_existing_password = true;
        }
        payload.proxy = proxyPayload;
      } else {
        payload.proxy = null;
      }
      limitFields.forEach((field) => delete payload[field.unlimitedName]);
      if (editingChannel) await updateChannel(editingChannel.id, payload);
      else await createChannel(payload);
      message.success(editingChannel ? '通道已更新，代理配置已同步生效' : '通道已创建');
      setChannelModalOpen(false);
      await loadData();
    } catch (error) {
      if (!error.errorFields) message.error(getErrorMessage(error, '保存通道失败'));
    } finally {
      setSaving(false);
    }
  };

  const removeChannel = async (record) => {
    try {
      await deleteChannel(record.id);
      message.success('通道已删除，专属代理已同步清理');
      await loadData();
    } catch (error) {
      message.error(getErrorMessage(error, '删除通道失败'));
    }
  };

  const testConnection = async (record) => {
    setTestingId(record.id);
    try {
      const result = await testChannelConnection(record.id);
      if (result.success) {
        message.success(`${record.name}：${result.message || '连接成功，API 密钥有效'}`);
      } else {
        message.error(`${record.name}：${result.message || '连接失败，请检查 API 密钥、网络或代理配置'}`);
      }
      await loadData();
    } catch (error) {
      message.error(getErrorMessage(error, '测试连接失败'));
    } finally {
      setTestingId(0);
    }
  };

  const columns = [
    { title: '账号名称', dataIndex: 'name' },
    { title: '发件人', render: (_, record) => (record.sender_emails || []).map((item) => <div key={item.email}>{item.enabled === false ? <Tag>已禁用</Tag> : null}{item.name ? `${item.name} <${item.email}>` : item.email}</div>) },
    { title: '限额', render: (_, record) => <Text>日 {formatLimit(record.daily_limit)} / 时 {formatLimit(record.hourly_limit)} / 分 {formatLimit(record.per_minute_limit)} / 秒 {formatLimit(record.second_limit)}</Text> },
    { title: '代理', render: (_, record) => record.use_proxy && record.proxy ? <Tag color="blue">{maskProxy(record.proxy)}</Tag> : <Tag>直连</Tag> },
    {
      title: '代理出口IP',
      dataIndex: 'proxy_outbound_ip',
      key: 'proxy_outbound_ip',
      width: 140,
      render: (value, record) => (record.use_proxy ? (value || '—') : '—'),
    },
    {
      title: '账号状态',
      key: 'health_status',
      width: 180,
      render: (_, record) => renderAccountHealth(record),
    },
    { title: '状态', render: (_, record) => <Tag color={record.is_active ? 'green' : 'default'}>{record.is_active ? '启用' : '禁用'}</Tag> },
    {
      title: '操作',
      render: (_, record) => {
        const unauthorized = isChannelUnauthorized(record);
        return (
          <Space wrap>
            <Tooltip title={unauthorized ? 'API 密钥已失效，可编辑更新凭据' : undefined}>
              <Button size="small" danger={unauthorized} onClick={() => openEditChannel(record)}>编辑</Button>
            </Tooltip>
            <Button size="small" loading={testingId === record.id} onClick={() => testConnection(record)}>测试连接</Button>
            <Popconfirm title="确认删除通道？" description="删除通道将同时删除其专属代理。" okText="删除" cancelText="取消" onConfirm={() => removeChannel(record)}>
              <Button size="small" danger>删除</Button>
            </Popconfirm>
          </Space>
        );
      },
    },
  ];

  return <Space direction="vertical" size="large" className="mm-page">
    <Card bordered={false} className="mm-panel-card"><Space style={{ width: '100%', justifyContent: 'space-between' }}><div><Title level={3} style={{ margin: 0 }}>通道管理</Title><Text type="secondary">管理 SendGrid 账号、发件人、代理与速率限额；页面可见时每 30 秒自动刷新健康状态。</Text></div><Space><Button icon={<ReloadOutlined />} onClick={loadData}>刷新</Button><Button type="primary" icon={<PlusOutlined />} onClick={openCreateChannel}>新增账号</Button></Space></Space></Card>
    <Table rowKey="id" loading={loading} columns={columns} dataSource={channels} pagination={false} />
    <Modal title={editingChannel ? '编辑 SendGrid 账号' : '新增 SendGrid 账号'} open={channelModalOpen} onCancel={() => setChannelModalOpen(false)} onOk={saveChannel} confirmLoading={saving} okText="保存" cancelText="取消" width={860} destroyOnHidden>
      <Form form={channelForm} layout="vertical" disabled={saving} initialValues={emptyChannel}>
        <Form.Item name="name" label="账号名称" rules={[{ required: true, message: '请输入账号名称' }]}><Input /></Form.Item>
        <Form.Item
          name="api_key"
          label="SendGrid API 密钥"
          tooltip={editingChannel ? `当前密钥：${editingChannel.api_key || '已配置'}；留空表示保留原 API Key。` : '请输入完整的 SendGrid API Key。'}
          rules={[{
            validator: (_, value) => {
              if (!editingChannel && !String(value || '').trim()) return Promise.reject(new Error('请输入 API 密钥'));
              return Promise.resolve();
            }
          }]}
        ><Input.Password placeholder={editingChannel ? '留空则保留原 API Key' : '请输入 API 密钥'} autoComplete="new-password" /></Form.Item>
        <Form.List name="sender_emails">
          {(fields, { add, remove }) => <Space direction="vertical" style={{ width: '100%' }}>
            {fields.map((field) => <Space key={field.key} align="baseline" style={{ display: 'flex', flexWrap: 'wrap' }}>
              <Form.Item {...field} name={[field.name, 'name']} label="发件人名称"><Input style={{ width: 200 }} /></Form.Item>
              <Form.Item {...field} name={[field.name, 'email']} label="发件人邮箱" rules={[{ required: true, message: '请输入发件人邮箱' }]}><Input style={{ width: 280 }} /></Form.Item>
              <Form.Item {...field} name={[field.name, 'enabled']} label="启用" valuePropName="checked"><Switch /></Form.Item>
              <Button danger disabled={fields.length === 1} onClick={() => remove(field.name)}>删除</Button>
            </Space>)}
            <Button onClick={() => add({ name: '', email: '', enabled: true })}>添加发件人</Button>
          </Space>}
        </Form.List>
        <Space wrap align="start">
          {limitFields.map((field) => {
            const unlimited = Boolean(limitUnlimitedValues[field.unlimitedName]);
            return <Space key={field.name} direction="vertical" size={0}>
              <Form.Item name={field.name} label={field.label} rules={unlimited ? [] : [{ required: true, message: `请输入${field.label}` }, { type: 'integer', min: 1, message: `${field.label}必须为正整数` }]}>
                <InputNumber style={limitInputStyle} min={1} precision={0} disabled={unlimited} placeholder={unlimited ? '无限制' : '请输入正整数'} onChange={(value) => {
                  if (isUnlimitedLimit(value)) channelForm.setFieldsValue({ [field.name]: undefined, [field.unlimitedName]: true });
                }} />
              </Form.Item>
              <Form.Item name={field.unlimitedName} valuePropName="checked" style={{ marginBottom: 0 }}>
                <Checkbox onChange={(event) => {
                  if (event.target.checked) channelForm.setFieldsValue({ [field.name]: undefined });
                }}>无限制</Checkbox>
              </Form.Item>
            </Space>;
          })}
        </Space>
        <Form.Item name="is_active" label="启用账号" valuePropName="checked"><Switch /></Form.Item>
        <Form.Item name="use_proxy" label="使用代理连接" valuePropName="checked"><Switch /></Form.Item>
        {useProxy ? <Card size="small" title="专属代理配置">
          <Form.Item name={["proxy", "name"]} label="代理名称"><Input placeholder="可选，默认使用账号名称" /></Form.Item>
          <Space align="start" wrap>
            <Form.Item name={["proxy", "host"]} label="主机" rules={[{ required: true, whitespace: true, message: '请输入代理主机' }]}><Input style={{ width: 260 }} /></Form.Item>
            <Form.Item name={["proxy", "port"]} label="端口" rules={[{ required: true, message: '请输入代理端口' }, { type: 'number', min: 1, max: 65535, message: '代理端口必须是 1-65535 之间的数字' }]}><InputNumber min={1} max={65535} precision={0} style={{ width: 140 }} /></Form.Item>
            <Form.Item name={["proxy", "type"]} label="类型" rules={[{ required: true, message: '请选择代理类型' }, { enum: ['HTTP', 'SOCKS5'], message: '代理类型只能是 HTTP 或 SOCKS5' }]}><Select style={{ width: 140 }} options={[{ value: 'HTTP', label: 'HTTP' }, { value: 'SOCKS5', label: 'SOCKS5' }]} /></Form.Item>
          </Space>
          <Space align="start" wrap>
            <Form.Item name={["proxy", "username"]} label="用户名"><Input style={{ width: 260 }} /></Form.Item>
            <Form.Item name={["proxy", "password"]} label="密码" tooltip="编辑时留空表示保留原代理密码；仅在需要修改代理密码时填写。"><Input.Password style={{ width: 260 }} placeholder={editingChannel?.proxy ? '留空则保留原密码' : ''} autoComplete="new-password" /></Form.Item>
            <Form.Item name={["proxy", "is_active"]} label="启用代理" valuePropName="checked"><Switch /></Form.Item>
          </Space>
        </Card> : null}
      </Form>
    </Modal>
  </Space>;
}
