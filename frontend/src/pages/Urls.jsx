import { getErrorMessage } from '../api/index.js';
import { useCallback, useEffect, useMemo, useState } from 'react';
import { Alert, Button, Card, Collapse, Form, Input, InputNumber, Modal, Popconfirm, Progress, Select, Space, Switch, Table, Tag, Typography, Upload, message } from 'antd';
import { DeleteOutlined, EditOutlined, InboxOutlined, LinkOutlined, PlusOutlined, ReloadOutlined, StopOutlined, UnorderedListOutlined } from '@ant-design/icons';
import {
  addPoolItems,
  batchDeletePoolItems,
  cancelUrlImportTask,
  clearAllPoolItems,
  createPool,
  deletePool,
  deletePoolItem,
  getNext,
  getPoolItems,
  getPools,
  getUrlImportTasks,
  startUrlImport,
  updatePool,
  updatePoolItem
} from '../api/urls.js';
import PageHeader from '../components/PageHeader.jsx';

const { Text, Title } = Typography;
const { TextArea } = Input;
const { Dragger } = Upload;

const defaultChars = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789';
const IMPORT_POLL_MS = 1000;
const MAX_IMPORT_FILE_SIZE_BYTES = 100 * 1024 * 1024;
const TEXT_IMPORT_EXTENSIONS = new Set(['.txt', '.csv']);

function validateImportFile(file) {
  if (!file) return '请选择要上传的文件';
  if (Number(file.size || 0) <= 0) return '上传文件为空，请选择包含 URL 的文本文件';
  if (Number(file.size || 0) > MAX_IMPORT_FILE_SIZE_BYTES) return '上传文件过大，请上传不超过 100MB 的文件';
  const name = String(file.name || '').toLowerCase();
  const extension = name.includes('.') ? name.slice(name.lastIndexOf('.')) : '';
  if (extension && !TEXT_IMPORT_EXTENSIONS.has(extension)) return '仅支持上传 .txt 或 .csv 文本文件';
  const type = String(file.type || '').toLowerCase();
  if (type && !type.startsWith('text/') && type !== 'application/vnd.ms-excel' && type !== 'text/csv') return '仅支持上传文本格式的 URL 文件';
  return '';
}
const paramOptions = [
  { value: 'uid', label: 'uid' },
  { value: 'id', label: 'id' },
  { value: 'sid', label: 'sid' },
  { value: 'track', label: 'track' },
  { value: 'token', label: 'token' },
  { value: 'r', label: 'r' },
  { value: '__custom__', label: '自定义' }
];
const emptyForm = { name: '', url_text: '', jump_enabled: false, random_prefix: 'uid', prefix_mode: 'uid', random_length: 12, random_chars: defaultChars };
const importStatusMeta = {
  pending: { color: 'default', label: '排队中' },
  running: { color: 'processing', label: '导入中' },
  completed: { color: 'success', label: '已完成' },
  failed: { color: 'error', label: '失败' },
  cancelled: { color: 'warning', label: '已取消' }
};

function paramModeFor(paramName) {
  return paramOptions.some((item) => item.value === paramName) ? paramName : '__custom__';
}

function normalizeForm(pool) {
  const randomParam = pool.random_prefix || 'uid';
  return {
    name: pool.name || '',
    url_text: '',
    jump_enabled: Boolean(pool.jump_enabled),
    random_prefix: randomParam,
    prefix_mode: paramModeFor(randomParam),
    random_length: pool.random_length || 12,
    random_chars: pool.random_chars || defaultChars
  };
}

function jumpSummary(pool) {
  if (!pool.jump_enabled) return { text: '关闭', color: 'default' };
  return { text: '开启：复用 URL 输入框', color: 'green' };
}

function formatDateTime(value) {
  if (!value) return '-';
  return String(value).replace('T', ' ').slice(0, 19);
}

function hasActiveImportTasks(tasks) {
  return (tasks || []).some((task) => task.status === 'pending' || task.status === 'running');
}

export default function Urls() {
  const [pools, setPools] = useState([]);
  const [page, setPage] = useState(1);
  const [pageSize, setPageSize] = useState(20);
  const [total, setTotal] = useState(0);
  const [loading, setLoading] = useState(false);
  const [saving, setSaving] = useState(false);
  const [testingId, setTestingId] = useState(0);
  const [deletingId, setDeletingId] = useState(0);
  const [modalOpen, setModalOpen] = useState(false);
  const [editing, setEditing] = useState(null);
  const [nextResult, setNextResult] = useState('');
  const [importFile, setImportFile] = useState(null);
  const [importTasks, setImportTasks] = useState([]);
  const [importTasksLoading, setImportTasksLoading] = useState(false);
  const [cancellingTaskId, setCancellingTaskId] = useState(0);
  const [form] = Form.useForm();
  const [itemsModalOpen, setItemsModalOpen] = useState(false);
  const [managingPool, setManagingPool] = useState(null);
  const [poolItems, setPoolItems] = useState([]);
  const [itemsPage, setItemsPage] = useState(1);
  const [itemsPageSize, setItemsPageSize] = useState(20);
  const [itemsTotal, setItemsTotal] = useState(0);
  const [itemsLoading, setItemsLoading] = useState(false);
  const [itemsSaving, setItemsSaving] = useState(false);
  const [editingItemId, setEditingItemId] = useState(0);
  const [editingUrl, setEditingUrl] = useState('');
  const [newUrlText, setNewUrlText] = useState('');
  const [deletingItemId, setDeletingItemId] = useState(0);
  const [selectedItemIds, setSelectedItemIds] = useState([]);
  const [batchDeleting, setBatchDeleting] = useState(false);
  const [clearingAll, setClearingAll] = useState(false);

  const totalPages = useMemo(() => Math.max(1, Math.ceil(total / pageSize)), [total, pageSize]);
  const itemsTotalPages = useMemo(() => Math.max(1, Math.ceil(itemsTotal / itemsPageSize)), [itemsTotal, itemsPageSize]);
  const activeImportCount = useMemo(
    () => (importTasks || []).filter((task) => task.status === 'pending' || task.status === 'running').length,
    [importTasks]
  );

  const loadPools = useCallback(async () => {
    setLoading(true);
    try {
      const data = await getPools(page, pageSize);
      setPools(data.items || []);
      setTotal(data.total || 0);
    } catch (error) {
      message.error(getErrorMessage(error, '加载 URL 池失败'));
    } finally {
      setLoading(false);
    }
  }, [page, pageSize]);

  const loadImportTasks = useCallback(async (silent = false) => {
    if (!silent) setImportTasksLoading(true);
    try {
      const data = await getUrlImportTasks({ page: 1, pageSize: 50 });
      setImportTasks(data.items || []);
    } catch (error) {
      if (!silent) message.error(getErrorMessage(error, '加载导入任务失败'));
    } finally {
      if (!silent) setImportTasksLoading(false);
    }
  }, []);

  useEffect(() => {
    loadPools();
  }, [loadPools]);

  useEffect(() => {
    loadImportTasks();
    const timer = window.setInterval(async () => {
      try {
        const data = await getUrlImportTasks({ page: 1, pageSize: 50 });
        const items = data.items || [];
        setImportTasks(items);
        if (hasActiveImportTasks(items)) {
          await loadPools();
        }
      } catch {
        // 轮询失败时静默忽略，避免页面崩溃
      }
    }, IMPORT_POLL_MS);
    return () => window.clearInterval(timer);
  }, [loadImportTasks, loadPools]);

  const openCreate = () => {
    setEditing(null);
    setImportFile(null);
    form.setFieldsValue(emptyForm);
    setModalOpen(true);
  };

  const openEdit = (pool) => {
    setEditing(pool);
    setImportFile(null);
    form.setFieldsValue(normalizeForm(pool));
    setModalOpen(true);
  };

  const submitPool = async () => {
    try {
      const values = await form.validateFields();
      const hasTextImport = Boolean((values.url_text || '').trim());
      const hasFileImport = Boolean(importFile);
      if (!editing && !hasTextImport && !hasFileImport) {
        message.error('请输入网址或上传文件');
        return;
      }
      if (hasFileImport) {
        const fileError = validateImportFile(importFile);
        if (fileError) {
          message.error(fileError);
          return;
        }
      }
      setSaving(true);
      const payload = {
        name: values.name,
        jump_enabled: Boolean(values.jump_enabled),
        jump_domain: '',
        random_prefix: values.random_prefix || 'uid',
        random_length: Number(values.random_length),
        random_chars: values.random_chars
      };
      const savedPool = editing ? await updatePool(editing.id, payload) : await createPool(payload);
      if (hasTextImport || hasFileImport) {
        await startUrlImport(savedPool.id, {
          file: hasTextImport ? null : importFile,
          text: hasTextImport ? values.url_text : '',
        });
        message.success(`${editing ? 'URL 池已更新' : 'URL 池已创建'}，导入任务已在后台开始`);
        await loadImportTasks();
      } else {
        message.success(editing ? 'URL 池已更新' : 'URL 池已创建');
      }
      setModalOpen(false);
      setImportFile(null);
      form.resetFields();
      await loadPools();
    } catch (error) {
      if (!error.errorFields) {
        message.error(getErrorMessage(error, '保存 URL 池失败'));
      }
    } finally {
      setSaving(false);
    }
  };

  const removePool = async (pool) => {
    setDeletingId(pool.id);
    try {
      await deletePool(pool.id);
      message.success('URL 池已删除');
      if (pools.length === 1 && page > 1) {
        setPage((value) => value - 1);
      } else {
        await loadPools();
      }
      await loadImportTasks(true);
    } catch (error) {
      message.error(getErrorMessage(error, '删除 URL 池失败'));
    } finally {
      setDeletingId(0);
    }
  };

  const testNext = async (pool) => {
    setTestingId(pool.id);
    try {
      const result = await getNext(pool.id);
      setNextResult(result.url);
      message.success('测试取用成功');
    } catch (error) {
      message.error(getErrorMessage(error, '测试取用失败'));
    } finally {
      setTestingId(0);
    }
  };

  const cancelImport = async (task) => {
    setCancellingTaskId(task.id);
    try {
      await cancelUrlImportTask(task.id);
      message.success('导入任务已取消');
      await loadImportTasks();
    } catch (error) {
      message.error(getErrorMessage(error, '取消导入任务失败'));
    } finally {
      setCancellingTaskId(0);
    }
  };

  const loadPoolItems = useCallback(async (pool, page = itemsPage, pageSize = itemsPageSize) => {
    if (!pool) return;
    setItemsLoading(true);
    try {
      const data = await getPoolItems(pool.id, page, pageSize);
      setPoolItems(data.items || []);
      setItemsTotal(data.total || 0);
      setItemsPage(data.page || page);
      setItemsPageSize(data.page_size || pageSize);
    } catch (error) {
      message.error(getErrorMessage(error, '加载 URL 链接失败'));
    } finally {
      setItemsLoading(false);
    }
  }, [itemsPage, itemsPageSize]);

  const openItemsModal = async (pool) => {
    setManagingPool(pool);
    setItemsModalOpen(true);
    setEditingItemId(0);
    setEditingUrl('');
    setNewUrlText('');
    setSelectedItemIds([]);
    setItemsPage(1);
    await loadPoolItems(pool, 1, itemsPageSize);
  };

  const closeItemsModal = () => {
    setItemsModalOpen(false);
    setManagingPool(null);
    setPoolItems([]);
    setEditingItemId(0);
    setEditingUrl('');
    setNewUrlText('');
    setSelectedItemIds([]);
  };

  const submitNewUrls = async () => {
    const lines = (newUrlText || '').split(/\r?\n/).map((line) => line.trim()).filter(Boolean);
    if (!lines.length) {
      message.error('请输入至少一个 URL');
      return;
    }
    setItemsSaving(true);
    try {
      const result = await addPoolItems(managingPool.id, { urls: lines });
      message.success(`新增 ${result.total_accepted || 0} 条，跳过重复 ${result.total_skipped || 0} 条`);
      setNewUrlText('');
      await loadPoolItems(managingPool, 1, itemsPageSize);
      await loadPools();
    } catch (error) {
      message.error(getErrorMessage(error, '新增 URL 失败'));
    } finally {
      setItemsSaving(false);
    }
  };

  const startEditItem = (item) => {
    setEditingItemId(item.id);
    setEditingUrl(item.url || '');
  };

  const cancelEditItem = () => {
    setEditingItemId(0);
    setEditingUrl('');
  };

  const saveEditItem = async (item) => {
    const nextUrl = (editingUrl || '').trim();
    if (!nextUrl) {
      message.error('URL 不能为空');
      return;
    }
    setItemsSaving(true);
    try {
      await updatePoolItem(managingPool.id, item.id, nextUrl);
      message.success('URL 已更新');
      cancelEditItem();
      await loadPoolItems(managingPool, itemsPage, itemsPageSize);
    } catch (error) {
      message.error(getErrorMessage(error, '更新 URL 失败'));
    } finally {
      setItemsSaving(false);
    }
  };

  const removePoolItem = async (item) => {
    setDeletingItemId(item.id);
    try {
      await deletePoolItem(managingPool.id, item.id);
      message.success('URL 已删除');
      setSelectedItemIds((current) => current.filter((id) => id !== item.id));
      if (poolItems.length === 1 && itemsPage > 1) {
        const nextPage = itemsPage - 1;
        setItemsPage(nextPage);
        await loadPoolItems(managingPool, nextPage, itemsPageSize);
      } else {
        await loadPoolItems(managingPool, itemsPage, itemsPageSize);
      }
      await loadPools();
    } catch (error) {
      message.error(getErrorMessage(error, '删除 URL 失败'));
    } finally {
      setDeletingItemId(0);
    }
  };

  const removeSelectedItems = async () => {
    if (!selectedItemIds.length) {
      message.warning('请先选择要删除的 URL 链接');
      return;
    }
    setBatchDeleting(true);
    try {
      const result = await batchDeletePoolItems(managingPool.id, selectedItemIds);
      message.success(`已批量删除 ${result.deleted_count || 0} 条链接`);
      setSelectedItemIds([]);
      const remainingTotal = Math.max(0, itemsTotal - (result.deleted_count || 0));
      const maxPage = Math.max(1, Math.ceil(remainingTotal / itemsPageSize));
      const nextPage = itemsPage > maxPage ? maxPage : itemsPage;
      setItemsPage(nextPage);
      await loadPoolItems(managingPool, nextPage, itemsPageSize);
      await loadPools();
    } catch (error) {
      message.error(getErrorMessage(error, '批量删除 URL 失败'));
    } finally {
      setBatchDeleting(false);
    }
  };

  const removeAllPoolItems = async () => {
    setClearingAll(true);
    try {
      const result = await clearAllPoolItems(managingPool.id);
      message.success(`已清空全部链接，共删除 ${result.deleted_count || 0} 条`);
      setSelectedItemIds([]);
      cancelEditItem();
      setItemsPage(1);
      await loadPoolItems(managingPool, 1, itemsPageSize);
      await loadPools();
    } catch (error) {
      message.error(getErrorMessage(error, '清空链接失败'));
    } finally {
      setClearingAll(false);
    }
  };

  const updateParamMode = (value) => {
    const next = value === '__custom__' ? form.getFieldValue('random_prefix') || 'uid' : value;
    form.setFieldsValue({ prefix_mode: value, random_prefix: next });
  };

  const importTaskColumns = [
    {
      title: 'URL 池',
      dataIndex: 'pool_name',
      key: 'pool_name',
      render: (value, task) => <Text strong>{value || `池 #${task.pool_id}`}</Text>
    },
    {
      title: '状态',
      dataIndex: 'status',
      key: 'status',
      width: 110,
      render: (value) => {
        const meta = importStatusMeta[value] || { color: 'default', label: value || '-' };
        return <Tag color={meta.color}>{meta.label}</Tag>;
      }
    },
    {
      title: '进度',
      key: 'progress',
      width: 260,
      render: (_, task) => (
        <Space direction="vertical" size={4} style={{ width: '100%' }}>
          <Progress
            percent={Number(task.progress_percent || 0)}
            size="small"
            status={task.status === 'failed' ? 'exception' : task.status === 'completed' ? 'success' : 'active'}
          />
          <Text type="secondary">{task.processed_rows || 0} / {task.total_rows || 0}</Text>
        </Space>
      )
    },
    {
      title: '来源',
      dataIndex: 'source_filename',
      key: 'source_filename',
      width: 160,
      render: (value) => value || '手动粘贴'
    },
    {
      title: '创建时间',
      dataIndex: 'created_at',
      key: 'created_at',
      width: 170,
      render: (value) => formatDateTime(value)
    },
    {
      title: '说明',
      dataIndex: 'error_message',
      key: 'error_message',
      render: (value) => <Text type="secondary">{value || '-'}</Text>
    },
    {
      title: '操作',
      key: 'actions',
      width: 100,
      render: (_, task) => (
        task.status === 'pending' || task.status === 'running' ? (
          <Button
            size="small"
            danger
            icon={<StopOutlined />}
            loading={cancellingTaskId === task.id}
            onClick={() => cancelImport(task)}
          >
            取消
          </Button>
        ) : null
      )
    }
  ];

  const columns = [
    { title: '名称', dataIndex: 'name', key: 'name', render: (value) => <Text strong>{value}</Text> },
    { title: '跳板状态', key: 'jump', render: (_, pool) => { const item = jumpSummary(pool); return <Tag color={item.color}>{item.text}</Tag>; } },
    { title: 'URL 数量', dataIndex: 'url_count', key: 'url_count', width: 110 },
    { title: '随机配置', key: 'random', render: (_, pool) => <Text>参数 {pool.random_prefix || 'uid'} / 长度 {pool.random_length}</Text> },
    {
      title: '操作', key: 'actions', width: 380, render: (_, pool) => (
        <Space wrap>
          <Button size="small" icon={<UnorderedListOutlined />} onClick={() => openItemsModal(pool)}>管理链接</Button>
          <Button size="small" icon={<EditOutlined />} onClick={() => openEdit(pool)}>编辑/追加 URL</Button>
          <Button size="small" icon={<LinkOutlined />} loading={testingId === pool.id} onClick={() => testNext(pool)}>测试取用</Button>
          <Popconfirm title="确认删除该 URL 池？" description={pool.name} okText="删除" cancelText="取消" onConfirm={() => removePool(pool)}>
            <Button size="small" danger icon={<DeleteOutlined />} loading={deletingId === pool.id}>删除</Button>
          </Popconfirm>
        </Space>
      )
    }
  ];

  const itemColumns = [
    {
      title: 'URL',
      dataIndex: 'url',
      key: 'url',
      render: (value, item) => (
        editingItemId === item.id ? (
          <Input value={editingUrl} onChange={(event) => setEditingUrl(event.target.value)} disabled={itemsSaving} />
        ) : (
          <Text copyable style={{ wordBreak: 'break-all' }}>{value}</Text>
        )
      )
    },
    {
      title: '序号',
      dataIndex: 'index',
      key: 'index',
      width: 80
    },
    {
      title: '创建时间',
      dataIndex: 'created_at',
      key: 'created_at',
      width: 170,
      render: (value) => formatDateTime(value)
    },
    {
      title: '操作',
      key: 'actions',
      width: 160,
      render: (_, item) => (
        editingItemId === item.id ? (
          <Space>
            <Button size="small" type="primary" loading={itemsSaving} onClick={() => saveEditItem(item)}>保存</Button>
            <Button size="small" onClick={cancelEditItem} disabled={itemsSaving}>取消</Button>
          </Space>
        ) : (
          <Space>
            <Button size="small" icon={<EditOutlined />} onClick={() => startEditItem(item)} disabled={Boolean(editingItemId)}>编辑</Button>
            <Popconfirm title="确认删除该 URL？" description={item.url} okText="删除" cancelText="取消" onConfirm={() => removePoolItem(item)}>
              <Button size="small" danger icon={<DeleteOutlined />} loading={deletingItemId === item.id} disabled={Boolean(editingItemId)}>删除</Button>
            </Popconfirm>
          </Space>
        )
      )
    }
  ];

  return (
    <Space direction="vertical" size="large" className="mm-page">
      <PageHeader
        title="URL 池"
        description="上传或粘贴 URL 后将在后台持续导入，切换页面不影响进度。"
        extra={<Space><Button icon={<ReloadOutlined />} loading={loading} onClick={() => { loadPools(); loadImportTasks(); }}>刷新</Button><Button type="primary" icon={<PlusOutlined />} onClick={openCreate}>添加 URL 池</Button></Space>}
      />

      <Collapse
        defaultActiveKey={activeImportCount > 0 ? ['imports'] : []}
        items={[{
          key: 'imports',
          label: `导入任务${activeImportCount > 0 ? `（进行中 ${activeImportCount}）` : ''}`,
          children: (
            <Table
              rowKey="id"
              size="small"
              bordered
              loading={importTasksLoading}
              columns={importTaskColumns}
              dataSource={importTasks}
              pagination={false}
              locale={{ emptyText: '暂无导入任务。保存 URL 池并上传文件后，任务会显示在这里。' }}
            />
          )
        }]}
      />

      {nextResult ? <Alert type="info" showIcon message="最近取用 URL" description={<Text copyable style={{ wordBreak: 'break-all' }}>{nextResult}</Text>} /> : null}

      <Card bordered={false} styles={{ body: { padding: 0 } }}>
        <Table
          rowKey="id"
          bordered
          loading={loading}
          columns={columns}
          dataSource={pools}
          locale={{ emptyText: '暂无 URL 池。' }}
          pagination={{
            current: page,
            pageSize,
            total,
            showSizeChanger: true,
            showTotal: (value) => `共 ${value} 条，第 ${page} / ${totalPages} 页`,
            onChange: (nextPage, nextPageSize) => {
              setPage(nextPage);
              setPageSize(nextPageSize);
            }
          }}
        />
      </Card>

      <Modal title={editing ? '编辑 URL 池 / 追加 URL' : '添加 URL 池'} open={modalOpen} onCancel={() => setModalOpen(false)} onOk={submitPool} confirmLoading={saving} okText="保存" cancelText="取消" width={820} destroyOnHidden>
        <Form form={form} layout="vertical" initialValues={emptyForm} disabled={saving}>
          <Form.Item name="name" label="名称" rules={[{ required: true, message: '请输入 URL 池名称' }]}>
            <Input />
          </Form.Item>
          <Form.Item label="上传 URL 文件（后台导入）">
            <Dragger
              maxCount={1}
              accept=".txt,.csv,text/plain,text/csv"
              beforeUpload={(file) => {
                const fileError = validateImportFile(file);
                if (fileError) {
                  message.error(fileError);
                  return Upload.LIST_IGNORE;
                }
                setImportFile(file);
                return false;
              }}
              onRemove={() => {
                setImportFile(null);
              }}
              fileList={importFile ? [{ uid: 'import-file', name: importFile.name, status: 'done' }] : []}
            >
              <p className="ant-upload-drag-icon"><InboxOutlined /></p>
              <p className="ant-upload-text">点击或拖拽文件到此处</p>
              <p className="ant-upload-hint">保存后立即开始后台导入，可离开页面稍后查看进度。</p>
            </Dragger>
          </Form.Item>
          <Form.Item name="url_text" label="或粘贴 URL 文本（后台导入）" tooltip={editing ? '编辑时填写的新 URL 会追加导入，不会清空已有 URL。' : '创建 URL 池时可直接批量导入 URL。'}>
            <TextArea rows={7} placeholder={'每行一个 URL，例如：\nexample.com\nhttps://example.com/page?a=1\nexample.com/path?source=mail'} />
          </Form.Item>
          <Form.Item name="jump_enabled" label="开启跳板参数拼接" valuePropName="checked">
            <Switch checkedChildren="开启" unCheckedChildren="关闭" />
          </Form.Item>
          <Alert type="info" showIcon style={{ marginBottom: 16 }} message="跳板说明" description="不开启时直接调用上方 URL；开启时在当前轮询出的 URL 上追加 r 和随机参数，r 参数来源直接复用上方 URL 输入框中的当前 URL。" />
          <Space align="start" style={{ width: '100%' }} size="large">
            <Form.Item name="prefix_mode" label="随机参数名选项" style={{ flex: 1, minWidth: 180 }}>
              <Select options={paramOptions} onChange={updateParamMode} />
            </Form.Item>
            <Form.Item noStyle shouldUpdate={(prev, next) => prev.prefix_mode !== next.prefix_mode}>
              {({ getFieldValue }) => getFieldValue('prefix_mode') === '__custom__' ? (
                <Form.Item name="random_prefix" label="自定义随机参数名" rules={[{ required: true, message: '请输入随机参数名' }]} style={{ flex: 1, minWidth: 180 }}>
                  <Input placeholder="例如 click_id" />
                </Form.Item>
              ) : <Form.Item name="random_prefix" hidden><Input /></Form.Item>}
            </Form.Item>
            <Form.Item name="random_length" label="随机长度" rules={[{ required: true, message: '请输入随机长度' }]} style={{ flex: 1, minWidth: 160 }}>
              <InputNumber min={1} max={128} style={{ width: '100%' }} />
            </Form.Item>
          </Space>
          <Form.Item name="random_chars" label="随机字符集" rules={[{ required: true, message: '请输入随机字符集' }]}>
            <Input />
          </Form.Item>
        </Form>
      </Modal>

      <Modal
        title={managingPool ? `管理链接 - ${managingPool.name}` : '管理链接'}
        open={itemsModalOpen}
        onCancel={closeItemsModal}
        footer={null}
        width={980}
        destroyOnHidden
      >
        <Space direction="vertical" size="middle" style={{ width: '100%' }}>
          <div>
            <Text type="secondary">每行一个 URL，支持批量新增。共 {itemsTotal} 条，第 {itemsPage} / {itemsTotalPages} 页</Text>
            <TextArea
              rows={4}
              value={newUrlText}
              placeholder={'每行一个 URL，例如：\nexample.com/page\nhttps://example.com/offer'}
              onChange={(event) => setNewUrlText(event.target.value)}
              disabled={itemsSaving}
              style={{ marginTop: 12 }}
            />
            <Space style={{ marginTop: 12 }}>
              <Button type="primary" icon={<PlusOutlined />} loading={itemsSaving} onClick={submitNewUrls}>
                新增链接
              </Button>
              <Popconfirm
                title={`确认批量删除 ${selectedItemIds.length} 条链接？`}
                description="删除后不可恢复"
                okText="删除"
                cancelText="取消"
                disabled={!selectedItemIds.length}
                onConfirm={removeSelectedItems}
              >
                <Button danger icon={<DeleteOutlined />} disabled={!selectedItemIds.length} loading={batchDeleting}>
                  批量删除{selectedItemIds.length ? `（${selectedItemIds.length}）` : ''}
                </Button>
              </Popconfirm>
              <Popconfirm
                title="确定要删除该池内的所有链接吗？"
                description="此操作不可恢复"
                okText="全部删除"
                cancelText="取消"
                disabled={!itemsTotal}
                onConfirm={removeAllPoolItems}
              >
                <Button danger disabled={!itemsTotal} loading={clearingAll}>
                  全部删除
                </Button>
              </Popconfirm>
            </Space>
          </div>
          <Table
            rowKey="id"
            bordered
            size="small"
            loading={itemsLoading}
            columns={itemColumns}
            dataSource={poolItems}
            rowSelection={{
              selectedRowKeys: selectedItemIds,
              onChange: setSelectedItemIds,
              getCheckboxProps: () => ({ disabled: Boolean(editingItemId) || batchDeleting || clearingAll })
            }}
            locale={{ emptyText: '该池暂无 URL 链接。' }}
            pagination={{
              current: itemsPage,
              pageSize: itemsPageSize,
              total: itemsTotal,
              showSizeChanger: true,
              showTotal: (value) => `共 ${value} 条`,
              onChange: (nextPage, nextPageSize) => {
                setItemsPage(nextPage);
                setItemsPageSize(nextPageSize);
                loadPoolItems(managingPool, nextPage, nextPageSize);
              }
            }}
          />
        </Space>
      </Modal>
    </Space>
  );
}
