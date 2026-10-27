import { DeleteOutlined, InboxOutlined, ReloadOutlined, UploadOutlined } from '@ant-design/icons';
import { Button, Card, Input, Modal, Popconfirm, Progress, Space, Table, Tabs, Tag, Typography, Upload, message } from 'antd';
import { useEffect, useMemo, useRef, useState } from 'react';
import { deleteList, getDeleteProgress, getImportProgress, paste, upload } from '../api/recipients.js';
import { getErrorMessage } from '../api/index.js';
import PageHeader from '../components/PageHeader.jsx';
import { IMPORT_STATUS_FAILED } from '../constants/importStatus.js';
import { useRecipientLists } from '../hooks/useRecipientLists.js';
import { useAbortController } from '../hooks/useAbortController.js';
import { addStoredImportTask, isImportTaskActive, isImportTaskFinished, loadStoredImportTasks, removeStoredImportTask } from '../utils/recipientImportTasks.js';

const { Text, Paragraph } = Typography;
const { TextArea } = Input;

function formatDateTime(value) {
  if (!value) return '-';
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return String(value);
  return date.toLocaleString('zh-CN', { hour12: false });
}

function formatCount(value) {
  const number = Number(value || 0);
  if (!Number.isFinite(number)) return '0';
  return number.toLocaleString('zh-CN');
}

function mergeImportTask(currentTasks, nextTask) {
  if (!nextTask?.taskId) return currentTasks;
  const existed = currentTasks.find((task) => task.taskId === nextTask.taskId);
  const mergedTask = {
    ...existed,
    ...nextTask,
    progress: {
      ...(existed?.progress || {}),
      ...(nextTask.progress || {}),
    },
  };
  return [mergedTask, ...currentTasks.filter((task) => task.taskId !== nextTask.taskId)];
}

const MAX_IMPORT_FILE_SIZE_BYTES = 100 * 1024 * 1024;
const RECIPIENT_IMPORT_EXTENSIONS = new Set(['.txt', '.csv']);

function validateRecipientImportFile(file) {
  if (!file) return '请选择要上传的文件';
  const size = Number(file.size || file.originFileObj?.size || 0);
  if (size <= 0) return '上传文件为空，请选择包含邮箱的文本文件';
  if (size > MAX_IMPORT_FILE_SIZE_BYTES) return '上传文件过大，请上传不超过 100MB 的文件';
  const name = String(file.name || file.originFileObj?.name || '').toLowerCase();
  const extension = name.includes('.') ? name.slice(name.lastIndexOf('.')) : '';
  if (extension && !RECIPIENT_IMPORT_EXTENSIONS.has(extension)) return '仅支持上传 .txt 或 .csv 文件';
  const type = String(file.type || file.originFileObj?.type || '').toLowerCase();
  if (type && !type.startsWith('text/') && type !== 'application/vnd.ms-excel' && type !== 'text/csv') return '仅支持上传文本格式的收件人文件';
  return '';
}

function ImportRecipientsModal({ open, importing, progress, onCancel, onSubmit }) {
  const [mode, setMode] = useState('file');
  const [poolName, setPoolName] = useState('');
  const [fileList, setFileList] = useState([]);
  const [content, setContent] = useState('');

  const resetForm = () => {
    setPoolName('');
    setFileList([]);
    setContent('');
    setMode('file');
  };

  const handleCancel = () => {
    if (importing) return;
    resetForm();
    onCancel();
  };

  const handleOk = async () => {
    const trimmedName = poolName.trim();
    if (mode === 'file') {
      const file = fileList[0];
      const fileError = validateRecipientImportFile(file);
      if (fileError) {
        message.warning(fileError);
        return;
      }
      await onSubmit({ mode, name: trimmedName, file });
      resetForm();
      return;
    }
    const trimmedContent = content.trim();
    if (!trimmedContent) {
      message.warning('请输入要导入的邮箱内容');
      return;
    }
    await onSubmit({ mode, name: trimmedName, content: trimmedContent });
    resetForm();
  };

  return (
    <Modal
      title="导入收件人"
      open={open}
      onCancel={handleCancel}
      onOk={handleOk}
      confirmLoading={importing}
      okText="开始导入"
      cancelText="取消"
      destroyOnHidden
      maskClosable={!importing}
    >
      <Space direction="vertical" size="middle" style={{ width: '100%' }}>
        <Input
          value={poolName}
          onChange={(event) => setPoolName(event.target.value)}
          placeholder="收件人池名称（可选，不填则自动生成）"
          maxLength={160}
          disabled={importing}
        />
        <Tabs
          activeKey={mode}
          onChange={setMode}
          items={[
            {
              key: 'file',
              label: '文件上传',
              children: (
                <Upload.Dragger
                  accept=".txt,.csv,text/plain,text/csv"
                  maxCount={1}
                  fileList={fileList}
                  beforeUpload={(file) => {
                    const fileError = validateRecipientImportFile(file);
                    if (fileError) {
                      message.warning(fileError);
                      return Upload.LIST_IGNORE;
                    }
                    return false;
                  }}
                  onChange={({ fileList: nextFileList }) => setFileList(nextFileList.slice(-1))}
                  onRemove={() => setFileList([])}
                  disabled={importing}
                >
                  <p className="ant-upload-drag-icon"><InboxOutlined /></p>
                  <p className="ant-upload-text">点击或拖拽 TXT / CSV 文件到此区域</p>
                  <p className="ant-upload-hint">每行一个邮箱，最大 100MB，后台导入期间可离开页面。</p>
                </Upload.Dragger>
              ),
            },
            {
              key: 'paste',
              label: '手动粘贴',
              children: (
                <TextArea
                  value={content}
                  onChange={(event) => setContent(event.target.value)}
                  placeholder="每行一个邮箱，例如：user@example.com"
                  rows={8}
                  disabled={importing}
                  showCount
                  maxLength={100 * 1024 * 1024}
                />
              ),
            },
          ]}
        />
        {importing ? <Progress percent={progress} status="active" /> : null}
        <Paragraph type="secondary" style={{ marginBottom: 0 }}>
          导入任务会在后台执行，列表刷新后可查看新分组和导入状态。
        </Paragraph>
      </Space>
    </Modal>
  );
}

export default function Recipients() {
  const { lists, loading, refreshLists, removeListFromState, upsertListToState } = useRecipientLists();
  const [importModalOpen, setImportModalOpen] = useState(false);
  const [importing, setImporting] = useState(false);
  const [uploadProgress, setUploadProgress] = useState(0);
  const [activeImportTasks, setActiveImportTasks] = useState([]);
  const [deleteTasks, setDeleteTasks] = useState(() => {
    try { return JSON.parse(window.sessionStorage.getItem('recipientDeleteTasks') || '{}'); } catch { return {}; }
  });
  const [selectedListIds, setSelectedListIds] = useState([]);
  const pollingRef = useRef(false);
  const { createAbortController, releaseAbortController } = useAbortController();
  const isPageVisibleRef = useRef(typeof document === 'undefined' ? true : document.visibilityState !== 'hidden');

  const pollStoredImportTasks = async ({ showMessage = false } = {}) => {
    if (pollingRef.current) return;
    pollingRef.current = true;
    try {
      const storedTasks = loadStoredImportTasks();
      if (!storedTasks.length) {
        setActiveImportTasks([]);
        return;
      }
      const nextActiveTasks = [];
      let shouldRefresh = false;
      for (const task of storedTasks) {
        try {
          const progress = await getImportProgress(task.taskId);
          const status = String(progress.status || '').toLowerCase();
          if (isImportTaskActive(status)) {
            nextActiveTasks.push({ ...task, progress });
          } else if (isImportTaskFinished(status)) {
            removeStoredImportTask(task.taskId);
            shouldRefresh = true;
            if (showMessage) {
              message.success(`收件人池「${task.poolName || task.listId}」导入完成`);
            }
          } else if (status === IMPORT_STATUS_FAILED) {
            removeStoredImportTask(task.taskId);
            shouldRefresh = true;
            if (showMessage) {
              message.error(progress.error || `收件人池「${task.poolName || task.listId}」导入失败`);
            }
          } else {
            nextActiveTasks.push({ ...task, progress: { ...progress, status: status || 'unknown' } });
          }
        } catch (error) {
          if (error?.response?.status === 404) {
            removeStoredImportTask(task.taskId);
            shouldRefresh = true;
          } else {
            nextActiveTasks.push({ ...task, progress: task.progress || {} });
          }
        }
      }
      setActiveImportTasks(nextActiveTasks);
      if (shouldRefresh) {
        await refreshLists().catch(() => {});
      }
    } finally {
      pollingRef.current = false;
    }
  };

  useEffect(() => {
    const handleVisibility = () => {
      isPageVisibleRef.current = document.visibilityState !== 'hidden';
      if (isPageVisibleRef.current) pollStoredImportTasks({ showMessage: false });
    };
    document.addEventListener('visibilitychange', handleVisibility);
    pollStoredImportTasks({ showMessage: false });
    const timer = window.setInterval(() => {
      if (isPageVisibleRef.current) pollStoredImportTasks({ showMessage: true });
    }, 2000);
    return () => {
      document.removeEventListener('visibilitychange', handleVisibility);
      window.clearInterval(timer);
    };
  }, []);

  const handleImportSubmit = async ({ mode, name, file, content }) => {
    setImporting(true);
    setUploadProgress(0);
    try {
      const result = mode === 'file'
        ? await upload(name, file, setUploadProgress)
        : await paste(name, content);
      addStoredImportTask({
        taskId: result.task_id,
        listId: result.list_id,
        poolName: result.name,
      });
      setActiveImportTasks((currentTasks) => mergeImportTask(currentTasks, {
        taskId: result.task_id,
        listId: result.list_id,
        poolName: result.name,
        progress: {
          task_id: result.task_id,
          list_id: result.list_id,
          list_name: result.name,
          processed: 0,
          total: result.total_lines || 0,
          accepted: 0,
          invalid: 0,
          skipped_duplicates: 0,
          percentage: 0,
          status: result.status,
          phase: 'parsing',
        },
      }));
      upsertListToState({
        id: result.list_id,
        name: result.name,
        total_count: result.total_accepted || 0,
        valid_count: 0,
        invalid_count: 0,
        status: result.status,
        created_at: new Date().toISOString(),
      });
      message.success('收件人导入任务已创建');
      setImportModalOpen(false);
      await pollStoredImportTasks({ showMessage: false });
      await refreshLists();
    } catch (error) {
      message.error(getErrorMessage(error, '导入收件人失败'));
    } finally {
      setUploadProgress(0);
      setImporting(false);
    }
  };

  useEffect(() => {
    window.sessionStorage.setItem('recipientDeleteTasks', JSON.stringify(deleteTasks));
  }, [deleteTasks]);

  useEffect(() => {
    const activeTasks = Object.values(deleteTasks).filter((task) => task.taskId && task.status !== 'completed');
    if (!activeTasks.length) return undefined;
    let mounted = true;
    const poll = async () => {
      await Promise.all(activeTasks.map(async (task) => {
        const controller = createAbortController();
        try {
          const progress = await getDeleteProgress(task.taskId, { signal: controller.signal });
          if (!mounted) return;
          if (progress.status === 'completed') {
            removeListFromState(progress.list_id);
            setSelectedListIds((ids) => ids.filter((id) => id !== progress.list_id));
          }
          setDeleteTasks((current) => ({ ...current, [progress.list_id]: { ...current[progress.list_id], ...progress, taskId: task.taskId } }));
        } catch (error) {
          if (error?.response?.status === 404 && mounted) {
            setDeleteTasks((current) => ({ ...current, [task.listId]: { ...task, status: 'failed', error: '删除任务不存在或已过期，请重试' } }));
          }
        } finally {
          releaseAbortController(controller);
        }
      }));
    };
    poll();
    const timer = window.setInterval(poll, 1500);
    return () => { mounted = false; window.clearInterval(timer); };
  }, [createAbortController, deleteTasks, releaseAbortController, removeListFromState]);

  const startDelete = async (record) => {
    setDeleteTasks((current) => ({ ...current, [record.id]: { listId: record.id, listName: record.name, status: 'starting', total_recipients: Number(record.total_count || 0), deleted_recipients: 0 } }));
    try {
      const result = await deleteList(record.id);
      setDeleteTasks((current) => ({ ...current, [record.id]: { ...current[record.id], taskId: result.task_id, status: 'running' } }));
    } catch (error) {
      setDeleteTasks((current) => ({ ...current, [record.id]: { ...current[record.id], status: 'failed', error: getErrorMessage(error, '删除收件人池失败') } }));
    }
  };

  const confirmBatchDelete = () => {
    const selected = lists.filter((item) => selectedListIds.includes(item.id));
    Modal.confirm({
      title: `确认删除选中的 ${selected.length} 个收件人池？`,
      content: '删除任务将在后台并行执行，删除后不可恢复。',
      okText: '确认删除', cancelText: '取消', okButtonProps: { danger: true },
      onOk: () => Promise.all(selected.map(startDelete)),
    });
  };

  const columns = useMemo(() => [
    {
      title: '分组名称',
      dataIndex: 'name',
      key: 'name',
      render: (value, record) => (
        <Space direction="vertical" size={2}>
          <Text strong>{value || `收件人池 ${record.id}`}</Text>
          <Text type="secondary">ID：{record.id}</Text>
        </Space>
      ),
    },
    {
      title: '收件人数',
      dataIndex: 'total_count',
      key: 'total_count',
      width: 140,
      render: (value, record) => formatCount(value ?? record.count ?? record.recipient_count),
    },
    {
      title: '状态',
      dataIndex: 'status',
      key: 'status',
      width: 140,
      render: (value, record) => {
        const deletion = deleteTasks[record.id];
        if (deletion) {
          const total = Number(deletion.total_recipients || record.total_count || 0);
          const deleted = Number(deletion.deleted_recipients || 0);
          const percent = deletion.status === 'completed' ? 100 : total > 0 ? Math.min(99, Math.round((deleted * 100) / total)) : 0;
          if (deletion.status === 'failed') return <Space direction="vertical" size={2}><Tag color="error">删除失败</Tag><Text type="danger">{deletion.error}</Text><Button size="small" onClick={() => startDelete(record)}>重试</Button></Space>;
          return <Space direction="vertical" size={2} style={{ width: 120 }}><Tag>删除中</Tag><Progress percent={percent} size="small" status="active" /></Space>;
        }
        const status = String(value || 'ready').toLowerCase();
        if (isImportTaskActive(status)) return <Tag color="processing">导入中</Tag>;
        if (status === IMPORT_STATUS_FAILED) return <Tag color="error">导入失败</Tag>;
        return <Tag color="success">可用</Tag>;
      },
    },
    {
      title: '创建时间',
      dataIndex: 'created_at',
      key: 'created_at',
      width: 220,
      render: formatDateTime,
    },
    {
      title: '操作',
      key: 'actions',
      width: 140,
      render: (_, record) => {
        const deletion = deleteTasks[record.id];
        return deletion?.status === 'completed' ? null : (
          <Popconfirm
            title="删除收件人池"
            description="删除前系统会检查任务引用，确认继续？"
            okText="删除"
            cancelText="取消"
            okButtonProps={{ danger: true }}
            onConfirm={() => startDelete(record)}
          >
            <Button danger icon={<DeleteOutlined />} disabled={deletion?.status === 'running' || deletion?.status === 'starting'}>删除</Button>
          </Popconfirm>
        );
      },
    },
  ], [deleteTasks, removeListFromState]);

  return (
    <div className="mm-page">
      <PageHeader
        title="收件人池"
        description="管理收件人分组，支持文件上传和手动粘贴导入。原始邮箱导出请在数据中心执行。"
        extra={(
          <Space>
            <Button type="primary" icon={<UploadOutlined />} onClick={() => setImportModalOpen(true)}>
              导入收件人
            </Button>
            <Button icon={<ReloadOutlined />} onClick={() => refreshLists().catch((error) => message.error(getErrorMessage(error, '刷新收件人池失败')))} loading={loading}>
              刷新
            </Button>
          </Space>
        )}
      />
      {activeImportTasks.length ? (
        <Card title="正在导入" size="small">
          <Space direction="vertical" style={{ width: '100%' }}>
            {activeImportTasks.map((task) => {
              const progress = task.progress || {};
              const percent = Math.round(Number(progress.percentage || 0));
              return (
                <Space key={task.taskId} direction="vertical" style={{ width: '100%' }} size={4}>
                  <Text>{task.poolName || progress.list_name || `收件人池 ${task.listId}`}</Text>
                  <Progress percent={percent} status="active" />
                  <Text type="secondary">
                    {progress.phase === 'writing' ? '正在写入数据库' : '正在解析文件'}，已处理 {formatCount(progress.processed)} / {formatCount(progress.total)} 行，有效 {formatCount(progress.accepted)}，无效 {formatCount(progress.invalid)}，重复 {formatCount(progress.skipped_duplicates)}
                  </Text>
                </Space>
              );
            })}
          </Space>
        </Card>
      ) : null}
      <Card title="收件人分组列表" extra={<Button danger icon={<DeleteOutlined />} disabled={!selectedListIds.length} onClick={confirmBatchDelete}>批量删除（{selectedListIds.length}）</Button>}>
        <Table
          rowKey="id"
          loading={loading}
          columns={columns}
          rowClassName={(record) => deleteTasks[record.id] ? 'mm-row-disabled' : ''}
          rowSelection={{ selectedRowKeys: selectedListIds, onChange: setSelectedListIds, getCheckboxProps: (record) => ({ disabled: ['running', 'starting'].includes(deleteTasks[record.id]?.status) }) }}
          dataSource={lists}
          locale={{ emptyText: <Space><InboxOutlined />暂无收件人池</Space> }}
          pagination={{ pageSize: 10, showSizeChanger: true }}
        />
      </Card>
      <ImportRecipientsModal
        open={importModalOpen}
        importing={importing}
        progress={uploadProgress}
        onCancel={() => setImportModalOpen(false)}
        onSubmit={handleImportSubmit}
      />
    </div>
  );
}
