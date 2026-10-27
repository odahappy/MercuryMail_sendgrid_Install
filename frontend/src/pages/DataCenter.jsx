import { getErrorMessage } from '../api/index.js';
import {
  downloadBlob,
  exportGroupRecipients,
  exportValidRecipients,
  exportValidRecipientsArchive,
  exportValidRecipientsHot,
  listExportTasks,
  normalizeTaskIds,
  readBlobErrorDetail,
} from '../api/export.js';
import { deleteList } from '../api/recipients.js';
import { useEffect, useMemo, useState } from 'react';
import { useRecipientLists } from '../hooks/useRecipientLists.js';
import { useAbortController } from '../hooks/useAbortController.js';
import {
  Alert,
  Button,
  Card,
  Col,
  Modal,
  Radio,
  Row,
  Space,
  Table,
  Tag,
  Typography,
  message,
} from 'antd';
import {
  DatabaseOutlined,
  DeleteOutlined,
  DownloadOutlined,
  FireOutlined,
  HistoryOutlined,
  TeamOutlined,
} from '@ant-design/icons';
import PageHeader from '../components/PageHeader.jsx';

const { Text, Paragraph } = Typography;

import { resolveTaskStatusLabel } from '../utils/taskDetailFormat.js';

const EXPORT_TYPE_OPTIONS = [
  { value: 'success', label: '已送达', description: '仅导出 SendGrid 最终状态为 delivered 的地址' },
  { value: 'soft', label: '可重试/待确认', description: '包含软弹、延迟、已受理但未确认送达等非硬失败地址' },
  { value: 'all', label: '全部可触达', description: '所选任务中未退订、未投诉且未硬失败的全部地址' },
];

function formatTaskStartTime(value, timezone) {
  if (!value) return '-';
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return String(value);
  const tz = String(timezone || 'Asia/Shanghai').trim() || 'Asia/Shanghai';
  try {
    const parts = new Intl.DateTimeFormat('zh-CN', {
      timeZone: tz,
      year: 'numeric',
      month: '2-digit',
      day: '2-digit',
      hour: '2-digit',
      minute: '2-digit',
      second: '2-digit',
      hour12: false,
    }).formatToParts(date);
    const pick = (type) => parts.find((part) => part.type === type)?.value || '';
    return `${pick('year')}-${pick('month')}-${pick('day')} ${pick('hour')}:${pick('minute')}:${pick('second')}`;
  } catch {
    return date.toLocaleString('zh-CN', { hour12: false });
  }
}

function formatDateTime(value) {
  if (!value) return '-';
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return String(value);
  return date.toLocaleString('zh-CN', { hour12: false });
}

function DeliveryExportModal({ open, windowKey, title, onClose, onExport }) {
  const [loadingTasks, setLoadingTasks] = useState(false);
  const [exporting, setExporting] = useState(false);
  const [tasks, setTasks] = useState([]);
  const [selectedTaskIds, setSelectedTaskIds] = useState([]);
  const [exportType, setExportType] = useState('all');
  const [progressText, setProgressText] = useState('');

  useEffect(() => {
    if (!open) {
      setSelectedTaskIds([]);
      setExportType('all');
      setProgressText('');
      return undefined;
    }
    let cancelled = false;
    const loadTasks = async () => {
      setLoadingTasks(true);
      try {
        const data = await listExportTasks(windowKey);
        if (!cancelled) {
          setTasks(Array.isArray(data) ? data : []);
        }
      } catch (error) {
        if (!cancelled) {
          message.error(getErrorMessage(error, '加载任务列表失败'));
          setTasks([]);
        }
      } finally {
        if (!cancelled) {
          setLoadingTasks(false);
        }
      }
    };
    loadTasks();
    return () => {
      cancelled = true;
    };
  }, [open, windowKey]);

  const columns = useMemo(
    () => [
      {
        title: '任务名称',
        dataIndex: 'name',
        key: 'name',
        ellipsis: true,
      },
      {
        title: '开始时间',
        dataIndex: 'started_at',
        key: 'started_at',
        width: 190,
        render: (value, record) => formatTaskStartTime(value, record.timezone),
      },
      {
        title: '状态',
        dataIndex: 'status',
        key: 'status',
        width: 110,
        render: (_, record) => (
          <Tag>{resolveTaskStatusLabel(record)}</Tag>
        ),
      },
    ],
    [],
  );

  const handleExport = async () => {
    const normalizedIds = normalizeTaskIds(selectedTaskIds);
    if (!normalizedIds.length) {
      message.warning('请至少选择一个有效任务');
      return;
    }
    setExporting(true);
    setProgressText('正在生成导出文件，请稍候...');
    try {
      const { blob, filename } = await onExport(normalizedIds, exportType, {
        onProgress: (progress) => setProgressText(progress?.message || '文件生成中，请稍候...'),
      });
      downloadBlob(blob, filename);
      message.success(`${title} 导出已开始下载`);
      onClose();
    } catch (error) {
      message.error(await readBlobErrorDetail(error, '导出失败，请稍后重试'));
    } finally {
      setProgressText('');
      setExporting(false);
    }
  };

  return (
    <Modal
      title={title}
      open={open}
      onCancel={onClose}
      width={860}
      destroyOnClose
      footer={[
        <Button key="cancel" onClick={onClose} disabled={exporting}>
          取消
        </Button>,
        <Button
          key="export"
          type="primary"
          icon={<DownloadOutlined />}
          loading={exporting}
          disabled={loadingTasks || !selectedTaskIds.length}
          onClick={handleExport}
        >
          开始导出
        </Button>,
      ]}
    >
      <Space direction="vertical" size="middle" style={{ width: '100%' }}>
        {progressText ? (
          <Alert type="info" showIcon message={progressText} />
        ) : null}
        <Paragraph type="secondary" style={{ marginBottom: 0 }}>
          请选择要导出的任务，并指定基于 SendGrid 投递结果的导出类型。系统将排除退订、投诉、硬弹、阻止与丢弃地址。
        </Paragraph>
        <Table
          rowKey="id"
          size="small"
          loading={loadingTasks}
          columns={columns}
          dataSource={tasks}
          pagination={{ pageSize: 8, showSizeChanger: false }}
          rowSelection={{
            selectedRowKeys: selectedTaskIds,
            onChange: (keys) => setSelectedTaskIds(keys),
          }}
          locale={{ emptyText: loadingTasks ? '加载中…' : '当前时间窗口内暂无任务' }}
        />
        <div>
          <Text strong style={{ display: 'block', marginBottom: 8 }}>导出类型</Text>
          <Radio.Group value={exportType} onChange={(event) => setExportType(event.target.value)}>
            <Space direction="vertical">
              {EXPORT_TYPE_OPTIONS.map((item) => (
                <Radio key={item.value} value={item.value}>
                  <Text>{item.label}</Text>
                  <Text type="secondary" style={{ marginLeft: 8, fontSize: 12 }}>
                    {item.description}
                  </Text>
                </Radio>
              ))}
            </Space>
          </Radio.Group>
        </div>
      </Space>
    </Modal>
  );
}

export default function DataCenter() {
  const [hotModalOpen, setHotModalOpen] = useState(false);
  const [archiveModalOpen, setArchiveModalOpen] = useState(false);
  const [exportingGlobal, setExportingGlobal] = useState(false);
  const { lists: groups, loading: loadingGroups, refreshLists: loadGroups, removeListFromState } = useRecipientLists();
  const [exportingGroupId, setExportingGroupId] = useState(null);
  const [deletingGroupId, setDeletingGroupId] = useState(null);
  const [globalProgressText, setGlobalProgressText] = useState('');
  const [groupProgressText, setGroupProgressText] = useState('');
  const { createAbortController } = useAbortController();

  useEffect(() => {
    loadGroups().catch((error) => {
      message.error(error?.message || getErrorMessage(error, '加载收件人分组失败'));
    });
  }, [loadGroups]);

  const handleGlobalExport = async () => {
    if (exportingGlobal) return;
    setExportingGlobal(true);
    setGlobalProgressText('正在生成导出文件，请稍候...');
    try {
      const { blob, filename } = await exportValidRecipients('all', {
        onProgress: (progress) => setGlobalProgressText(progress?.message || '文件生成中，请稍候...'),
      });
      downloadBlob(blob, filename);
      message.success('全局有效收件人导出已开始下载');
    } catch (error) {
      message.error(await readBlobErrorDetail(error, '导出失败，请稍后重试'));
    } finally {
      setGlobalProgressText('');
      setExportingGlobal(false);
    }
  };

  const handleGroupExport = async (group) => {
    if (exportingGroupId) return;
    setExportingGroupId(group.id);
    setGroupProgressText('正在生成导出文件，请稍候...');
    try {
      const safeName = String(group.name || 'group').replace(/[^\w\-\u4e00-\u9fff]+/g, '_').slice(0, 80) || 'group';
      const { blob, filename } = await exportGroupRecipients(group.id, `group_${safeName}.txt`, {
        onProgress: (progress) => setGroupProgressText(progress?.message || '文件生成中，请稍候...'),
      });
      downloadBlob(blob, filename);
      message.success(`分组「${group.name}」导出已开始下载`);
    } catch (error) {
      message.error(await readBlobErrorDetail(error, '导出失败，请稍后重试'));
    } finally {
      setGroupProgressText('');
      setExportingGroupId(null);
    }
  };

  const handleDeleteGroup = (group) => {
    if (!group?.id || deletingGroupId) return;
    Modal.confirm({
      title: '确认删除收件人分组',
      content: `删除后将移除分组「${group.name || `#${group.id}`}」及其全部收件人，历史发送日志会保留。是否继续？`,
      okText: '确认删除',
      cancelText: '取消',
      okButtonProps: { danger: true },
      onOk: async () => {
        setDeletingGroupId(group.id);
        try {
          await deleteList(group.id);
          message.success(`分组「${group.name}」已删除`);
          removeListFromState(group.id);
          await loadGroups({ showError: false });
        } catch (error) {
          message.error(getErrorMessage(error, '删除分组失败'));
        } finally {
          setDeletingGroupId(null);
        }
      },
    });
  };

  const groupColumns = [
    {
      title: '分组名称',
      dataIndex: 'name',
      key: 'name',
      ellipsis: true,
    },
    {
      title: '邮箱总数',
      dataIndex: 'total_count',
      key: 'total_count',
      width: 110,
    },
    {
      title: '创建时间',
      dataIndex: 'created_at',
      key: 'created_at',
      width: 180,
      render: (value) => formatDateTime(value),
    },
    {
      title: '操作',
      key: 'action',
      width: 260,
      render: (_, record) => (
        <Space size="small">
          <Button
            type="link"
            icon={<DownloadOutlined />}
            loading={exportingGroupId === record.id}
            disabled={Boolean(exportingGroupId) && exportingGroupId !== record.id}
            onClick={() => handleGroupExport(record)}
          >
            导出全部邮箱
          </Button>
          <Button
            danger
            type="link"
            icon={<DeleteOutlined />}
            loading={deletingGroupId === record.id}
            disabled={Boolean(deletingGroupId) && deletingGroupId !== record.id}
            onClick={() => handleDeleteGroup(record)}
          >
            删除分组
          </Button>
        </Space>
      ),
    },
  ];

  return (
    <Space direction="vertical" size="large" className="mm-page">
      <PageHeader
        title="数据中心"
        description="有效收件人导出基于 SendGrid 投递最终结果；分组导出为收件人池原始数据。任务级 SDK 即时状态请在任务详情页导出。"
      />
      {globalProgressText ? <Alert type="info" showIcon message={globalProgressText} /> : null}
      {groupProgressText ? <Alert type="info" showIcon message={groupProgressText} /> : null}

      <Row gutter={[16, 16]}>
        <Col xs={24} lg={8}>
          <Card title="有效收件人（热数据）" bordered={false} className="mm-export-card">
            <div style={{ fontSize: 32, color: 'var(--mm-primary)', marginBottom: 12 }}>
              <FireOutlined />
            </div>
            <Paragraph type="secondary" style={{ minHeight: 72 }}>
              最近 3 天内的发送任务，按投递结果筛选有效邮箱（排除退订与硬弹）。
            </Paragraph>
            <Button type="primary" icon={<DownloadOutlined />} onClick={() => setHotModalOpen(true)}>
              选择任务并导出
            </Button>
          </Card>
        </Col>
        <Col xs={24} lg={8}>
          <Card title="有效收件人（归档数据）" bordered={false} className="mm-export-card">
            <div style={{ fontSize: 32, color: 'var(--mm-primary)', marginBottom: 12 }}>
              <HistoryOutlined />
            </div>
            <Paragraph type="secondary" style={{ minHeight: 72 }}>
              3~30 天前的发送任务，数据源 send_log_archive，支持相同导出类型。
            </Paragraph>
            <Button type="primary" icon={<DownloadOutlined />} onClick={() => setArchiveModalOpen(true)}>
              选择任务并导出
            </Button>
          </Card>
        </Col>
        <Col xs={24} lg={8}>
          <Card title="有效收件人（全局）" bordered={false} className="mm-export-card">
            <div style={{ fontSize: 32, color: 'var(--mm-primary)', marginBottom: 12 }}>
              <DatabaseOutlined />
            </div>
            <Paragraph type="secondary" style={{ minHeight: 72 }}>
              导出历史有效收件人（基于投递日志），系统会排除退订与硬弹地址，无需选择任务。
            </Paragraph>
            <Button
              type="primary"
              icon={<DownloadOutlined />}
              loading={exportingGlobal}
              onClick={handleGlobalExport}
            >
              直接导出
            </Button>
          </Card>
        </Col>
      </Row>

      <Card
        title={(
          <Space>
            <TeamOutlined />
            <span>按分组导出（原始数据）</span>
          </Space>
        )}
        bordered={false}
      >
        <Paragraph type="secondary">
          导出各收件人池分组内的全部原始邮箱，不排除退订、硬弹或任何投递状态。
        </Paragraph>
        <Table
          rowKey="id"
          size="middle"
          loading={loadingGroups}
          columns={groupColumns}
          dataSource={groups}
          pagination={{ pageSize: 10, showSizeChanger: false }}
          locale={{ emptyText: loadingGroups ? '加载中…' : '暂无收件人分组' }}
        />
      </Card>

      <DeliveryExportModal
        open={hotModalOpen}
        windowKey="hot"
        title="导出有效收件人（热数据）"
        onClose={() => setHotModalOpen(false)}
        onExport={exportValidRecipientsHot}
      />
      <DeliveryExportModal
        open={archiveModalOpen}
        windowKey="archive"
        title="导出有效收件人（归档数据）"
        onClose={() => setArchiveModalOpen(false)}
        onExport={exportValidRecipientsArchive}
      />

    </Space>
  );
}
