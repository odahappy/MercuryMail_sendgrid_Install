import { getErrorMessage } from '../api/index.js';
import { useCallback, useEffect, useState } from 'react';
import { Card, Table, Tooltip, Typography, message } from 'antd';
import { getTaskFailures } from '../api/tasks.js';
import { formatDateTime } from '../utils/taskDetailFormat.js';

const { Text } = Typography;

function truncateText(text, maxLength = 48) {
  const value = String(text || '').trim() || '发送失败';
  if (value.length <= maxLength) return value;
  return `${value.slice(0, maxLength)}…`;
}

function formatFailureReason(text) {
  const value = String(text || '').trim();
  if (!value) return '发送失败';
  return value.replace(/^error:\s*/i, '').trim() || '发送失败';
}

export default function FailuresTable({ taskId, active = true, scrollRef }) {
  const [loading, setLoading] = useState(false);
  const [items, setItems] = useState([]);
  const [total, setTotal] = useState(0);
  const [page, setPage] = useState(1);
  const [pageSize, setPageSize] = useState(20);

  const loadFailures = useCallback(async (nextPage, nextPageSize) => {
    if (!taskId || !active) return;
    setLoading(true);
    try {
      const data = await getTaskFailures(taskId, nextPage, nextPageSize);
      setItems(data.items || []);
      setTotal(data.total || 0);
      setPage(data.page || nextPage);
      setPageSize(data.page_size || nextPageSize);
    } catch (error) {
      message.error(getErrorMessage(error, '加载失败明细失败'));
    } finally {
      setLoading(false);
    }
  }, [taskId, active]);

  useEffect(() => {
    if (!active || !taskId) return undefined;
    loadFailures(1, pageSize);
    return undefined;
  }, [taskId, active, loadFailures, pageSize]);

  const columns = [
    { title: '邮箱', dataIndex: 'email', key: 'email', width: 240, render: (value) => value || '—' },
    {
      title: '发件人',
      dataIndex: 'sender_email',
      key: 'sender_email',
      width: 200,
      render: (value) => value || '—',
    },
    {
      title: '失败原因',
      dataIndex: 'error',
      key: 'error',
      render: (value) => (
        <Tooltip title={formatFailureReason(value)}>
          <Text>{truncateText(formatFailureReason(value))}</Text>
        </Tooltip>
      ),
    },
    {
      title: '失败时间',
      dataIndex: 'created_at',
      key: 'created_at',
      width: 180,
      render: (value) => formatDateTime(value),
    },
    { title: '重试次数', dataIndex: 'retry_count', key: 'retry_count', width: 90 },
  ];

  return (
    <div ref={scrollRef} className="task-cockpit__failures-tab">
      <Card title="SDK 发送失败明细" bordered={false}>
        <Text type="secondary" style={{ display: 'block', marginBottom: 12 }}>
          仅展示 SendGrid SDK 即时发送失败记录。
        </Text>
        <Table
          rowKey={(row, index) => `${row.email || 'unknown'}-${row.sender_email || ''}-${row.created_at || ''}-${row.retry_count ?? 0}-${index}`}
          loading={loading}
          columns={columns}
          dataSource={items}
          locale={{ emptyText: '暂无 SDK 发送失败记录' }}
          pagination={{
            current: page,
            pageSize,
            total,
            showSizeChanger: true,
            onChange: (nextPage, nextPageSize) => loadFailures(nextPage, nextPageSize),
          }}
        />
      </Card>
    </div>
  );
}
