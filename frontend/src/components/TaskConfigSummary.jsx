import { Collapse, Descriptions, Space, Tag, Typography } from 'antd';

const { Text } = Typography;

function formatScheduledAtDisplay(task) {
  if (!task?.scheduled_at) return '未设置';
  const localText = String(task.scheduled_at).replace('T', ' ').slice(0, 16);
  return `${localText}（${task.timezone || 'Asia/Shanghai'}）`;
}

export default function TaskConfigSummary({ task, flat = false }) {
  if (!task) return null;

  const senderText = (task.channel_senders || [])
    .filter((item) => item.enabled !== false)
    .map((item) => (item.name ? `${item.name} <${item.email}>` : item.email))
    .join('；') || '-';
  const monitorEmails = Array.isArray(task.monitor_emails)
    ? task.monitor_emails.map((item) => String(item || '').trim()).filter(Boolean)
    : [];
  const monitorText = monitorEmails.length > 0 ? monitorEmails.join('；') : '未配置';

  const content = (
    <Descriptions bordered size="small" column={{ xs: 1, sm: 2, md: 3 }}>
      <Descriptions.Item label="通道">{task.channel_name || '-'}</Descriptions.Item>
      <Descriptions.Item label="模板">{task.template_name || '-'}</Descriptions.Item>
      <Descriptions.Item label="收件人池">{task.recipient_list_name || '-'}</Descriptions.Item>
      <Descriptions.Item label="URL 池">{task.url_pool_name || (task.url_pool_id ? `池 #${task.url_pool_id}` : '未绑定')}</Descriptions.Item>
      <Descriptions.Item label="监控邮箱" span={3}>{monitorText}</Descriptions.Item>
      <Descriptions.Item label="发件人（脱敏）" span={3}>
        <Space direction="vertical" size={4}>
          <Text>{senderText}</Text>
        </Space>
      </Descriptions.Item>
      <Descriptions.Item label="发件人轮询">
        {task.sender_rotation_enabled ? <Tag color="processing">已启用</Tag> : <Tag>未启用</Tag>}
      </Descriptions.Item>
      <Descriptions.Item label="定时启动">{formatScheduledAtDisplay(task)}</Descriptions.Item>
      <Descriptions.Item label="发送速率">
        {Number(task.rate_limit || 0) > 0 ? `${task.rate_limit} 封/秒` : '不限速'}
      </Descriptions.Item>
      <Descriptions.Item label="监控间隔">
        {Number(task.monitor_interval || 0) > 0 ? `每成功 ${task.monitor_interval} 封触发` : '已关闭'}
      </Descriptions.Item>
      <Descriptions.Item label="批量大小">{task.send_http_batch_size || 50}</Descriptions.Item>
      <Descriptions.Item label="并发模式">
        {task.auto_concurrency ? <Tag color="processing">自动调整</Tag> : <Tag>固定并发</Tag>}
      </Descriptions.Item>
      <Descriptions.Item label="并发数">{task.concurrency || 1}</Descriptions.Item>
      <Descriptions.Item label="代理">
        {task.use_proxy ? <Tag color="blue">已启用</Tag> : <Tag>未启用</Tag>}
        {task.use_proxy ? ` ${task.proxy_name || ''}（${task.proxy_address || '-'}）` : ''}
      </Descriptions.Item>
      <Descriptions.Item label="代理延迟">
        {task.proxy_latency_ms ? `${Number(task.proxy_latency_ms).toFixed(1)} ms` : '暂无记录'}
      </Descriptions.Item>
    </Descriptions>
  );

  if (flat) return content;

  return (
    <Collapse
      items={[{
        key: 'config',
        label: '任务配置摘要',
        children: content,
      }]}
    />
  );
}
