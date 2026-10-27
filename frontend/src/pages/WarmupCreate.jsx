import { getErrorMessage } from '../api/index.js';
import { useEffect, useMemo, useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { Alert, Button, Card, Form, Input, InputNumber, Radio, Select, Space, Switch, TimePicker, Typography, message } from 'antd';
import dayjs from 'dayjs';
import { listChannels } from '../api/channels.js';
import { createWarmupPlan, previewWarmupContent } from '../api/warmup.js';

const { Title, Text } = Typography;
const { TextArea } = Input;

const TIMEZONE_OPTIONS = [
  { value: 'Asia/Shanghai', label: 'Asia/Shanghai 北京时间' },
  { value: 'Asia/Tokyo', label: 'Asia/Tokyo 东京时间' },
];

const LANGUAGE_OPTIONS = [
  { value: 'zh', label: '中文' },
  { value: 'ja', label: '日文' },
  { value: 'en', label: '英文' },
  { value: 'de', label: '德语' },
  { value: 'fr', label: '法语' },
  { value: 'es', label: '西班牙语' },
  { value: 'it', label: '意大利语' },
  { value: 'pt', label: '葡萄牙语' },
  { value: 'ko', label: '韩语' },
  { value: 'random', label: '随机语言' },
];

const EMAIL_TYPE_OPTIONS = [
  { value: 'business', label: '商务邮件' },
  { value: 'personal', label: '个人邮件' },
  { value: 'notice', label: '通知邮件' },
  { value: 'greeting', label: '问候邮件' },
  { value: 'thanks', label: '感谢邮件' },
  { value: 'random', label: '随机类型' },
];

const TONE_OPTIONS = ['自然专业', '正式严谨', '友好亲切', '轻松日常', '简洁直接', '热情感谢', '礼貌委婉', '随机语气'].map((value) => ({ value, label: value }));

function countActiveSenders(channel) {
  const senders = Array.isArray(channel?.sender_emails) ? channel.sender_emails : [];
  return senders.filter((item) => item?.enabled !== false && String(item?.email || '').trim()).length;
}

export default function WarmupCreate() {
  const navigate = useNavigate();
  const [form] = Form.useForm();
  const [channels, setChannels] = useState([]);
  const [loading, setLoading] = useState(false);
  const [saving, setSaving] = useState(false);
  const [previewing, setPreviewing] = useState(false);
  const [previewContent, setPreviewContent] = useState(null);
  const contentMode = Form.useWatch('content_mode', form) || 'ai';
  const aiGenerationMode = Form.useWatch('ai_generation_mode', form) || 'conditions';
  const selectedChannelId = Form.useWatch('channel_id', form);
  const selectedChannel = useMemo(
    () => channels.find((item) => Number(item.id) === Number(selectedChannelId)) || null,
    [channels, selectedChannelId],
  );
  const selectedChannelSenderCount = countActiveSenders(selectedChannel);
  const senderRotationAvailable = selectedChannelSenderCount > 1;
  const previewKey = useMemo(() => JSON.stringify(previewContent?.request || {}), [previewContent]);

  useEffect(() => {
    let mounted = true;
    async function loadOptions() {
      setLoading(true);
      try {
        const channelData = await listChannels({ page: 1, page_size: 100 });
        if (!mounted) return;
        setChannels((channelData.items || []).filter((item) => item.enabled !== false && item.is_active !== false && item.use_proxy));
      } catch (error) {
        message.error(getErrorMessage(error, '加载通道失败'));
      } finally {
        if (mounted) setLoading(false);
      }
    }
    loadOptions();
    return () => { mounted = false; };
  }, []);

  useEffect(() => {
    if (!senderRotationAvailable && form.getFieldValue('sender_rotation_enabled')) {
      form.setFieldsValue({ sender_rotation_enabled: false });
    }
  }, [form, senderRotationAvailable, selectedChannelId]);

  const contentRequestFromValues = (values) => {
    const mode = values.ai_generation_mode || 'conditions';
    if (mode === 'manual_prompt') {
      return {
        ai_generation_mode: 'manual_prompt',
        prompt: (values.prompt || '').trim(),
        content_language: 'zh',
        tone: '自然专业',
        must_include: '',
        avoid: '',
        fidelity_mode: 'strict',
        email_type: 'business',
        include_url: false,
      };
    }
    return {
      ai_generation_mode: 'conditions',
      prompt: '',
      content_language: values.content_language || 'zh',
      tone: values.tone || '自然专业',
      must_include: '',
      avoid: '',
      fidelity_mode: 'balanced',
      email_type: values.email_type || 'business',
      include_url: Boolean(values.include_url),
    };
  };

  const generatePreview = async () => {
    try {
      const fields = aiGenerationMode === 'manual_prompt' ? ['ai_generation_mode', 'prompt'] : ['ai_generation_mode', 'email_type', 'content_language', 'tone', 'include_url'];
      const values = await form.validateFields(fields);
      const request = contentRequestFromValues(values);
      setPreviewing(true);
      const content = await previewWarmupContent(request);
      setPreviewContent({ ...content, request });
      message.success('AI 文案已生成，创建后将固定保存到该计划');
    } catch (error) {
      if (!error.errorFields) message.error(getErrorMessage(error, '生成 AI 文案失败'));
    } finally {
      setPreviewing(false);
    }
  };

  const submit = async () => {
    try {
      const values = await form.validateFields();
      const scheduledTime = dayjs.isDayjs(values.scheduled_time) ? values.scheduled_time : dayjs(values.scheduled_time, 'HH:mm:ss');
      const selectedMode = values.content_mode || 'ai';
      const request = selectedMode === 'ai' ? contentRequestFromValues(values) : { ai_generation_mode: 'conditions', prompt: '', content_language: 'zh', tone: '自然专业', must_include: '', avoid: '', fidelity_mode: 'balanced', email_type: 'business', include_url: false };
      const requestKey = JSON.stringify(request);
      const reusableContent = selectedMode === 'ai' && previewContent && previewKey === requestKey ? { subject: previewContent.subject, text_body: previewContent.text_body || '', html_body: previewContent.html_body || '' } : null;
      const manualContent = selectedMode === 'manual' ? {
        subject: values.manual_subject.trim(),
        html_body: (values.manual_html_body || '').trim(),
        text_body: (values.manual_text_body || '').trim(),
      } : null;
      if (selectedMode === 'manual' && !manualContent.html_body && !manualContent.text_body) {
        message.error('HTML 正文和纯文本正文至少填写一个');
        return;
      }
      const payload = {
        name: values.name.trim(),
        channel_id: Number(values.channel_id),
        start_count: Number(values.start_count),
        total_days: Number(values.total_days || 0),
        daily_increase_rate: Number(values.daily_increase_rate),
        max_daily_count: Number(values.max_daily_count),
        base_interval_seconds: Number(values.base_interval_seconds),
        interval_jitter_seconds: Number(values.interval_jitter_seconds),
        scheduled_time: scheduledTime.format('HH:mm:ss'),
        timezone: values.timezone,
        recipients_text: values.recipients_text,
        ...request,
        sender_rotation_enabled: senderRotationAvailable && Boolean(values.sender_rotation_enabled),
        content_mode: selectedMode,
        content: selectedMode === 'manual' ? manualContent : reusableContent,
      };
      setSaving(true);
      const plan = await createWarmupPlan(payload);
      if (selectedMode === 'ai' && plan?.id) {
        window.localStorage.setItem(`warmup_ai_params_${plan.id}`, JSON.stringify(request));
      }
      message.success('AI 养号计划已创建');
      navigate(`/warmup/${plan.id}`);
    } catch (error) {
      if (!error.errorFields) message.error(getErrorMessage(error, '创建养号计划失败'));
    } finally {
      setSaving(false);
    }
  };

  return <Space direction="vertical" size="large" className="mm-page">
    <div><Title level={2} style={{ margin: 0 }}>创建 AI 养号计划</Title><Text type="secondary">每个计划拥有独立收件人、独立文案和独立调度，不占用群发任务锁与队列。</Text></div>
    <Card loading={loading} bordered={false}>
      <Form form={form} layout="vertical" initialValues={{ start_count: 5, total_days: 0, daily_increase_rate: 0.2, max_daily_count: 100, base_interval_seconds: 60, interval_jitter_seconds: 10, timezone: 'Asia/Shanghai', content_language: 'zh', tone: '自然专业', fidelity_mode: 'balanced', sender_rotation_enabled: false, content_mode: 'ai', ai_generation_mode: 'conditions', email_type: 'business', include_url: false }}>
        <Form.Item name="name" label="计划名称" rules={[{ required: true, message: '请输入计划名称' }]}><Input maxLength={160} /></Form.Item>
        <Form.Item name="channel_id" label="绑定通道" extra="养号发送必须使用该通道当前绑定的代理；未绑定代理或代理不可用时会拒绝创建，禁止直连发送。" rules={[{ required: true, message: '请选择通道' }]}><Select showSearch optionFilterProp="label" options={channels.map((item) => ({ value: item.id, label: item.name }))} /></Form.Item>
        <Form.Item
          name="sender_rotation_enabled"
          label="启用发件人轮询"
          valuePropName="checked"
          extra={senderRotationAvailable ? `当前通道有 ${selectedChannelSenderCount} 个启用发件人，可按批次轮询。` : '当前通道少于 2 个启用发件人，无法启用轮询；请先在通道管理中添加并启用多个发件人。'}
        >
          <Switch checkedChildren="开" unCheckedChildren="关" disabled={!senderRotationAvailable} />
        </Form.Item>
        <Space wrap align="start">
          <Form.Item name="start_count" label="起始量" rules={[{ required: true }]}><InputNumber min={1} max={100000} /></Form.Item>
          <Form.Item name="total_days" label="总发送天数" tooltip="填写 0 表示不限制天数" rules={[{ required: true, message: '请输入总发送天数' }]}><InputNumber min={0} max={3650} precision={0} addonAfter="天" /></Form.Item>
          <Form.Item name="daily_increase_rate" label="每日递增比例" rules={[{ required: true }]}><InputNumber min={0} max={10} step={0.01} /></Form.Item>
          <Form.Item name="max_daily_count" label="最大日发送量" rules={[{ required: true }]}><InputNumber min={1} max={1000000} /></Form.Item>
          <Form.Item name="base_interval_seconds" label="基础间隔（秒）" extra="最低1秒，实际等待时间会叠加随机抖动但不会低于1秒。" rules={[{ required: true }]}><InputNumber min={1} max={86400} placeholder="最低1秒" /></Form.Item>
          <Form.Item name="interval_jitter_seconds" label="随机抖动（秒）" rules={[{ required: true }]}><InputNumber min={0} max={86400} /></Form.Item>
          <Form.Item name="scheduled_time" label="每日启动时间" rules={[{ required: true, message: '请选择启动时间' }]}><TimePicker format="HH:mm:ss" /></Form.Item>
          <Form.Item name="timezone" label="时区" rules={[{ required: true }]}><Select style={{ width: 240 }} options={TIMEZONE_OPTIONS} /></Form.Item>
        </Space>
        <Form.Item name="recipients_text" label="独立收件人列表" rules={[{ required: true, message: '请输入收件人' }]}><TextArea rows={8} placeholder="每行一个邮箱，也支持逗号、空格或分号分隔。创建时会去重，发送成功后永久标记。" /></Form.Item>
        <Form.Item name="content_mode" label="生成方式" rules={[{ required: true, message: '请选择生成方式' }]}><Radio.Group onChange={() => setPreviewContent(null)} options={[{ value: 'ai', label: 'AI 生成' }, { value: 'manual', label: '手动输入' }]} /></Form.Item>
        {contentMode === 'ai' ? <>
          <Form.Item name="ai_generation_mode" label="AI 生成方式" rules={[{ required: true }]}>
            <Radio.Group onChange={() => setPreviewContent(null)} options={[{ value: 'conditions', label: '根据条件一键生成' }, { value: 'manual_prompt', label: '手动描述生成' }]} />
          </Form.Item>
          {aiGenerationMode === 'conditions' ? <Card size="small" title="根据条件一键生成" style={{ marginBottom: 16 }}>
            <Space wrap align="start">
              <Form.Item name="email_type" label="邮件类型" rules={[{ required: true }]}><Select style={{ width: 170 }} options={EMAIL_TYPE_OPTIONS} onChange={() => setPreviewContent(null)} /></Form.Item>
              <Form.Item name="content_language" label="语言" rules={[{ required: true }]}><Select style={{ width: 170 }} options={LANGUAGE_OPTIONS} onChange={() => setPreviewContent(null)} /></Form.Item>
              <Form.Item name="tone" label="语气" rules={[{ required: true }]}><Select style={{ width: 180 }} options={TONE_OPTIONS} onChange={() => setPreviewContent(null)} /></Form.Item>
              <Form.Item name="include_url" label="生成带 URL 的邮件" valuePropName="checked"><Switch checkedChildren="带 {{url}}" unCheckedChildren="不带 URL" onChange={() => setPreviewContent(null)} /></Form.Item>
            </Space>
            <Alert type="info" showIcon message="条件模式会根据类型、语言、语气和 URL 开关自动生成 HTML 邮件，不读取手动描述。" />
          </Card> : <Card size="small" title="手动描述生成" style={{ marginBottom: 16 }}>
            <Alert type="info" showIcon message="手动描述会完全按你的输入控制生成，不读取类型、语言、语气和 URL 条件。" style={{ marginBottom: 12 }} />
            <Form.Item name="prompt" label="描述你想生成的邮件" rules={[{ required: true, message: '请输入描述' }]}><TextArea rows={6} onChange={() => setPreviewContent(null)} placeholder="例如：写一封英文商务跟进邮件，礼貌提醒客户查看上周发送的报价，并邀请对方本周安排 15 分钟沟通。" /></Form.Item>
          </Card>}
        </> : <>
          <Alert type="info" showIcon message="手动输入模式不会调用 AI" description="保存后主题、HTML 正文和纯文本正文会直接写入固定养号文案，发送时立即使用这份文案。" />
          <Form.Item name="manual_subject" label="邮件主题" rules={[{ required: true, message: '请输入邮件主题' }]}><Input maxLength={500} /></Form.Item>
          <Form.Item name="manual_html_body" label="HTML 正文"><TextArea rows={10} wrap="soft" style={{ whiteSpace: 'pre-wrap', overflowWrap: 'anywhere', wordBreak: 'break-word' }} placeholder="可选，填写 HTML 邮件正文" /></Form.Item>
          <Form.Item name="manual_text_body" label="纯文本正文"><TextArea rows={8} placeholder="可选，填写纯文本正文；HTML 正文和纯文本正文至少填写一个" /></Form.Item>
        </>}
        <Space><Button loading={previewing} disabled={contentMode !== 'ai'} onClick={generatePreview}>生成并预览文案</Button><Button type="primary" loading={saving} onClick={submit}>创建计划</Button><Button onClick={() => navigate('/warmup')}>返回</Button></Space>
      </Form>
    </Card>
    {previewContent && <Card title="AI 文案预览" bordered={false}>
      <Space direction="vertical" style={{ width: '100%' }}>
        <Alert type="success" showIcon message="文案已生成" description="创建计划后，该文案会固定保存，除非在详情页手动重新生成。" />
        <Input value={previewContent.subject} readOnly />
        {previewContent.html_body ? <div style={{ border: '1px solid #f0f0f0', borderRadius: 8, padding: 16 }} dangerouslySetInnerHTML={{ __html: previewContent.html_body }} /> : null}
        <TextArea value={previewContent.text_body || previewContent.html_body || ''} rows={8} readOnly />
      </Space>
    </Card>}
  </Space>;
}
