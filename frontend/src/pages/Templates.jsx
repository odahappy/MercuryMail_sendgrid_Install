import { getErrorMessage } from '../api/index.js';
import { useEffect, useMemo, useRef, useState } from 'react';
import { Button, Card, Col, Form, Input, InputNumber, Modal, Popconfirm, Row, Select, Space, Switch, Table, Tabs, Tag, Typography, message } from 'antd';
import { PlusOutlined, ReloadOutlined } from '@ant-design/icons';
import { createTemplate, deleteTemplate, getTemplates, previewTemplate, updateTemplate } from '../api/templates.js';
import { getPools } from '../api/urls.js';

const { Title, Text, Paragraph } = Typography;
const { TextArea } = Input;
const DEFAULT_RANDOM_CHARS = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789';
const emptyForm = { name: '', subject: '', content_type: 'html', html_content: '<p>你好 {{name}}，请访问：{{url}}</p>', text_content: '', random_length: 16, random_chars: DEFAULT_RANDOM_CHARS, random_rules: {}, campaign_id: '', unsubscribe_enabled: false, unsubscribe_url: '', unsubscribe_description: '一键退订' };
const variables = [
  { label: '收件人邮箱', placeholder: '{{email}}' }, { label: '收件人姓名', placeholder: '{{name}}' }, { label: '发件人名称', placeholder: '{{from_name}}' }, { label: '发件人邮箱', placeholder: '{{from_email}}' }, { label: 'URL 池链接', placeholder: '{{url}}' }, { label: '退订链接', placeholder: '{{unsubscribe_url}}' }, { label: '唯一 ID', placeholder: '{{uuid}}' }, { label: '随机字符串', placeholder: '{{random}}' }, { label: '活动 ID', placeholder: '{{campaign_id}}' }, { label: '当前日期', placeholder: '{{date}}' }
];
const variableLabels = { email: '收件人邮箱', name: '收件人姓名', from_name: '发件人名称', from_email: '发件人邮箱', url: 'URL 池链接', unsubscribe_url: '退订链接', uuid: '唯一 ID', random: '随机字符串', campaign_id: '活动 ID', date: '当前日期' };

function formatTime(value) { return value ? new Date(value).toLocaleString() : '-'; }
function normalizeTemplate(template) {
  const rawRules = template.random_rules && typeof template.random_rules === 'object' ? template.random_rules : {};
  const normalizedRules = {};
  Object.keys(rawRules || {}).forEach((key) => {
    const rule = rawRules[key];
    if (!rule || typeof rule !== 'object') return;
    normalizedRules[key] = {
      upper: rule.upper ?? rule.uppercase_count ?? 0,
      lower: rule.lower ?? rule.lowercase_count ?? 0,
      digits: rule.digits ?? rule.digit_count ?? 0,
      special: rule.special ?? 0
    };
  });
  return {
    ...emptyForm,
    name: template.name || '',
    subject: template.subject || '',
    content_type: template.content_type || (template.text_content && !template.html_content ? 'text' : 'html'),
    html_content: template.html_content || '',
    text_content: template.text_content || '',
    random_length: template.random_length || 16,
    random_chars: template.random_chars || DEFAULT_RANDOM_CHARS,
    random_rules: normalizedRules,
    campaign_id: template.campaign_id || '',
    unsubscribe_enabled: Boolean(template.unsubscribe_enabled),
    unsubscribe_url: template.unsubscribe_url || '',
    unsubscribe_description: template.unsubscribe_description || '一键退订'
  };
}
function extractVariables(content) { const found = new Set(); const pattern = /{{\s*([a-zA-Z_][a-zA-Z0-9_]*)\s*}}/g; let match = pattern.exec(content || ''); while (match) { found.add(match[1]); match = pattern.exec(content || ''); } return Array.from(found).sort((a, b) => a.localeCompare(b, 'zh-Hans-CN')); }
function htmlToPlainText(html) { const source = String(html || ''); if (!source.trim()) return ''; const prepared = source.replace(/<\s*br\s*\/?\s*>/gi, '\n').replace(/<\s*\/\s*(p|div|section|article|header|footer|tr|li|h[1-6])\s*>/gi, '\n').replace(/<\s*li\b[^>]*>/gi, '- '); const doc = new DOMParser().parseFromString(prepared, 'text/html'); return (doc.body.textContent || '').replace(/\u00a0/g, ' ').replace(/[ \t]+\n/g, '\n').replace(/\n[ \t]+/g, '\n').replace(/\n{3,}/g, '\n\n').trim(); }

export default function Templates() {
  const htmlRef = useRef(null); const textRef = useRef(null); const selectionRef = useRef({ editor: 'html', start: 0, end: 0 }); const pendingSelectionRef = useRef(null); const [templates, setTemplates] = useState([]); const [urlPools, setUrlPools] = useState([]); const [page, setPage] = useState(1); const [pageSize, setPageSize] = useState(20); const [total, setTotal] = useState(0); const [loading, setLoading] = useState(false); const [saving, setSaving] = useState(false); const [editorOpen, setEditorOpen] = useState(false); const [editing, setEditing] = useState(null); const [form, setForm] = useState(emptyForm); const [activeEditor, setActiveEditor] = useState('html'); const [previewOpen, setPreviewOpen] = useState(false); const [previewTarget, setPreviewTarget] = useState(null); const [previewResult, setPreviewResult] = useState(null); const [previewUrlPoolId, setPreviewUrlPoolId] = useState(undefined);
  const activeContent = form.content_type === 'text' ? form.text_content : form.html_content;
  const detectedVariables = useMemo(() => extractVariables(`${form.subject}\n${activeContent}\n${form.unsubscribe_url}`), [form.subject, activeContent, form.unsubscribe_url]);
  const randomVariables = detectedVariables.filter((name) => /^random\d*$/.test(name));

  const loadTemplates = async () => { setLoading(true); try { const data = await getTemplates(page, pageSize); setTemplates(data.items || []); setTotal(data.total || 0); } catch (error) { message.error(getErrorMessage(error, '加载模板列表失败')); } finally { setLoading(false); } };
  const loadUrlPools = async () => { try { const data = await getPools(1, 100); setUrlPools(data.items || []); } catch (error) { message.error(getErrorMessage(error, '加载 URL 池失败')); } };
  useEffect(() => { loadTemplates(); }, [page, pageSize]); useEffect(() => { loadUrlPools(); }, []); useEffect(() => { if (previewUrlPoolId || !urlPools.length) return; setPreviewUrlPoolId(urlPools[0].id); }, [urlPools, previewUrlPoolId]);

  const resolveTextArea = (editor) => {
    const ref = editor === 'html' ? htmlRef : textRef;
    const current = ref.current;
    if (!current) return null;
    if (current.resizableTextArea?.textArea) return current.resizableTextArea.textArea;
    if (current.textArea) return current.textArea;
    if (current instanceof HTMLTextAreaElement) return current;
    return null;
  };
  const rememberSelection = (editor) => {
    const element = resolveTextArea(editor);
    const field = editor === 'html' ? 'html_content' : 'text_content';
    const contentLength = String(form[field] || '').length;
    const start = element ? element.selectionStart : contentLength;
    const end = element ? element.selectionEnd : start;
    selectionRef.current = { editor, start: Math.max(0, start), end: Math.max(0, end) };
    setActiveEditor(editor);
  };
  const restoreEditorSelection = (selection) => {
    if (!selection) return false;
    const element = resolveTextArea(selection.editor);
    if (!element) return false;
    element.focus({ preventScroll: true });
    element.setSelectionRange(selection.start, selection.end);
    selectionRef.current = selection;
    return true;
  };
  useEffect(() => {
    const pendingSelection = pendingSelectionRef.current;
    if (!pendingSelection || pendingSelection.editor !== form.content_type) return undefined;
    const animationFrame = window.requestAnimationFrame(() => {
      if (restoreEditorSelection(pendingSelection)) pendingSelectionRef.current = null;
    });
    return () => window.cancelAnimationFrame(animationFrame);
  }, [form.html_content, form.text_content, form.content_type]);
  const openCreate = () => { setEditing(null); setForm({ ...emptyForm, random_rules: {} }); setActiveEditor('html'); selectionRef.current = { editor: 'html', start: emptyForm.html_content.length, end: emptyForm.html_content.length }; setEditorOpen(true); };
  const openEdit = (template) => { const normalized = normalizeTemplate(template); setEditing(template); setForm(normalized); setActiveEditor(normalized.content_type); const initialLength = String(normalized.content_type === 'html' ? normalized.html_content : normalized.text_content).length; selectionRef.current = { editor: normalized.content_type, start: initialLength, end: initialLength }; setEditorOpen(true); };
  const updateField = (field, value) => setForm((current) => ({ ...current, [field]: value }));
  const applyContentTypeSwitch = (value) => { setActiveEditor(value); const field = value === 'html' ? 'html_content' : 'text_content'; const length = String(form[field] || '').length; selectionRef.current = { editor: value, start: length, end: length }; setForm((current) => ({ ...current, content_type: value })); };
  const switchContentType = (value) => { if (value === form.content_type) return; applyContentTypeSwitch(value); };
  const convertHtmlDraftToText = () => { const converted = htmlToPlainText(form.html_content); if (!converted) { message.warning('当前 HTML 内容为空，无法生成纯文本'); return; } const applyConverted = () => { setForm((current) => ({ ...current, content_type: 'text', text_content: converted })); setActiveEditor('text'); selectionRef.current = { editor: 'text', start: converted.length, end: converted.length }; message.success('已从 HTML 生成纯文本草稿，保存时只会保存纯文本'); }; if ((form.text_content || '').trim()) { Modal.confirm({ title: '覆盖纯文本草稿？', content: '当前已有纯文本草稿，继续操作会用 HTML 提取结果覆盖它。', okText: '覆盖并切换', cancelText: '取消', onOk: applyConverted }); return; } applyConverted(); };
  const insertIntoEditor = (placeholder, extraPatchFactory) => {
    const remembered = selectionRef.current || { editor: activeEditor, start: 0, end: 0 };
    const editor = remembered.editor || activeEditor;
    const field = editor === 'html' ? 'html_content' : 'text_content';
    const element = resolveTextArea(editor);
    const content = String(form[field] || '');
    const rawStart = element ? element.selectionStart : remembered.start;
    const rawEnd = element ? element.selectionEnd : remembered.end;
    const start = Math.min(Math.max(0, Number.isInteger(rawStart) ? rawStart : 0), content.length);
    const end = Math.min(Math.max(start, Number.isInteger(rawEnd) ? rawEnd : start), content.length);
    const cursor = start + placeholder.length;
    const nextContent = `${content.slice(0, start)}${placeholder}${content.slice(end)}`;
    const nextSelection = { editor, start: cursor, end: cursor };
    selectionRef.current = nextSelection;
    pendingSelectionRef.current = nextSelection;
    setActiveEditor(editor);
    setForm((current) => ({ ...current, [field]: nextContent, ...(extraPatchFactory ? extraPatchFactory(current) : {}) }));
    window.requestAnimationFrame(() => restoreEditorSelection(nextSelection));
  };
  const insertVariable = (placeholder) => insertIntoEditor(placeholder);
  const normalizeRandomRule = (rule) => {
    const src = rule && typeof rule === 'object' ? rule : {};
    // Backward compatible mapping from old keys.
    const mappedUpper = src.upper ?? src.uppercase_count ?? 0;
    const mappedLower = src.lower ?? src.lowercase_count ?? 0;
    const mappedDigits = src.digits ?? src.digit_count ?? 0;
    const mappedSpecial = src.special ?? 0;
    return {
      upper: Number.isFinite(Number(mappedUpper)) ? Number(mappedUpper) : 0,
      lower: Number.isFinite(Number(mappedLower)) ? Number(mappedLower) : 0,
      digits: Number.isFinite(Number(mappedDigits)) ? Number(mappedDigits) : 0,
      special: Number.isFinite(Number(mappedSpecial)) ? Number(mappedSpecial) : 0
    };
  };

  const addRandomVariable = () => {
    const existing = new Set(randomVariables);
    let index = 1;
    while (existing.has(`random${index}`)) index += 1;
    const name = `random${index}`;
    const placeholder = `{{${name}}}`;
    insertIntoEditor(placeholder, (value) => ({
      random_rules: {
        ...value.random_rules,
        [name]: normalizeRandomRule(value.random_rules?.[name])
      }
    }));
  };

  const updateRandomRule = (name, patch) => setForm((current) => {
    const existingRule = normalizeRandomRule(current.random_rules?.[name]);
    return {
      ...current,
      random_rules: {
        ...current.random_rules,
        [name]: {
          ...existingRule,
          ...patch
        }
      }
    };
  });
  const submitTemplate = async () => {
    const payload = { ...form, html_content: form.content_type === 'html' ? form.html_content : '', text_content: form.content_type === 'text' ? form.text_content : '' };
    setSaving(true);
    try {
      const saved = editing ? await updateTemplate(editing.id, payload) : await createTemplate(payload);
      if (Array.isArray(saved?.substitution_warnings) && saved.substitution_warnings.length > 0) {
        message.warning(saved.substitution_warnings.join('；'));
      }
      message.success(editing ? '模板已更新' : '模板已创建');
      setEditorOpen(false);
      await loadTemplates();
    } catch (error) {
      message.error(getErrorMessage(error, '保存模板失败'));
    } finally {
      setSaving(false);
    }
  };
  const removeTemplate = async (template) => { try { await deleteTemplate(template.id); message.success('模板已删除'); await loadTemplates(); } catch (error) { message.error(getErrorMessage(error, '删除模板失败')); } };
  const runPreview = async (template = previewTarget, urlPoolId = previewUrlPoolId) => { if (!template) return; setSaving(true); try { const result = await previewTemplate(template.id, {}, urlPoolId || null); setPreviewResult(result); } catch (error) { message.error(getErrorMessage(error, '预览失败')); } finally { setSaving(false); } };
  const openPreview = (template) => { setPreviewTarget(template); setPreviewResult(null); setPreviewUrlPoolId(urlPools[0]?.id); setPreviewOpen(true); runPreview(template, urlPools[0]?.id); };
  const columns = [
    { title: '名称', dataIndex: 'name', key: 'name', render: (value) => <Text strong>{value}</Text> },
    { title: '邮件主题', dataIndex: 'subject', key: 'subject', ellipsis: true, render: (value) => value || <Text type="secondary">未设置</Text> },
    { title: '正文类型', dataIndex: 'content_type', key: 'content_type', width: 100, render: (value) => <Tag color={value === 'text' ? 'cyan' : 'purple'}>{value === 'text' ? '纯文本' : 'HTML'}</Tag> },
    { title: '正文摘要', key: 'content_summary', ellipsis: true, render: (_, record) => { const value = record.content_type === 'text' ? record.text_content : record.html_content; return value ? `${value.slice(0, 120)}${value.length > 120 ? '…' : ''}` : '-'; } },
    { title: '退订', dataIndex: 'unsubscribe_enabled', key: 'unsubscribe_enabled', width: 90, render: (value) => <Tag color={value ? 'green' : 'default'}>{value ? '启用' : '关闭'}</Tag> },
    { title: '创建时间', dataIndex: 'created_at', key: 'created_at', render: formatTime },
    { title: '操作', key: 'actions', width: 230, render: (_, record) => <Space><Button size="small" onClick={() => openEdit(record)}>编辑</Button><Button size="small" onClick={() => openPreview(record)}>预览</Button><Popconfirm title="确认删除模板？" description={record.name} okText="删除" cancelText="取消" onConfirm={() => removeTemplate(record)}><Button size="small" danger>删除</Button></Popconfirm></Space> }
  ];

  const unsubscribeNotice = form.unsubscribe_enabled ? <Paragraph><Tag color="red">{'{{unsubscribe_url}}'}</Tag>软件退订链接，发送任务启用退订后会注入 List-Unsubscribe 邮件头和退订变量。</Paragraph> : null;
  const variablePanel = <Card title="变量管理" size="small" style={{ height: '100%' }}><Paragraph type="secondary">变量只在侧边栏标识，不改变编辑框 HTML/CSS。</Paragraph><Space wrap>{detectedVariables.length ? detectedVariables.map((name) => <Tag key={name} color={name.startsWith('random') ? 'magenta' : name === 'url' ? 'green' : name === 'unsubscribe_url' ? 'red' : 'blue'} onClick={() => insertVariable(`{{${name}}}`)} style={{ cursor: 'pointer', padding: '4px 8px' }}>{variableLabels[name] || name} · {`{{${name}}}`}</Tag>) : <Text type="secondary">当前文案未检测到变量。</Text>}</Space><Button type="primary" block style={{ marginTop: 16 }} onClick={addRandomVariable}>新增随机变量</Button><Title level={5} style={{ marginTop: 18 }}>随机变量规则</Title>{randomVariables.length === 0 ? <Text type="secondary">添加 {'{{random}}'}、{'{{random1}}'} 后可单独设置规则。</Text> : randomVariables.map((name) => { const rule = normalizeRandomRule(form.random_rules?.[name]); const total = Math.max(0, (rule.upper || 0) + (rule.lower || 0) + (rule.digits || 0) + (rule.special || 0)); const fallback = total === 0; return <Card key={name} size="small" style={{ marginBottom: 10 }} title={<Tag color="magenta">{`{{${name}}}`}</Tag>} extra={<Button size="small" onClick={() => insertVariable(`{{${name}}}`)}>插入</Button>}><Row gutter={8}><Col span={12}><Text>大写数量</Text><InputNumber min={0} max={256} value={rule.upper} onChange={(value) => updateRandomRule(name, { upper: value ?? 0 })} style={{ width: '100%' }} /></Col><Col span={12}><Text>小写数量</Text><InputNumber min={0} max={256} value={rule.lower} onChange={(value) => updateRandomRule(name, { lower: value ?? 0 })} style={{ width: '100%' }} /></Col><Col span={12}><Text>数字数量</Text><InputNumber min={0} max={256} value={rule.digits} onChange={(value) => updateRandomRule(name, { digits: value ?? 0 })} style={{ width: '100%' }} /></Col><Col span={12}><Text>特殊字符数量</Text><InputNumber min={0} max={256} value={rule.special} onChange={(value) => updateRandomRule(name, { special: value ?? 0 })} style={{ width: '100%' }} /></Col><Col span={24}><Text type={fallback ? 'secondary' : undefined}>总长度：{total}{fallback ? '（全部为 0，将回退使用模板默认 random_length/random_chars）' : ''}</Text></Col></Row></Card>; })}<Title level={5}>URL 说明</Title><Paragraph><Tag color="green">{'{{url}}'}</Tag>在任务中绑定 URL 池后，发送时轮换出的业务链接。</Paragraph>{unsubscribeNotice}</Card>;

  return <Space direction="vertical" size="large" className="mm-page"><div style={{ display: 'flex', justifyContent: 'space-between' }}><div><Title level={2} style={{ margin: 0 }}>模板管理</Title><Text type="secondary">创建邮件模板并插入变量。群发任务统一使用 SendGrid SDK 批量发送（每 HTTP 请求多名收件人）；变量在保存时由 {'{{name}}'} 转为 SendGrid 占位符 {'-name-'}。</Text></div><Space><Button icon={<ReloadOutlined />} onClick={loadTemplates} loading={loading}>刷新</Button><Button type="primary" icon={<PlusOutlined />} onClick={openCreate}>添加模板</Button></Space></div><Card bordered={false} styles={{ body: { padding: 0 } }}><Table rowKey="id" bordered loading={loading} columns={columns} dataSource={templates} locale={{ emptyText: '暂无邮件模板。' }} pagination={{ current: page, pageSize, total, showSizeChanger: true, onChange: (p, ps) => { setPage(p); setPageSize(ps); } }} /></Card><Modal title={editing ? '编辑模板' : '添加模板'} open={editorOpen} onCancel={() => setEditorOpen(false)} onOk={submitTemplate} confirmLoading={saving} okText="保存" cancelText="取消" width="92vw" destroyOnHidden><Form layout="vertical" disabled={saving}><Row gutter={16}><Col span={8}><Form.Item label="模板名称" required><Input value={form.name} onChange={(e) => updateField('name', e.target.value)} placeholder="例如：活动模板 A" /></Form.Item></Col><Col span={16}><Form.Item label="邮件主题" required tooltip="发送任务会使用该主题；支持 {{name}}、{{random}} 等变量"><Input value={form.subject} onChange={(e) => updateField('subject', e.target.value)} placeholder="例如：{{name}}，请查收您的通知" /></Form.Item></Col></Row><Space wrap style={{ marginBottom: 16 }}>{variables.map((item) => <Button key={item.placeholder} onClick={() => insertVariable(item.placeholder)}>{item.label}</Button>)}</Space><Card size="small" style={{ marginBottom: 16 }}><Form.Item label="启用退订"><Switch checked={form.unsubscribe_enabled} checkedChildren="开" unCheckedChildren="关" onChange={(value) => updateField('unsubscribe_enabled', value)} /></Form.Item>{form.unsubscribe_enabled ? <Paragraph type="secondary">开启后，发送任务也启用退订时会注入 List-Unsubscribe 邮件头和 {'{{unsubscribe_url}}'} 变量。</Paragraph> : <Paragraph type="secondary">关闭后，模板预览和使用该模板的发送任务不会自动注入退订变量。</Paragraph>}<Row gutter={16}><Col span={12}><Form.Item label="退订链接 URL"><Input value={form.unsubscribe_url} onChange={(e) => updateField('unsubscribe_url', e.target.value)} placeholder="留空使用 {{unsubscribe_url}} 默认软件退订链接" disabled={!form.unsubscribe_enabled} /></Form.Item></Col><Col span={12}><Form.Item label="退订链接描述"><Input value={form.unsubscribe_description} onChange={(e) => updateField('unsubscribe_description', e.target.value)} disabled={!form.unsubscribe_enabled} /></Form.Item></Col></Row></Card><Tabs activeKey={form.content_type} onChange={switchContentType} items={[{ key: 'html', label: '只保存 HTML 源码' }, { key: 'text', label: '只保存纯文本' }]} /><Space wrap style={{ marginBottom: 8 }}><Button onClick={convertHtmlDraftToText} disabled={!form.html_content.trim()}>从 HTML 生成纯文本草稿</Button><Tag color="blue">保存时仅提交当前正文类型</Tag><Tag color="default">另一种正文只在本弹窗内临时保留</Tag></Space><Paragraph type="secondary">当前模板只会保存并发送一种正文类型；切换标签不会丢失本次编辑弹窗内的另一种草稿，但关闭弹窗或保存后，数据库仍只保留当前正文类型。</Paragraph><Row gutter={16}><Col span={16}>{form.content_type === 'html' ? <TextArea ref={htmlRef} value={form.html_content} onFocus={() => rememberSelection('html')} onSelect={() => rememberSelection('html')} onKeyUp={() => rememberSelection('html')} onMouseUp={() => rememberSelection('html')} onChange={(e) => { updateField('html_content', e.target.value); selectionRef.current = { editor: 'html', start: e.target.selectionStart || 0, end: e.target.selectionEnd || e.target.selectionStart || 0 }; }} rows={22} style={{ fontFamily: 'Consolas, Monaco, monospace' }} /> : <TextArea ref={textRef} value={form.text_content} onFocus={() => rememberSelection('text')} onSelect={() => rememberSelection('text')} onKeyUp={() => rememberSelection('text')} onMouseUp={() => rememberSelection('text')} onChange={(e) => { updateField('text_content', e.target.value); selectionRef.current = { editor: 'text', start: e.target.selectionStart || 0, end: e.target.selectionEnd || e.target.selectionStart || 0 }; }} rows={22} style={{ fontFamily: 'Consolas, Monaco, monospace' }} />}</Col><Col span={8}>{variablePanel}</Col></Row></Form></Modal><Modal title={`预览模板：${previewTarget?.name || ''}`} open={previewOpen} onCancel={() => setPreviewOpen(false)} footer={[<Button key="refresh" type="primary" loading={saving} onClick={() => runPreview(previewTarget, previewUrlPoolId)}>重新预览</Button>, <Button key="close" onClick={() => setPreviewOpen(false)}>关闭</Button>]} width={980} destroyOnHidden><Space direction="vertical" style={{ width: '100%' }}><Form.Item label="预览 URL 池" tooltip="用于示例追踪链接，与真实发送路径一致"><Select allowClear virtual showSearch optionFilterProp="label" value={previewUrlPoolId} onChange={(value) => { setPreviewUrlPoolId(value); runPreview(previewTarget, value); }} options={urlPools.map((pool) => ({ value: pool.id, label: pool.name }))} placeholder="不选则使用占位链接" /></Form.Item>{saving ? <Text type="secondary">正在生成预览...</Text> : previewResult ? <Space direction="vertical" style={{ width: '100%' }}>{previewResult.subject ? <Card title="主题" size="small"><Text copyable>{previewResult.subject}</Text></Card> : null}<Card title="HTML 预览" styles={{ body: { padding: 16 } }}>{previewResult.html ? <div dangerouslySetInnerHTML={{ __html: previewResult.html }} /> : <Text type="secondary">当前模板没有 HTML 内容。</Text>}</Card>{previewResult.text ? <Card title="纯文本预览" size="small"><Paragraph copyable style={{ whiteSpace: 'pre-wrap' }}>{previewResult.text}</Paragraph></Card> : null}</Space> : <Text type="secondary">暂无预览内容。</Text>}</Space></Modal></Space>;
}
