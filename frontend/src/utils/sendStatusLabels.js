import { resolveSendResultStatusLabel } from './taskDetailFormat.js';

/** SDK 即时发送状态（仅 sent / failed 及监控类） */
export function resolveSdkSendStatusLabel(status) {
  const normalized = String(status || '').trim().toLowerCase();
  if (normalized === 'sent' || normalized === 'success') return '发送成功';
  if (normalized === 'failed') return '发送失败';
  if (normalized === 'unknown') return '投递未知';
  if (normalized === 'bounced') return '发送失败';
  if (normalized === 'blocked' || normalized === 'dropped') return '发送失败';
  if (normalized === 'pending') return '等待发送';
  if (normalized === 'monitor_sent') return '监控成功';
  if (normalized === 'monitor_failed') return '监控失败';
  if (!normalized) return '—';
  return '未知';
}

/** Event Poller 最终投递状态 */
export function resolveFinalDeliveryStatusLabel(finalStatus, row = {}) {
  const normalized = String(finalStatus || '').trim().toLowerCase();
  const bounceType = String(row?.bounce_type || '').trim().toLowerCase();
  if (!normalized) return row?.status === 'sent' ? '等待最终投递事件' : '等待中';
  if (normalized === 'processed') return '已进入 SendGrid 队列';
  if (normalized === 'delivered') return '已送达';
  if (normalized === 'bounced') return bounceType === 'soft' ? '软弹' : '硬弹';
  if (normalized === 'blocked') return '发送失败/被阻止';
  if (normalized === 'dropped') return '被 SendGrid 丢弃';
  if (normalized === 'deferred') return '软弹/延迟';
  if (normalized === 'spamreported' || normalized === 'spam_report') return '垃圾邮件投诉';
  return resolveSendResultStatusLabel(normalized);
}

export function sdkSendStatusColor(status) {
  const normalized = String(status || '').trim().toLowerCase();
  if (normalized === 'sent' || normalized === 'success' || normalized === 'monitor_sent') return 'green';
  if (normalized === 'failed' || normalized === 'bounced' || normalized === 'blocked' || normalized === 'dropped' || normalized === 'monitor_failed') return 'red';
  if (normalized === 'unknown') return 'gold';
  if (normalized === 'pending') return 'default';
  return 'default';
}

export function finalDeliveryStatusColor(finalStatus) {
  const normalized = String(finalStatus || '').trim().toLowerCase();
  if (!normalized) return 'default';
  if (normalized === 'processed') return 'blue';
  if (normalized === 'delivered') return 'green';
  if (normalized === 'bounced' || normalized === 'blocked' || normalized === 'dropped') return 'red';
  if (normalized === 'deferred') return 'orange';
  if (normalized === 'spamreported') return 'volcano';
  return 'default';
}

const DELIVERY_FAILURE_STATUSES = new Set(['bounced', 'blocked', 'dropped', 'deferred', 'spamreported', 'spam_report']);

export function isDeliveryFailureStatus(finalStatus) {
  return DELIVERY_FAILURE_STATUSES.has(String(finalStatus || '').trim().toLowerCase());
}

/** 失败明细：投递失败原因列 */
export function formatDeliveryFailureReason(row) {
  const finalStatus = String(row?.final_status || '').trim().toLowerCase();
  const eventReason = String(row?.event_reason || '').trim();
  const sdkStatus = String(row?.sdk_status || '').trim().toLowerCase();

  if (!finalStatus) {
    if (sdkStatus === 'failed') return '等待最终结果';
    return '';
  }

  let base = '';
  switch (finalStatus) {
    case 'bounced':
      base = '硬弹：邮箱不存在/域名无效等';
      break;
    case 'blocked':
      base = '发送失败/被阻止';
      break;
    case 'dropped':
      base = '被 SendGrid 丢弃';
      break;
    case 'deferred':
      base = '软弹：邮件延迟/暂时拒收';
      break;
    case 'spamreported':
    case 'spam_report':
      base = '垃圾邮件投诉';
      break;
    default:
      base = resolveFinalDeliveryStatusLabel(finalStatus);
  }

  if (eventReason && !base.includes(eventReason)) {
    return `${base}（${eventReason}）`;
  }
  return base;
}

export function formatSdkErrorCell(row) {
  const sdkStatus = String(row?.sdk_status || '').trim().toLowerCase();
  if (sdkStatus === 'sent') return '';
  const error = String(row?.error || '').trim();
  return error || (sdkStatus === 'failed' ? '发送失败' : '');
}
