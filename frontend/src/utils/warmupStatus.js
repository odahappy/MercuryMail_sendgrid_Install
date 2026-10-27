/**
 * 根据 status + running 组合养号计划展示状态（仅前端文案映射）。
 */
export function getWarmupDisplayStatus(plan) {
  const status = String(plan?.status || 'draft').toLowerCase();
  const running = Boolean(plan?.running);

  switch (status) {
    case 'draft':
      return { label: '草稿', tagColor: 'default', badgeStatus: 'default' };
    case 'paused':
      return { label: '已暂停', tagColor: 'orange', badgeStatus: 'warning' };
    case 'completed':
      return { label: '已完成', tagColor: 'green', badgeStatus: 'success' };
    case 'scheduled':
      return { label: '等待定时', tagColor: 'cyan', badgeStatus: 'default' };
    case 'active':
      if (running) {
        return { label: '发送中', tagColor: 'processing', badgeStatus: 'processing' };
      }
      return { label: '已启用', tagColor: 'blue', badgeStatus: 'default' };
    default:
      return { label: '未知', tagColor: 'default', badgeStatus: 'default' };
  }
}
