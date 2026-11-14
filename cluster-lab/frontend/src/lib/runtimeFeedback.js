/** @param {unknown} value */
const number = value => Number.isFinite(Number(value)) ? Math.max(0, Number(value)) : 0

/** @param {any} job @param {string} key */
const unitCount = (job, key) => number(job?.unit_counts?.[key])

/**
 * Turn scheduler/node state into an operator-facing explanation.
 * @param {any} job
 * @param {any[]} nodes
 * @param {number} now
 */
export function buildRuntimeFeedback(job, nodes = [], now = Date.now() / 1000) {
  if (!job) return null
  const online = nodes.filter(node => node?.status !== 'offline')
  const accepting = online.filter(node => node?.enabled && node?.status !== 'stopping')
  const active = unitCount(job, 'leased') + unitCount(job, 'running')
  const pending = unitCount(job, 'pending')
  const unknown = unitCount(job, 'unknown')
  const completedUnits = unitCount(job, 'completed')
  const processed = number(job.processed)
  const confirmed = number(job.confirmed)
  const latestHeartbeat = online.reduce((latest, node) => Math.max(latest, number(node.last_seen)), 0)
  const waitFrom = number(job.updated) || number(job.created) || number(now)
  const waitingSeconds = Math.max(0, Math.floor(number(now) - waitFrom))
  const base = {
    online: online.length, accepting: accepting.length, active, pending, unknown,
    completedUnits, processed, confirmed, latestHeartbeat, waitingSeconds, action: '',
  }

  if (job.status === 'preparing') return {
    ...base, phase: '准备文件', type: 'info', step: 1,
    title: '正在读取文件并建立分片索引',
    description: '总控正在按行扫描文件；准备完成后会自动进入等待队列。页面每秒更新一次。',
  }
  if (job.status === 'queued') return {
    ...base, phase: '等待队列', type: 'info', step: 1,
    title: `任务正在排队${job.queue_position ? `，当前第 ${job.queue_position} 位` : ''}`,
    description: '前面的任务结束或暂停后，本任务会自动开始，不需要重复提交。',
  }
  if (job.status === 'ready') return {
    ...base, phase: '等待开始', type: 'info', step: 1,
    title: '文件与工作包已经准备完成',
    description: '任务尚未加入执行队列，点击“加入执行队列”后开始调度。',
  }
  if (job.status === 'paused') return {
    ...base, phase: '已暂停', type: 'warning', step: active ? 2 : 1,
    title: '任务已暂停派发',
    description: active
      ? `仍有 ${active} 个已领取工作包正在收尾，已确认结果会继续保留。`
      : '当前没有工作包继续派发；已确认结果可以直接导出。',
  }
  if (job.status === 'completed') return {
    ...base, phase: '已完成', type: 'success', step: 4,
    title: '全部工作包已经处理并确认',
    description: `总控已确认 ${confirmed.toLocaleString('zh-CN')} 条结果，可以查看或导出。`,
  }
  if (job.status === 'failed') return {
    ...base, phase: '准备失败', type: 'danger', step: 0,
    title: '文件准备没有完成', description: job.error || '请查看操作记录中的错误信息。',
  }
  if (job.status !== 'running') return {
    ...base, phase: '状态更新', type: 'info', step: 1,
    title: '正在同步任务状态', description: '总控每秒刷新任务与节点状态。',
  }

  if (!online.length) return {
    ...base, phase: '等待节点', type: 'danger', step: 1, action: 'nodes',
    title: '当前没有在线执行节点',
    description: '任务本身仍在运行队列中；节点恢复心跳后会自动领取工作包。',
  }
  if (active > 0) {
    const activeNodes = online.filter(node => number(node.active) > 0).length || 1
    if (processed > 0) return {
      ...base, phase: '实时执行', type: 'success', step: 3,
      title: '节点正在处理并每秒回传进度',
      description: `${activeNodes} 个节点正在执行 ${active} 个工作包；已处理 ${processed.toLocaleString('zh-CN')} 条，总控已确认 ${confirmed.toLocaleString('zh-CN')} 条。`,
    }
    return {
      ...base, phase: '启动执行器', type: 'info', step: 2,
      title: '工作包已领取，节点正在启动执行器',
      description: `${activeNodes} 个节点已领取 ${active} 个工作包，正在初始化进程、线程、设备池和代理；收到首批结果后这里会立即显示。`,
    }
  }
  if (!accepting.length) return {
    ...base, phase: '等待节点', type: 'warning', step: 1, action: 'nodes',
    title: '在线节点全部暂停接单',
    description: `${online.length} 个节点心跳正常，但当前都不领取新工作包。恢复接单后，本任务会自动继续。`,
  }
  if (unknown > 0 && !pending) return {
    ...base, phase: '等待确认', type: 'warning', step: 3,
    title: '存在待确认工作包',
    description: `${unknown} 个工作包的租约或回传状态异常，请查看操作记录和节点日志。`,
  }
  return {
    ...base, phase: '等待领取', type: waitingSeconds >= 10 ? 'warning' : 'info', step: 1,
    title: pending ? '节点在线，正在等待领取工作包' : '正在等待调度状态更新',
    description: pending
      ? `${accepting.length} 个节点可以接单；节点按心跳轮询领取，通常 1 秒内开始。已等待 ${waitingSeconds} 秒。`
      : '总控正在检查剩余工作包与节点状态。',
  }
}
