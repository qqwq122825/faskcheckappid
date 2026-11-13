/** Read-only presentation of scheduler state. Never changes or retries a job.
 * @typedef {{status?: string, unit_counts?: Record<string, number>}} JobState
 */

/** @param {unknown} value */
const count = value => typeof value === 'number' && Number.isFinite(value) ? Math.max(0, value) : 0

/** @param {JobState | null | undefined} job */
export function jobDisplayStatus(job) {
  const status = job?.status || ''
  if (!count(job?.unit_counts?.unknown) || status === 'failed') return status
  if (status === 'paused') return 'paused_with_errors'
  if (status === 'stopped') return 'stopped_with_errors'
  const active = count(job?.unit_counts?.leased) + count(job?.unit_counts?.running)
  return status === 'running' && active > 0 ? 'running_with_errors' : 'needs_attention'
}

/** @param {JobState | null | undefined} job */
export function jobIssueMessage(job) {
  const unknown = count(job?.unit_counts?.unknown)
  if (!unknown) return ''
  const active = count(job?.unit_counts?.leased) + count(job?.unit_counts?.running)
  return `有 ${unknown} 个异常工作包待确认；${active ? `仍有 ${active} 个工作包在途。` : '当前没有在途工作包。'}结果尚未全部确认，请核对节点日志与通信记录。未自动重新执行。`
}

/** @param {JobState} job @param {string} filter */
export function matchesJobStatus(job, filter) {
  if (!filter) return true
  if (filter === 'attention') return count(job.unit_counts?.unknown) > 0
  if (filter === 'running') return ['running', 'running_with_errors'].includes(jobDisplayStatus(job))
  return job.status === filter
}
