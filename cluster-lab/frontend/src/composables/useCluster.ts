import { computed, ref } from 'vue'
import { ElMessage } from 'element-plus'

export const session = ref<any>(null)
export const state = ref<any>(null)
export const connected = ref(false)
export const bootError = ref('')
export const lastUpdated = ref<number | null>(null)
export const isNode = computed(() => session.value?.role === 'node')
export const busy = ref(false)
export const createTaskOpen = ref(false)
export const addNodeOpen = ref(false)
export const jobRates = ref<Record<string, number>>({})
const observations = new Map<string, { at: number, count: number }>()
let timer: ReturnType<typeof setTimeout> | undefined
let polling = false

export async function api(path: string, data?: any, method?: string): Promise<any> {
  const verb = method || (data === undefined ? 'GET' : 'POST')
  const headers: Record<string, string> = {}
  if (verb !== 'GET') headers['X-CSRF-Token'] = session.value?.csrf || ''
  const options: RequestInit = { method: verb, headers }
  if (verb === 'GET') options.signal = AbortSignal.timeout(15000)
  if (data instanceof FormData) options.body = data
  else if (data !== undefined) { headers['Content-Type'] = 'application/json'; options.body = JSON.stringify(data) }
  const response = await fetch(path, options)
  const result = await response.json()
  if (!response.ok) {
    if (response.status === 401 && session.value) session.value.authenticated = false
    throw new Error(result.error || `请求失败（${response.status}）`)
  }
  return result
}

export async function refresh() {
  if (polling || !session.value?.authenticated) return
  polling = true
  try {
    const next = await api('/api/state')
    const now = next.server_time || Date.now() / 1000
    for (const job of next.jobs || []) {
      const before = observations.get(job.id)
      if (job.status !== 'running' && !(job.row_counts?.leased || job.row_counts?.running)) jobRates.value[job.id] = 0
      else if (before && now > before.at) {
        const rate = Math.max(0, (job.processed - before.count) / (now - before.at))
        jobRates.value[job.id] = Math.round((jobRates.value[job.id] || 0) * .5 + rate * .5)
      }
      observations.set(job.id, { at: now, count: job.processed })
    }
    state.value = next
    connected.value = true
    lastUpdated.value = Date.now()
  } catch { connected.value = false }
  finally { polling = false }
}
function schedule() {
  timer = setTimeout(async () => { await refresh(); schedule() }, 1500)
}
export async function initialize() {
  clearTimeout(timer)
  try { session.value = await api('/api/session'); bootError.value = ''; await refresh(); schedule() }
  catch (error) { bootError.value = (error as Error).message }
}
export function stopPolling() { clearTimeout(timer) }
export async function login(username: string, password: string) {
  const result = await api('/api/login', { username, password })
  session.value.csrf = result.csrf
  session.value.authenticated = true
  await refresh()
}
export async function logout() { await api('/api/logout', {}); state.value = null; await initialize() }
export async function mutate(path: string, data: any = {}, message = '操作已保存') {
  busy.value = true
  try { const result = await api(path, data); await refresh(); ElMessage.success(message); return result }
  finally { busy.value = false }
}
export const fmt = (value: any) => Number(value || 0).toLocaleString('zh-CN', { maximumFractionDigits: 1 })
export const percent = (job: any) => job?.rows ? Math.min(100, job.confirmed / job.rows * 100) : 0
export const date = (timestamp: number) => timestamp ? new Date(timestamp * 1000).toLocaleString('zh-CN', { hour12: false }) : '尚未连接'
export const bandwidth = (value: number | null | undefined) => value == null ? '—' : value >= 1048576 ? `${(value / 1048576).toFixed(2)} MiB/s` : `${(value / 1024).toFixed(1)} KiB/s`
export const metric = (value: number | null | undefined) => value == null ? '—' : `${fmt(value)}%`
