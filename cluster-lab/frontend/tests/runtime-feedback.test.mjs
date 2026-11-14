import test from 'node:test'
import assert from 'node:assert/strict'
import { buildRuntimeFeedback } from '../src/lib/runtimeFeedback.js'

const job = overrides => ({
  status: 'running', created: 100, updated: 110, processed: 0, confirmed: 0,
  unit_counts: { pending: 2, leased: 0, running: 0, completed: 0, unknown: 0 },
  ...overrides,
})
const node = overrides => ({ status: 'idle', enabled: true, active: 0, last_seen: 118, ...overrides })

test('a running task explains that every online node is paused', () => {
  const value = buildRuntimeFeedback(job({}), [node({ status: 'draining', enabled: false })], 120)
  assert.equal(value.phase, '等待节点')
  assert.equal(value.type, 'warning')
  assert.match(value.title, /暂停接单/)
  assert.equal(value.action, 'nodes')
})

test('a running task explains when there is no online node', () => {
  const value = buildRuntimeFeedback(job({}), [node({ status: 'offline' })], 120)
  assert.equal(value.type, 'danger')
  assert.match(value.title, /没有在线/)
})

test('a leased package reports executor startup before the first row', () => {
  const value = buildRuntimeFeedback(job({ unit_counts: { pending: 1, leased: 1 } }), [node({ status: 'busy', active: 1 })], 120)
  assert.equal(value.step, 2)
  assert.match(value.title, /工作包已领取/)
  assert.match(value.description, /进程、线程、设备池和代理/)
})

test('live progress reports processed and confirmed rows', () => {
  const value = buildRuntimeFeedback(job({ processed: 320, confirmed: 100, unit_counts: { running: 1 } }), [node({ status: 'busy', active: 1 })], 120)
  assert.equal(value.step, 3)
  assert.equal(value.type, 'success')
  assert.match(value.description, /已处理 320 条/)
  assert.match(value.description, /已确认 100 条/)
})

test('queued, preparing, and completed tasks expose their current phase', () => {
  assert.equal(buildRuntimeFeedback(job({ status: 'preparing' }), [], 120).phase, '准备文件')
  assert.equal(buildRuntimeFeedback(job({ status: 'queued', queue_position: 2 }), [], 120).phase, '等待队列')
  assert.equal(buildRuntimeFeedback(job({ status: 'completed', confirmed: 10002 }), [], 120).step, 4)
})
