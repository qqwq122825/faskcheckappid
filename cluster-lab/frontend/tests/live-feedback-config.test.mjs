import test from 'node:test'
import assert from 'node:assert/strict'
import { readFileSync } from 'node:fs'
import { fileURLToPath } from 'node:url'
import { dirname, resolve } from 'node:path'

const root = resolve(dirname(fileURLToPath(import.meta.url)), '..')
const source = path => readFileSync(resolve(root, path), 'utf8')

test('Task creation exposes retries and node settings allow 10 x 100', () => {
  const task = source('src/components/CreateTaskDialog.vue')
  const nodes = source('src/components/NodeDialogs.vue')
  assert.match(task, /超时后重试次数/)
  assert.match(task, /v-model="retries"/)
  assert.match(task, /预计设备池数量/)
  assert.match(task, /device_capacity: String\(deviceCapacity\.value\)/)
  assert.match(task, /deviceCapacity = ref\(30\)/)
  assert.match(nodes, /processes: 10, threads: 100/)
  assert.match(nodes, /:max="100"/)
  assert.match(nodes, /使用本机上限/)
})

test('Running elapsed time and speed use live processed rows', () => {
  const detail = source('src/views/JobDetail.vue')
  assert.match(detail, /state\.value\?\.server_time/)
  assert.match(detail, /Math\.max\(job\.value\?\.processed/)
})

test('Application omits the offline platform label everywhere', () => {
  const app = source('src/App.vue')
  const styles = source('src/style.css')
  assert.doesNotMatch(app, /\u79bb\u7ebf\u4efb\u52a1\u5e73\u53f0/)
  assert.match(app, /苹果集群系统/)
  assert.match(app, /sidebar-bottom"><span>苹果id校验集群系统<\/span><span>v\{\{ session\.version \}\}/)
  assert.doesNotMatch(app, /Vue \+ Python/)
  assert.match(styles, /\.brand\{[^}]*font-size:22px/)
})
