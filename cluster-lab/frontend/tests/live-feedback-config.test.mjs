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

test('Proxy settings add, list, delete, and explain node distribution', () => {
  const settings = source('src/views/Settings.vue')
  assert.match(settings, /action: 'add'/)
  assert.match(settings, /action: 'delete'/)
  assert.match(settings, /添加代理配置/)
  assert.match(settings, /按节点创建顺序轮询分配/)
  assert.match(settings, /proxy_apis/)
  assert.doesNotMatch(settings, /内置任务/)
  assert.doesNotMatch(settings, /离线行摘要、合成模拟任务/)
})

test('Executive-facing pages omit test, demo, preview, and unfinished feature copy', () => {
  const files = [
    'src/views/Settings.vue', 'src/views/JobDetail.vue',
    'src/components/JobTable.vue', 'src/components/TaskFeedback.vue',
    'src/components/TaskActions.vue', 'src/components/NodeDialogs.vue',
    '../clusterlab/static/index.html', '../clusterlab/static/app.js',
  ]
  const visible = files.map(source).join('\n')
  for (const label of [
    '数据保留策略、传输限速和设备服务尚未接入',
    '本地预览', '离线演示', '合成演示', '推荐演示',
    '预留配置', '未接入', 'OFFLINE TASK PLATFORM', 'ClusterLab',
  ]) assert.doesNotMatch(visible, new RegExp(label))
})

test('Large task totals use a non-overlapping responsive feedback grid', () => {
  const styles = source('src/style.css')
  assert.match(styles, /feedback-cell>strong\{[^}]*white-space:nowrap/)
  assert.match(styles, /@media\(max-width:1600px\)\{.*?feedback-grid\{grid-template-columns:repeat\(5,minmax\(0,1fr\)\)/)
  assert.match(styles, /@media\(max-width:1000px\)\{\.feedback-grid\{grid-template-columns:repeat\(3,minmax\(0,1fr\)\)/)
  assert.match(styles, /@media\(max-width:600px\)\{\.feedback-grid\{grid-template-columns:repeat\(2,minmax\(0,1fr\)\)/)
})

test('Device pool explicitly saves target and shows persisted registration history', () => {
  const drawer = source('src/components/DevicePoolDrawer.vue')
  assert.match(drawer, /保存并应用/)
  assert.match(drawer, /节点收到后会自动补齐新设备/)
  assert.match(drawer, /pool\.value\.attempted/)
  assert.match(drawer, /注册出口 IP/)
  assert.match(drawer, /注册时间/)
})

test('Deleting a running task explicitly stops its node work', () => {
  const table = source('src/components/JobTable.vue')
  assert.doesNotMatch(table, /const removable/)
  assert.match(table, /如果节点正在执行，将在下一次心跳立即停止/)
  assert.match(table, /confirmButtonText: '删除并停止任务'/)
  assert.match(table, /:disabled="row\.status === 'preparing'"/)
  assert.match(table, /已通知节点停止/)
})
