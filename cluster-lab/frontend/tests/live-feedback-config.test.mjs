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
  assert.match(drawer, /已用账号/)
  assert.match(drawer, /item\.used/)
})

test('Task detail lists all valid rows by page and caps each other category at 100', () => {
  const detail = source('src/views/JobDetail.vue')
  const results = source('src/components/ResultList.vue')
  assert.match(detail, /<ResultList/)
  assert.match(results, /const pageSize = 100/)
  assert.match(results, /密正结果可逐页查看全部/)
  assert.match(results, /其他分类各显示前 100 条/)
  assert.match(results, /category=\$\{category\.value\}/)
  assert.match(results, /props\.job\?\.confirmed/)
})

test('Paused tasks can export their confirmed result subset', () => {
  const detail = source('src/views/JobDetail.vue')
  const actions = source('src/components/TaskActions.vue')
  assert.match(detail, /\['paused', 'completed'\]\.includes\(job\.status\)/)
  assert.match(detail, /导出已确认账号/)
  assert.match(actions, /\['paused', 'completed'\]\.includes\(job\.status\)/)
  assert.match(actions, /导出已确认结果/)
  assert.doesNotMatch(actions, /全部确认后可导出/)
})

test('Deleting a running task explicitly stops its node work', () => {
  const table = source('src/components/JobTable.vue')
  assert.doesNotMatch(table, /const removable/)
  assert.match(table, /如果节点正在执行，将在下一次心跳立即停止/)
  assert.match(table, /confirmButtonText: '删除并停止任务'/)
  assert.match(table, /:disabled="row\.status === 'preparing'"/)
  assert.match(table, /已通知节点停止/)
})

test('Device pool has a guarded one-click clear action', () => {
  const drawer = source('src/components/DevicePoolDrawer.vue')
  assert.match(drawer, /ElMessageBox/)
  assert.match(drawer, /一键清空设备/)
  assert.match(drawer, /'DELETE'/)
  assert.match(drawer, /所有设备身份、注册记录和使用计数都会删除/)
  assert.match(drawer, /Boolean\(node\?\.active\)/)
  assert.match(drawer, /pool\.capacity \?\? capacity/)
})

test('Legacy categories remain visible while new -20970 tasks can be rechecked', () => {
  const feedback = source('src/components/TaskFeedback.vue')
  const detail = source('src/views/JobDetail.vue')
  assert.match(feedback, /label: '已失效'/)
  assert.match(feedback, /categories\.value\.invalid/)
  assert.doesNotMatch(feedback, /label: '未判断'/)
  assert.match(detail, /label: '已失效', value: 'invalid'/)
  assert.match(detail, /label: '其他未验证', value: 'undetermined'/)
})

test('Task creation exposes one concise dual-protocol checkbox', () => {
  const dialog = source('src/components/CreateTaskDialog.vue')
  const detail = source('src/views/JobDetail.vue')
  assert.match(dialog, /const dualRecheck = ref\(false\)/)
  assert.match(dialog, /dual_recheck: String\(dualRecheck\.value\)/)
  assert.match(dialog, /dual_recheck: dualRecheck\.value/)
  assert.match(dialog, />双协议校验<\/el-checkbox>/)
  assert.doesNotMatch(dialog, /校验协议|GSA（默认）|-20970|-20209|HTTP 503\/5xx/)
  assert.match(detail, /GSA \+ 网页复检/)
})
