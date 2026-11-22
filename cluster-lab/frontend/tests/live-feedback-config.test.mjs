import test from 'node:test'
import assert from 'node:assert/strict'
import { readFileSync } from 'node:fs'
import { fileURLToPath } from 'node:url'
import { dirname, resolve } from 'node:path'

const root = resolve(dirname(fileURLToPath(import.meta.url)), '..')
const source = path => readFileSync(resolve(root, path), 'utf8')

test('Task creation exposes retries and node settings allow up to 4 x 500', () => {
  const task = source('src/components/CreateTaskDialog.vue')
  const nodes = source('src/components/NodeDialogs.vue')
  assert.match(task, /超时后重试次数/)
  assert.match(task, /v-model="retries"/)
  assert.doesNotMatch(task, /预计设备池数量/)
  assert.doesNotMatch(task, /device_capacity/)
  assert.doesNotMatch(task, /deviceCapacity/)
  assert.match(nodes, /processes: 4, threads: 250/)
  assert.match(nodes, /:max="4"/)
  assert.match(nodes, /:max="500"/)
  assert.match(nodes, /最高可设 4 × 500/)
  assert.match(nodes, /使用本机上限/)
  assert.doesNotMatch(nodes, /v-model="config\.enabled"/)
  assert.doesNotMatch(nodes, /active-text="允许领取新工作包"/)
})

test('New nodes stay paused until all default devices are initialized', () => {
  const nodes = source('src/views/Nodes.vue')
  const dialogs = source('src/components/NodeDialogs.vue')
  assert.match(nodes, /!node\.enabled && !node\.device_initialization_complete/)
  assert.match(nodes, /设备初始化 \{\{ node\.device_ready \|\| 0 \}\}\/\{\{ node\.device_request \|\| 30 \}\}/)
  assert.match(nodes, /开始接单/)
  assert.match(dialogs, /新节点先暂停接单并自动初始化 30 台设备/)
  assert.doesNotMatch(dialogs, /允许领取新工作包/)
})

test('Controller exposes guarded rolling Git updates with visible node phases', () => {
  const nodes = source('src/views/Nodes.vue')
  assert.match(nodes, /滚动更新/)
  assert.match(nodes, /\/api\/nodes\/update/)
  assert.match(nodes, /node_ids: updateNodes\.value/)
  assert.match(nodes, /parallel: updateParallel\.value/)
  assert.match(nodes, /暂停接单 → 等待在途包结束 → git fetch\/checkout → 安装依赖 → systemd 重启 → 健康检查/)
  assert.match(nodes, /updateLabels\[node\.update_state\]/)
  assert.match(nodes, /版本 \{\{ node\.version \|\| '—' \}\}/)
})

test('Device rows use short Base64 labels instead of reusable slot numbers', () => {
  const drawer = source('src/components/DevicePoolDrawer.vue')
  const results = source('src/components/ResultList.vue')
  assert.match(drawer, /label: item\.label \|\| `#\$\{item\.slot\}`/)
  assert.match(drawer, /prop="label" label="设备标识"/)
  assert.match(results, /prop="device_display" label="设备"/)
  assert.match(results, /v-model="accountSearch"/)
  assert.match(results, /placeholder="搜索账号"/)
  assert.match(results, /search=\$\{search\}/)
  assert.doesNotMatch(results, /label="二次验证方式"/)
  assert.doesNotMatch(results, /prop="device" label="设备"/)
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
  assert.match(settings, /action: 'resize'/)
  assert.match(settings, /proxy_pool_size/)
  assert.match(settings, /每个节点只维护自己的代理池/)
  assert.match(settings, /单次最多从该节点的 API 提取 1,000 条/)
  assert.match(settings, /代理明细不会每秒传回总控/)
  assert.match(settings, /添加代理配置/)
  assert.match(settings, /按节点创建顺序轮询分配/)
  assert.match(settings, /proxy_apis/)
  assert.doesNotMatch(settings, /内置任务/)
  assert.doesNotMatch(settings, /离线行摘要、合成模拟任务/)
})

test('Proxy debug setting is explicit, persistent, and warns about log volume', () => {
  const settings = source('src/views/Settings.vue')
  assert.match(settings, /\/api\/settings\/debug/)
  assert.match(settings, /proxy_debug: enabled/)
  assert.match(settings, /代理调试日志/)
  assert.match(settings, /proxy-debug\.jsonl/)
  assert.match(settings, /节点直接回传 Apple 原始英文原因/)
  assert.match(settings, /节点只回传快捷分类，总控显示中文原因/)
  assert.match(settings, /避免产生大量日志/)
})

test('Authentication protocol setting selects one primary protocol', () => {
  const settings = source('src/views/Settings.vue')
  assert.match(settings, /\/api\/settings\/auth-protocol/)
  assert.match(settings, /auth_protocol: protocol/)
  assert.match(settings, /GSA（默认）/)
  assert.match(settings, /Web（IDMSA）/)
  assert.match(settings, /不会先执行 GSA/)
  assert.match(settings, /GSA 二次验证/)
  assert.match(settings, /web_secondary_auth: enabled/)
  assert.match(settings, /仅对 GSA 返回 au=repair/)
  assert.match(settings, /协议在创建任务时固定/)
})

test('Thread mode reuses one expandable shared pool and retires at limit', () => {
  const settings = source('src/views/Settings.vue')
  assert.match(settings, /\/api\/settings\/device-mode/)
  assert.match(settings, /线程共用设备（当前默认）/)
  assert.match(settings, /每个节点只有一个共享设备池/)
  assert.match(settings, /预注册容量可按需设为 1000 或 2000，但不是上限/)
  assert.match(settings, /没有空闲设备时不等待/)
  assert.match(settings, /验证完成后放回同一个共享池供其他线程复用/)
  assert.match(settings, /本次验证结束后删除该设备/)
  assert.match(settings, /等待 10 秒后丢弃旧出口并换 IP/)
})

test('Result table uses one protocol-neutral header set for all auth paths', () => {
  const results = source('src/components/ResultList.vue')
  assert.match(results, /验证路径/)
  assert.match(results, /结果阶段/)
  assert.match(results, /HTTP 链路/)
  assert.match(results, /协议结果/)
  assert.match(results, /GSA → Web（二次）/)
  assert.match(results, /row\.protocol \|\| props\.job\?\.auth_protocol \|\| 'gsa'/)
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

test('Device pool starts initialization only from its explicit action and shows persisted registration history', () => {
  const drawer = source('src/components/DevicePoolDrawer.vue')
  assert.match(drawer, /开始初始化设备池/)
  assert.match(drawer, /点击“开始初始化设备池”才会注册设备/)
  assert.match(drawer, /代理异常：未获取到可用代理，请确认新节点出口 IP 已加入白名单/)
  assert.match(drawer, /代理正常/)
  assert.match(drawer, /pool\.value\.proxy_ip/)
  assert.match(drawer, /building\.value\s*\? Math\.max\(completed\.value/)
  assert.match(drawer, /Number\(pool\.value\.current\).*?- 1/)
  assert.match(drawer, /Math\.max\(Number\(pool\.value\.attempted\).*?completed\.value\)/)
  assert.match(drawer, /@submit\.prevent="initialize"/)
  assert.match(drawer, /api\(`\/api\/nodes\/\$\{node\.value\.id\}\/devices`, \{ capacity: capacity\.value \}, 'POST'\)/)
  assert.match(drawer, /pool\.value\.attempted/)
  assert.match(drawer, /注册出口 IP/)
  assert.ok(drawer.indexOf('label="注册出口 IP"') < drawer.indexOf('label="注册结果"'),
    '注册出口 IP 应放在注册结果前，窄抽屉无需横向滚动即可看到')
  assert.match(drawer, /item\.state === 'ready' \? '成功' : item\.error \|\| '—'/)
  assert.match(drawer, /注册时间/)
  assert.match(drawer, /已用账号/)
  assert.match(drawer, /item\.used/)
})

test('Task detail lists all valid rows by page and caps each other category at 100', () => {
  const detail = source('src/views/JobDetail.vue')
  const results = source('src/components/ResultList.vue')
  assert.match(detail, /<ResultList/)
  assert.match(results, /const pageSize = 100/)
  assert.match(results, /密码正确结果可逐页查看全部/)
  assert.match(results, /其他分类各显示前 100 条/)
  assert.match(results, /category=\$\{category\.value\}/)
  assert.match(results, /props\.job\?\.confirmed/)
})

test('Live result updates refresh silently without flashing the table loading mask', () => {
  const results = source('src/components/ResultList.vue')
  assert.match(results, /async function load\(reset = false, showLoading = true\)/)
  assert.match(results, /async function refreshInBackground\(\)/)
  assert.match(results, /await load\(false, false\)/)
  assert.match(results, /if \(showLoading\) \{[\s\S]*loading\.value = true/)
  assert.doesNotMatch(results, /\[props\.job\?\.id, props\.job\?\.confirmed\], \(\) => load\(\)/)
  assert.match(results, /if \(reset\) \{[\s\S]*rows\.value = \[\][\s\S]*total\.value = 0/)
  assert.match(results, /if \(backgroundPending \|\| loading\.value\) return/)
  assert.doesNotMatch(results, /backgroundQueued/)
})

test('GET timeout errors identify the controller query instead of exposing a signal error', () => {
  const cluster = source('src/composables/useCluster.ts')
  assert.match(cluster, /总控查询超时，请稍后重试/)
  assert.match(cluster, /error\?\.name === 'TimeoutError'/)
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

test('GSA result categories expose invalid and locked business states', () => {
  const feedback = source('src/components/TaskFeedback.vue')
  const detail = source('src/views/JobDetail.vue')
  assert.match(feedback, /label: '未激活'/)
  assert.match(feedback, /label: '不存在'/)
  assert.match(feedback, /categories\.value\.invalid/)
  assert.match(detail, /label: '未激活', value: 'invalid'/)
  assert.match(detail, /label: '不存在', value: 'not_found'/)
  assert.match(detail, /label: '锁定', value: 'locked'/)
  assert.match(feedback, /label: '其他错误'/)
  assert.match(feedback, /categories\.value\.unknown/)
})

test('Task creation uses the saved primary protocol without per-task overrides', () => {
  const dialog = source('src/components/CreateTaskDialog.vue')
  const detail = source('src/views/JobDetail.vue')
  assert.doesNotMatch(dialog, /dualRecheck|dual_recheck|双协议|网页复检/)
  assert.doesNotMatch(detail, /dual_recheck|双协议|网页复检|GSA \+ /)
  assert.match(detail, /job\.auth_protocol \|\| 'gsa'/)
})

test('Result list keeps one neutral result column without protocol-specific columns', () => {
  const results = source('src/components/ResultList.vue')
  assert.match(results, /label: '全部结果', value: 'all'/)
  assert.match(results, /const category = ref\('all'\)/)
  assert.match(results, /label="协议结果"/)
  assert.match(results, /protocolResult\(scope\.row\)/)
  assert.match(results, /prop="reason_cn" label="原因"/)
  assert.doesNotMatch(results, /GSA 码|Web 码|网页码|gsa_code|web_code|网页复检/)
  assert.match(results, /labels\[row\.status\]/)
})


test('Device refill switch reopens from persisted settings and publishes save immediately', () => {
  const drawer = source('src/components/DevicePoolDrawer.vue')
  assert.match(drawer, /const settings = state\.value\?\.settings/)
  assert.match(drawer, /Boolean\(settings\.device_auto_rotate/)
  assert.doesNotMatch(drawer, /telemetry\?\.device_auto_rotate/)
  assert.match(drawer, /const saved = await api\('\/api\/settings\/rotation'/)
  assert.match(drawer, /state\.value\.settings =/)
  assert.match(drawer, /:disabled="building \|\| savingAuto"/)
  assert.match(drawer, /先注册一台替换设备/)
  assert.match(drawer, /新设备成功后才移除出生日期最早且空闲的旧设备/)
})

test('Task detail shows one node-reported traffic card with the 1 GB estimate', () => {
  const detail = source('src/views/JobDetail.vue')
  const feedback = source('src/components/TaskFeedback.vue')
  assert.match(detail, /:traffic-bytes="job\.traffic_bytes"/)
  assert.equal((feedback.match(/label: '任务流量'/g) || []).length, 1)
  assert.match(feedback, /1 GB ≈ 5 万账号/)
  assert.match(feedback, /本机上下行累计差值/)
})

test('Task summary omits the in-flight-record card and keeps its other ten metrics', () => {
  const feedback = source('src/components/TaskFeedback.vue')
  assert.doesNotMatch(feedback, /在途记录/)
  for (const label of ['总数', '已确认', '剩余', '密码正确', '密码错误', '双重验证', '锁定', '未激活', '不存在', '请重试', '其他错误']) {
    assert.ok(feedback.includes(`label: '${label}'`))
  }
})

test('Phone layout uses a bottom navigation and task cards without horizontal overflow', () => {
  const app = source('src/App.vue')
  const table = source('src/components/JobTable.vue')
  const styles = source('src/style.css')
  assert.match(app, /class="mobile-topbar-title"/)
  assert.match(app, /class="sign-out-button" aria-label="退出"/)
  assert.match(table, /class="mobile-job-list"/)
  assert.match(table, /class="mobile-job-card"/)
  assert.match(styles, /@media\(max-width:700px\)/)
  assert.match(styles, /\.sidebar\{position:fixed;[^}]*right:0;bottom:0;left:0/)
  assert.match(styles, /padding-bottom:calc\(72px \+ env\(safe-area-inset-bottom\)\)/)
  assert.match(styles, /\.desktop-job-table,\.job-table\{display:none\}/)
  assert.match(styles, /\.mobile-job-list\{display:grid/)
  assert.match(styles, /overflow-x:hidden/)
})

test('Collapsed desktop rail hides labels while phone navigation keeps them visible', () => {
  const styles = source('src/style.css')
  assert.match(styles, /\.sidebar \.el-menu--collapse \.el-menu-item span\{display:none\}/)
  assert.match(styles, /@media\(max-width:700px\)[\s\S]*?\.sidebar \.el-menu--collapse \.el-menu-item span\{display:block/)
})
