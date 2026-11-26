import test from 'node:test'
import assert from 'node:assert/strict'
import { readFileSync } from 'node:fs'
import { dirname, resolve } from 'node:path'
import { fileURLToPath } from 'node:url'
import vm from 'node:vm'
import ts from 'typescript'

const root = resolve(dirname(fileURLToPath(import.meta.url)), '..')
const task = readFileSync(resolve(root, 'src/components/CreateTaskDialog.vue'), 'utf8')
const settings = readFileSync(resolve(root, 'src/views/Settings.vue'), 'utf8')
const jobTable = readFileSync(resolve(root, 'src/components/JobTable.vue'), 'utf8')

// Execute the real setup script with in-memory transport and ref fixtures.
// No browser, filesystem upload, backend, or production request is involved.
function setupFixture() {
  const calls = [], watches = []
  const ref = value => ({ value })
  const busy = ref(false), createTaskOpen = ref(true)
  const state = ref({ settings: { max_unit_rows: 45000, public_header_use_limit: 21,
    device_use_limit: 1800, web_secondary_auth: false } })
  const context = {
    ref, computed: get => ({ get value() { return get() } }),
    watch: (source, callback) => watches.push({ source, callback }),
    useRouter: () => ({ push: async route => calls.push({ kind: 'route', route }) }),
    api: async (url, payload, method) => {
      calls.push({ kind: 'api', url, payload, method })
      return url === '/api/files' ? { files: [] } : {}
    },
    refresh: async () => calls.push({ kind: 'refresh' }),
    busy, createTaskOpen, state, session: ref({ csrf: 'fixture-csrf' }), URLSearchParams,
    XMLHttpRequest: class {
      upload = {}
      status = 201
      responseText = '{}'
      open(method, url) { this.method = method; this.url = url }
      setRequestHeader() {}
      send(file) { calls.push({ kind: 'stream', method: this.method, url: this.url, file }); this.onload() }
    },
  }
  const script = task.match(/<script setup lang="ts">([\s\S]*?)<\/script>/)[1].replace(/^import .*$/gm, '')
  const executable = ts.transpileModule(`${script}\nglobalThis.__ui = { submit, timeout, retries, itemTimeout, publicHeaderUseLimit, deviceUseLimit, webSecondaryAuth, mode, chosen, file, error, unitRows };`, {
    compilerOptions: { target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.None },
  }).outputText
  vm.createContext(context)
  vm.runInContext(executable, context)
  return { ui: context.__ui, calls, busy, createTaskOpen, state, watches }
}

function fixtureRequests(fixture) {
  return fixture.calls.filter(call => call.kind === 'stream' || call.kind === 'api')
}

test('Device limits are task fields, not global settings controls', () => {
  assert.doesNotMatch(settings, /saveDevicePolicy|\/api\/settings\/device-policy|v-model="(?:publicHeaderUseLimit|deviceUseLimit)"/)
  assert.match(settings, /公共头刷新、设备淘汰已移至/)
  assert.match(task, /v-model="publicHeaderUseLimit"[^>]*:min="1"[^>]*:max="10000"/)
  assert.match(task, /v-model="deviceUseLimit"[^>]*:min="1"[^>]*:max="1000000"/)
  assert.match(task, /v-model="webSecondaryAuth"/)
  assert.match(task, /GSA 返回 <code>au=repair<\/code>/)
  assert.match(task, /随任务保存/)
})

test('Raw stream task creation transmits both chosen policy values', async () => {
  const fixture = setupFixture()
  fixture.ui.file.value = { name: 'fixture.txt' }
  fixture.ui.publicHeaderUseLimit.value = 23
  fixture.ui.deviceUseLimit.value = 2300
  fixture.ui.webSecondaryAuth.value = true
  await fixture.ui.submit()
  const request = fixture.calls.find(call => call.kind === 'stream')
  assert.ok(request)
  const url = new URL(request.url, 'http://fixture.invalid')
  assert.equal(url.pathname, '/api/jobs/stream')
  assert.equal(url.searchParams.get('public_header_use_limit'), '23')
  assert.equal(url.searchParams.get('device_use_limit'), '2300')
  assert.equal(url.searchParams.get('web_secondary_auth'), 'true')
  assert.equal(url.searchParams.get('enqueue'), 'true')
  assert.equal(fixture.busy.value, false)
  assert.equal(fixture.createTaskOpen.value, false)
  assert.match(task, /xhr\.send\(selected\)/, 'Retain raw streaming instead of multipart conversion')
})

test('Reused-file task creation transmits both chosen policy values', async () => {
  const fixture = setupFixture()
  fixture.ui.mode.value = 'reuse'
  fixture.ui.chosen.value = 'fixture-source'
  fixture.ui.publicHeaderUseLimit.value = 31
  fixture.ui.deviceUseLimit.value = 3100
  fixture.ui.webSecondaryAuth.value = false
  await fixture.ui.submit()
  const request = fixture.calls.find(call => call.kind === 'api')
  assert.equal(request.url, '/api/jobs/from-file')
  assert.equal(request.method, 'POST')
  assert.equal(request.payload.source, 'fixture-source')
  assert.equal(request.payload.public_header_use_limit, 31)
  assert.equal(request.payload.device_use_limit, 3100)
  assert.equal(request.payload.web_secondary_auth, false)
  assert.equal(fixture.calls.some(call => call.kind === 'stream'), false)
  assert.equal(fixture.busy.value, false)
})

test('Out-of-range, fractional, and missing device policy values prevent submission', async () => {
  for (const [field, values] of [
    ['publicHeaderUseLimit', [0, 10001, 1.5, NaN, undefined]],
    ['deviceUseLimit', [0, 1000001, 1.5, NaN, undefined]],
  ]) {
    for (const value of values) {
      const fixture = setupFixture()
      fixture.ui.file.value = { name: 'fixture.txt' }
      fixture.ui[field].value = value
      await fixture.ui.submit()
      assert.equal(fixtureRequests(fixture).length, 0, `${field}=${value} must not send a request`)
      assert.ok(fixture.ui.error.value)
      assert.equal(fixture.busy.value, false)
    }
  }
})

test('Invalid request policy and duplicate submission do not send requests', async () => {
  for (const [field, value] of [['timeout', 4], ['timeout', 121], ['retries', -1], ['retries', 6], ['itemTimeout', 24], ['itemTimeout', 601]]) {
    const fixture = setupFixture()
    fixture.ui.file.value = { name: 'fixture.txt' }
    fixture.ui[field].value = value
    await fixture.ui.submit()
    assert.equal(fixtureRequests(fixture).length, 0)
    assert.ok(fixture.ui.error.value)
  }
  const fixture = setupFixture()
  fixture.ui.file.value = { name: 'fixture.txt' }
  fixture.busy.value = true
  await fixture.ui.submit()
  assert.equal(fixtureRequests(fixture).length, 0)
  assert.equal(fixture.busy.value, true, 'Existing submission retains ownership of busy state')
})

test('Opening creation resets defaults from settings and total budget follows timeout', () => {
  const fixture = setupFixture()
  const onOpen = fixture.watches.find(watch => watch.source === fixture.createTaskOpen)
  onOpen.callback(true)
  assert.equal(fixture.ui.publicHeaderUseLimit.value, 21)
  assert.equal(fixture.ui.deviceUseLimit.value, 1800)
  assert.equal(fixture.ui.webSecondaryAuth.value, false)
  assert.equal(fixture.ui.unitRows.value, 10000)
  const onTimeout = fixture.watches.find(watch => watch.source === fixture.ui.timeout)
  onTimeout.callback(80)
  assert.equal(fixture.ui.itemTimeout.value, 80)
})

test('Dialog has task, request, and device sections with scrollable body and stable footer', () => {
  for (const section of ['source-panel', 'execution-panel', 'device-policy-panel']) {
    assert.match(task, new RegExp(`class="${section}"`))
  }
  assert.match(task, /\.create-task-dialog\.el-dialog\{[^}]*max-height:88dvh;[^}]*display:flex;flex-direction:column/)
  assert.match(task, /\.create-task-dialog \.el-dialog__body\{[^}]*overflow-y:auto/)
  assert.match(task, /\.create-task-dialog \.el-dialog__footer\{[^}]*flex-shrink:0/)
  assert.match(task, /<template #footer>/)
  assert.match(task, /native-type="submit" form="create-task-form"/)
  assert.match(task, /@media\(max-width:900px\)[\s\S]*?\.create-task-columns\{grid-template-columns:1fr/)
  assert.match(task, /每个分片最多 \{\{ unitRows\.toLocaleString\('zh-CN'\) \}\}/)
  assert.doesNotMatch(task, /总控自动按每包最多 10,000 条/)
})

test('Native step alignment does not block custom-validated default values', () => {
  assert.match(task, /<el-form[^>]*novalidate/)
  assert.match(task, /Number\.isInteger\(deviceUseLimit\.value\)/)
})

test('Task table exposes selection, select-all, and guarded batch deletion', () => {
  assert.match(jobTable, />全选当前列表</)
  assert.match(jobTable, />批量删除</)
  assert.match(jobTable, /selectedIds/)
  assert.match(jobTable, /job\.status !== 'preparing'/)
  assert.match(jobTable, /for \(const job of jobs\)/)
  assert.match(jobTable, /api\(`\/api\/jobs\/\$\{job\.id\}`,[^\n]*'DELETE'\)/)
})
