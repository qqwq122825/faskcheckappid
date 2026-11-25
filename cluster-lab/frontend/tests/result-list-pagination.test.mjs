import test from 'node:test'
import assert from 'node:assert/strict'
import { readFileSync } from 'node:fs'
import { Script, createContext } from 'node:vm'
import { setTimeout as delay } from 'node:timers/promises'
import ts from 'typescript'
import * as vue from 'vue'

// Execute the actual component script with Vue's real reactivity. The only
// application import is replaced with an in-memory, manually settled API.
const filename = new URL('../src/components/ResultList.vue', import.meta.url)
const source = readFileSync(filename, 'utf8')
const setup = source.match(/<script setup lang="ts">([\s\S]*?)<\/script>/)?.[1]
assert.ok(setup, 'ResultList must expose its TypeScript setup script')
const compiled = ts.transpileModule(`${setup}\n;globalThis.resultList = {
  category, accountSearch, offset, rows, total, loading, error,
  page, pageCount, visibleTotal, load, previous, next,
  changeCategory, refreshInBackground,
}\n`, {
  fileName: filename.pathname,
  compilerOptions: { target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.CommonJS },
  reportDiagnostics: true,
})
assert.deepEqual(compiled.diagnostics, [])
const script = new Script(compiled.outputText, { filename: filename.pathname })

async function flush() {
  await Promise.resolve()
  await vue.nextTick()
  await Promise.resolve()
}

function harness(t) {
  const props = vue.reactive({ job: { id: 'mock-job-a', confirmed: 1 } })
  const requests = []
  const timers = new Set()
  const scope = vue.effectScope()
  const context = createContext({
    exports: {},
    defineProps: () => props,
    require(name) {
      if (name === 'vue') return vue
      assert.equal(name, '../composables/useCluster', 'Unexpected application import')
      return {
        fmt: String,
        state: vue.ref({}),
        api(path) {
          let resolve, reject
          const promise = new Promise((accept, fail) => { resolve = accept; reject = fail })
          requests.push({ url: new URL(path, 'http://result-list.invalid'), resolve, reject })
          return promise
        },
      }
    },
    setTimeout(callback, milliseconds) {
      const timer = setTimeout(() => { timers.delete(timer); callback() }, milliseconds)
      timers.add(timer)
      return timer
    },
    clearTimeout(timer) { timers.delete(timer); clearTimeout(timer) },
  })
  scope.run(() => script.runInContext(context))
  t.after(() => { scope.stop(); for (const timer of timers) clearTimeout(timer) })
  const model = context.resultList
  return {
    model, props, requests,
    latest: () => requests.at(-1),
    accounts: () => Array.from(model.rows.value, row => row.account),
    async succeed(request, account, total = 350) {
      request.resolve({ rows: [{ account }], total })
      await flush()
    },
    async fail(request, message = 'mock GET failed') {
      request.reject(new Error(message))
      await flush()
    },
  }
}

async function validFirstPage(t) {
  const h = harness(t)
  assert.equal(h.requests.length, 1, 'Immediate real Vue watch starts the initial query')
  await h.succeed(h.latest(), 'all-first')
  h.model.category.value = 'valid'
  h.model.changeCategory()
  await h.succeed(h.latest(), 'valid-first')
  return h
}

function requestedOffset(request) {
  return Number(request.url.searchParams.get('offset'))
}

function navigate(h, direction) {
  h.model[direction]()
  return h.latest()
}

test('failed next keeps the first page and its rows', async t => {
  const h = await validFirstPage(t)
  const request = navigate(h, 'next')
  assert.equal(requestedOffset(request), 100)
  assert.equal(h.model.loading.value, true)
  await h.fail(request)
  assert.equal(h.model.offset.value, 0)
  assert.equal(h.model.page.value, 1)
  assert.deepEqual(h.accounts(), ['valid-first'])
  assert.equal(h.model.total.value, 350)
  assert.equal(h.model.error.value, 'mock GET failed')
  assert.equal(h.model.loading.value, false)
})

test('retry after failed next requests the same page and commits on success', async t => {
  const h = await validFirstPage(t)
  await h.fail(navigate(h, 'next'))
  const retry = navigate(h, 'next')
  await h.succeed(retry, 'valid-second')
  assert.equal(requestedOffset(retry), 100, 'Retry must not skip the failed page')
  assert.equal(h.model.offset.value, 100)
  assert.equal(h.model.page.value, 2)
  assert.equal(h.model.pageCount.value, 4)
  assert.deepEqual(h.accounts(), ['valid-second'])
  assert.equal(h.model.error.value, '')
})

test('pending next leaves the committed page unchanged until its GET succeeds', async t => {
  const h = await validFirstPage(t)
  const request = navigate(h, 'next')
  const pending = { offset: h.model.offset.value, page: h.model.page.value, rows: h.accounts() }
  await h.succeed(request, 'valid-second')
  assert.deepEqual(pending, { offset: 0, page: 1, rows: ['valid-first'] })
  assert.equal(h.model.offset.value, 100)
  assert.equal(h.model.page.value, 2)
  assert.deepEqual(h.accounts(), ['valid-second'])
})

test('failed previous keeps the current page and can retry that same previous page', async t => {
  const h = await validFirstPage(t)
  await h.succeed(navigate(h, 'next'), 'valid-second')
  const request = navigate(h, 'previous')
  assert.equal(requestedOffset(request), 0)
  await h.fail(request)
  assert.equal(h.model.offset.value, 100)
  assert.equal(h.model.page.value, 2)
  assert.deepEqual(h.accounts(), ['valid-second'])
  const retry = navigate(h, 'previous')
  assert.equal(requestedOffset(retry), 0)
  await h.succeed(retry, 'valid-first-again')
  assert.equal(h.model.offset.value, 0)
  assert.equal(h.model.page.value, 1)
  assert.deepEqual(h.accounts(), ['valid-first-again'])
})

test('successful silent refresh clears an earlier error without flashing loading', async t => {
  const h = await validFirstPage(t)
  void h.model.load()
  await h.fail(h.latest(), 'old foreground error')
  const refresh = h.model.refreshInBackground()
  assert.equal(h.model.loading.value, false)
  assert.equal(h.model.error.value, 'old foreground error')
  await h.succeed(h.latest(), 'refreshed-first')
  await refresh
  assert.equal(h.model.loading.value, false)
  assert.equal(h.model.error.value, '')
  assert.deepEqual(h.accounts(), ['refreshed-first'])
})

test('heartbeat refresh is silent, coalesced, and stays on the committed page', async t => {
  const h = await validFirstPage(t)
  await h.succeed(navigate(h, 'next'), 'valid-second')
  const count = h.requests.length
  h.props.job.confirmed += 1
  await flush()
  const refresh = h.latest()
  assert.equal(h.requests.length, count + 1)
  assert.equal(requestedOffset(refresh), 100)
  assert.equal(h.model.loading.value, false)
  h.props.job.confirmed += 1
  await flush()
  assert.equal(h.requests.length, count + 1, 'Only one background request may be pending')
  await h.fail(refresh, 'silent network failure')
  assert.equal(h.model.error.value, '')
  assert.equal(h.model.page.value, 2)
  assert.deepEqual(h.accounts(), ['valid-second'])
  h.props.job.confirmed += 1
  await flush()
  await h.succeed(h.latest(), 'refreshed-second')
  assert.equal(h.model.page.value, 2)
  assert.equal(h.model.loading.value, false)
  assert.deepEqual(h.accounts(), ['refreshed-second'])
})

test('late background success cannot replace a newer foreground page or clear its error', async t => {
  const h = await validFirstPage(t)
  const refresh = h.model.refreshInBackground()
  const background = h.latest()
  const foreground = navigate(h, 'next')
  await h.fail(foreground, 'new foreground error')
  await h.succeed(background, 'stale-background')
  await refresh
  assert.equal(h.model.offset.value, 0)
  assert.equal(h.model.error.value, 'new foreground error')
  assert.deepEqual(h.accounts(), ['valid-first'])
})

test('an older failed page request never rolls back a newer successful request', async t => {
  const h = await validFirstPage(t)
  const older = navigate(h, 'next')
  const newer = navigate(h, 'next')
  await h.succeed(newer, 'newer-second')
  assert.equal(h.model.loading.value, true, 'Keep the foreground mask until all requests settle')
  await h.fail(older, 'stale page failure')
  assert.equal(requestedOffset(older), 100)
  assert.equal(requestedOffset(newer), 100)
  assert.equal(h.model.offset.value, 100)
  assert.equal(h.model.page.value, 2)
  assert.equal(h.model.error.value, '')
  assert.equal(h.model.loading.value, false)
  assert.deepEqual(h.accounts(), ['newer-second'])
})

for (const outcome of ['success', 'failure']) {
  test(`category reset ignores a late previous-category page ${outcome}`, async t => {
    const h = await validFirstPage(t)
    await h.succeed(navigate(h, 'next'), 'valid-second')
    const stale = navigate(h, 'next')
    h.model.category.value = 'wrong_password'
    h.model.changeCategory()
    const current = h.latest()
    assert.equal(current.url.searchParams.get('category'), 'wrong_password')
    assert.equal(requestedOffset(current), 0)
    assert.equal(h.model.offset.value, 0)
    assert.equal(h.model.page.value, 1)
    assert.deepEqual(h.accounts(), [])
    assert.equal(h.model.total.value, 0)
    await h.succeed(current, 'wrong-first', 500)
    if (outcome === 'success') await h.succeed(stale, 'stale-valid-third', 700)
    else await h.fail(stale, 'stale category failure')
    assert.equal(h.model.offset.value, 0)
    assert.equal(h.model.total.value, 500)
    assert.equal(h.model.visibleTotal.value, 100)
    assert.equal(h.model.error.value, '')
    assert.equal(h.model.loading.value, false)
    assert.deepEqual(h.accounts(), ['wrong-first'])
  })
}

test('debounced search resets the page and ignores a late pre-search response', async t => {
  const h = await validFirstPage(t)
  await h.succeed(navigate(h, 'next'), 'valid-second')
  const stale = navigate(h, 'next')
  h.model.accountSearch.value = '  alice+mock@example.invalid  '
  await flush()
  await delay(330)
  const current = h.latest()
  assert.notEqual(current, stale)
  assert.equal(current.url.searchParams.get('search'), 'alice+mock@example.invalid')
  assert.equal(requestedOffset(current), 0)
  assert.equal(h.model.offset.value, 0)
  assert.deepEqual(h.accounts(), [])
  await h.succeed(current, 'search-first', 120)
  await h.succeed(stale, 'stale-search-page', 700)
  assert.equal(h.model.page.value, 1)
  assert.equal(h.model.total.value, 120)
  assert.equal(h.model.error.value, '')
  assert.deepEqual(h.accounts(), ['search-first'])
})

test('a response arriving inside the search debounce cannot commit stale rows or offset', async t => {
  const h = await validFirstPage(t)
  await h.succeed(navigate(h, 'next'), 'valid-second')
  const stale = navigate(h, 'next')
  h.model.accountSearch.value = 'bob'
  await flush()
  await h.succeed(stale, 'stale-during-debounce')
  const duringDebounce = { offset: h.model.offset.value, rows: h.accounts() }
  await delay(330)
  const current = h.latest()
  assert.equal(current.url.searchParams.get('search'), 'bob')
  await h.succeed(current, 'bob-first', 1)
  assert.deepEqual(duringDebounce, { offset: 100, rows: ['valid-second'] })
  assert.equal(h.model.offset.value, 0)
  assert.equal(h.model.page.value, 1)
  assert.deepEqual(h.accounts(), ['bob-first'])
})

test('rapid search edits debounce to the latest query and reset the page once', async t => {
  const h = await validFirstPage(t)
  await h.succeed(navigate(h, 'next'), 'valid-second')
  const count = h.requests.length
  h.model.accountSearch.value = 'older'
  await flush()
  h.model.accountSearch.value = 'latest'
  await flush()
  await delay(330)
  assert.equal(h.requests.length, count + 1)
  assert.equal(h.latest().url.searchParams.get('search'), 'latest')
  assert.equal(requestedOffset(h.latest()), 0)
  await h.succeed(h.latest(), 'latest-first', 1)
  assert.equal(h.model.page.value, 1)
  assert.deepEqual(h.accounts(), ['latest-first'])
})

test('job reset ignores a late request from the previous job', async t => {
  const h = await validFirstPage(t)
  const stale = navigate(h, 'next')
  h.props.job = { id: 'mock-job-b', confirmed: 2 }
  await flush()
  const current = h.latest()
  assert.equal(current.url.pathname, '/api/jobs/mock-job-b/rows')
  assert.equal(requestedOffset(current), 0)
  assert.deepEqual(h.accounts(), [])
  await h.succeed(current, 'job-b-first', 5)
  await h.fail(stale, 'stale job failure')
  assert.equal(h.model.offset.value, 0)
  assert.equal(h.model.total.value, 5)
  assert.equal(h.model.error.value, '')
  assert.equal(h.model.loading.value, false)
  assert.deepEqual(h.accounts(), ['job-b-first'])
})
