import test from 'node:test'
import assert from 'node:assert/strict'
import { readFileSync } from 'node:fs'
import { fileURLToPath } from 'node:url'
import { dirname, resolve } from 'node:path'

const root = resolve(dirname(fileURLToPath(import.meta.url)), '..')
const source = path => readFileSync(resolve(root, path), 'utf8')

test('Controller job list exposes select-all and guarded batch deletion', () => {
  const jobs = source('src/views/Jobs.vue')
  const table = source('src/components/JobTable.vue')
  assert.match(jobs, /全选当前列表/)
  assert.match(jobs, /取消全选/)
  assert.match(jobs, /删除选中/)
  assert.match(jobs, /\/api\/jobs\/batch-delete/)
  assert.match(jobs, /jobTable\.value\?\.selectAllVisible/)
  assert.match(table, /defineExpose\(\{ selectAllVisible, clearSelection \}\)/)
  assert.match(table, /:selectable="selectable"/)
  assert.match(table, /row\.status !== 'preparing'/)
})
