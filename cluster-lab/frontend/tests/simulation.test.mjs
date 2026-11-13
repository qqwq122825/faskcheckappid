import test from 'node:test'
import assert from 'node:assert/strict'
import { DEMO_TOTAL, simulatedCounts } from '../src/lib/simulation.js'

test('Every simulated completion belongs to exactly one category', () => {
  let previous = simulatedCounts(0)
  for (let completed = 0; completed <= DEMO_TOTAL; completed++) {
    const counts = simulatedCounts(completed)
    assert.equal(Object.values(counts).reduce((a, b) => a + b, 0), completed)
    for (const key of Object.keys(counts)) assert.ok(counts[key] >= previous[key])
    previous = counts
  }
  assert.deepEqual(previous, { success: 7000, failed: 1200, timeout: 800, skipped: 1000 })
})
test('Invalid progress never creates misleading counts', () => {
  for (const value of [-1, .5, NaN, Infinity, DEMO_TOTAL + 1]) assert.throws(() => simulatedCounts(value), RangeError)
})
