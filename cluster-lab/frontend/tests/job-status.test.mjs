import test from 'node:test'
import assert from 'node:assert/strict'
import { jobDisplayStatus, jobIssueMessage, matchesJobStatus } from '../src/lib/jobStatus.js'

test('An unknown package with no in-flight work is visibly exceptional', () => {
  const job = Object.freeze({ status: 'running', unit_counts: Object.freeze({ unknown: 1 }) })
  assert.equal(jobDisplayStatus(job), 'needs_attention')
  assert.match(jobIssueMessage(job), /1 个异常工作包待确认/)
  assert.match(jobIssueMessage(job), /当前没有在途工作包/)
  assert.equal(matchesJobStatus(job, 'running'), false)
  assert.equal(matchesJobStatus(job, 'attention'), true)
  assert.equal(job.status, 'running', 'Presentation must not alter scheduler state')
})

test('Remaining pending work does not hide an unknown package', () => {
  assert.equal(jobDisplayStatus({ status: 'running', unit_counts: { unknown: 1, pending: 4 } }), 'needs_attention')
})

test('A partly active task keeps its activity and its error visible', () => {
  const job = { status: 'running', unit_counts: { unknown: 2, leased: 1, running: 3 } }
  assert.equal(jobDisplayStatus(job), 'running_with_errors')
  assert.match(jobIssueMessage(job), /仍有 4 个工作包在途/)
  assert.equal(matchesJobStatus(job, 'running'), true)
  assert.equal(matchesJobStatus(job, 'attention'), true)
})

test('Paused and stopped tasks retain their control state alongside errors', () => {
  assert.equal(jobDisplayStatus({ status: 'paused', unit_counts: { unknown: 1 } }), 'paused_with_errors')
  assert.equal(jobDisplayStatus({ status: 'stopped', unit_counts: { unknown: 1 } }), 'stopped_with_errors')
  assert.equal(jobDisplayStatus({ status: 'failed', unit_counts: { unknown: 1 } }), 'failed')
})

test('Normal, missing, and completed data do not create spurious errors', () => {
  for (const status of ['preparing', 'ready', 'queued', 'running', 'paused', 'completed', 'failed']) {
    assert.equal(jobDisplayStatus({ status }), status)
    assert.equal(jobDisplayStatus({ status, unit_counts: { unknown: 0 } }), status)
  }
  assert.equal(jobDisplayStatus(null), '')
  assert.equal(jobIssueMessage(undefined), '')
  const completed = { status: 'completed', unit_counts: { completed: 1 }, categorized: { unknown: 110 } }
  assert.equal(jobDisplayStatus(completed), 'completed', 'Result categories and scheduler errors are different')
})

test('Inconsistent completed state with unknown packages is not shown as success', () => {
  assert.equal(jobDisplayStatus({ status: 'completed', unit_counts: { unknown: 1 } }), 'needs_attention')
})
