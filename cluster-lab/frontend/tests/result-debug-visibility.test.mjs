import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
const source = readFileSync(new URL('../src/components/ResultList.vue', import.meta.url), 'utf8');
test('Chinese reason is always visible and independent from proxy debug', () => {
  assert.match(source, /prop="reason_cn" label="中文原因"/);
  assert.doesNotMatch(source, /showErrorReasons/);
  assert.doesNotMatch(source, /v-if="showErrorReasons"/);
});
