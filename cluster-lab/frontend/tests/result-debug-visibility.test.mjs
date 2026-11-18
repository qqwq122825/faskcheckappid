import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
const source = readFileSync(new URL('../src/components/ResultList.vue', import.meta.url), 'utf8');
test('The result column renders the reason selected by the controller debug policy', () => {
  assert.match(source, /prop="reason_cn" label="原因"/);
  assert.doesNotMatch(source, /showErrorReasons/);
  assert.doesNotMatch(source, /v-if="showErrorReasons"/);
});
