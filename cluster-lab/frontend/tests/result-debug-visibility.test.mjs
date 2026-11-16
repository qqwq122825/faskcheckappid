import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
const source = readFileSync(new URL('../src/components/ResultList.vue', import.meta.url), 'utf8');
test('error reason column requires the existing debug switch', () => {
  assert.match(source, /showErrorReasons = computed\(\(\) => state.value\?\.settings\?\.proxy_debug === true\)/);
  assert.match(source, /v-if="showErrorReasons" prop="reason"/);
  assert.match(source, /watch\(showErrorReasons/);
  assert.match(source, /void refreshInBackground\(\)/);
});
