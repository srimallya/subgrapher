const test = require('node:test');
const assert = require('node:assert/strict');

const { createBundledSmallLlmRuntime } = require('../runtime/bundled_small_llm');

test('bundled small llm diagnostics expose model info and active work state', async () => {
  const runtime = createBundledSmallLlmRuntime({ projectRoot: process.cwd() });
  const before = runtime.diagnostics();
  assert.equal(before.ok, true);
  assert.ok(before.model_id);
  assert.equal(before.active_count, 0);

  const res = await runtime.classifyNotePolicy({
    title: 'Day 23 of the war',
    body_markdown: 'As of today, the current conflict continues.',
  });
  assert.equal(res.ok, true);
  assert.ok(res.note_mode);
  const after = runtime.diagnostics();
  assert.equal(after.active_count, 0);
});
