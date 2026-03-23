const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('fs');
const os = require('os');
const path = require('path');

const { createBundledSmallLlmRuntime } = require('../runtime/bundled_small_llm');

function makeTempBundledRuntime() {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'subgrapher-llm-'));
  const binDir = path.join(root, 'bin');
  const modelDir = path.join(root, 'models');
  fs.mkdirSync(binDir, { recursive: true });
  fs.mkdirSync(modelDir, { recursive: true });
  const executablePath = path.join(binDir, 'subgrapher-llm');
  const modelPath = path.join(modelDir, 'Qwen3.5-0.8B-Q8_0.gguf');
  const script = `#!/usr/bin/env node
const chunks = [];
process.stdin.on('data', (chunk) => chunks.push(chunk));
process.stdin.on('end', () => {
  const args = process.argv.slice(2);
  const taskIndex = args.indexOf('--task');
  const task = taskIndex >= 0 ? args[taskIndex + 1] : '';
  if (task === 'note_policy_classification') {
    process.stdout.write(JSON.stringify({
      note_mode: 'live_update',
      freshness_bias: 'high',
      source_mix: 'latest_news',
      contradiction_scan: true,
      result_budget: 8,
      staleness_ttl_minutes: 90,
      prefer_recent_window_days: 3
    }));
    return;
  }
  process.stdout.write(JSON.stringify({
    summary: 'Cleaned summary from bundled runtime. It removes scraper junk from the fetched article. It keeps the factual core of the story intact. It returns a longer gist suitable for the status panel. It stays within the structured output contract.',
    excerpt: 'Cleaned summary from bundled runtime.',
    entities: ['Iran', 'United States'],
    topics: ['world'],
    content_quality: 'clean'
  }));
});
`;
  fs.writeFileSync(executablePath, script, { mode: 0o755 });
  fs.writeFileSync(modelPath, 'placeholder-model');
  fs.writeFileSync(path.join(root, 'runtime-manifest.json'), `${JSON.stringify({
    bundled: true,
    backend: 'bundled-cli',
    model_id: 'test-bundled-model',
    model_name: 'Test Bundled Model',
    tasks: ['note_policy_classification', 'rss_article_cleanup_summary'],
    schema_version: 1,
    prompt_versions: {
      note_policy_classification: 1,
      rss_article_cleanup_summary: 1,
    },
    executable_rel_path: 'bin/subgrapher-llm',
    model_rel_path: 'models/Qwen3.5-0.8B-Q8_0.gguf',
    timeout_ms: 5000,
  }, null, 2)}\n`);
  return root;
}

test('bundled small llm diagnostics report missing bundled assets by default', async () => {
  const emptyRoot = fs.mkdtempSync(path.join(os.tmpdir(), 'subgrapher-llm-empty-'));
  const runtime = createBundledSmallLlmRuntime({
    projectRoot: process.cwd(),
    bundledRootDir: emptyRoot,
  });
  const before = runtime.diagnostics();
  assert.equal(before.ok, true);
  assert.equal(before.available, false);
  assert.match(before.unavailable_reason, /missing/i);

  const res = await runtime.classifyNotePolicy({
    title: 'Day 23 of the war',
    body_markdown: 'As of today, the current conflict continues.',
  });
  assert.equal(res.ok, true);
  assert.equal(res.analysis_source, 'fallback');
  assert.equal(res.analysis_detail, 'bundled_llm_unavailable');
  assert.match(String(res.fallback_reason || ''), /missing|unavailable/i);
});

test('bundled small llm uses packaged runtime when executable and model are present', async () => {
  const bundledRootDir = makeTempBundledRuntime();
  const runtime = createBundledSmallLlmRuntime({
    projectRoot: process.cwd(),
    bundledRootDir,
  });

  const diag = runtime.diagnostics();
  assert.equal(diag.available, true);
  assert.equal(diag.model_id, 'test-bundled-model');

  const notePolicy = await runtime.classifyNotePolicy({
    title: 'Day 23 of the war',
    body_markdown: 'As of today, the current conflict continues.',
  });
  assert.equal(notePolicy.ok, true);
  assert.equal(notePolicy.analysis_source, 'llm');
  assert.equal(notePolicy.note_mode, 'live_update');
  assert.equal(notePolicy.freshness_bias, 'high');

  const summary = await runtime.summarizeFeedArticle({
    title: 'Example article',
    raw_content_text: 'Raw content text.',
  });
  assert.equal(summary.ok, true);
  assert.equal(summary.analysis_source, 'llm');
  assert.match(summary.summary, /Cleaned summary from bundled runtime/);
});
