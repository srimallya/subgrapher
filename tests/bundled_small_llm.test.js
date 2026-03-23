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

test('bundled small llm retries malformed feed-summary outputs before falling back', async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'subgrapher-llm-retry-'));
  const binDir = path.join(root, 'bin');
  const modelDir = path.join(root, 'models');
  fs.mkdirSync(binDir, { recursive: true });
  fs.mkdirSync(modelDir, { recursive: true });
  const executablePath = path.join(binDir, 'subgrapher-llm');
  const modelPath = path.join(modelDir, 'Qwen3.5-0.8B-Q8_0.gguf');
  const counterPath = path.join(root, 'attempt-count.txt');
  const script = `#!/usr/bin/env node
const fs = require('fs');
const counterPath = ${JSON.stringify(counterPath)};
const prior = fs.existsSync(counterPath) ? Number(fs.readFileSync(counterPath, 'utf8')) || 0 : 0;
const attempt = prior + 1;
fs.writeFileSync(counterPath, String(attempt));
if (attempt < 3) {
  process.stdout.write('{"summary":"too short"}');
  process.exit(0);
}
process.stdout.write(JSON.stringify({
  summary: 'Sentence one states the core news clearly. Sentence two adds the immediate context. Sentence three explains the reported decision. Sentence four notes who is affected. Sentence five describes the next step in the story.',
  excerpt: 'Sentence one states the core news clearly. Sentence two adds the immediate context.',
  entities: ['Microsoft'],
  topics: ['tech'],
  content_quality: 'clean'
}));
`;
  fs.writeFileSync(executablePath, script, { mode: 0o755 });
  fs.writeFileSync(modelPath, 'placeholder-model');
  fs.writeFileSync(path.join(root, 'runtime-manifest.json'), `${JSON.stringify({
    bundled: true,
    backend: 'bundled-cli',
    model_id: 'retry-model',
    model_name: 'Retry Model',
    tasks: ['note_policy_classification', 'rss_article_cleanup_summary'],
    schema_version: 1,
    prompt_versions: {
      note_policy_classification: 1,
      rss_article_cleanup_summary: 1,
    },
    executable_rel_path: 'bin/subgrapher-llm',
    model_rel_path: 'models/Qwen3.5-0.8B-Q8_0.gguf',
    timeout_ms: 5000,
    max_retries: 5,
  }, null, 2)}\n`);

  const runtime = createBundledSmallLlmRuntime({
    projectRoot: process.cwd(),
    bundledRootDir: root,
  });
  const summary = await runtime.summarizeFeedArticle({
    title: 'Retry article',
    raw_content_text: 'Raw article body that should eventually summarize correctly.',
  });
  assert.equal(summary.ok, true);
  assert.equal(summary.analysis_source, 'llm');
  assert.equal(summary.attempts, 3);
  assert.match(summary.summary, /Sentence one states the core news clearly/);
});

test('bundled small llm runs note and rss lanes in parallel while serializing each lane', async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'subgrapher-llm-queues-'));
  const binDir = path.join(root, 'bin');
  const modelDir = path.join(root, 'models');
  fs.mkdirSync(binDir, { recursive: true });
  fs.mkdirSync(modelDir, { recursive: true });
  const executablePath = path.join(binDir, 'subgrapher-llm');
  const modelPath = path.join(modelDir, 'Qwen3.5-0.8B-Q8_0.gguf');
  const logPath = path.join(root, 'task-log.txt');
  const script = `#!/usr/bin/env node
const fs = require('fs');
const logPath = ${JSON.stringify(logPath)};
const chunks = [];
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
process.stdin.on('data', (chunk) => chunks.push(chunk));
process.stdin.on('end', async () => {
  const payload = JSON.parse(Buffer.concat(chunks).toString() || '{}');
  const args = process.argv.slice(2);
  const taskIndex = args.indexOf('--task');
  const task = taskIndex >= 0 ? args[taskIndex + 1] : '';
  const label = payload.id || payload.title || task;
  fs.appendFileSync(logPath, 'start ' + task + ' ' + label + ' ' + Date.now() + '\\n');
  await sleep(150);
  fs.appendFileSync(logPath, 'end ' + task + ' ' + label + ' ' + Date.now() + '\\n');
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
    summary: 'Sentence one states the core news clearly. Sentence two adds the immediate context. Sentence three explains the reported decision. Sentence four notes who is affected. Sentence five describes the next step in the story.',
    excerpt: 'Sentence one states the core news clearly. Sentence two adds the immediate context.',
    entities: ['Microsoft'],
    topics: ['tech'],
    content_quality: 'clean'
  }));
});
`;
  fs.writeFileSync(executablePath, script, { mode: 0o755 });
  fs.writeFileSync(modelPath, 'placeholder-model');
  fs.writeFileSync(path.join(root, 'runtime-manifest.json'), `${JSON.stringify({
    bundled: true,
    backend: 'bundled-cli',
    model_id: 'queue-model',
    model_name: 'Queue Model',
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

  const runtime = createBundledSmallLlmRuntime({
    projectRoot: process.cwd(),
    bundledRootDir: root,
  });

  await Promise.all([
    runtime.summarizeFeedArticle({ id: 'rss1', title: 'RSS 1', raw_content_text: 'Article one body.' }),
    runtime.summarizeFeedArticle({ id: 'rss2', title: 'RSS 2', raw_content_text: 'Article two body.' }),
    runtime.classifyNotePolicy({ id: 'note1', title: 'Note 1', body_markdown: 'Current update on the story.' }),
    runtime.classifyNotePolicy({ id: 'note2', title: 'Note 2', body_markdown: 'Another current update on the story.' }),
  ]);

  const entries = fs.readFileSync(logPath, 'utf8')
    .trim()
    .split('\n')
    .filter(Boolean)
    .map((line) => {
      const [phase, task, label, ts] = line.trim().split(/\s+/);
      return { phase, task, label, ts: Number(ts) || 0 };
    });
  const eventFor = (phase, label) => entries.find((entry) => entry.phase === phase && entry.label === label);
  const rss1Start = eventFor('start', 'rss1');
  const rss1End = eventFor('end', 'rss1');
  const rss2Start = eventFor('start', 'rss2');
  const rss2End = eventFor('end', 'rss2');
  const note1Start = eventFor('start', 'note1');
  const note1End = eventFor('end', 'note1');
  const note2Start = eventFor('start', 'note2');
  const note2End = eventFor('end', 'note2');

  assert.ok(rss1Start && rss1End && rss2Start && rss2End);
  assert.ok(note1Start && note1End && note2Start && note2End);
  assert.ok(rss2Start.ts >= rss1End.ts, 'rss lane should be serialized');
  assert.ok(note2Start.ts >= note1End.ts, 'note lane should be serialized');
  assert.ok(note1Start.ts < rss1End.ts && rss1Start.ts < note1End.ts, 'note and rss lanes should overlap');
});
