const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('fs');
const os = require('os');
const path = require('path');

const { createDashboardStore } = require('../runtime/dashboard_store');

function makeTempDir() {
  return fs.mkdtempSync(path.join(os.tmpdir(), 'subgrapher-dashboard-test-'));
}

test('dashboard store preserves raw article text and prefers clean summaries in listings', async () => {
  const tempDir = makeTempDir();
  const store = createDashboardStore({
    userDataPath: tempDir,
    getEmbeddingConfig: () => ({}),
  });

  const statePath = path.join(tempDir, 'dashboard_state.json');
  fs.writeFileSync(statePath, JSON.stringify({
    version: 3,
    events: [],
    tasks: [],
    filters: { selected_topic: 'all' },
    rss: {
      last_refreshed_at: 1_710_000_000_000,
      items: [{
        id: 'feed_1',
        url: 'https://example.com/article',
        title: 'Raw title',
        display_title: 'Raw title',
        summary: 'Raw summary',
        raw_content_text: 'Noisy raw fetched content with cookie text and extra boilerplate.',
        clean_summary: 'Clean summary: factual article body.',
        clean_excerpt: 'Clean excerpt',
        summary_status: 'generated',
        summary_model_id: 'policy-test-model',
        summary_generated_at: 1_710_000_000_000,
        source_name: 'Example Feed',
        source_domain: 'example.com',
        topic: 'world',
        source_topic: 'world',
        published_at: Date.now(),
        fetched_at: Date.now(),
        content_fetched_at: Date.now(),
        fetch_status: 'fetched',
      }],
    },
  }, null, 2), 'utf8');

  const listed = await store.listFeedItems({ topic: 'all', limit: 20, query: '' });
  assert.equal(listed.ok, true);
  assert.equal(listed.items.length, 1);
  assert.equal(listed.items[0].clean_excerpt, 'Clean excerpt');
  assert.match(listed.items[0].clean_summary, /Clean summary:/);
  assert.match(listed.items[0].raw_content_text, /Noisy raw fetched content/);
  assert.equal(listed.items[0].summary_status, 'generated');

  const itemRes = store.getFeedItem('feed_1');
  assert.equal(itemRes.ok, true);
  assert.equal(itemRes.item.clean_excerpt, 'Clean excerpt');
  assert.ok(String(itemRes.item.raw_content_text || '').includes('Noisy raw fetched content'));

  const backlog = store.getSummaryBacklogStatus();
  assert.equal(backlog.ok, true);
  assert.equal(backlog.total_candidates, 1);
  assert.equal(backlog.pending_items, 1);
});

test('dashboard store resetFeedCache clears stored RSS items without touching other dashboard state', async () => {
  const tempDir = makeTempDir();
  const store = createDashboardStore({
    userDataPath: tempDir,
    getEmbeddingConfig: () => ({}),
  });

  const statePath = path.join(tempDir, 'dashboard_state.json');
  fs.writeFileSync(statePath, JSON.stringify({
    version: 3,
    events: [{ id: 'evt_1', title: 'Existing event', start_date: '2026-03-23', end_date: '2026-03-23', repeat: 'off', note: '', time: '', created_at: Date.now(), updated_at: Date.now() }],
    tasks: [{ id: 'tsk_1', title: 'Existing task', created_at: Date.now(), updated_at: Date.now() }],
    filters: { selected_topic: 'all' },
    rss: {
      last_refreshed_at: Date.now(),
      items: [{
        id: 'feed_reset_1',
        url: 'https://example.com/reset',
        title: 'Reset me',
        display_title: 'Reset me',
        raw_content_text: 'Raw article text',
        clean_summary: 'Existing clean summary',
        source_name: 'Example Feed',
        source_domain: 'example.com',
        topic: 'world',
        source_topic: 'world',
        published_at: Date.now(),
        fetched_at: Date.now(),
        content_fetched_at: Date.now(),
        fetch_status: 'fetched',
      }],
    },
  }, null, 2), 'utf8');

  const resetRes = store.resetFeedCache();
  assert.equal(resetRes.ok, true);
  assert.equal(resetRes.cleared_items, 1);

  const nextState = store.getState();
  assert.equal(nextState.ok, true);
  assert.equal(Array.isArray(nextState.state.events), true);
  assert.equal(nextState.state.events.length, 1);
  assert.equal(Array.isArray(nextState.state.tasks), true);
  assert.equal(nextState.state.tasks.length, 1);

  const listed = await store.listFeedItems({ topic: 'all', limit: 20, query: '' });
  assert.equal(listed.ok, true);
  assert.equal(listed.items.length, 0);
});

test('dashboard store rerunFeedItemSummary refetches content and regenerates summary', async () => {
  const tempDir = makeTempDir();
  const store = createDashboardStore({
    userDataPath: tempDir,
    getEmbeddingConfig: () => ({}),
    fetchArticlePreview: async () => ({
      ok: true,
      title: 'Recovered article title',
      text: 'Recovered article body with the real story content after refetch.',
      markdown: 'Recovered article body with the real story content after refetch.',
      fetch_status: 'fetched',
    }),
    summarizeArticle: async (article = {}) => ({
      ok: true,
      status: 'generated',
      summary: `Recovered summary from: ${String(article.raw_content_text || '').trim()} Sentence two adds context. Sentence three adds detail. Sentence four names affected parties. Sentence five covers what comes next.`,
      excerpt: 'Recovered summary from refetch.',
      entities: ['Example Org'],
      topics: ['world'],
      content_quality: 'clean',
      model_id: 'test-model',
      generated_at: 1_710_000_000_123,
      analysis_source: 'llm',
      reason: '',
    }),
  });

  const statePath = path.join(tempDir, 'dashboard_state.json');
  fs.writeFileSync(statePath, JSON.stringify({
    version: 3,
    events: [],
    tasks: [],
    filters: { selected_topic: 'all' },
    rss: {
      last_refreshed_at: Date.now(),
      items: [{
        id: 'feed_retry_1',
        url: 'https://example.com/retry',
        title: 'Retry me',
        display_title: 'Retry me',
        raw_content_text: 'Old cached body.',
        clean_summary: 'Old clean summary.',
        clean_excerpt: 'Old excerpt.',
        source_name: 'Example Feed',
        source_domain: 'example.com',
        topic: 'world',
        source_topic: 'world',
        published_at: Date.now(),
        fetched_at: Date.now(),
        content_fetched_at: Date.now(),
        fetch_status: 'fetched',
      }],
    },
  }, null, 2), 'utf8');

  const res = await store.rerunFeedItemSummary('feed_retry_1');
  assert.equal(res.ok, true);
  assert.match(String(res.item.raw_content_text || ''), /Recovered article body/);
  assert.match(String(res.item.clean_summary || ''), /Recovered summary from:/);
  assert.equal(res.item.summary_status, 'generated');
  assert.equal(res.item.manual_retry_count, 1);
  assert.equal(res.item.hidden_from_feed, false);
});

test('dashboard store deletes item after second unavailable manual retry', async () => {
  const tempDir = makeTempDir();
  const store = createDashboardStore({
    userDataPath: tempDir,
    getEmbeddingConfig: () => ({}),
    fetchArticlePreview: async () => ({
      ok: false,
      fetch_status: 'rss_only',
    }),
  });

  const statePath = path.join(tempDir, 'dashboard_state.json');
  fs.writeFileSync(statePath, JSON.stringify({
    version: 3,
    events: [],
    tasks: [],
    filters: { selected_topic: 'all' },
    rss: {
      last_refreshed_at: Date.now(),
      items: [{
        id: 'feed_unavailable_1',
        url: 'https://example.com/unavailable',
        title: 'Unavailable story',
        display_title: 'Unavailable story',
        summary: '',
        raw_content_text: '',
        clean_summary: '',
        clean_excerpt: '',
        source_name: 'Example Feed',
        source_domain: 'example.com',
        topic: 'world',
        source_topic: 'world',
        published_at: Date.now(),
        fetched_at: Date.now(),
        content_fetched_at: Date.now(),
        fetch_status: 'rss_only',
        manual_retry_count: 1,
      }],
    },
  }, null, 2), 'utf8');

  const res = await store.rerunFeedItemSummary('feed_unavailable_1');
  assert.equal(res.ok, true);
  assert.equal(res.deleted, true);
  assert.equal(res.item.summary_status, 'unavailable');
  assert.equal(res.item.manual_retry_count, 2);

  const listed = await store.listFeedItems({ topic: 'all', limit: 20, query: '' });
  assert.equal(listed.ok, true);
  assert.equal(listed.items.length, 0);

  const fetched = store.getFeedItem('feed_unavailable_1');
  assert.equal(fetched.ok, false);
  assert.match(String(fetched.message || ''), /not found/i);
});

test('dashboard store keeps first unavailable result visible until retry', async () => {
  const tempDir = makeTempDir();
  const store = createDashboardStore({
    userDataPath: tempDir,
    getEmbeddingConfig: () => ({}),
    fetchArticlePreview: async () => ({
      ok: false,
      fetch_status: 'rss_only',
    }),
  });

  const statePath = path.join(tempDir, 'dashboard_state.json');
  fs.writeFileSync(statePath, JSON.stringify({
    version: 3,
    events: [],
    tasks: [],
    filters: { selected_topic: 'all' },
    rss: {
      last_refreshed_at: Date.now(),
      items: [{
        id: 'feed_unavailable_first',
        url: 'https://example.com/unavailable-first',
        title: 'Unavailable story',
        display_title: 'Unavailable story',
        summary: '',
        raw_content_text: '',
        clean_summary: '',
        clean_excerpt: '',
        source_name: 'Example Feed',
        source_domain: 'example.com',
        topic: 'world',
        source_topic: 'world',
        published_at: Date.now(),
        fetched_at: Date.now(),
        content_fetched_at: Date.now(),
        fetch_status: 'rss_only',
        manual_retry_count: 0,
      }],
    },
  }, null, 2), 'utf8');

  const res = await store.rerunFeedItemSummary('feed_unavailable_first');
  assert.equal(res.ok, true);
  assert.equal(res.deleted, undefined);
  assert.equal(res.item.summary_status, 'unavailable');
  assert.equal(res.item.manual_retry_count, 1);

  const listed = await store.listFeedItems({ topic: 'all', limit: 20, query: '' });
  assert.equal(listed.ok, true);
  assert.equal(listed.items.length, 1);

  const fetched = store.getFeedItem('feed_unavailable_first');
  assert.equal(fetched.ok, true);
  assert.equal(fetched.item.summary_status, 'unavailable');
});
