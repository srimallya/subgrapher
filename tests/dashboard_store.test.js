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
