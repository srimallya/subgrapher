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

test('dashboard store balances sources and prioritizes readable items in listings', async () => {
  const tempDir = makeTempDir();
  const store = createDashboardStore({
    userDataPath: tempDir,
    getEmbeddingConfig: () => ({}),
  });

  const now = Date.now();
  const makeItem = (id, sourceName, minutesAgo, extra = {}) => ({
    id,
    url: `https://${sourceName.toLowerCase().replace(/\s+/g, '')}.example.com/${id}`,
    title: id,
    display_title: id,
    source_name: sourceName,
    source_domain: `${sourceName.toLowerCase().replace(/\s+/g, '')}.example.com`,
    topic: 'tech',
    source_topic: 'tech',
    published_at: now - (minutesAgo * 60 * 1000),
    fetched_at: now,
    ...extra,
  });

  const statePath = path.join(tempDir, 'dashboard_state.json');
  fs.writeFileSync(statePath, JSON.stringify({
    version: 3,
    events: [],
    tasks: [],
    filters: { selected_topic: 'all' },
    rss: {
      last_refreshed_at: now,
      items: [
        makeItem('verge-1', 'The Verge', 1, {
          raw_content_text: 'Readable Verge body 1',
          clean_summary: 'Readable Verge summary 1',
          clean_excerpt: 'Readable Verge excerpt 1',
          content_fetched_at: now,
          fetch_status: 'fetched',
          has_full_content: true,
        }),
        makeItem('verge-2', 'The Verge', 2, {
          raw_content_text: 'Readable Verge body 2',
          clean_summary: 'Readable Verge summary 2',
          clean_excerpt: 'Readable Verge excerpt 2',
          content_fetched_at: now,
          fetch_status: 'fetched',
          has_full_content: true,
        }),
        makeItem('verge-3', 'The Verge', 3, {
          raw_content_text: 'Readable Verge body 3',
          clean_summary: 'Readable Verge summary 3',
          clean_excerpt: 'Readable Verge excerpt 3',
          content_fetched_at: now,
          fetch_status: 'fetched',
          has_full_content: true,
        }),
        makeItem('ap-1', 'AP Top News', 4, {
          raw_content_text: 'Readable AP body',
          clean_summary: 'Readable AP summary',
          clean_excerpt: 'Readable AP excerpt',
          content_fetched_at: now,
          fetch_status: 'fetched',
          has_full_content: true,
        }),
        makeItem('ars-1', 'Ars Technica', 5, {
          raw_content_text: 'Readable Ars body',
          clean_summary: 'Readable Ars summary',
          clean_excerpt: 'Readable Ars excerpt',
          content_fetched_at: now,
          fetch_status: 'fetched',
          has_full_content: true,
        }),
        makeItem('headline-1', 'Reuters Technology', 0, {
          summary: 'Headline-only summary',
          clean_excerpt: 'Headline-only excerpt',
          fetch_status: 'rss_only',
          content_fetched_at: 0,
          has_full_content: false,
        }),
      ],
    },
  }, null, 2), 'utf8');

  const listed = await store.listFeedItems({ topic: 'all', limit: 5, query: '' });
  assert.equal(listed.ok, true);
  assert.deepEqual(
    listed.items.map((item) => item.id),
    ['verge-1', 'verge-2', 'ap-1', 'ars-1', 'headline-1']
  );
  assert.equal(listed.items[0].readability_state, 'readable');
  assert.equal(listed.items[4].readability_state, 'headline_only');
});

test('dashboard store excludes unavailable items from default listings and starter feeds omit FT', async () => {
  const tempDir = makeTempDir();
  const store = createDashboardStore({
    userDataPath: tempDir,
    getEmbeddingConfig: () => ({}),
  });

  const now = Date.now();
  const statePath = path.join(tempDir, 'dashboard_state.json');
  fs.writeFileSync(statePath, JSON.stringify({
    version: 3,
    events: [],
    tasks: [],
    filters: { selected_topic: 'all' },
    rss: {
      last_refreshed_at: now,
      items: [{
        id: 'unavailable-story',
        url: 'https://example.com/unavailable-story',
        title: 'Unavailable story',
        display_title: 'Unavailable story',
        source_name: 'Example Feed',
        source_domain: 'example.com',
        topic: 'world',
        source_topic: 'world',
        published_at: now,
        fetched_at: now,
        fetch_status: 'rss_only',
        summary_status: 'unavailable',
        failure_reason: 'refetch_empty_article_body',
      }],
    },
  }, null, 2), 'utf8');

  const listed = await store.listFeedItems({ topic: 'all', limit: 20, query: '' });
  assert.equal(listed.ok, true);
  assert.equal(listed.items.length, 0);

  const fetched = store.getFeedItem('unavailable-story');
  assert.equal(fetched.ok, true);
  assert.equal(fetched.item.readability_state, 'unavailable');

  const state = store.getState();
  assert.equal(state.ok, true);
  const sourceIds = (state.state.rss.sources || []).map((item) => item.id);
  assert.equal(sourceIds.includes('ft-world'), false);
  assert.equal(sourceIds.includes('ft-companies'), false);
});

test('dashboard store keeps first unavailable result retrievable but hidden from listings', async () => {
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
  assert.equal(listed.items.length, 0);

  const fetched = store.getFeedItem('feed_unavailable_first');
  assert.equal(fetched.ok, true);
  assert.equal(fetched.item.summary_status, 'unavailable');
  assert.equal(fetched.item.readability_state, 'unavailable');
});

test('dashboard store uses RSS category metadata before semantic fallback', async () => {
  const tempDir = makeTempDir();
  const now = Date.now();
  const originalFetch = global.fetch;
  global.fetch = async (url) => {
    const target = String(url || '');
    if (target === 'https://apnews.com/hub/ap-top-news/rss.xml') {
      return new Response(`<?xml version="1.0" encoding="UTF-8"?>
        <rss version="2.0">
          <channel>
            <item>
              <title>Congress returns to Washington</title>
              <link>https://example.com/politics-story</link>
              <pubDate>${new Date(now).toUTCString()}</pubDate>
              <description>Top news summary.</description>
              <category>Politics</category>
              <dc:subject>Congress, Government</dc:subject>
            </item>
          </channel>
        </rss>`, { status: 200 });
    }
    return new Response('<?xml version="1.0" encoding="UTF-8"?><rss version="2.0"><channel></channel></rss>', { status: 200 });
  };

  try {
    const allSourceIds = [
      'reuters-politics',
      'reuters-world',
      'reuters-business',
      'reuters-tech',
      'ap-politics',
      'verge',
      'ars',
    ];
    const store = createDashboardStore({
      userDataPath: tempDir,
      getEmbeddingConfig: () => ({}),
      getFeedSettings: () => ({ rss_disabled_source_ids: allSourceIds }),
      fetchArticlePreview: async () => ({
        ok: true,
        title: 'Congress returns to Washington',
        text: 'Lawmakers are back in Washington for a new session.',
        markdown: 'Lawmakers are back in Washington for a new session.',
        fetch_status: 'fetched',
      }),
      summarizeArticle: async () => ({
        ok: true,
        status: 'generated',
        summary: 'A politics story.',
        excerpt: 'A politics story.',
        entities: [],
        content_quality: 'clean',
        model_id: 'test-model',
        generated_at: now,
        analysis_source: 'llm',
      }),
    });

    const refreshed = await store.refreshFeeds({ force: true, topic: 'all', limit: 20 });
    assert.equal(refreshed.ok, true);
    assert.equal(refreshed.items.length, 1);
    assert.equal(refreshed.items[0].source_topic, 'other');
    assert.equal(refreshed.items[0].topic, 'politics');
    assert.equal(refreshed.items[0].topic_source, 'rss_tag');
    assert.deepEqual(refreshed.items[0].rss_topics, ['politics']);
    assert.ok(Array.isArray(refreshed.items[0].rss_tags));
    assert.ok(refreshed.items[0].rss_tags.includes('Politics'));
  } finally {
    global.fetch = originalFetch;
  }
});

test('dashboard store hides removed-source cached entries from default feed listings', async () => {
  const tempDir = makeTempDir();
  const store = createDashboardStore({
    userDataPath: tempDir,
    getEmbeddingConfig: () => ({}),
  });

  const now = Date.now();
  const statePath = path.join(tempDir, 'dashboard_state.json');
  fs.writeFileSync(statePath, JSON.stringify({
    version: 3,
    events: [],
    tasks: [],
    filters: { selected_topic: 'all' },
    rss: {
      last_refreshed_at: now,
      items: [{
        id: 'removed-source-1',
        url: 'https://removed.example.com/story',
        canonical_article_url: '',
        title: 'Removed source story',
        display_title: 'Removed source story',
        source_id: 'google-tech',
        source_name: 'Removed Source',
        source_domain: 'removed.example.com',
        source_kind: 'aggregator',
        topic: 'tech',
        source_topic: 'tech',
        published_at: now,
        fetched_at: now,
        fetch_status: 'rss_only',
      }],
    },
  }, null, 2), 'utf8');

  const listed = await store.listFeedItems({ topic: 'all', limit: 20, query: '' });
  assert.equal(listed.ok, true);
  assert.equal(listed.items.length, 0);
});

test('dashboard store can filter topic tabs by source topic instead of classified topic', async () => {
  const tempDir = makeTempDir();
  const now = Date.now();
  const statePath = path.join(tempDir, 'dashboard_state.json');
  fs.writeFileSync(statePath, JSON.stringify({
    version: 3,
    events: [],
    tasks: [],
    filters: { selected_topic: 'all' },
    rss: {
      last_refreshed_at: now,
      items: [{
        id: 'feed_topic_1',
        url: 'https://example.com/doge-lawsuit',
        canonical_article_url: 'https://example.com/doge-lawsuit',
        title: 'A tech story that looks political',
        display_title: 'A tech story that looks political',
        source_id: 'ars',
        source_name: 'Ars Technica',
        source_domain: 'arstechnica.com',
        source_kind: 'publisher',
        topic: 'politics',
        topic_source: 'embedding',
        source_topic: 'tech',
        published_at: now,
        fetched_at: now,
        content_fetched_at: now,
        fetch_status: 'fetched',
        raw_content_text: 'This story mentions DOGE and Congress but is still a tech publication story.',
        clean_summary: 'A tech publication covered a political angle.',
      }],
    },
  }, null, 2), 'utf8');

  const strictStore = createDashboardStore({
    userDataPath: tempDir,
    getEmbeddingConfig: () => ({}),
    getFeedSettings: () => ({ rss_strict_source_topics: true }),
  });
  const strictListed = await strictStore.listFeedItems({ topic: 'politics', limit: 20, query: '' });
  assert.equal(strictListed.ok, true);
  assert.equal(strictListed.items.length, 0);

  const looseStore = createDashboardStore({
    userDataPath: tempDir,
    getEmbeddingConfig: () => ({}),
    getFeedSettings: () => ({ rss_strict_source_topics: false }),
  });
  const looseListed = await looseStore.listFeedItems({ topic: 'politics', limit: 20, query: '' });
  assert.equal(looseListed.ok, true);
  assert.equal(looseListed.items.length, 1);
  assert.equal(looseListed.items[0].id, 'feed_topic_1');
});

test('dashboard store hides disabled RSS sources from state and listings', async () => {
  const tempDir = makeTempDir();
  const now = Date.now();
  const store = createDashboardStore({
    userDataPath: tempDir,
    getEmbeddingConfig: () => ({}),
    getFeedSettings: () => ({ rss_disabled_source_ids: ['ap-top'] }),
  });

  const statePath = path.join(tempDir, 'dashboard_state.json');
  fs.writeFileSync(statePath, JSON.stringify({
    version: 3,
    events: [],
    tasks: [],
    filters: { selected_topic: 'all' },
    rss: {
      last_refreshed_at: now,
      items: [{
        id: 'feed_ap_1',
        url: 'https://apnews.com/article/example-story',
        canonical_article_url: 'https://apnews.com/article/example-story',
        title: 'AP top story',
        display_title: 'AP top story',
        source_id: 'ap-top',
        source_name: 'AP Top News',
        source_domain: 'apnews.com',
        source_kind: 'publisher',
        aggregator_resolved: true,
        topic: 'other',
        source_topic: 'other',
        published_at: now,
        fetched_at: now,
        content_fetched_at: now,
        fetch_status: 'fetched',
        raw_content_text: 'Resolved article body.',
        clean_summary: 'Resolved article summary.',
      }],
    },
  }, null, 2), 'utf8');

  const stateRes = store.getState();
  assert.equal(stateRes.ok, true);
  const apTopSource = stateRes.state.rss.sources.find((item) => String((item && item.id) || '') === 'ap-top');
  assert.equal(!!apTopSource, true);
  assert.equal(apTopSource.enabled, false);
  assert.equal(stateRes.state.rss.topic_labels.all, 'All');
  assert.equal(stateRes.state.rss.topic_labels.econ, 'Business');
  assert.equal(stateRes.state.rss.topic_labels.tech, 'Technology');

  const listed = await store.listFeedItems({ topic: 'all', limit: 20, query: '' });
  assert.equal(listed.ok, true);
  assert.equal(listed.items.length, 0);
  assert.equal(listed.topic_labels.other, undefined);
});
