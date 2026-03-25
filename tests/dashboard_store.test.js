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

test('dashboard store resolves Google News entries to publisher article URLs before listing', async () => {
  const tempDir = makeTempDir();
  const now = Date.now();
  const originalFetch = global.fetch;
  global.fetch = async (url) => {
    const target = String(url || '');
    const emptyRss = '<?xml version="1.0" encoding="UTF-8"?><rss version="2.0"><channel></channel></rss>';
    if (target.includes('news.google.com/rss/search?q=technology')) {
      return new Response(`<?xml version="1.0" encoding="UTF-8"?>
        <rss version="2.0">
          <channel>
            <item>
              <title>Aggregator story</title>
              <link>https://news.google.com/rss/articles/CBMiFakeGoogleId?oc=5</link>
              <pubDate>${new Date(now).toUTCString()}</pubDate>
              <description>Aggregator summary</description>
              <source url="https://publisher.example.com">Publisher Example</source>
            </item>
          </channel>
        </rss>`, { status: 200 });
    }
    return new Response(emptyRss, { status: 200 });
  };

  try {
    const store = createDashboardStore({
      userDataPath: tempDir,
      getEmbeddingConfig: () => ({}),
      resolveAggregatorItems: async (items = []) => items.map((item) => (
        String(item.source_kind || '') === 'aggregator'
          ? {
            ...item,
            url: 'https://publisher.example.com/articles/story-1',
            canonical_article_url: 'https://publisher.example.com/articles/story-1',
            aggregator_resolved: true,
          }
          : item
      )),
      fetchArticlePreview: async (url) => ({
        ok: true,
        title: `Fetched ${url}`,
        text: 'Resolved publisher article body.',
        markdown: 'Resolved publisher article body.',
        fetch_status: 'fetched',
      }),
      summarizeArticle: async () => ({
        ok: true,
        status: 'generated',
        summary: 'Resolved publisher summary.',
        excerpt: 'Resolved publisher summary.',
        entities: [],
        content_quality: 'clean',
        model_id: 'test-model',
        generated_at: now,
        analysis_source: 'llm',
      }),
    });

    const refreshed = await store.refreshFeeds({ force: true, topic: 'all', limit: 20 });
    assert.equal(refreshed.ok, true);
    assert.ok(refreshed.items.some((item) => item.url === 'https://publisher.example.com/articles/story-1'));

    const item = refreshed.items.find((entry) => entry.url === 'https://publisher.example.com/articles/story-1');
    assert.equal(item.source_kind, 'aggregator');
    assert.equal(item.discovery_source_id, 'google-tech');
    assert.equal(item.aggregator_resolved, true);
    assert.equal(item.source_name, 'Publisher Example');
    assert.equal(item.readability_state, 'readable');
  } finally {
    global.fetch = originalFetch;
  }
});

test('dashboard store hides unresolved aggregator entries from default feed listings', async () => {
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
        id: 'google-unresolved-1',
        url: 'https://news.google.com/rss/articles/CBMiUnresolved?oc=5',
        canonical_article_url: '',
        title: 'Unresolved aggregator story',
        display_title: 'Unresolved aggregator story',
        source_id: 'google-tech',
        source_name: 'Publisher Example',
        source_domain: 'publisher.example.com',
        source_kind: 'aggregator',
        discovery_source_id: 'google-tech',
        discovery_source_name: 'Google News Technology',
        discovery_source_domain: 'news.google.com',
        aggregator_resolved: false,
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

test('dashboard store prefers direct publisher items over aggregator duplicates', async () => {
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
      items: [
        {
          id: 'google-dup-1',
          url: 'https://publisher.example.com/articles/story-dup',
          canonical_article_url: 'https://publisher.example.com/articles/story-dup',
          title: 'Duplicate story',
          display_title: 'Duplicate story',
          source_id: 'google-tech',
          source_name: 'Publisher Example',
          source_domain: 'publisher.example.com',
          source_kind: 'aggregator',
          discovery_source_id: 'google-tech',
          discovery_source_name: 'Google News Technology',
          discovery_source_domain: 'news.google.com',
          aggregator_resolved: true,
          topic: 'tech',
          source_topic: 'tech',
          published_at: now - 1000,
          fetched_at: now - 1000,
          fetch_status: 'fetched',
          content_fetched_at: now - 1000,
          has_full_content: true,
          raw_content_text: 'Aggregator body',
          clean_summary: 'Aggregator summary',
        },
        {
          id: 'publisher-dup-1',
          url: 'https://publisher.example.com/articles/story-dup',
          canonical_article_url: 'https://publisher.example.com/articles/story-dup',
          title: 'Duplicate story',
          display_title: 'Duplicate story',
          source_id: 'reuters-tech',
          source_name: 'Reuters Technology',
          source_domain: 'publisher.example.com',
          source_kind: 'publisher',
          aggregator_resolved: true,
          topic: 'tech',
          source_topic: 'tech',
          published_at: now - 2000,
          fetched_at: now - 2000,
          fetch_status: 'fetched',
          content_fetched_at: now - 2000,
          has_full_content: true,
          raw_content_text: 'Publisher body',
          clean_summary: 'Publisher summary',
        },
      ],
    },
  }, null, 2), 'utf8');

  const listed = await store.listFeedItems({ topic: 'all', limit: 20, query: '' });
  assert.equal(listed.ok, true);
  assert.equal(listed.items.length, 1);
  assert.equal(listed.items[0].source_kind, 'publisher');
  assert.equal(listed.items[0].source_id, 'reuters-tech');
  assert.equal(listed.items[0].discovery_source_id, '');
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
    getFeedSettings: () => ({ rss_disabled_source_ids: ['google-tech'] }),
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
        id: 'feed_google_1',
        url: 'https://www.theverge.com/example-story',
        canonical_article_url: 'https://www.theverge.com/example-story',
        title: 'Resolved Google story',
        display_title: 'Resolved Google story',
        source_id: 'google-tech',
        source_name: 'The Verge',
        source_domain: 'theverge.com',
        source_kind: 'aggregator',
        discovery_source_id: 'google-tech',
        discovery_source_name: 'Google News Technology',
        discovery_source_domain: 'news.google.com',
        aggregator_resolved: true,
        topic: 'tech',
        source_topic: 'tech',
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
  const googleSource = stateRes.state.rss.sources.find((item) => String((item && item.id) || '') === 'google-tech');
  assert.equal(!!googleSource, true);
  assert.equal(googleSource.enabled, false);

  const listed = await store.listFeedItems({ topic: 'all', limit: 20, query: '' });
  assert.equal(listed.ok, true);
  assert.equal(listed.items.length, 0);
});
