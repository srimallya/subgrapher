const test = require('node:test');
const assert = require('node:assert/strict');

const { createNoteAnalysisEngine } = require('../runtime/note_analysis');

function makeTemporalScorer() {
  return async (input = {}) => {
    const rows = Array.isArray(input.rows) ? input.rows : [];
    const keys = Array.from(new Set(rows.map((row) => String(row.source_key || row.src || '')).filter(Boolean)));
    return {
      ok: true,
      scores: keys.map((sourceKey, index) => ({
        source_key: sourceKey,
        recent_7d_score: 0.2 + (index * 0.1),
        global_score: 0.2 + (index * 0.1),
      })),
    };
  };
}

test('note analysis extracts URLs, finds factual claims, and ranks evidence', async () => {
  const searchQueries = [];
  const fetchedUrls = [];
  const localQueries = [];

  const engine = createNoteAnalysisEngine({
    webSearch: async ({ query }) => {
      searchQueries.push(query);
      return {
        results: [{
          title: 'OpenAI announcement',
          url: 'https://example.com/openai-gpt5',
          snippet: 'OpenAI released GPT-5 in 2025 according to the company announcement.',
        }],
      };
    },
    fetchUrl: async (url) => {
      fetchedUrls.push(url);
      return {
        ok: true,
        title: 'OpenAI announcement',
        markdown: 'OpenAI released GPT-5 in 2025 according to the company announcement. Microsoft later adopted the model in enterprise deployments.',
      };
    },
    localEvidenceSearch: async (query) => {
      localQueries.push(query);
      return {
        ok: true,
        results: [{
          reference_title: 'Local memo',
          url: 'https://local.example/memo',
          snippet: 'Internal memo repeats that OpenAI released GPT-5 in 2025.',
        }],
      };
    },
    temporalGraphScorer: makeTemporalScorer(),
    makeId: (() => {
      let counter = 0;
      return (prefix) => `${prefix}_${++counter}`;
    })(),
  });

  const note = {
    id: 'note_1',
    analysis_revision: 4,
    body_markdown: [
      'OpenAI released GPT-5 in 2025.',
      'Source: https://example.com/openai-gpt5',
      'Maybe it is better than everything else?',
    ].join('\n'),
  };

  const result = await engine.analyze(note, {
    scopedRefs: [{ id: 'ref_1', title: 'Reference' }],
    localEvidenceOptions: {},
  });

  assert.equal(result.ok, true);
  assert.equal(result.note_revision, 4);
  assert.equal(result.explicit_urls.length, 1);
  assert.equal(result.explicit_urls[0].canonical_url, 'https://example.com/openai-gpt5');
  assert.equal(result.claims.length, 1);
  assert.equal(result.claims[0].claim_text, 'OpenAI released GPT-5 in 2025.');
  assert.ok(searchQueries.some((query) => query.toLowerCase().includes('openai')));
  assert.ok(searchQueries.some((query) => query.toLowerCase().includes('official announcement')));
  assert.ok(localQueries.some((query) => query.toLowerCase().includes('released')));
  assert.ok(fetchedUrls.includes('https://example.com/openai-gpt5'));
  assert.ok(result.sources.length >= 2);
  assert.ok(result.passages.length >= 2);
  assert.ok(result.citations.length >= 1);
  assert.ok(['supported', 'uncertain', 'contested', 'no_evidence'].includes(result.claims[0].status));
  assert.ok(Number(result.claims[0].top_score || 0) > 0);
});

test('compound factual sentences are split into smaller claim spans', async () => {
  const seenQueries = [];
  const engine = createNoteAnalysisEngine({
    webSearch: async ({ query }) => {
      seenQueries.push(query);
      if (query.toLowerCase().includes('released')) {
        return {
          results: [{
            title: 'OpenAI release',
            url: 'https://example.com/release',
            snippet: 'OpenAI released GPT-5 in early 2025.',
          }],
        };
      }
      return { results: [] };
    },
    fetchUrl: async (url) => ({
      ok: true,
      title: url.includes('release') ? 'OpenAI release' : 'Other page',
      markdown: url.includes('release')
        ? 'OpenAI released GPT-5 in early 2025.'
        : 'No useful evidence here.',
    }),
    localEvidenceSearch: async () => ({ ok: true, results: [] }),
    temporalGraphScorer: makeTemporalScorer(),
    makeId: (() => {
      let counter = 0;
      return (prefix) => `${prefix}_${++counter}`;
    })(),
  });

  const result = await engine.analyze({
    id: 'note_compound',
    analysis_revision: 2,
    body_markdown: 'OpenAI released GPT-5 in early 2025 and its enterprise adoption grew faster than GPT-4.',
  }, {});

  assert.equal(result.ok, true);
  assert.equal(result.claims.length, 2);
  assert.match(result.claims[0].claim_text, /released GPT-5/i);
  assert.match(result.claims[1].claim_text, /adoption grew faster/i);
  assert.ok(seenQueries.some((query) => query.toLowerCase().includes('released')));
  assert.ok(seenQueries.some((query) => query.toLowerCase().includes('adoption')));
  assert.ok(seenQueries.some((query) => query.toLowerCase().includes('metric source')));
});

test('note analysis ignores non-factual and speculative-only notes', async () => {
  const engine = createNoteAnalysisEngine({
    webSearch: async () => ({ results: [] }),
    fetchUrl: async () => ({ ok: false }),
    localEvidenceSearch: async () => ({ ok: true, results: [] }),
    temporalGraphScorer: makeTemporalScorer(),
  });

  const result = await engine.analyze({
    id: 'note_2',
    analysis_revision: 1,
    body_markdown: 'Maybe this startup could be huge? I think it might win someday.',
  }, {});

  assert.equal(result.ok, true);
  assert.equal(result.claims.length, 0);
  assert.equal(result.sources.length, 0);
  assert.equal(result.citations.length, 0);
  assert.equal(result.message, 'No factual claims detected.');
});
