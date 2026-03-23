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
  assert.equal(result.evidence_mode, 'web_only');
  assert.equal(result.note_revision, 4);
  assert.equal(result.explicit_urls.length, 1);
  assert.equal(result.explicit_urls[0].canonical_url, 'https://example.com/openai-gpt5');
  assert.equal(result.claims.length, 1);
  assert.equal(result.claims[0].claim_text, 'OpenAI released GPT-5 in 2025.');
  assert.ok(searchQueries.some((query) => query.toLowerCase().includes('openai')));
  assert.ok(searchQueries.some((query) => query.toLowerCase().includes('official announcement')));
  assert.ok(fetchedUrls.includes('https://example.com/openai-gpt5'));
  assert.ok(result.sources.length >= 1);
  assert.ok(result.sources.every((source) => source.source_kind !== 'local_evidence'));
  assert.ok(result.passages.length >= 1);
  assert.ok(result.citations.length >= 1);
  assert.ok(['supported', 'mostly_supported', 'contradicted', 'mixed', 'weak_evidence'].includes(result.claims[0].status));
  assert.ok(Number(result.claims[0].top_score || 0) > 0);
  assert.ok(Number(result.claims[0].truth_confidence || 0) > 0);
  assert.ok(Number(result.note_score || 0) >= 0);
  assert.ok(['clean', 'needs_review', 'high_contradiction_risk'].includes(String(result.risk_level || '')));
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

test('note analysis ignores markdown headings while keeping factual body lines', async () => {
  const engine = createNoteAnalysisEngine({
    webSearch: async () => ({
      results: [{
        title: 'Reference',
        url: 'https://example.com/reference',
        snippet: 'OpenAI released GPT-5 in 2025 according to the company announcement.',
      }],
    }),
    fetchUrl: async () => ({
      ok: true,
      title: 'Reference',
      markdown: 'OpenAI released GPT-5 in 2025 according to the company announcement.',
    }),
    temporalGraphScorer: makeTemporalScorer(),
    makeId: (() => {
      let counter = 0;
      return (prefix) => `${prefix}_${++counter}`;
    })(),
  });

  const result = await engine.analyze({
    id: 'note_heading_filter',
    analysis_revision: 1,
    body_markdown: [
      '## How it started',
      '',
      'OpenAI released GPT-5 in 2025.',
    ].join('\n'),
  }, {});

  assert.equal(result.ok, true);
  assert.equal(result.claims.length, 1);
  assert.equal(result.claims[0].claim_text, 'OpenAI released GPT-5 in 2025.');
});

test('note analysis uses orthogonal queries and snippet fallback when fetch fails', async () => {
  const seenQueries = [];
  const fetchedUrls = [];
  const engine = createNoteAnalysisEngine({
    webSearch: async ({ query }) => {
      seenQueries.push(query);
      const normalized = String(query || '').toLowerCase();
      if (normalized.includes('release date')) {
        return {
          results: [{
            title: 'AcmeAI announcement',
            url: 'https://example.com/acme-release',
            snippet: 'AcmeAI released Model-Z in early 2025 in its official announcement.',
          }],
        };
      }
      if (normalized.includes('enterprise adoption') || normalized.includes('adoption data')) {
        return {
          results: [{
            title: 'Enterprise model adoption report',
            url: 'https://example.com/acme-adoption',
            snippet: 'AcmeAI Model-Z enterprise adoption outpaced Baseline-4 in early enterprise rollouts, according to a market report.',
          }],
        };
      }
      return { results: [] };
    },
    fetchUrl: async (url) => {
      fetchedUrls.push(url);
      return { ok: false };
    },
    temporalGraphScorer: makeTemporalScorer(),
    makeId: (() => {
      let counter = 0;
      return (prefix) => `${prefix}_${++counter}`;
    })(),
  });

  const result = await engine.analyze({
    id: 'note_fallback',
    analysis_revision: 3,
    body_markdown: 'AcmeAI released Model-Z in early 2025 and its enterprise adoption grew faster than Baseline-4.',
  }, {});

  assert.equal(result.ok, true);
  assert.ok(seenQueries.some((query) => {
    const value = query.toLowerCase();
    return value.includes('official announcement') || value.includes('release date');
  }));
  assert.ok(seenQueries.some((query) => {
    const value = query.toLowerCase();
    return value.includes('acmeai') && value.includes('enterprise adoption');
  }));
  assert.ok(fetchedUrls.includes('https://example.com/acme-release'));
  assert.ok(fetchedUrls.includes('https://example.com/acme-adoption'));
  assert.ok(result.passages.some((passage) => String(passage.passage_text || '').toLowerCase().includes('official announcement')));
  assert.ok(result.passages.some((passage) => String(passage.passage_text || '').toLowerCase().includes('model-z enterprise adoption outpaced')));
  assert.ok(result.citations.length >= 2);
});

test('lowercase current-event conflict claims are still treated as factual claims', async () => {
  const seenQueries = [];
  const engine = createNoteAnalysisEngine({
    webSearch: async ({ query }) => {
      seenQueries.push(query);
      return {
        results: [{
          title: 'Reuters conflict report',
          url: 'https://example.com/conflict-report',
          snippet: 'Iran and the US are not in a declared war, but conflict reports discuss the latest military confrontation and strikes.',
        }],
      };
    },
    fetchUrl: async () => ({ ok: false }),
    temporalGraphScorer: makeTemporalScorer(),
    makeId: (() => {
      let counter = 0;
      return (prefix) => `${prefix}_${++counter}`;
    })(),
  });

  const result = await engine.analyze({
    id: 'note_conflict',
    analysis_revision: 5,
    body_markdown: 'iran and US is fighting a war now.',
  }, {});

  assert.equal(result.ok, true);
  assert.equal(result.claims.length, 1);
  assert.match(result.claims[0].claim_text, /iran and US/i);
  assert.ok(seenQueries.some((query) => query.toLowerCase().includes('iran') && query.toLowerCase().includes('us')));
  assert.ok(result.sources.length >= 1);
  assert.ok(result.passages.length >= 1);
});

test('pipeline distinguishes false, partial, and correct claims with deterministic fixtures', async () => {
  const engine = createNoteAnalysisEngine({
    webSearch: async ({ query }) => {
      const normalized = String(query || '').toLowerCase();
      if (normalized.includes('novaai acquired nasa')) {
        if (normalized.includes('rumor') || normalized.includes('not released') || normalized.includes('criticism') || normalized.includes('false') || normalized.includes('denied')) {
          return {
            results: [{
              title: 'No acquisition announcement',
              url: 'https://example.com/novaai-nasa-denial',
              snippet: 'There is no official record that NovaAI acquired NASA.',
            }],
          };
        }
        return { results: [] };
      }
      if (normalized.includes('iran') && normalized.includes('conflict')) {
        return {
          results: [{
            title: 'Reuters conflict report',
            url: 'https://example.com/iran-us-conflict',
            snippet: 'Iran and the US exchanged strikes and military threats, but reports stop short of calling it a declared war.',
          }],
        };
      }
      if (normalized.includes('novaai') && normalized.includes('atlas-7') && normalized.includes('enterprise team')) {
        return {
          results: [{
            title: 'Launch coverage',
            url: 'https://example.com/novaai-atlas7-enterprise',
            snippet: 'NovaAI released Atlas-7 in 2025 and began an enterprise rollout, but reports do not say every team received access immediately.',
          }],
        };
      }
      if (normalized.includes('novaai') && normalized.includes('atlas-7')) {
        if (normalized.includes('official')) {
          return {
            results: [{
              title: 'NovaAI official announcement',
              url: 'https://example.com/novaai-atlas7-official',
              snippet: 'NovaAI released Atlas-7 in 2025 according to its official announcement.',
            }],
          };
        }
        return {
          results: [{
            title: 'NovaAI released Atlas-7 in 2025',
            url: 'https://example.com/novaai-atlas7-news',
            snippet: 'NovaAI released Atlas-7 in 2025 and documented the launch publicly.',
          }],
        };
      }
      return { results: [] };
    },
    fetchUrl: async (url) => {
      if (url === 'https://example.com/novaai-nasa-denial') {
        return {
          ok: true,
          title: 'No acquisition announcement',
          markdown: 'There is no official record that NovaAI acquired NASA. NASA remains a US government agency.',
        };
      }
      if (url === 'https://example.com/iran-us-conflict') {
        return {
          ok: true,
          title: 'Reuters conflict report',
          markdown: 'Iran and the US exchanged strikes and military threats in the latest confrontation, but Reuters did not describe it as a declared war.',
        };
      }
      if (url === 'https://example.com/novaai-atlas7-enterprise') {
        return {
          ok: true,
          title: 'Launch coverage',
          markdown: 'NovaAI released Atlas-7 in 2025 for enterprise customers, but the rollout happened in stages rather than reaching every team at once.',
        };
      }
      if (url === 'https://example.com/novaai-atlas7-official') {
        return {
          ok: true,
          title: 'NovaAI official announcement',
          markdown: 'NovaAI released Atlas-7 in 2025 according to the company announcement.',
        };
      }
      if (url === 'https://example.com/novaai-atlas7-news') {
        return {
          ok: true,
          title: 'Launch coverage',
          markdown: 'Independent coverage confirmed that NovaAI released Atlas-7 in 2025 and described the rollout timeline.',
        };
      }
      return { ok: false };
    },
    temporalGraphScorer: makeTemporalScorer(),
    makeId: (() => {
      let counter = 0;
      return (prefix) => `${prefix}_${++counter}`;
    })(),
  });

  const falseResult = await engine.analyze({
    id: 'note_false',
    analysis_revision: 1,
    body_markdown: 'NovaAI acquired NASA in 2025.',
  }, {});
  const partialResult = await engine.analyze({
    id: 'note_partial',
    analysis_revision: 1,
    body_markdown: 'NovaAI released Atlas-7 to every enterprise team in 2025.',
  }, {});
  const correctResult = await engine.analyze({
    id: 'note_correct',
    analysis_revision: 1,
    body_markdown: 'NovaAI released Atlas-7 in 2025.',
  }, {});

  assert.equal(falseResult.claims.length, 1);
  assert.equal(falseResult.claims[0].status, 'contradicted');
  assert.equal(partialResult.claims.length, 1);
  assert.equal(partialResult.claims[0].status, 'mixed');
  assert.equal(correctResult.claims.length, 1);
  assert.equal(correctResult.claims[0].status, 'supported');
  assert.ok(Array.isArray(falseResult.claims[0].rewrite_suggestions));
  assert.ok(Array.isArray(correctResult.claims[0].rewrite_suggestions));
});
