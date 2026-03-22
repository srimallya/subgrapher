const fs = require('fs');
const os = require('os');
const path = require('path');

const { createNotesStore } = require('../runtime/notes_store');
const { createNoteAnalysisEngine } = require('../runtime/note_analysis');

function makeTemporalScorer() {
  return async (input = {}) => {
    const rows = Array.isArray(input.rows) ? input.rows : [];
    const keys = Array.from(new Set(rows.map((row) => String(row.source_key || row.src || '')).filter(Boolean)));
    return {
      ok: true,
      scores: keys.map((sourceKey, index) => ({
        source_key: sourceKey,
        recent_7d_score: 0.25 + (index * 0.1),
        global_score: 0.25 + (index * 0.1),
      })),
    };
  };
}

function makeFixtureEngine() {
  return createNoteAnalysisEngine({
    webSearch: async ({ query }) => {
      const normalized = String(query || '').toLowerCase();
      if (normalized.includes('novaai acquired nasa')) {
        if (normalized.includes('rumor') || normalized.includes('criticism')) {
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
      if (normalized.includes('iran') && (normalized.includes('conflict') || normalized.includes('latest'))) {
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
            title: 'Launch coverage',
            url: 'https://example.com/novaai-atlas7-news',
            snippet: 'Independent coverage confirmed that NovaAI released Atlas-7 in 2025 and described the rollout timeline.',
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
}

async function runCase(store, engine, testCase) {
  const createRes = await store.createNote({
    title: testCase.name,
    body_markdown: testCase.body,
    active_mode: 'edit',
  });
  if (!createRes || !createRes.ok || !createRes.note) {
    throw new Error(`Unable to create note for ${testCase.name}`);
  }
  const note = createRes.note;
  const analysisRes = await engine.analyze(note, {});
  if (!analysisRes || analysisRes.ok === false) {
    throw new Error(`Analysis failed for ${testCase.name}: ${(analysisRes && analysisRes.message) || 'unknown error'}`);
  }
  const saveRes = await store.saveAnalysis(note.id, analysisRes);
  if (!saveRes || !saveRes.ok) {
    throw new Error(`Unable to persist analysis for ${testCase.name}: ${(saveRes && saveRes.message) || 'unknown error'}`);
  }
  const stored = await store.getAnalysis(note.id);
  if (!stored || !stored.ok) {
    throw new Error(`Unable to reload analysis for ${testCase.name}`);
  }
  const claim = Array.isArray(stored.claims) ? stored.claims[0] : null;
  const citationsRes = claim ? await store.getCitations(note.id, claim.id) : { ok: true, citations: [] };
  return {
    note,
    analysis: stored,
    claim,
    citations: Array.isArray(citationsRes && citationsRes.citations) ? citationsRes.citations : [],
  };
}

async function main() {
  const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), 'subgrapher-note-pipeline-'));
  const store = createNotesStore({ userDataPath: tempDir });
  const engine = makeFixtureEngine();
  const cases = [
    {
      name: 'False claim',
      body: 'NovaAI acquired NASA in 2025.',
      expectedStatus: 'no_evidence',
    },
    {
      name: 'Partial claim',
      body: 'NovaAI released Atlas-7 to every enterprise team in 2025.',
      expectedStatus: 'uncertain',
    },
    {
      name: 'Correct claim',
      body: 'NovaAI released Atlas-7 in 2025.',
      expectedStatus: 'supported',
    },
  ];

  let hasFailure = false;
  for (const testCase of cases) {
    const result = await runCase(store, engine, testCase);
    const claim = result.claim;
    const actualStatus = String((claim && claim.status) || 'missing');
    const confidence = Math.round(Number((claim && (claim.highlight_score || claim.top_score)) || 0) * 100);
    const summary = [
      `[${testCase.name}]`,
      `expected=${testCase.expectedStatus}`,
      `actual=${actualStatus}`,
      `claims=${Array.isArray(result.analysis.claims) ? result.analysis.claims.length : 0}`,
      `citations=${result.citations.length}`,
      `confidence=${confidence}%`,
    ].join(' ');
    console.log(summary);
    if (claim) {
      console.log(`  claim: ${claim.claim_text}`);
    }
    if (result.citations[0]) {
      console.log(`  top citation: ${result.citations[0].source.title} (${result.citations[0].support_label})`);
    }
    if (actualStatus !== testCase.expectedStatus) {
      hasFailure = true;
      console.error(`  status mismatch for ${testCase.name}`);
    }
  }

  if (hasFailure) {
    process.exitCode = 1;
    return;
  }
  console.log('Pipeline check passed.');
}

main().catch((err) => {
  console.error(err && err.stack ? err.stack : String(err));
  process.exitCode = 1;
});
