const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('fs');
const os = require('os');
const path = require('path');

const {
  searchLocalEvidence,
  expandLocalEvidenceGraph,
  getLocalEvidenceRagStatus,
  reindexLocalEvidenceReference,
  __private,
} = require('../runtime/local_evidence_search');
const { getRagIndexPath, readReferenceGraphSidecar } = require('../runtime/rag_index');

function makeTempDir() {
  return fs.mkdtempSync(path.join(os.tmpdir(), 'subgrapher-graph-test-'));
}

function writeTextFile(dir, name, content) {
  const filePath = path.join(dir, name);
  fs.writeFileSync(filePath, content, 'utf8');
  return filePath;
}

function makeStubGraphScorer() {
  let calls = 0;
  const fn = async (input = {}) => {
    calls += 1;
    const rows = Array.isArray(input.rows) ? input.rows : [];
    const keys = Array.from(new Set(rows.flatMap((row) => [String(row.src || '').trim(), String(row.dst || '').trim()]).filter(Boolean))).sort();
    return {
      ok: true,
      scores: keys.map((sourceKey, idx) => ({
        source_key: sourceKey,
        global_score: 0.2 + (idx * 0.1),
        recent_30d_score: 0.1 + (idx * 0.05),
        recent_7d_score: 0.05 + (idx * 0.03),
        top_neighbors: [],
        computed_at: Number(input.now_ts || 0),
      })),
    };
  };
  fn.getCalls = () => calls;
  return fn;
}

function buildFixtureRefs(tempDir, now) {
  const contextPath = writeTextFile(tempDir, 'fixture-context.txt', 'Gamma ledger notes and epsilon rollout details.');
  return [
    {
      id: 'root-ref',
      title: 'Root Reference',
      updated_at: now,
      artifacts: [
        {
          id: 'artifact-a',
          title: 'Gamma Consensus Memo',
          content: 'Gamma beta roadmap and temporal graph notes for the main evidence trail.',
          updated_at: now - 5_000,
          content_hash: 'artifact-a-hash',
        },
      ],
      highlights: [
        {
          id: 'highlight-web-root',
          source: 'web',
          url: 'https://example.com/spec',
          text: 'Gamma protocol update and beta scheduling notes.',
          updated_at: now - 7_000,
        },
      ],
      context_files: [
        {
          id: 'context-a',
          original_name: 'fixture-context.txt',
          relative_path: 'fixture-context.txt',
          summary: 'Gamma ledger and epsilon rollout summary.',
          stored_path: contextPath,
          mime_type: 'text/plain',
          updated_at: now - 6_000,
          content_hash: 'context-a-hash',
          size_bytes: 128,
        },
      ],
    },
    {
      id: 'child-ref',
      title: 'Child Reference',
      parent_id: 'root-ref',
      updated_at: now - 1_000,
      artifacts: [
        {
          id: 'artifact-b',
          title: 'Beta Followup',
          content: 'Beta gamma execution details and epsilon dependencies.',
          updated_at: now - 4_000,
          content_hash: 'artifact-b-hash',
        },
      ],
      highlights: [
        {
          id: 'highlight-web-child',
          source: 'web',
          url: 'https://example.com/spec',
          text: 'Repeated gamma mention from the same URL.',
          updated_at: now - 3_000,
        },
      ],
      context_files: [],
    },
  ];
}

test('buildScopedSourceGraph is deterministic, excludes self loops, and collapses same URL highlights', () => {
  const tempDir = makeTempDir();
  const refs = buildFixtureRefs(tempDir, 1_710_000_000_000);
  const graph = __private.buildScopedSourceGraph(refs);

  const nodeKeys = graph.nodes.map((node) => node.source_key);
  assert.deepEqual(nodeKeys, [
    'artifact:child-ref:artifact-b',
    'artifact:root-ref:artifact-a',
    'context_file:root-ref:context-a',
    'url:https://example.com/spec',
  ]);
  assert.equal(graph.nodes.find((node) => node.source_key === 'url:https://example.com/spec').scope_reference_ids.length, 2);
  assert.ok(graph.edges.length > 0);
  assert.ok(graph.edges.every((edge) => edge.src_key !== edge.dst_key));
  assert.ok(graph.edges.every((edge) => Array.isArray(edge.shared_terms) && edge.shared_terms.length > 0));
});

test('graph timestamps use source updated_at and fall back to parent reference updated_at', () => {
  const now = 1_710_000_010_000;
  const tempDir = makeTempDir();
  const contextPath = writeTextFile(tempDir, 'fallback.txt', 'Fallback context content.');
  const refs = [{
    id: 'fallback-ref',
    title: 'Fallback Reference',
    updated_at: now,
    artifacts: [
      { id: 'artifact-fallback', title: 'Fallback Artifact', content: 'Temporal fallback content.', content_hash: 'x' },
    ],
    highlights: [
      { id: 'highlight-fallback', source: 'web', url: 'https://example.com/fallback', text: 'Fallback web note.' },
    ],
    context_files: [
      {
        id: 'context-fallback',
        original_name: 'fallback.txt',
        relative_path: 'fallback.txt',
        summary: 'Fallback summary.',
        stored_path: contextPath,
        mime_type: 'text/plain',
        content_hash: 'y',
      },
    ],
  }];

  const graph = __private.buildScopedSourceGraph(refs);
  for (const node of graph.nodes) {
    assert.equal(node.source_updated_at, now);
  }
});

test('reindex writes graph sidecar, reports graph status, and upgrades stale schema cleanly', async () => {
  const tempDir = makeTempDir();
  const userDataPath = path.join(tempDir, 'userdata');
  fs.mkdirSync(userDataPath, { recursive: true });
  const now = 1_710_000_020_000;
  const refs = buildFixtureRefs(tempDir, now);
  const scorer = makeStubGraphScorer();

  const dbPath = getRagIndexPath(userDataPath, 'root-ref');
  fs.mkdirSync(path.dirname(dbPath), { recursive: true });
  const initSqlJs = require('sql.js');
  const wasmPath = require.resolve('sql.js/dist/sql-wasm.wasm');
  const SQL = await initSqlJs({ locateFile: () => wasmPath });
  const db = new SQL.Database();
  db.run('CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT);');
  db.run("INSERT INTO meta (key, value) VALUES ('schema_version', '1');");
  fs.writeFileSync(dbPath, Buffer.from(db.export()));
  db.close();

  const reindexRes = await reindexLocalEvidenceReference('root-ref', refs, {
    userDataPath,
    ragEnabled: true,
    temporalGraphScorer: scorer,
  });
  assert.equal(reindexRes.ok, true);
  assert.equal(reindexRes.graph_state, 'ready');
  assert.ok(reindexRes.graph_node_count >= 4);
  assert.ok(reindexRes.graph_edge_count >= 1);
  assert.equal(scorer.getCalls(), 1);

  const status = await getLocalEvidenceRagStatus('root-ref', { userDataPath });
  assert.equal(status.ok, true);
  assert.equal(status.graph_ready, true);
  assert.ok(status.graph_node_count >= 4);
  assert.ok(status.graph_edge_count >= 1);

  const sidecar = await readReferenceGraphSidecar({ userDataPath, referenceId: 'root-ref' });
  assert.equal(sidecar.ok, true);
  assert.equal(sidecar.graph_state, 'ready');
  assert.equal(sidecar.nodes.length, reindexRes.graph_node_count);
});

test('stale scoped graph version triggers a graph rebuild on search', async () => {
  const tempDir = makeTempDir();
  const userDataPath = path.join(tempDir, 'userdata');
  const now = 1_710_000_030_000;
  const refs = buildFixtureRefs(tempDir, now);
  const scorer = makeStubGraphScorer();

  await reindexLocalEvidenceReference('root-ref', refs, {
    userDataPath,
    ragEnabled: true,
    temporalGraphScorer: scorer,
  });
  const statusBefore = await getLocalEvidenceRagStatus('root-ref', { userDataPath });
  assert.equal(scorer.getCalls(), 1);

  const nextRefs = structuredClone(refs);
  nextRefs[1].artifacts[0].content = 'Beta gamma execution details with new zeta evidence.';
  nextRefs[1].artifacts[0].updated_at = now + 10_000;
  nextRefs[1].updated_at = now + 10_000;

  await searchLocalEvidence('gamma evidence', nextRefs, {
    anchorReferenceId: 'root-ref',
    userDataPath,
    ragEnabled: true,
    temporalGraphScorer: scorer,
  });
  const statusAfter = await getLocalEvidenceRagStatus('root-ref', { userDataPath });
  assert.equal(scorer.getCalls(), 2);
  assert.notEqual(statusBefore.graph_version, statusAfter.graph_version);
});

test('expandLocalEvidenceGraph derives seeds when omitted and returns one-hop metadata', async () => {
  const tempDir = makeTempDir();
  const userDataPath = path.join(tempDir, 'userdata');
  const now = 1_710_000_040_000;
  const refs = buildFixtureRefs(tempDir, now);
  const scorer = makeStubGraphScorer();

  await reindexLocalEvidenceReference('root-ref', refs, {
    userDataPath,
    ragEnabled: true,
    temporalGraphScorer: scorer,
  });

  const res = await expandLocalEvidenceGraph('gamma roadmap', refs, {
    anchorReferenceId: 'root-ref',
    userDataPath,
    ragEnabled: true,
    temporalGraphScorer: scorer,
    top_k: 6,
  });

  assert.equal(res.ok, true);
  assert.ok(Array.isArray(res.seed_results));
  assert.ok(Array.isArray(res.expanded_results));
  assert.ok(res.seed_results.length > 0);
  const seedKeys = new Set(res.seed_results.map((item) => item.source_key));
  res.expanded_results.forEach((item) => {
    assert.equal(seedKeys.has(item.source_key), false);
    assert.ok(res.graph_signals[item.source_key]);
  });
});

test('expandLocalEvidenceGraph accepts explicit seeds and returns clean empty results for empty graphs', async () => {
  const tempDir = makeTempDir();
  const userDataPath = path.join(tempDir, 'userdata');
  const contextPath = writeTextFile(tempDir, 'solo.txt', 'Lambda context payload.');
  const refs = [{
    id: 'solo-ref',
    title: 'Solo Reference',
    updated_at: 1_710_000_050_000,
    artifacts: [{ id: 'solo-artifact', title: 'Unique Omega', content: 'omega only', updated_at: 1_710_000_049_000, content_hash: 'solo' }],
    highlights: [],
    context_files: [{
      id: 'solo-context',
      original_name: 'solo.txt',
      relative_path: 'solo.txt',
      summary: 'lambda only',
      stored_path: contextPath,
      mime_type: 'text/plain',
      updated_at: 1_710_000_048_000,
      content_hash: 'solo-context',
    }],
  }];
  const scorer = makeStubGraphScorer();

  await reindexLocalEvidenceReference('solo-ref', refs, {
    userDataPath,
    ragEnabled: true,
    temporalGraphScorer: scorer,
  });

  const res = await expandLocalEvidenceGraph('omega', refs, {
    anchorReferenceId: 'solo-ref',
    userDataPath,
    ragEnabled: true,
    temporalGraphScorer: scorer,
    seed_source_keys: ['artifact:solo-ref:solo-artifact', 'missing-source'],
  });

  assert.equal(res.ok, true);
  assert.ok(Array.isArray(res.seed_results));
  assert.equal(res.seed_results.length, 1);
  assert.deepEqual(res.expanded_results, []);
  assert.deepEqual(res.graph_signals, {});
});
