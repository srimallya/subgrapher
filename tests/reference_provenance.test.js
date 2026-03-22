const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('fs');
const path = require('path');
const vm = require('vm');

function loadForkHelpers() {
  const code = fs.readFileSync(path.join(__dirname, '..', 'main.js'), 'utf8');
  const startMarker = 'function normalizeReferenceForkKind(';
  const endMarker = 'function sanitizeHighlightEntry(';
  const start = code.indexOf(startMarker);
  const end = code.indexOf(endMarker, start);
  if (start < 0 || end < 0) throw new Error('Unable to locate fork provenance helpers in main.js');
  const bundle = code.slice(start, end);
  const sandbox = {
    cloneValue: (value) => JSON.parse(JSON.stringify(value)),
    nowTs: () => 1_710_000_000_000,
    makeReferenceUid: (() => {
      let count = 0;
      return () => `refuid_${++count}`;
    })(),
    makeId: (() => {
      let count = 0;
      return (prefix) => `${prefix}_${++count}`;
    })(),
    createWebTab: (tab = {}) => ({ id: `tab_${String(tab.id || 'seed')}`, ...tab }),
    createArtifact: (artifact = {}) => ({ id: `artifact_${String(artifact.id || 'seed')}`, ...artifact }),
    normalizeReferenceAgentMeta: () => null,
    sanitizeReferenceColorTag: (value) => String(value || ''),
    sanitizeSkillDescriptor: (value) => value,
    memoryDefaultState: () => ({}),
  };
  vm.createContext(sandbox);
  vm.runInContext(bundle, sandbox);
  return sandbox;
}

test('createForkReference preserves imported provenance from parent by default', () => {
  const { createForkReference } = loadForkHelpers();
  const parent = {
    id: 'sr_parent',
    title: 'Imported Parent',
    intent: 'Keep provenance intact',
    color_tag: 'c2',
    tags: ['history'],
    tabs: [{ id: 'tabA', title: 'One tab' }],
    artifacts: [{ id: 'artA', title: 'Research Draft', type: 'markdown', content: 'hello' }],
    mail_threads: [],
    context_files: [],
    folder_mounts: [],
    youtube_transcripts: {},
    reference_graph: { nodes: [], edges: [] },
    agent_weights: {},
    decision_trace: [],
    program: '',
    skills: [],
    highlights: [],
    public_manifest: null,
    manifest_summary: null,
    source_peer_id: 'peer_x',
    source_peer_name: 'X',
    source_metadata: {
      imported_from_snapshot_id: 'snap_origin',
      imported_from_reference_uid: 'upstream_ref',
      source_node: 'X',
    },
    reference_uid: 'ref_local_import',
    lineage_id: 'lineage_upstream',
    parent_reference_uid: 'upstream_ref',
    origin_snapshot_id: 'snap_origin',
    last_synced_snapshot_id: 'snap_latest',
  };

  const child = createForkReference(parent, {});

  assert.equal(child.source_peer_id, 'PEER_X');
  assert.equal(child.source_peer_name, 'X');
  assert.deepEqual(child.source_metadata, parent.source_metadata);
  assert.notEqual(child.source_metadata, parent.source_metadata);
  assert.equal(child.origin_snapshot_id, 'snap_origin');
  assert.equal(child.last_synced_snapshot_id, 'snap_latest');
  assert.equal(child.parent_reference_uid, 'ref_local_import');
  assert.equal(child.lineage_id, 'lineage_upstream');
  assert.equal(child.fork_kind, 'local_fork');
});

test('createForkReference still respects explicit provenance overrides', () => {
  const { createForkReference } = loadForkHelpers();
  const parent = {
    id: 'sr_parent',
    title: 'Imported Parent',
    intent: '',
    tags: [],
    tabs: [{ id: 'tabA', title: 'One tab' }],
    artifacts: [{ id: 'artA', title: 'Research Draft', type: 'markdown', content: 'hello' }],
    mail_threads: [],
    context_files: [],
    folder_mounts: [],
    youtube_transcripts: {},
    reference_graph: { nodes: [], edges: [] },
    agent_weights: {},
    decision_trace: [],
    program: '',
    skills: [],
    highlights: [],
    source_peer_id: 'peer_x',
    source_peer_name: 'X',
    source_metadata: { imported_from_snapshot_id: 'snap_origin' },
    reference_uid: 'ref_local_import',
    lineage_id: 'lineage_upstream',
    origin_snapshot_id: 'snap_origin',
    last_synced_snapshot_id: 'snap_latest',
  };

  const child = createForkReference(parent, {
    source_peer_id: 'peer_y',
    source_peer_name: 'Y',
    source_metadata: { imported_from_snapshot_id: 'snap_override' },
    origin_snapshot_id: 'snap_override',
    last_synced_snapshot_id: 'snap_override_latest',
  });

  assert.equal(child.source_peer_id, 'PEER_Y');
  assert.equal(child.source_peer_name, 'Y');
  assert.deepEqual(child.source_metadata, { imported_from_snapshot_id: 'snap_override' });
  assert.equal(child.origin_snapshot_id, 'snap_override');
  assert.equal(child.last_synced_snapshot_id, 'snap_override_latest');
});
