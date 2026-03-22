const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('fs');
const os = require('os');
const path = require('path');

const { createNotesStore, getNotesDbPath } = require('../runtime/notes_store');

function makeTempDir() {
  return fs.mkdtempSync(path.join(os.tmpdir(), 'subgrapher-notes-store-'));
}

test('notes store supports CRUD and analysis persistence', async () => {
  const userDataPath = makeTempDir();
  const store = createNotesStore({ userDataPath });

  const createRes = await store.createNote({
    title: 'Claim Draft',
    body_markdown: 'OpenAI released GPT-5 in 2025.',
    active_mode: 'edit',
  });
  assert.equal(createRes.ok, true);
  assert.ok(createRes.note.id);
  assert.equal(createRes.note.analysis_revision, 1);

  const dbPath = getNotesDbPath(userDataPath);
  assert.equal(fs.existsSync(dbPath), true);

  const listRes = await store.listNotes();
  assert.equal(listRes.ok, true);
  assert.equal(listRes.notes.length, 1);

  const noteId = String(createRes.note.id || '').trim();
  const updateRes = await store.updateNote(noteId, {
    title: 'Claim Draft Updated',
    body_markdown: 'OpenAI released GPT-5 in 2025. Microsoft adopted it.',
  });
  assert.equal(updateRes.ok, true);
  assert.equal(updateRes.note.title, 'Claim Draft Updated');
  assert.equal(updateRes.note.analysis_revision, 2);

  const saveAnalysisRes = await store.saveAnalysis(noteId, {
    analysis_run_id: 'analysis_1',
    note_revision: updateRes.note.analysis_revision,
    extractor_version: 'test-engine',
    started_at: 1_710_000_000_000,
    completed_at: 1_710_000_000_500,
    status: 'completed',
    message: 'Evidence scan completed.',
    claims: [{
      id: 'claim_1',
      claim_index: 0,
      start_offset: 0,
      end_offset: 31,
      claim_text: 'OpenAI released GPT-5 in 2025.',
      normalized_claim_text: 'openai released gpt 5 in 2025',
      subject_text: 'OpenAI',
      predicate_text: 'released',
      object_text: 'GPT-5',
      time_text: '2025',
      modality: 'statement',
      factuality: 'factual',
      status: 'supported',
      top_score: 0.82,
      highlight_score: 0.82,
    }],
    sources: [{
      id: 'src_1',
      source_kind: 'web_search',
      source_query: 'OpenAI released GPT-5 2025',
      url: 'https://example.com/openai',
      canonical_url: 'https://example.com/openai',
      title: 'OpenAI Announcement',
      published_at: 1_710_000_000_000,
      fetched_at: 1_710_000_000_100,
      content_hash: 'hash_src_1',
    }],
    passages: [{
      id: 'passage_1',
      source_id: 'src_1',
      passage_index: 0,
      passage_text: 'OpenAI released GPT-5 in 2025 according to the company announcement.',
      passage_start: 0,
      passage_end: 68,
      fetched_at: 1_710_000_000_100,
    }],
    citations: [{
      id: 'citation_1',
      claim_id: 'claim_1',
      source_id: 'src_1',
      passage_id: 'passage_1',
      citation_index: 0,
      support_label: 'supported',
      score: 0.82,
      semantic_score: 0.88,
      lexical_score: 0.7,
      time_score: 1,
      corroboration_score: 0.4,
      temporal_score: 0.3,
      excerpt: 'OpenAI released GPT-5 in 2025 according to the company announcement.',
    }],
  });
  assert.equal(saveAnalysisRes.ok, true);
  assert.equal(saveAnalysisRes.analysis_summary.claim_count, 1);
  assert.equal(saveAnalysisRes.analysis_summary.supported_count, 1);

  const analysisRes = await store.getAnalysis(noteId);
  assert.equal(analysisRes.ok, true);
  assert.equal(analysisRes.claims.length, 1);
  assert.equal(analysisRes.claims[0].status, 'supported');

  const citationsRes = await store.getCitations(noteId, 'claim_1');
  assert.equal(citationsRes.ok, true);
  assert.equal(citationsRes.citations.length, 1);
  assert.equal(citationsRes.citations[0].source.title, 'OpenAI Announcement');

  const deleteRes = await store.deleteNote(noteId);
  assert.equal(deleteRes.ok, true);
  const finalList = await store.listNotes();
  assert.equal(finalList.notes.length, 0);
});

test('stale analysis revisions are rejected', async () => {
  const userDataPath = makeTempDir();
  const store = createNotesStore({ userDataPath });
  const createRes = await store.createNote({
    title: 'Stale Check',
    body_markdown: 'FTC sued Amazon.',
  });
  const noteId = String(createRes.note.id || '').trim();
  const updateRes = await store.updateNote(noteId, {
    body_markdown: 'The FTC sued Amazon over Prime cancellation practices.',
  });

  const staleRes = await store.saveAnalysis(noteId, {
    analysis_run_id: 'analysis_stale',
    note_revision: 1,
    claims: [],
    sources: [],
    passages: [],
    citations: [],
  });
  assert.equal(staleRes.ok, false);
  assert.equal(staleRes.stale, true);

  const freshRes = await store.saveAnalysis(noteId, {
    analysis_run_id: 'analysis_fresh',
    note_revision: updateRes.note.analysis_revision,
    claims: [],
    sources: [],
    passages: [],
    citations: [],
  });
  assert.equal(freshRes.ok, true);
});
