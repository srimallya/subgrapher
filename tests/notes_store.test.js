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
      claim_type: 'date_time',
      status: 'supported',
      truth_confidence: 0.82,
      support_confidence: 0.82,
      contradict_confidence: 0.08,
      corroboration: 0.6,
      authority: 0.9,
      freshness: 0.6,
      explanation: 'Multiple web sources support this wording.',
      rewrite_suggestions: [{
        key: 'attribute',
        label: 'Add attribution',
        description: 'Tie the wording to the strongest source.',
        replacement: 'OpenAI released GPT-5 in 2025, according to OpenAI Announcement.',
      }],
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
      support_label: 'support',
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
  assert.equal(analysisRes.claims[0].truth_confidence, 0.82);
  assert.equal(analysisRes.claims[0].support_confidence, 0.82);
  assert.equal(analysisRes.claims[0].contradict_confidence, 0.08);
  assert.equal(analysisRes.claims[0].rewrite_suggestions.length, 1);
  assert.equal(analysisRes.analysis_summary.evidence_mode, 'web_only');
  assert.equal(analysisRes.claims[0].claim_type, 'date_time');
  assert.ok(analysisRes.note_score > 0);
  assert.ok(analysisRes.coverage_score > 0);
  assert.ok(['clean', 'needs_review', 'high_contradiction_risk'].includes(analysisRes.risk_level));
  assert.equal(Array.isArray(analysisRes.regions), true);
  assert.equal(Array.isArray(analysisRes.evidence_feed), true);
  assert.equal(analysisRes.regions.length, 1);
  assert.equal(analysisRes.evidence_feed.length, 1);
  assert.equal(analysisRes.evidence_feed[0].region_text, 'OpenAI released GPT-5 in 2025.');
  assert.equal(analysisRes.evidence_feed[0].support_items.length, 1);

  const citationsRes = await store.getCitations(noteId, 'claim_1');
  assert.equal(citationsRes.ok, true);
  assert.equal(citationsRes.citations.length, 1);
  assert.equal(citationsRes.citations[0].source.title, 'OpenAI Announcement');
  assert.equal(citationsRes.citations[0].search_intent, 'support');
  assert.equal(citationsRes.citations[0].stance, 'support');
  assert.equal(citationsRes.citations[0].provenance_label, 'from web search');
  assert.match(citationsRes.citations[0].relevance_reason, /support/i);

  const evidenceFeedRes = await store.getEvidenceFeed(noteId);
  assert.equal(evidenceFeedRes.ok, true);
  assert.equal(evidenceFeedRes.evidence_feed.length, 1);
  assert.equal(evidenceFeedRes.evidence_feed[0].region_status, 'supported');

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

test('notes citations filter out local evidence rows and keep web provenance', async () => {
  const userDataPath = makeTempDir();
  const store = createNotesStore({ userDataPath });
  const createRes = await store.createNote({
    title: 'Web Only',
    body_markdown: 'OpenAI released GPT-5 in 2025.',
  });
  const noteId = String(createRes.note.id || '').trim();

  const saveAnalysisRes = await store.saveAnalysis(noteId, {
    analysis_run_id: 'analysis_web_only',
    note_revision: createRes.note.analysis_revision,
    extractor_version: 'note-analyzer-v3',
    claims: [{
      id: 'claim_web_only',
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
      claim_type: 'date_time',
      status: 'mixed',
      truth_confidence: 0.58,
      support_confidence: 0.58,
      contradict_confidence: 0.31,
      top_score: 0.58,
      highlight_score: 0.58,
    }],
    sources: [{
      id: 'src_web',
      source_kind: 'web_search',
      source_query: 'OpenAI released GPT-5 2025',
      url: 'https://example.com/web',
      canonical_url: 'https://example.com/web',
      title: 'Web Source',
    }, {
      id: 'src_local',
      source_kind: 'local_evidence',
      source_query: 'OpenAI released GPT-5 2025',
      url: 'https://example.com/local',
      canonical_url: 'https://example.com/local',
      title: 'Local Source',
    }],
    passages: [{
      id: 'passage_web',
      source_id: 'src_web',
      passage_index: 0,
      passage_text: 'OpenAI released GPT-5 in 2025.',
      passage_start: 0,
      passage_end: 31,
    }, {
      id: 'passage_local',
      source_id: 'src_local',
      passage_index: 1,
      passage_text: 'Local memo says OpenAI released GPT-5 in 2025.',
      passage_start: 0,
      passage_end: 47,
    }],
    citations: [{
      id: 'citation_web',
      claim_id: 'claim_web_only',
      source_id: 'src_web',
      passage_id: 'passage_web',
      citation_index: 0,
      support_label: 'support',
      score: 0.58,
      semantic_score: 0.61,
      lexical_score: 0.55,
      time_score: 1,
      corroboration_score: 0.2,
      temporal_score: 0.1,
      excerpt: 'OpenAI released GPT-5 in 2025.',
    }, {
      id: 'citation_local',
      claim_id: 'claim_web_only',
      source_id: 'src_local',
      passage_id: 'passage_local',
      citation_index: 1,
      support_label: 'support',
      score: 0.9,
      semantic_score: 0.9,
      lexical_score: 0.9,
      time_score: 1,
      corroboration_score: 0.2,
      temporal_score: 0.1,
      excerpt: 'Local memo says OpenAI released GPT-5 in 2025.',
    }],
  });
  assert.equal(saveAnalysisRes.ok, true);

  const citationsRes = await store.getCitations(noteId, 'claim_web_only');
  assert.equal(citationsRes.ok, true);
  assert.equal(citationsRes.evidence_mode, 'web_only');
  assert.equal(citationsRes.citations.length, 1);
  assert.equal(citationsRes.citations[0].source.title, 'Web Source');
  assert.equal(citationsRes.citations[0].search_intent, 'support');
  assert.equal(citationsRes.citations[0].provenance_label, 'from web search');
});

test('notes citations can resolve stored claims by text and range when ids change', async () => {
  const userDataPath = makeTempDir();
  const store = createNotesStore({ userDataPath });
  const createRes = await store.createNote({
    title: 'Fallback Claim Lookup',
    body_markdown: 'OpenAI released GPT-5 in 2025.',
  });
  const noteId = String(createRes.note.id || '').trim();
  await store.saveAnalysis(noteId, {
    analysis_run_id: 'analysis_lookup',
    note_revision: createRes.note.analysis_revision,
    claims: [{
      id: 'claim_lookup',
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
      claim_type: 'date_time',
      status: 'supported',
      truth_confidence: 0.8,
      support_confidence: 0.8,
      contradict_confidence: 0.1,
      top_score: 0.8,
      highlight_score: 0.8,
    }],
    sources: [{
      id: 'src_lookup',
      source_kind: 'web_search',
      source_query: 'OpenAI released GPT-5 2025',
      url: 'https://example.com/openai',
      canonical_url: 'https://example.com/openai',
      title: 'OpenAI Announcement',
    }],
    passages: [{
      id: 'passage_lookup',
      source_id: 'src_lookup',
      passage_index: 0,
      passage_text: 'OpenAI released GPT-5 in 2025.',
      passage_start: 0,
      passage_end: 31,
    }],
    citations: [{
      id: 'citation_lookup',
      claim_id: 'claim_lookup',
      source_id: 'src_lookup',
      passage_id: 'passage_lookup',
      citation_index: 0,
      support_label: 'support',
      score: 0.8,
      semantic_score: 0.8,
      lexical_score: 0.8,
      time_score: 1,
      corroboration_score: 0.2,
      temporal_score: 0.1,
      excerpt: 'OpenAI released GPT-5 in 2025.',
    }],
  });

  const citationsRes = await store.getCitations(noteId, 'missing_claim_id', {
    claim_text: 'OpenAI released GPT-5 in 2025.',
    start_offset: 0,
    end_offset: 31,
  });
  assert.equal(citationsRes.ok, true);
  assert.equal(citationsRes.claim.id, 'claim_lookup');
  assert.equal(citationsRes.citations.length, 1);
});
