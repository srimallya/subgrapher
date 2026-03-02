function getPathCScopedReferences(anchorSrId, refsInput = []) {
  const refs = Array.isArray(refsInput) ? refsInput : [];
  const idMap = new Map();
  refs.forEach((ref) => {
    const id = String((ref && ref.id) || '').trim();
    if (id) idMap.set(id, ref);
  });
  const anchorId = String(anchorSrId || '').trim();
  const anchor = idMap.get(anchorId);
  if (!anchor) return [];

  const allowed = new Set([anchorId]);
  const parentId = String((anchor && anchor.parent_id) || '').trim();
  if (parentId && idMap.has(parentId)) allowed.add(parentId);

  const children = Array.isArray(anchor && anchor.children) ? anchor.children : [];
  children.forEach((childId) => {
    const id = String(childId || '').trim();
    if (id && idMap.has(id)) allowed.add(id);
  });

  return refs.filter((ref) => allowed.has(String((ref && ref.id) || '').trim()));
}

function sanitizeTraceStep(step) {
  if (!step || typeof step !== 'object') return null;
  return {
    id: String(step.id || ''),
    ts: step.ts || null,
    action: String(step.action || ''),
    outcome: String(step.outcome || '').slice(0, 240),
    next_sr_ids: Array.isArray(step.next_sr_ids) ? step.next_sr_ids.slice(0, 12) : [],
    evidence_refs: Array.isArray(step.evidence_refs) ? step.evidence_refs.slice(0, 12) : [],
  };
}

function sanitizeReferenceForHarness(ref) {
  if (!ref || typeof ref !== 'object') return null;
  const tabs = Array.isArray(ref.tabs) ? ref.tabs : [];
  const decisionTrace = Array.isArray(ref.decision_trace) ? ref.decision_trace : [];
  return {
    id: String(ref.id || ''),
    title: String(ref.title || 'Untitled'),
    intent: String(ref.intent || ''),
    parent_id: ref.parent_id ? String(ref.parent_id) : null,
    children: Array.isArray(ref.children) ? ref.children.slice(0, 200).map((id) => String(id || '')).filter(Boolean) : [],
    relation_type: String(ref.relation_type || (ref.parent_id ? 'child' : 'root')),
    program: String(ref.program || ''),
    skills: Array.isArray(ref.skills)
      ? ref.skills.slice(0, 80).map((item) => ({
        id: String((item && item.id) || ''),
        scope: String((item && item.scope) || 'local'),
        name: String((item && item.name) || ''),
      }))
      : [],
    lineage: Array.isArray(ref.lineage) ? ref.lineage.slice(0, 200).map((id) => String(id || '')).filter(Boolean) : [],
    created_at: ref.created_at || null,
    updated_at: ref.updated_at || null,
    tab_count: tabs.length,
    tabs: tabs.slice(0, 8).map((tab) => ({
      id: String((tab && tab.id) || ''),
      url: String((tab && tab.url) || ''),
      title: String((tab && tab.title) || ''),
      tab_kind: String((tab && tab.tab_kind) || 'web'),
      excerpt: String((tab && tab.excerpt) || '').slice(0, 200),
    })),
    agent_weights: (ref.agent_weights && typeof ref.agent_weights === 'object') ? ref.agent_weights : {},
    decision_trace: decisionTrace.slice(-20).map(sanitizeTraceStep).filter(Boolean),
    reference_graph: (ref.reference_graph && typeof ref.reference_graph === 'object') ? ref.reference_graph : { nodes: [], edges: [] },
    context_files: Array.isArray(ref.context_files)
      ? ref.context_files.slice(0, 12).map((file) => ({
        id: String((file && file.id) || ''),
        original_name: String((file && file.original_name) || (file && file.filename) || ''),
        summary: String((file && file.summary) || '').slice(0, 320),
        mime_type: String((file && file.mime_type) || ''),
        source_type: String((file && file.source_type) || ''),
        updated_at: (file && file.updated_at) || null,
      }))
      : [],
    artifacts: Array.isArray(ref.artifacts) ? ref.artifacts.slice(0, 40) : [],
    highlights: Array.isArray(ref.highlights)
      ? ref.highlights.slice(-120).map((item) => ({
        id: String((item && item.id) || ''),
        source: String((item && item.source) || 'web'),
        url: String((item && item.url) || ''),
        artifact_id: String((item && item.artifact_id) || ''),
        text: String((item && item.text) || '').slice(0, 320),
        context_before: String((item && item.context_before) || '').slice(0, 100),
        context_after: String((item && item.context_after) || '').slice(0, 100),
        created_at: (item && item.created_at) || null,
        updated_at: (item && item.updated_at) || null,
      }))
      : [],
  };
}

function buildPathCHarnessPayload(activeRef, scopedRefs = []) {
  const active = sanitizeReferenceForHarness(activeRef);
  const refs = (Array.isArray(scopedRefs) ? scopedRefs : [])
    .map((ref) => sanitizeReferenceForHarness(ref))
    .filter(Boolean);
  return {
    active_reference: active,
    scoped_references: refs,
    scoped_count: refs.length,
  };
}

module.exports = {
  getPathCScopedReferences,
  sanitizeReferenceForHarness,
  buildPathCHarnessPayload,
};
