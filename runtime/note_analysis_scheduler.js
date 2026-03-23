function normalizeStage(value = '') {
  const stage = String(value || '').trim();
  if (['queued', 'detecting_claims', 'retrieving_sources', 'scoring_evidence', 'ready', 'error'].includes(stage)) {
    return stage;
  }
  return 'queued';
}

function clampRevision(value) {
  const numeric = Number(value);
  if (!Number.isFinite(numeric) || numeric < 0) return 0;
  return Math.round(numeric);
}

function createNoteAnalysisScheduler(options = {}) {
  const now = typeof options.now === 'function' ? options.now : () => Date.now();
  const setTimer = typeof options.setTimeout === 'function' ? options.setTimeout : setTimeout;
  const clearTimer = typeof options.clearTimeout === 'function' ? options.clearTimeout : clearTimeout;
  const runAnalysis = typeof options.runAnalysis === 'function' ? options.runAnalysis : (async () => ({ ok: true }));
  const jobs = new Map();

  function ensureEntry(noteId = '') {
    const targetId = String(noteId || '').trim();
    if (!targetId) return null;
    if (!jobs.has(targetId)) {
      jobs.set(targetId, {
        note_id: targetId,
        latest_scheduled_revision: 0,
        queued_revision: 0,
        running_revision: 0,
        stage: 'queued',
        started_at: 0,
        completed_at: 0,
        last_error: '',
        timer: null,
      });
    }
    return jobs.get(targetId) || null;
  }

  function snapshot(entry = null) {
    if (!entry) return null;
    return {
      note_id: String(entry.note_id || '').trim(),
      latest_scheduled_revision: clampRevision(entry.latest_scheduled_revision),
      queued_revision: clampRevision(entry.queued_revision),
      running_revision: clampRevision(entry.running_revision),
      stage: normalizeStage(entry.stage),
      started_at: Number(entry.started_at || 0) || 0,
      completed_at: Number(entry.completed_at || 0) || 0,
      last_error: String(entry.last_error || ''),
    };
  }

  function setStage(noteId = '', revision = 0, stage = 'queued') {
    const entry = ensureEntry(noteId);
    if (!entry) return null;
    const targetRevision = clampRevision(revision);
    if (targetRevision > 0 && clampRevision(entry.running_revision) !== targetRevision) {
      return snapshot(entry);
    }
    entry.stage = normalizeStage(stage);
    return snapshot(entry);
  }

  async function launch(noteId = '', revision = 0) {
    const entry = ensureEntry(noteId);
    const targetRevision = clampRevision(revision);
    if (!entry || targetRevision <= 0) return null;
    if (entry.running_revision > 0) return snapshot(entry);
    entry.timer = null;
    entry.running_revision = targetRevision;
    entry.queued_revision = targetRevision;
    entry.stage = 'detecting_claims';
    entry.started_at = now();
    entry.completed_at = 0;
    entry.last_error = '';

    let result = null;
    try {
      result = await runAnalysis(String(noteId || '').trim(), targetRevision, {
        setStage: (nextStage) => setStage(noteId, targetRevision, nextStage),
        getJob: () => snapshot(ensureEntry(noteId)),
      });
    } catch (err) {
      result = { ok: false, message: String((err && err.message) || 'Note analysis failed.') };
    }

    const active = ensureEntry(noteId);
    if (!active) return null;
    active.completed_at = now();
    active.running_revision = 0;
    const newerRevisionQueued = clampRevision(active.latest_scheduled_revision) > targetRevision;
    if (result && result.ok === false && !result.stale) {
      active.last_error = String((result && result.message) || 'Note analysis failed.');
      active.stage = newerRevisionQueued ? 'queued' : 'error';
    } else if (newerRevisionQueued || (result && result.stale)) {
      active.stage = 'queued';
      active.last_error = '';
    } else {
      active.stage = 'ready';
      active.last_error = '';
      active.queued_revision = targetRevision;
    }

    if (newerRevisionQueued) {
      schedule(noteId, active.latest_scheduled_revision, { delayMs: 0, force: true });
    }
    return snapshot(active);
  }

  function schedule(noteId = '', revision = 0, options = {}) {
    const entry = ensureEntry(noteId);
    if (!entry) return null;
    const targetRevision = clampRevision(revision || entry.latest_scheduled_revision);
    if (targetRevision <= 0) return snapshot(entry);
    if (targetRevision > entry.latest_scheduled_revision) {
      entry.latest_scheduled_revision = targetRevision;
    }
    entry.queued_revision = targetRevision;
    if (entry.running_revision > 0) {
      entry.stage = entry.running_revision === targetRevision ? normalizeStage(entry.stage || 'detecting_claims') : 'queued';
      return snapshot(entry);
    }
    const duplicatePending = !!entry.timer && clampRevision(entry.queued_revision) === targetRevision && !options.force;
    if (duplicatePending) return snapshot(entry);
    if (entry.timer) {
      clearTimer(entry.timer);
      entry.timer = null;
    }
    entry.stage = 'queued';
    entry.last_error = '';
    const delayMs = Math.max(0, Math.round(Number((options && options.delayMs) || 0) || 0));
    entry.timer = setTimer(() => {
      const active = ensureEntry(noteId);
      if (!active) return;
      active.timer = null;
      const nextRevision = clampRevision(active.queued_revision || active.latest_scheduled_revision);
      if (nextRevision <= 0 || active.running_revision > 0) return;
      void launch(noteId, nextRevision);
    }, delayMs);
    return snapshot(entry);
  }

  function getJob(noteId = '') {
    return snapshot(ensureEntry(noteId));
  }

  function clear(noteId = '') {
    const targetId = String(noteId || '').trim();
    const entry = jobs.get(targetId);
    if (!entry) return null;
    if (entry.timer) {
      clearTimer(entry.timer);
      entry.timer = null;
    }
    jobs.delete(targetId);
    return snapshot(entry);
  }

  return {
    schedule,
    getJob,
    clear,
    setStage,
  };
}

module.exports = {
  createNoteAnalysisScheduler,
  normalizeStage,
};
