const fs = require('fs');
const path = require('path');

function nowTs() {
  return Date.now();
}

function clone(value) {
  return JSON.parse(JSON.stringify(value));
}

function makeId(prefix = 'job') {
  const rand = Math.random().toString(36).slice(2, 10);
  return `${prefix}_${nowTs()}_${rand}`;
}

function asInt(value, fallback) {
  const n = Number(value);
  return Number.isFinite(n) ? Math.round(n) : fallback;
}

function normalizeScheduleType(value) {
  const s = String(value || 'daily').trim().toLowerCase();
  if (s === 'once' || s === 'daily' || s === 'weekly') return s;
  return 'daily';
}

function normalizeDay(value) {
  const v = String(value || '').trim().toLowerCase();
  const allowed = ['monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday'];
  return allowed.includes(v) ? v : '';
}

function normalizeTime(value) {
  const raw = String(value || '09:00').trim();
  const m = raw.match(/^(\d{1,2}):(\d{2})$/);
  if (!m) return '09:00';
  const hh = Math.max(0, Math.min(23, Number(m[1] || 0)));
  const mm = Math.max(0, Math.min(59, Number(m[2] || 0)));
  return `${String(hh).padStart(2, '0')}:${String(mm).padStart(2, '0')}`;
}

function normalizeTz(value) {
  const tz = String(value || '').trim();
  if (!tz) return Intl.DateTimeFormat().resolvedOptions().timeZone || 'UTC';
  return tz;
}

function getZonedParts(ts, timeZone) {
  const fmt = new Intl.DateTimeFormat('en-US', {
    timeZone,
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
    weekday: 'long',
    hour12: false,
  });
  const parts = fmt.formatToParts(new Date(ts));
  const map = {};
  parts.forEach((p) => {
    if (p && p.type && p.type !== 'literal') map[p.type] = p.value;
  });
  return {
    year: Number(map.year || 0),
    month: Number(map.month || 0),
    day: Number(map.day || 0),
    hour: Number(map.hour || 0),
    minute: Number(map.minute || 0),
    second: Number(map.second || 0),
    weekday: String(map.weekday || '').toLowerCase(),
    iso_date: `${map.year || '0000'}-${map.month || '00'}-${map.day || '00'}`,
  };
}

function minuteKey(ts, tz) {
  const p = getZonedParts(ts, tz);
  return `${p.iso_date}T${String(p.hour).padStart(2, '0')}:${String(p.minute).padStart(2, '0')}`;
}

function scheduleMatchesAt(job, ts) {
  const tz = normalizeTz(job.timezone);
  const parts = getZonedParts(ts, tz);
  const [hh, mm] = normalizeTime(job.time).split(':').map((v) => Number(v || 0));
  if (parts.hour !== hh || parts.minute !== mm) return false;

  const type = normalizeScheduleType(job.schedule_type);
  if (type === 'daily') return true;
  if (type === 'weekly') {
    const day = normalizeDay(job.day || '');
    if (!day) return false;
    return parts.weekday === day;
  }
  if (type === 'once') {
    const targetDate = String(job.once_date || '').trim();
    if (!targetDate) return false;
    return parts.iso_date === targetDate;
  }
  return false;
}

function computeNextRunAt(job, fromTs = nowTs()) {
  const start = Math.max(0, Number(fromTs || nowTs()));
  const maxMinutes = 14 * 24 * 60;
  for (let i = 1; i <= maxMinutes; i += 1) {
    const ts = start + (i * 60 * 1000);
    if (scheduleMatchesAt(job, ts)) {
      return ts;
    }
  }
  return 0;
}

function shouldRunJob(job, ts = nowTs()) {
  const item = (job && typeof job === 'object') ? job : {};
  const status = String(item.status || 'active').trim().toLowerCase();
  if (status !== 'active') return false;
  if (!scheduleMatchesAt(item, ts)) return false;

  const tz = normalizeTz(item.timezone);
  const nowMinute = minuteKey(ts, tz);
  const lastRunAt = Number(item.last_run_at || 0);
  if (lastRunAt > 0) {
    const lastMinute = minuteKey(lastRunAt, tz);
    if (lastMinute === nowMinute) return false;
  }
  return true;
}

function createOrchestratorJobsStore(options = {}) {
  const userDataPath = String(options.userDataPath || '').trim();
  const filePath = path.join(userDataPath || process.cwd(), 'orchestrator_jobs.json');

  function readState() {
    try {
      if (!fs.existsSync(filePath)) return { version: 1, jobs: [] };
      const parsed = JSON.parse(fs.readFileSync(filePath, 'utf8'));
      return {
        version: 1,
        jobs: Array.isArray(parsed && parsed.jobs) ? parsed.jobs : [],
      };
    } catch (_) {
      return { version: 1, jobs: [] };
    }
  }

  function writeState(state) {
    const next = {
      version: 1,
      jobs: Array.isArray(state && state.jobs) ? state.jobs : [],
    };
    fs.writeFileSync(filePath, JSON.stringify(next, null, 2), 'utf8');
  }

  function normalizeJob(input = {}, existing = null) {
    const base = (existing && typeof existing === 'object') ? existing : {};
    const src = (input && typeof input === 'object') ? input : {};
    const scheduleType = normalizeScheduleType(Object.prototype.hasOwnProperty.call(src, 'schedule_type') ? src.schedule_type : base.schedule_type);
    const tz = normalizeTz(Object.prototype.hasOwnProperty.call(src, 'timezone') ? src.timezone : base.timezone);
    const out = {
      id: String(base.id || src.id || makeId('job')).trim(),
      name: String(Object.prototype.hasOwnProperty.call(src, 'name') ? src.name : (base.name || 'Untitled Job')).trim().slice(0, 120) || 'Untitled Job',
      task: String(Object.prototype.hasOwnProperty.call(src, 'task') ? src.task : (base.task || '')).trim(),
      schedule_type: scheduleType,
      time: normalizeTime(Object.prototype.hasOwnProperty.call(src, 'time') ? src.time : base.time),
      day: scheduleType === 'weekly'
        ? normalizeDay(Object.prototype.hasOwnProperty.call(src, 'day') ? src.day : base.day)
        : '',
      once_date: scheduleType === 'once'
        ? String(Object.prototype.hasOwnProperty.call(src, 'once_date') ? src.once_date : base.once_date).trim()
        : '',
      timezone: tz,
      status: String(Object.prototype.hasOwnProperty.call(src, 'status') ? src.status : (base.status || 'active')).trim().toLowerCase() === 'paused'
        ? 'paused'
        : (String(Object.prototype.hasOwnProperty.call(src, 'status') ? src.status : (base.status || 'active')).trim().toLowerCase() === 'deleted' ? 'deleted' : 'active'),
      enabled: Object.prototype.hasOwnProperty.call(src, 'enabled')
        ? !!src.enabled
        : String(Object.prototype.hasOwnProperty.call(src, 'status') ? src.status : (base.status || 'active')).trim().toLowerCase() === 'active',
      notify_telegram: Object.prototype.hasOwnProperty.call(src, 'notify_telegram')
        ? !!src.notify_telegram
        : !!base.notify_telegram,
      created_by_chat_id: String(Object.prototype.hasOwnProperty.call(src, 'created_by_chat_id') ? src.created_by_chat_id : (base.created_by_chat_id || '')).trim(),
      run_count: asInt(Object.prototype.hasOwnProperty.call(src, 'run_count') ? src.run_count : base.run_count, Number(base.run_count || 0)),
      last_run_at: asInt(Object.prototype.hasOwnProperty.call(src, 'last_run_at') ? src.last_run_at : base.last_run_at, Number(base.last_run_at || 0)),
      last_error: String(Object.prototype.hasOwnProperty.call(src, 'last_error') ? src.last_error : (base.last_error || '')).trim(),
      next_run_at: asInt(Object.prototype.hasOwnProperty.call(src, 'next_run_at') ? src.next_run_at : base.next_run_at, Number(base.next_run_at || 0)),
      created_at: asInt(base.created_at || nowTs(), nowTs()),
      updated_at: nowTs(),
    };

    if (!out.next_run_at || out.status !== 'active') {
      out.next_run_at = out.status === 'active' ? computeNextRunAt(out, nowTs()) : 0;
    }
    out.enabled = out.status === 'active';
    return out;
  }

  function listJobs(optionsIn = {}) {
    const opts = (optionsIn && typeof optionsIn === 'object') ? optionsIn : {};
    const includeDeleted = !!opts.include_deleted;
    const chatId = String(opts.created_by_chat_id || '').trim();
    const state = readState();
    let jobs = Array.isArray(state.jobs) ? state.jobs : [];
    if (!includeDeleted) {
      jobs = jobs.filter((job) => String((job && job.status) || 'active') !== 'deleted');
    }
    if (chatId) {
      jobs = jobs.filter((job) => String((job && job.created_by_chat_id) || '').trim() === chatId);
    }
    jobs = jobs.sort((a, b) => Number((a && a.next_run_at) || 0) - Number((b && b.next_run_at) || 0));
    return { ok: true, jobs: clone(jobs) };
  }

  function getJob(jobId) {
    const id = String(jobId || '').trim();
    if (!id) return { ok: false, message: 'job_id is required.' };
    const state = readState();
    const found = (Array.isArray(state.jobs) ? state.jobs : []).find((job) => String((job && job.id) || '') === id);
    if (!found) return { ok: false, message: 'Job not found.' };
    return { ok: true, job: clone(found) };
  }

  function createJob(payload = {}) {
    const state = readState();
    const job = normalizeJob(payload, null);
    if (!job.task) return { ok: false, message: 'task is required.' };
    if (job.schedule_type === 'weekly' && !job.day) return { ok: false, message: 'day is required for weekly schedule.' };
    if (job.schedule_type === 'once' && !job.once_date) return { ok: false, message: 'once_date is required for once schedule.' };
    state.jobs = Array.isArray(state.jobs) ? state.jobs : [];
    state.jobs.push(job);
    writeState(state);
    return { ok: true, job: clone(job) };
  }

  function editJob(jobId, patch = {}) {
    const id = String(jobId || '').trim();
    if (!id) return { ok: false, message: 'job_id is required.' };
    const state = readState();
    const jobs = Array.isArray(state.jobs) ? state.jobs : [];
    const idx = jobs.findIndex((job) => String((job && job.id) || '') === id);
    if (idx < 0) return { ok: false, message: 'Job not found.' };
    const next = normalizeJob(patch, jobs[idx]);
    next.id = id;
    if (!next.task) return { ok: false, message: 'task is required.' };
    jobs[idx] = next;
    state.jobs = jobs;
    writeState(state);
    return { ok: true, job: clone(next) };
  }

  function setStatus(jobId, status) {
    const id = String(jobId || '').trim();
    const nextStatus = String(status || '').trim().toLowerCase();
    if (!id) return { ok: false, message: 'job_id is required.' };
    if (!['active', 'paused', 'deleted'].includes(nextStatus)) return { ok: false, message: 'Invalid status.' };
    const state = readState();
    const jobs = Array.isArray(state.jobs) ? state.jobs : [];
    const idx = jobs.findIndex((job) => String((job && job.id) || '') === id);
    if (idx < 0) return { ok: false, message: 'Job not found.' };
    const job = normalizeJob({ ...jobs[idx], status: nextStatus }, jobs[idx]);
    if (nextStatus !== 'active') job.next_run_at = 0;
    else job.next_run_at = computeNextRunAt(job, nowTs());
    jobs[idx] = job;
    state.jobs = jobs;
    writeState(state);
    return { ok: true, job: clone(job) };
  }

  function pauseJob(jobId) {
    return setStatus(jobId, 'paused');
  }

  function resumeJob(jobId) {
    return setStatus(jobId, 'active');
  }

  function deleteJob(jobId) {
    return setStatus(jobId, 'deleted');
  }

  function markRun(jobId, payload = {}) {
    const id = String(jobId || '').trim();
    if (!id) return { ok: false, message: 'job_id is required.' };
    const state = readState();
    const jobs = Array.isArray(state.jobs) ? state.jobs : [];
    const idx = jobs.findIndex((job) => String((job && job.id) || '') === id);
    if (idx < 0) return { ok: false, message: 'Job not found.' };
    const job = { ...jobs[idx] };
    const ranAt = Number(payload.ran_at || nowTs());
    const success = !!payload.success;
    job.last_run_at = ranAt;
    job.run_count = Number(job.run_count || 0) + 1;
    job.last_error = success ? '' : String(payload.error || 'unknown_error');
    if (job.schedule_type === 'once') {
      job.status = 'paused';
      job.next_run_at = 0;
    } else if (job.status === 'active') {
      job.next_run_at = computeNextRunAt(job, ranAt + 1000);
    } else {
      job.next_run_at = 0;
    }
    job.updated_at = nowTs();
    jobs[idx] = job;
    state.jobs = jobs;
    writeState(state);
    return { ok: true, job: clone(job) };
  }

  return {
    filePath,
    listJobs,
    getJob,
    createJob,
    editJob,
    pauseJob,
    resumeJob,
    deleteJob,
    markRun,
    shouldRunJob,
    computeNextRunAt,
    getZonedParts,
  };
}

module.exports = {
  createOrchestratorJobsStore,
  shouldRunJob,
  computeNextRunAt,
};
