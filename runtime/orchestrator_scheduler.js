function nowTs() {
  return Date.now();
}

function createOrchestratorScheduler(options = {}) {
  const jobsStore = options.jobsStore;
  const runJob = typeof options.runJob === 'function' ? options.runJob : null;
  const logger = options.logger || console;
  const tickMs = Math.max(1_000, Number(options.tickMs || 10_000));

  if (!jobsStore || typeof jobsStore.listJobs !== 'function' || typeof jobsStore.markRun !== 'function' || !runJob) {
    throw new Error('createOrchestratorScheduler requires jobsStore and runJob.');
  }

  let timer = null;
  let active = false;
  let lastTickAt = 0;
  let runningJobIds = new Set();

  async function tick() {
    lastTickAt = nowTs();
    let jobs = [];
    try {
      const listRes = jobsStore.listJobs({ include_deleted: false });
      jobs = Array.isArray(listRes && listRes.jobs) ? listRes.jobs : [];
    } catch (err) {
      logger.warn('[scheduler] list jobs failed:', String((err && err.message) || err));
      return;
    }

    const now = nowTs();
    for (const job of jobs) {
      const jobId = String((job && job.id) || '').trim();
      if (!jobId) continue;
      if (runningJobIds.has(jobId)) continue;
      if (!jobsStore.shouldRunJob(job, now)) continue;

      runningJobIds.add(jobId);
      try {
        const runRes = await runJob(job);
        jobsStore.markRun(jobId, {
          ran_at: nowTs(),
          success: !!(runRes && runRes.ok),
          error: String((runRes && runRes.error) || (runRes && runRes.message) || ''),
        });
      } catch (err) {
        jobsStore.markRun(jobId, {
          ran_at: nowTs(),
          success: false,
          error: String((err && err.message) || 'scheduler_job_failed'),
        });
      } finally {
        runningJobIds.delete(jobId);
      }
    }
  }

  function start() {
    if (active) return { ok: true, running: true };
    active = true;
    timer = setInterval(() => {
      tick().catch((err) => {
        logger.warn('[scheduler] tick failed:', String((err && err.message) || err));
      });
    }, tickMs);
    return { ok: true, running: true };
  }

  function stop() {
    active = false;
    if (timer) {
      clearInterval(timer);
      timer = null;
    }
    runningJobIds = new Set();
    return { ok: true, running: false };
  }

  function status() {
    return {
      ok: true,
      running: active,
      tick_ms: tickMs,
      last_tick_at: lastTickAt,
      in_flight_jobs: Array.from(runningJobIds),
    };
  }

  return {
    start,
    stop,
    status,
    tick,
  };
}

module.exports = {
  createOrchestratorScheduler,
};
