const test = require('node:test');
const assert = require('node:assert/strict');

const { createNoteAnalysisScheduler } = require('../runtime/note_analysis_scheduler');

function createTimerHarness() {
  let nextId = 0;
  const tasks = new Map();
  return {
    setTimeout(fn) {
      const id = ++nextId;
      tasks.set(id, fn);
      return id;
    },
    clearTimeout(id) {
      tasks.delete(id);
    },
    flush() {
      const pending = Array.from(tasks.entries());
      tasks.clear();
      pending.forEach(([, fn]) => fn());
    },
    count() {
      return tasks.size;
    },
  };
}

function deferred() {
  let resolve;
  let reject;
  const promise = new Promise((res, rej) => {
    resolve = res;
    reject = rej;
  });
  return { promise, resolve, reject };
}

async function flushMicrotasks() {
  await new Promise((resolve) => setImmediate(resolve));
}

test('note analysis scheduler coalesces duplicate queued revisions', async () => {
  const timer = createTimerHarness();
  let runCount = 0;
  const scheduler = createNoteAnalysisScheduler({
    setTimeout: (fn) => timer.setTimeout(fn),
    clearTimeout: (id) => timer.clearTimeout(id),
    runAnalysis: async () => {
      runCount += 1;
      return { ok: true };
    },
  });

  scheduler.schedule('note-1', 3, { delayMs: 10 });
  scheduler.schedule('note-1', 3, { delayMs: 10 });
  assert.equal(timer.count(), 1);
  timer.flush();
  await flushMicrotasks();

  assert.equal(runCount, 1);
  const job = scheduler.getJob('note-1');
  assert.equal(job.latest_scheduled_revision, 3);
  assert.equal(job.stage, 'ready');
});

test('note analysis scheduler queues a newer revision behind a running analysis', async () => {
  const timer = createTimerHarness();
  const firstRun = deferred();
  const secondRun = deferred();
  const calls = [];
  const scheduler = createNoteAnalysisScheduler({
    setTimeout: (fn) => timer.setTimeout(fn),
    clearTimeout: (id) => timer.clearTimeout(id),
    runAnalysis: async (_noteId, revision, controls) => {
      calls.push(revision);
      controls.setStage('retrieving_sources');
      if (revision === 1) return firstRun.promise;
      return secondRun.promise;
    },
  });

  scheduler.schedule('note-2', 1, { delayMs: 0 });
  timer.flush();
  await flushMicrotasks();
  assert.deepEqual(calls, [1]);
  assert.equal(scheduler.getJob('note-2').stage, 'retrieving_sources');

  const queuedJob = scheduler.schedule('note-2', 2, { delayMs: 0 });
  assert.equal(queuedJob.latest_scheduled_revision, 2);
  assert.equal(queuedJob.running_revision, 1);
  assert.equal(queuedJob.stage, 'queued');

  firstRun.resolve({ ok: true });
  await flushMicrotasks();
  assert.equal(timer.count(), 1);
  timer.flush();
  await flushMicrotasks();
  assert.deepEqual(calls, [1, 2]);

  secondRun.resolve({ ok: true });
  await flushMicrotasks();
  const job = scheduler.getJob('note-2');
  assert.equal(job.latest_scheduled_revision, 2);
  assert.equal(job.running_revision, 0);
  assert.equal(job.stage, 'ready');
});

test('note analysis scheduler exposes queued state immediately while analysis is running', async () => {
  const timer = createTimerHarness();
  const firstRun = deferred();
  const scheduler = createNoteAnalysisScheduler({
    setTimeout: (fn) => timer.setTimeout(fn),
    clearTimeout: (id) => timer.clearTimeout(id),
    runAnalysis: async (_noteId, revision) => {
      if (revision === 1) return firstRun.promise;
      return { ok: true };
    },
  });

  const scheduled = scheduler.schedule('note-3', 1, { delayMs: 0 });
  assert.equal(scheduled.stage, 'queued');
  timer.flush();
  await flushMicrotasks();

  const queued = scheduler.schedule('note-3', 2, { delayMs: 0 });
  assert.equal(queued.stage, 'queued');
  assert.equal(queued.running_revision, 1);
  assert.equal(queued.latest_scheduled_revision, 2);

  firstRun.resolve({ ok: true });
  await flushMicrotasks();
});
