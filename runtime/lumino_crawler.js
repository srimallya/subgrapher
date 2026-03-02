const fs = require('fs');
const path = require('path');
const { EventEmitter } = require('events');

function nowTs() {
  return Date.now();
}

function makeId(prefix) {
  return `${prefix}_${nowTs()}_${Math.random().toString(36).slice(2, 10)}`;
}

function normalizeUrl(raw) {
  const input = String(raw || '').trim();
  if (!input) return '';
  try {
    return new URL(input).toString();
  } catch (_) {
    if (/^[a-zA-Z0-9-]+\.[a-zA-Z]{2,}/.test(input)) {
      try {
        return new URL(`https://${input}`).toString();
      } catch (_) {
        return '';
      }
    }
    return '';
  }
}

function extractLinks(baseUrl, bodyText) {
  const links = [];
  const seen = new Set();
  const raw = String(bodyText || '');
  const hrefRegex = /href\s*=\s*["']([^"'#]+)["']/gi;
  let match;
  while ((match = hrefRegex.exec(raw)) !== null) {
    const candidate = String(match[1] || '').trim();
    if (!candidate) continue;
    try {
      const resolved = new URL(candidate, baseUrl).toString();
      if (!/^https?:\/\//i.test(resolved)) continue;
      if (seen.has(resolved)) continue;
      seen.add(resolved);
      links.push(resolved);
    } catch (_) {
      // noop
    }
  }
  return links;
}

function htmlToText(html) {
  const raw = String(html || '');
  return raw
    .replace(/<script[\s\S]*?<\/script>/gi, ' ')
    .replace(/<style[\s\S]*?<\/style>/gi, ' ')
    .replace(/<[^>]+>/g, ' ')
    .replace(/&nbsp;/g, ' ')
    .replace(/&amp;/g, '&')
    .replace(/\s+/g, ' ')
    .trim();
}

class LuminoCrawler extends EventEmitter {
  constructor(options = {}) {
    super();
    this.logger = options.logger || console;
    this.defaultDepth = Number.isFinite(Number(options.defaultDepth))
      ? Math.max(1, Math.min(6, Math.round(Number(options.defaultDepth))))
      : 3;
    this.defaultPageCap = Number.isFinite(Number(options.defaultPageCap))
      ? Math.max(5, Math.min(300, Math.round(Number(options.defaultPageCap))))
      : 80;
    this.markdownFirst = options.markdownFirst !== false;
    this.jobs = new Map();
  }

  getStatus(jobId = '') {
    const clean = String(jobId || '').trim();
    if (clean) {
      const job = this.jobs.get(clean);
      if (!job) return { ok: false, message: 'Crawler job not found.' };
      return { ok: true, job: this._publicJob(job) };
    }
    return {
      ok: true,
      jobs: Array.from(this.jobs.values()).map((job) => this._publicJob(job)),
    };
  }

  _publicJob(job) {
    return {
      id: String(job.id || ''),
      sr_id: String(job.sr_id || ''),
      mode: String(job.mode || 'broad'),
      state: String(job.state || 'queued'),
      started_at: Number(job.started_at || 0),
      updated_at: Number(job.updated_at || 0),
      finished_at: Number(job.finished_at || 0),
      depth: Number(job.depth || 0),
      page_cap: Number(job.page_cap || 0),
      visited_count: Number(job.visited_count || 0),
      enqueued_count: Number(job.enqueued_count || 0),
      error: String(job.error || ''),
      seed_url: String(job.seed_url || ''),
      source_path: String(job.source_path || ''),
      result_count: Number((job.result_pages && job.result_pages.length) || 0),
    };
  }

  async start(payload = {}) {
    const mode = String(payload.mode || 'broad').trim().toLowerCase() === 'safe' ? 'safe' : 'broad';
    const sourceType = String(payload.source_type || 'web').trim().toLowerCase() === 'local' ? 'local' : 'web';
    const job = {
      id: makeId('crawl'),
      sr_id: String(payload.sr_id || payload.srId || '').trim(),
      mode,
      source_type: sourceType,
      seed_url: sourceType === 'web' ? normalizeUrl(payload.url || payload.seed_url || '') : '',
      source_path: sourceType === 'local' ? path.resolve(String(payload.absolute_path || payload.source_path || '.')) : '',
      state: 'running',
      started_at: nowTs(),
      updated_at: nowTs(),
      finished_at: 0,
      depth: Number.isFinite(Number(payload.depth))
        ? Math.max(1, Math.min(6, Math.round(Number(payload.depth))))
        : this.defaultDepth,
      page_cap: Number.isFinite(Number(payload.page_cap))
        ? Math.max(5, Math.min(300, Math.round(Number(payload.page_cap))))
        : this.defaultPageCap,
      markdown_first: Object.prototype.hasOwnProperty.call(payload, 'markdown_first')
        ? !!payload.markdown_first
        : this.markdownFirst,
      robots_policy: String(payload.robots_policy || 'respect').trim().toLowerCase() === 'ignore' ? 'ignore' : 'respect',
      visited_count: 0,
      enqueued_count: 0,
      error: '',
      abort: false,
      result_pages: [],
    };

    if (job.source_type === 'web' && !job.seed_url) {
      return { ok: false, message: 'A valid crawl URL is required.' };
    }
    if (job.source_type === 'local' && (!job.source_path || !fs.existsSync(job.source_path))) {
      return { ok: false, message: 'A valid local source path is required.' };
    }

    this.jobs.set(job.id, job);
    this.emit('job_update', { phase: 'started', job: this._publicJob(job) });
    if (job.source_type === 'local') {
      this._runLocal(job).catch(() => {});
    } else {
      this._runWeb(job).catch(() => {});
    }
    return { ok: true, job: this._publicJob(job) };
  }

  stop(jobId) {
    const id = String(jobId || '').trim();
    if (!id) return { ok: false, message: 'job_id is required.' };
    const job = this.jobs.get(id);
    if (!job) return { ok: false, message: 'Crawler job not found.' };
    if (job.state === 'completed' || job.state === 'failed' || job.state === 'stopped') {
      return { ok: true, job: this._publicJob(job) };
    }
    job.abort = true;
    job.state = 'stopping';
    job.updated_at = nowTs();
    this.emit('job_update', { phase: 'stopping', job: this._publicJob(job) });
    return { ok: true, job: this._publicJob(job) };
  }

  async _runLocal(job) {
    try {
      const rootPath = job.source_path;
      const files = [];
      const walk = (dir, depth = 0) => {
        if (job.abort) return;
        if (depth > job.depth) return;
        let entries = [];
        try {
          entries = fs.readdirSync(dir, { withFileTypes: true });
        } catch (_) {
          return;
        }
        entries.forEach((entry) => {
          if (job.abort) return;
          const full = path.join(dir, entry.name);
          if (entry.isDirectory()) {
            walk(full, depth + 1);
            return;
          }
          if (!entry.isFile()) return;
          files.push(full);
        });
      };
      walk(rootPath, 0);
      files.slice(0, job.page_cap).forEach((filePath) => {
        if (job.abort) return;
        let text = '';
        try {
          text = fs.readFileSync(filePath, 'utf8');
        } catch (_) {
          return;
        }
        const relative = path.relative(rootPath, filePath);
        job.result_pages.push({
          url: `file://${filePath}`,
          title: path.basename(filePath),
          markdown: text,
          text,
          links: [],
          source_type: 'crawler_local',
          relative_path: relative,
        });
      });
      job.visited_count = job.result_pages.length;
      job.enqueued_count = job.result_pages.length;
      if (job.abort) {
        job.state = 'stopped';
      } else {
        job.state = 'completed';
      }
      job.updated_at = nowTs();
      job.finished_at = nowTs();
      this.emit('job_update', { phase: job.state, job: this._publicJob(job), result_pages: job.result_pages });
    } catch (err) {
      job.state = 'failed';
      job.error = String((err && err.message) || 'Local crawl failed.');
      job.updated_at = nowTs();
      job.finished_at = nowTs();
      this.emit('job_update', { phase: 'failed', job: this._publicJob(job) });
    }
  }

  async _runWeb(job) {
    const queue = [{ url: job.seed_url, depth: 0 }];
    const visited = new Set();
    const enqueued = new Set([job.seed_url]);
    const fetchHeaders = {
      'user-agent': 'Subgrapher-Lumino-Crawler/1.0',
      accept: job.markdown_first ? 'text/markdown, text/html;q=0.9, */*;q=0.1' : 'text/html, */*;q=0.1',
    };

    while (queue.length > 0 && visited.size < job.page_cap) {
      if (job.abort) break;
      const current = queue.shift();
      if (!current) break;
      const url = String((current && current.url) || '').trim();
      const depth = Number((current && current.depth) || 0);
      if (!url || visited.has(url)) continue;
      visited.add(url);
      job.visited_count = visited.size;
      job.updated_at = nowTs();

      try {
        const res = await fetch(url, {
          method: 'GET',
          headers: fetchHeaders,
        });
        if (!res.ok) continue;
        const body = await res.text();
        const contentType = String(res.headers.get('content-type') || '').toLowerCase();
        const markdown = contentType.includes('text/markdown') ? body : '';
        const text = markdown || htmlToText(body);
        if (text) {
          job.result_pages.push({
            url,
            title: url,
            markdown: markdown || '',
            text: text.slice(0, 20000),
            links: [],
            source_type: 'crawler_web',
          });
        }
        if (depth < job.depth) {
          const links = extractLinks(url, body);
          links.forEach((link) => {
            if (visited.has(link) || enqueued.has(link)) return;
            if (queue.length + visited.size >= job.page_cap * 3) return;
            enqueued.add(link);
            queue.push({ url: link, depth: depth + 1 });
          });
          const page = job.result_pages[job.result_pages.length - 1];
          if (page) page.links = links.slice(0, 80);
        }
        job.enqueued_count = enqueued.size;
        this.emit('job_update', { phase: 'progress', job: this._publicJob(job) });
      } catch (_) {
        // continue crawling other URLs
      }
    }

    if (job.abort) {
      job.state = 'stopped';
    } else {
      job.state = 'completed';
    }
    job.updated_at = nowTs();
    job.finished_at = nowTs();
    this.emit('job_update', { phase: job.state, job: this._publicJob(job), result_pages: job.result_pages });
  }
}

module.exports = {
  LuminoCrawler,
};
