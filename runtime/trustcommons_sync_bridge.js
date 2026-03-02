const http = require('http');
const crypto = require('crypto');
const { EventEmitter } = require('events');

const MAX_BODY_BYTES = 5 * 1024 * 1024;
const DEFAULT_SYNC_INTERVAL_MS = 8000;
const MAX_CLOCK_SKEW_MS = 2 * 60 * 1000;
const NONCE_TTL_MS = 5 * 60 * 1000;

function nowTs() {
  return Date.now();
}

function sha256Hex(input) {
  return crypto.createHash('sha256').update(input).digest('hex');
}

function hmacHex(secret, text) {
  return crypto.createHmac('sha256', secret).update(text).digest('hex');
}

function normalizeText(value) {
  return String(value || '').trim();
}

function buildSigningString({ method, path, timestamp, nonce, bodyHash }) {
  return [
    String(method || '').toUpperCase(),
    String(path || ''),
    String(timestamp || ''),
    String(nonce || ''),
    String(bodyHash || ''),
  ].join('\n');
}

function safeEqualHex(expected, incoming) {
  const a = Buffer.from(String(expected || ''), 'utf8');
  const b = Buffer.from(String(incoming || ''), 'utf8');
  if (a.length !== b.length || a.length === 0) return false;
  try {
    return crypto.timingSafeEqual(a, b);
  } catch (_) {
    return false;
  }
}

function isLoopbackHost(hostname) {
  const host = normalizeText(hostname).toLowerCase();
  return host === '127.0.0.1' || host === 'localhost' || host === '::1' || host === '[::1]';
}

function isLoopbackUrl(raw) {
  const text = normalizeText(raw);
  if (!text) return false;
  try {
    const parsed = new URL(text);
    if (String(parsed.protocol || '') !== 'http:') return false;
    return isLoopbackHost(parsed.hostname);
  } catch (_) {
    return false;
  }
}

function parseJsonBody(raw) {
  if (!raw) return {};
  try {
    const parsed = JSON.parse(raw);
    return (parsed && typeof parsed === 'object') ? parsed : {};
  } catch (_) {
    return {};
  }
}

class TrustCommonsSyncBridge extends EventEmitter {
  constructor(options = {}) {
    super();
    this.logger = options.logger || console;
    this.secretProvider = typeof options.secretProvider === 'function' ? options.secretProvider : (() => '');
    this.refsProvider = typeof options.refsProvider === 'function' ? options.refsProvider : (() => []);
    this.refsConsumer = typeof options.refsConsumer === 'function' ? options.refsConsumer : (() => ({ ok: false, message: 'refsConsumer missing' }));

    this.server = null;
    this.port = 0;
    this.enabled = true;
    this.peerBridgeUrl = '';
    this.syncIntervalMs = DEFAULT_SYNC_INTERVAL_MS;

    this.syncTimer = null;
    this.lastSyncAt = 0;
    this.lastSyncError = '';
    this.lastInboundAt = 0;
    this.lastInboundCount = 0;
    this.lastOutboundCount = 0;

    this.nonceCache = new Map();
    this._lifecycleToken = 0;
  }

  setEnabled(enabled) {
    this.enabled = !!enabled;
  }

  setPeerBridgeUrl(url) {
    const next = normalizeText(url);
    this.peerBridgeUrl = isLoopbackUrl(next) ? next : '';
  }

  setSyncIntervalMs(ms) {
    const num = Number(ms);
    if (!Number.isFinite(num) || num < 2000) return;
    this.syncIntervalMs = Math.min(60000, Math.max(2000, Math.round(num)));
  }

  getStatus() {
    return {
      ok: true,
      enabled: this.enabled,
      running: !!this.server,
      port: this.port || 0,
      local_url: this.port ? `http://127.0.0.1:${this.port}` : '',
      peer_url: this.peerBridgeUrl || '',
      sync_interval_ms: this.syncIntervalMs,
      last_sync_at: this.lastSyncAt || 0,
      last_sync_error: this.lastSyncError || '',
      last_inbound_at: this.lastInboundAt || 0,
      last_inbound_count: this.lastInboundCount || 0,
      last_outbound_count: this.lastOutboundCount || 0,
    };
  }

  _gcNonceCache() {
    const cutoff = nowTs() - NONCE_TTL_MS;
    for (const [nonce, ts] of this.nonceCache.entries()) {
      if (ts < cutoff) this.nonceCache.delete(nonce);
    }
  }

  _readRequestBody(req) {
    return new Promise((resolve, reject) => {
      let total = 0;
      const chunks = [];
      req.on('data', (chunk) => {
        total += chunk.length;
        if (total > MAX_BODY_BYTES) {
          reject(new Error('Request body too large.'));
          req.destroy();
          return;
        }
        chunks.push(chunk);
      });
      req.on('end', () => {
        resolve(Buffer.concat(chunks).toString('utf8'));
      });
      req.on('error', (err) => reject(err));
    });
  }

  _json(res, statusCode, payload) {
    const body = JSON.stringify(payload || {});
    res.writeHead(statusCode, {
      'content-type': 'application/json; charset=utf-8',
      'content-length': Buffer.byteLength(body),
      'cache-control': 'no-store',
    });
    res.end(body);
  }

  _verifyRequestAuth(req, pathOnly, bodyRaw) {
    const secret = normalizeText(this.secretProvider());
    if (!secret) {
      return { ok: false, status: 503, message: 'Sync secret unavailable.' };
    }

    const tsHeader = normalizeText(req.headers['x-tc-timestamp']);
    const nonce = normalizeText(req.headers['x-tc-nonce']);
    const signature = normalizeText(req.headers['x-tc-signature']).toLowerCase();
    const method = String(req.method || 'GET').toUpperCase();

    if (!tsHeader || !nonce || !signature) {
      return { ok: false, status: 401, message: 'Missing authentication headers.' };
    }

    const ts = Number(tsHeader);
    if (!Number.isFinite(ts)) {
      return { ok: false, status: 401, message: 'Invalid timestamp.' };
    }

    const skew = Math.abs(nowTs() - ts);
    if (skew > MAX_CLOCK_SKEW_MS) {
      return { ok: false, status: 401, message: 'Timestamp out of range.' };
    }

    this._gcNonceCache();
    if (this.nonceCache.has(nonce)) {
      return { ok: false, status: 401, message: 'Nonce replay detected.' };
    }

    const bodyHash = sha256Hex(Buffer.from(bodyRaw || '', 'utf8'));
    const text = buildSigningString({
      method,
      path: pathOnly,
      timestamp: ts,
      nonce,
      bodyHash,
    });
    const expected = hmacHex(secret, text);
    if (!safeEqualHex(expected, signature)) {
      return { ok: false, status: 401, message: 'Signature validation failed.' };
    }

    this.nonceCache.set(nonce, nowTs());
    return { ok: true };
  }

  _exportSyncReferences() {
    const refs = Array.isArray(this.refsProvider()) ? this.refsProvider() : [];
    return refs
      .filter((ref) => ref && !ref.is_public_candidate)
      .map((ref) => ({
        id: String((ref && ref.id) || ''),
        title: String((ref && ref.title) || 'Untitled'),
        intent: String((ref && ref.intent) || ''),
        tags: Array.isArray(ref && ref.tags) ? ref.tags : [],
        parent_id: ref && ref.parent_id ? String(ref.parent_id) : null,
        children: Array.isArray(ref && ref.children) ? ref.children.map((item) => String(item || '')).filter(Boolean) : [],
        lineage: Array.isArray(ref && ref.lineage) ? ref.lineage.map((item) => String(item || '')).filter(Boolean) : [],
        relation_type: String((ref && ref.relation_type) || 'root'),
        color_tag: String((ref && ref.color_tag) || '').trim().toLowerCase(),
        visibility: String((ref && ref.visibility) || 'private') === 'public' ? 'public' : 'private',
        tabs: Array.isArray(ref && ref.tabs) ? ref.tabs.map((tab) => ({
          id: String((tab && tab.id) || ''),
          tab_kind: String((tab && tab.tab_kind) || 'web'),
          url: String((tab && tab.url) || ''),
          title: String((tab && tab.title) || ''),
          renderer: String((tab && tab.renderer) || ''),
          viz_request: (tab && typeof tab.viz_request === 'object') ? tab.viz_request : {},
          files_view_state: (tab && typeof tab.files_view_state === 'object') ? tab.files_view_state : {},
          updated_at: Number((tab && tab.updated_at) || 0),
        })) : [],
        artifacts: Array.isArray(ref && ref.artifacts) ? ref.artifacts.map((artifact) => ({
          id: String((artifact && artifact.id) || ''),
          type: String((artifact && artifact.type) || 'markdown'),
          title: String((artifact && artifact.title) || ''),
          content: String((artifact && artifact.content) || ''),
          updated_at: Number((artifact && artifact.updated_at) || 0),
        })) : [],
        reference_graph: (ref && typeof ref.reference_graph === 'object' && ref.reference_graph) ? ref.reference_graph : { nodes: [], edges: [] },
        pinned_root: !!(ref && ref.pinned_root),
        updated_at: Number((ref && ref.updated_at) || 0),
        created_at: Number((ref && ref.created_at) || 0),
      }))
      .filter((ref) => !!ref.id);
  }

  async _handleRequest(req, res) {
    const method = String(req.method || 'GET').toUpperCase();
    const urlObj = new URL(req.url || '/', `http://127.0.0.1:${this.port || 0}`);
    const pathOnly = urlObj.pathname || '/';

    if (pathOnly === '/v1/health' && method === 'GET') {
      this._json(res, 200, {
        ok: true,
        app: 'subgrapher',
        bridge: this.getStatus(),
      });
      return;
    }

    let raw = '';
    if (method !== 'GET' && method !== 'HEAD') {
      try {
        raw = await this._readRequestBody(req);
      } catch (err) {
        this._json(res, 413, { ok: false, message: err.message || 'Request body too large.' });
        return;
      }
    }

    const auth = this._verifyRequestAuth(req, pathOnly, raw);
    if (!auth.ok) {
      this._json(res, auth.status || 401, { ok: false, message: auth.message || 'Unauthorized.' });
      return;
    }

    if (pathOnly === '/v1/references' && method === 'GET') {
      const refs = this._exportSyncReferences();
      this._json(res, 200, {
        ok: true,
        app: 'subgrapher',
        references: refs,
        count: refs.length,
        exported_at: nowTs(),
      });
      return;
    }

    if (pathOnly === '/v1/references/upsert' && method === 'POST') {
      const payload = parseJsonBody(raw);
      const refs = Array.isArray(payload.references) ? payload.references : [];
      const mergeRes = this.refsConsumer(refs, {
        source_app: normalizeText(payload.source_app || 'trustcommons'),
        source_peer_id: normalizeText(payload.source_peer_id || 'trustcommons-local-bridge'),
      });
      this.lastInboundAt = nowTs();
      this.lastInboundCount = refs.length;
      this._json(res, 200, {
        ok: !!(mergeRes && mergeRes.ok),
        result: mergeRes || { ok: false, message: 'No merge result.' },
      });
      return;
    }

    this._json(res, 404, { ok: false, message: 'Not found.' });
  }

  async start(port) {
    const lifecycleToken = ++this._lifecycleToken;
    const nextPort = Math.max(1024, Math.min(65535, Number(port) || 0));
    if (!nextPort) {
      return { ok: false, message: 'Invalid bridge port.' };
    }

    if (this.server && this.port === nextPort) {
      this._restartSyncTimer();
      this.emit('status', this.getStatus());
      return { ok: true, status: this.getStatus(), already_running: true };
    }

    if (this.server) {
      await this.stop();
    }

    const serverRef = http.createServer((req, res) => {
      this._handleRequest(req, res).catch((err) => {
        this._json(res, 500, { ok: false, message: err && err.message ? err.message : 'Internal sync bridge error.' });
      });
    });
    this.server = serverRef;
    this.port = 0;

    try {
      await new Promise((resolve, reject) => {
        let settled = false;
        const onError = (err) => {
          if (settled) return;
          settled = true;
          reject(err);
        };
        const onListening = () => {
          if (settled) return;
          settled = true;
          serverRef.removeListener('error', onError);
          resolve();
        };
        serverRef.once('error', onError);
        serverRef.listen(nextPort, '127.0.0.1', onListening);
      });
    } catch (err) {
      try {
        await new Promise((resolve) => serverRef.close(() => resolve()));
      } catch (_) {
        // noop
      }
      if (this.server === serverRef) this.server = null;
      this.port = 0;
      this.emit('status', this.getStatus());
      return { ok: false, message: err && err.message ? err.message : 'Unable to start sync bridge.', status: this.getStatus() };
    }

    if (lifecycleToken !== this._lifecycleToken || this.server !== serverRef) {
      try {
        await new Promise((resolve) => serverRef.close(() => resolve()));
      } catch (_) {
        // noop
      }
      return { ok: true, stale_start: true, status: this.getStatus() };
    }

    this.port = nextPort;
    this._restartSyncTimer();
    this.emit('status', this.getStatus());
    return { ok: true, status: this.getStatus() };
  }

  async stop() {
    this._lifecycleToken += 1;
    if (this.syncTimer) {
      clearInterval(this.syncTimer);
      this.syncTimer = null;
    }

    if (!this.server) {
      return { ok: true, status: this.getStatus() };
    }

    const toClose = this.server;
    this.server = null;
    await new Promise((resolve) => {
      try {
        toClose.close(() => resolve());
      } catch (_) {
        resolve();
      }
    });
    this.port = 0;
    this.emit('status', this.getStatus());
    return { ok: true, status: this.getStatus() };
  }

  _buildSignedRequest(method, pathOnly, bodyRaw) {
    const secret = normalizeText(this.secretProvider());
    if (!secret) return null;

    const timestamp = nowTs();
    const nonce = crypto.randomUUID();
    const bodyHash = sha256Hex(Buffer.from(bodyRaw || '', 'utf8'));
    const text = buildSigningString({
      method,
      path: pathOnly,
      timestamp,
      nonce,
      bodyHash,
    });
    const signature = hmacHex(secret, text);

    return {
      method,
      headers: {
        'content-type': 'application/json; charset=utf-8',
        'x-tc-timestamp': String(timestamp),
        'x-tc-nonce': nonce,
        'x-tc-signature': signature,
      },
      body: bodyRaw || '',
    };
  }

  async _peerRequest(method, pathOnly, payload = null) {
    if (!this.peerBridgeUrl || !isLoopbackUrl(this.peerBridgeUrl)) {
      return { ok: false, message: 'Peer bridge URL is not configured.' };
    }

    const url = new URL(pathOnly, this.peerBridgeUrl);
    const bodyRaw = payload ? JSON.stringify(payload) : '';
    const signed = this._buildSignedRequest(method, pathOnly, bodyRaw);
    if (!signed) return { ok: false, message: 'Sync secret unavailable.' };

    const res = await fetch(url.toString(), {
      method,
      headers: signed.headers,
      body: method === 'GET' ? undefined : signed.body,
    });

    const text = await res.text();
    const json = parseJsonBody(text);
    if (!res.ok) {
      return {
        ok: false,
        status: res.status,
        message: String((json && json.message) || `Peer bridge request failed (${res.status}).`),
        payload: json,
      };
    }
    return { ok: true, status: res.status, payload: json };
  }

  async syncOnce() {
    if (!this.enabled) {
      return { ok: true, skipped: true, reason: 'disabled' };
    }
    if (!this.server || !this.port) {
      return { ok: true, skipped: true, reason: 'bridge_not_running' };
    }
    if (!this.peerBridgeUrl) {
      return { ok: true, skipped: true, reason: 'peer_not_configured' };
    }

    const localRefs = this._exportSyncReferences();
    const pushRes = await this._peerRequest('POST', '/v1/references/upsert', {
      source_app: 'subgrapher',
      source_peer_id: 'subgrapher-local-bridge',
      references: localRefs,
    });

    if (!pushRes.ok) {
      this.lastSyncError = pushRes.message || 'Sync push failed.';
      return { ok: false, message: this.lastSyncError };
    }

    this.lastOutboundCount = localRefs.length;

    const pullRes = await this._peerRequest('GET', '/v1/references');
    if (!pullRes.ok) {
      this.lastSyncError = pullRes.message || 'Sync pull failed.';
      return { ok: false, message: this.lastSyncError };
    }

    const incoming = Array.isArray(pullRes.payload && pullRes.payload.references)
      ? pullRes.payload.references
      : [];
    const merged = this.refsConsumer(incoming, {
      source_app: normalizeText((pullRes.payload && pullRes.payload.app) || 'trustcommons'),
      source_peer_id: 'trustcommons-local-bridge',
    });

    this.lastSyncAt = nowTs();
    this.lastSyncError = '';
    this.lastInboundAt = this.lastSyncAt;
    this.lastInboundCount = incoming.length;
    this.emit('status', this.getStatus());

    return {
      ok: !!(merged && merged.ok),
      pushed: localRefs.length,
      pulled: incoming.length,
      merged,
    };
  }

  _restartSyncTimer() {
    if (this.syncTimer) {
      clearInterval(this.syncTimer);
      this.syncTimer = null;
    }

    this.syncTimer = setInterval(() => {
      this.syncOnce().catch((err) => {
        this.lastSyncError = err && err.message ? err.message : 'Unknown sync error.';
        this.emit('status', this.getStatus());
      });
    }, this.syncIntervalMs);
  }
}

module.exports = {
  TrustCommonsSyncBridge,
  isLoopbackUrl,
};
