const crypto = require('crypto');
const { EventEmitter } = require('events');

const CHUNK_SIZE = 45_000;
const QUERY_TIMEOUT_MS = 2_800;

function nowTs() {
  return Date.now();
}

function makeId(prefix) {
  return `${prefix}_${crypto.randomUUID()}`;
}

function normalizeText(value) {
  return String(value || '').trim();
}

function toWords(text) {
  return normalizeText(text).toLowerCase().split(/\s+/).filter(Boolean);
}

function scoreMatch(reference, query) {
  const q = normalizeText(query).toLowerCase();
  if (!q) return 0;
  const tags = Array.isArray(reference && reference.tags) ? reference.tags.join(' ') : '';
  const artifacts = Array.isArray(reference && reference.artifacts)
    ? reference.artifacts.map((item) => `${item.title || ''} ${item.content || ''}`).join(' ')
    : '';
  const tabs = Array.isArray(reference && reference.tabs)
    ? reference.tabs.map((tab) => `${tab.title || ''} ${tab.url || ''}`).join(' ')
    : '';
  const blob = [reference && reference.title, reference && reference.intent, tags, artifacts, tabs]
    .map((item) => String(item || '').toLowerCase())
    .join(' ');
  if (!blob) return 0;
  if (blob.includes(q)) return 1;
  const terms = toWords(q);
  if (!terms.length) return 0;
  let score = 0;
  terms.forEach((term) => {
    if (blob.includes(term)) score += 0.18;
  });
  return Math.max(0, Math.min(0.95, score));
}

function pickRtcFactory() {
  if (typeof RTCPeerConnection !== 'undefined') {
    return {
      RTCPeerConnection,
      RTCSessionDescription: typeof RTCSessionDescription !== 'undefined' ? RTCSessionDescription : null,
      RTCIceCandidate: typeof RTCIceCandidate !== 'undefined' ? RTCIceCandidate : null,
    };
  }
  for (const pkg of ['wrtc', '@roamhq/wrtc']) {
    try {
      const wrtc = require(pkg);
      if (wrtc && wrtc.RTCPeerConnection) {
        return {
          RTCPeerConnection: wrtc.RTCPeerConnection,
          RTCSessionDescription: wrtc.RTCSessionDescription || null,
          RTCIceCandidate: wrtc.RTCIceCandidate || null,
        };
      }
    } catch (_) {
      // optional dependency
    }
  }
  return null;
}

function pickSocketFactory() {
  try {
    const mod = require('socket.io-client');
    if (typeof mod === 'function') return mod;
    if (mod && typeof mod.io === 'function') return mod.io;
  } catch (_) {
    // optional dependency
  }
  return null;
}

class HyperwebManager extends EventEmitter {
  constructor(options = {}) {
    super();
    this.peerId = makeId('peer');
    this.identity = null;
    this.relayUrl = normalizeText(options.relayUrl || '');
    this.enabled = options.enabled !== false;
    this.logger = options.logger || console;

    this.socketFactory = pickSocketFactory();
    this.rtcFactory = pickRtcFactory();

    this.socket = null;
    this.connected = false;
    this.lastError = '';

    this.publicIndexProvider = typeof options.publicIndexProvider === 'function'
      ? options.publicIndexProvider
      : (() => []);

    this.peerStates = new Map();
    this.pendingQueries = new Map();
    this.pendingFetches = new Map();
    this.suggestionCache = new Map();
    this.chunkBuffers = new Map();

    this.localIndexMeta = [];
    this.localFullById = new Map();
  }

  setIdentity(identity) {
    if (!identity || typeof identity !== 'object') return;
    this.identity = {
      identity_id: normalizeText(identity.identity_id),
      display_name: normalizeText(identity.display_name),
      token: String(identity.token || ''),
    };
  }

  setRelayUrl(url) {
    this.relayUrl = normalizeText(url);
  }

  setEnabled(enabled) {
    this.enabled = !!enabled;
  }

  setPublicIndexProvider(fn) {
    if (typeof fn === 'function') this.publicIndexProvider = fn;
  }

  getStatus() {
    const peers = Array.from(this.peerStates.values());
    return {
      ok: true,
      enabled: this.enabled,
      relay_url: this.relayUrl,
      connected: this.connected,
      signaling_available: !!this.socketFactory,
      rtc_available: !!(this.rtcFactory && this.rtcFactory.RTCPeerConnection),
      peer_id: this.peerId,
      peer_count: peers.filter((item) => item && item.channelOpen).length,
      known_peers: peers.map((item) => ({
        peer_id: item.peerId,
        display_name: item.displayName,
        channel_open: !!item.channelOpen,
        has_index: Array.isArray(item.publicIndex) && item.publicIndex.length > 0,
      })),
      suggestions_cached: this.suggestionCache.size,
      last_error: this.lastError,
      identity_id: this.identity && this.identity.identity_id ? this.identity.identity_id : '',
      identity_name: this.identity && this.identity.display_name ? this.identity.display_name : '',
    };
  }

  listPeers() {
    return Array.from(this.peerStates.values()).map((item) => ({
      peer_id: item.peerId,
      display_name: item.displayName || item.peerId,
      channel_open: !!item.channelOpen,
      has_index: Array.isArray(item.publicIndex) && item.publicIndex.length > 0,
      last_seen_at: Number(item.lastSeenAt || 0),
    }));
  }

  _setError(err) {
    this.lastError = normalizeText(err);
    if (this.lastError) this.emit('status', this.getStatus());
  }

  _ensurePeerState(peerId) {
    const id = normalizeText(peerId);
    if (!id) return null;
    if (!this.peerStates.has(id)) {
      this.peerStates.set(id, {
        peerId: id,
        displayName: id,
        pc: null,
        channel: null,
        channelOpen: false,
        publicIndex: [],
        lastSeenAt: nowTs(),
      });
    }
    const state = this.peerStates.get(id);
    state.lastSeenAt = nowTs();
    return state;
  }

  _clearPeerState(peerId) {
    const id = normalizeText(peerId);
    if (!id) return;
    const state = this.peerStates.get(id);
    if (!state) return;
    try {
      if (state.channel) {
        state.channel.onopen = null;
        state.channel.onclose = null;
        state.channel.onerror = null;
        state.channel.onmessage = null;
        state.channel.close();
      }
    } catch (_) {
      // noop
    }
    try {
      if (state.pc) {
        state.pc.onicecandidate = null;
        state.pc.ondatachannel = null;
        state.pc.onconnectionstatechange = null;
        state.pc.close();
      }
    } catch (_) {
      // noop
    }
    this.peerStates.delete(id);
  }

  async connect(identity = null) {
    if (identity) this.setIdentity(identity);
    if (!this.enabled) return { ok: true, status: this.getStatus(), message: 'Hyperweb is disabled in settings.' };
    if (!this.relayUrl) return { ok: false, status: this.getStatus(), message: 'Hyperweb relay URL is not configured.' };

    await this.refreshLocalPublicIndex();

    if (!this.socketFactory) {
      this.connected = false;
      this._setError('socket.io-client is not installed; Hyperweb relay signaling unavailable.');
      return {
        ok: true,
        degraded: true,
        status: this.getStatus(),
        message: 'Running in local-only Hyperweb mode. Install socket.io-client to enable peer signaling.',
      };
    }

    if (this.socket && this.connected) {
      return { ok: true, status: this.getStatus(), already_connected: true };
    }

    if (this.socket) {
      try { this.socket.disconnect(); } catch (_) { /* noop */ }
      this.socket = null;
    }

    const socket = this.socketFactory(this.relayUrl, {
      transports: ['websocket'],
      timeout: 8000,
      reconnection: true,
      reconnectionAttempts: 6,
    });

    this.socket = socket;

    socket.on('connect', () => {
      this.connected = true;
      this.lastError = '';
      socket.emit('hyperweb:join', {
        peer_id: this.peerId,
        identity_id: this.identity && this.identity.identity_id ? this.identity.identity_id : '',
        display_name: this.identity && this.identity.display_name ? this.identity.display_name : this.peerId,
      });
      this.announcePublicIndex().catch(() => {});
      this.emit('status', this.getStatus());
    });

    socket.on('disconnect', () => {
      this.connected = false;
      this.emit('status', this.getStatus());
    });

    socket.on('connect_error', (err) => {
      this.connected = false;
      this._setError(err && err.message ? err.message : 'Unable to connect to Hyperweb relay.');
      this.emit('status', this.getStatus());
    });

    socket.on('hyperweb:peer_list', async (payload) => {
      const peers = Array.isArray(payload && payload.peers) ? payload.peers : [];
      for (const peer of peers) {
        const peerId = normalizeText(peer && peer.peer_id);
        if (!peerId || peerId === this.peerId) continue;
        const state = this._ensurePeerState(peerId);
        if (state) state.displayName = normalizeText(peer && peer.display_name) || peerId;
        await this._ensureRtcConnection(peerId, { initiator: true });
      }
    });

    socket.on('hyperweb:peer_joined', async (payload) => {
      const peerId = normalizeText(payload && payload.peer_id);
      if (!peerId || peerId === this.peerId) return;
      const state = this._ensurePeerState(peerId);
      if (state) state.displayName = normalizeText(payload && payload.display_name) || peerId;
      await this._ensureRtcConnection(peerId, { initiator: this.peerId < peerId });
    });

    socket.on('hyperweb:peer_left', (payload) => {
      const peerId = normalizeText(payload && payload.peer_id);
      if (!peerId) return;
      this._clearPeerState(peerId);
      this.emit('status', this.getStatus());
    });

    socket.on('hyperweb:signal', async (payload) => {
      await this._handleRelaySignal(payload || {});
    });

    socket.on('hyperweb:announce_public_index', (payload) => {
      const peerId = normalizeText(payload && payload.peer_id);
      if (!peerId || peerId === this.peerId) return;
      const state = this._ensurePeerState(peerId);
      if (!state) return;
      state.publicIndex = Array.isArray(payload && payload.index) ? payload.index : [];
      state.displayName = normalizeText(payload && payload.display_name) || state.displayName || peerId;
      state.lastSeenAt = nowTs();
      this.emit('status', this.getStatus());
    });

    return { ok: true, status: this.getStatus() };
  }

  disconnect() {
    if (this.socket) {
      try {
        this.socket.disconnect();
      } catch (_) {
        // noop
      }
      this.socket = null;
    }
    this.connected = false;
    for (const peerId of Array.from(this.peerStates.keys())) {
      this._clearPeerState(peerId);
    }
    this.emit('status', this.getStatus());
    return { ok: true, status: this.getStatus() };
  }

  async refreshLocalPublicIndex() {
    const refs = await Promise.resolve(this.publicIndexProvider());
    const list = Array.isArray(refs) ? refs : [];
    this.localIndexMeta = [];
    this.localFullById.clear();

    list.forEach((item) => {
      const id = normalizeText(item && item.id);
      if (!id) return;
      const tabs = Array.isArray(item.tabs) ? item.tabs : [];
      const artifacts = Array.isArray(item.artifacts) ? item.artifacts : [];
      const normalized = {
        ...item,
        id,
        title: String(item && item.title ? item.title : 'Untitled'),
        intent: String(item && item.intent ? item.intent : ''),
        tags: Array.isArray(item && item.tags) ? item.tags : [],
        tabs,
        artifacts,
      };
      this.localFullById.set(id, normalized);
      this.localIndexMeta.push({
        id,
        title: normalized.title,
        intent: normalized.intent,
        tags: normalized.tags,
        updated_at: Number(normalized.updated_at || nowTs()),
        tab_count: tabs.length,
        artifact_count: artifacts.length,
      });
    });

    return { index: this.localIndexMeta, references: list };
  }

  async announcePublicIndex() {
    await this.refreshLocalPublicIndex();
    const payload = {
      type: 'hyperweb:announce_public_index',
      peer_id: this.peerId,
      identity_id: this.identity && this.identity.identity_id ? this.identity.identity_id : '',
      display_name: this.identity && this.identity.display_name ? this.identity.display_name : this.peerId,
      index: this.localIndexMeta,
      ts: nowTs(),
    };

    this._broadcastProtocol(payload);
    if (this.socket && this.connected) {
      this.socket.emit('hyperweb:announce_public_index', payload);
    }

    return { ok: true, index_count: this.localIndexMeta.length };
  }

  _trackSuggestion(suggestion) {
    if (!suggestion || typeof suggestion !== 'object') return null;
    const existingId = normalizeText(suggestion.suggestion_id);
    const suggestionId = existingId || makeId('hwsug');
    const next = {
      ...suggestion,
      suggestion_id: suggestionId,
    };
    this.suggestionCache.set(suggestionId, next);
    if (this.suggestionCache.size > 500) {
      const first = this.suggestionCache.keys().next();
      if (!first.done) this.suggestionCache.delete(first.value);
    }
    return next;
  }

  _buildSuggestionFromReference(ref, options = {}) {
    const full = !!options.full;
    const suggestion = {
      suggestion_id: makeId('hwsug'),
      peer_id: normalizeText(options.peer_id || this.peerId),
      peer_name: normalizeText(options.peer_name || (this.identity && this.identity.display_name) || this.peerId),
      reference_id: normalizeText(ref && ref.id),
      score: Number(options.score || 0),
      title: String((ref && ref.title) || 'Untitled'),
      intent: String((ref && ref.intent) || ''),
      tags: Array.isArray(ref && ref.tags) ? ref.tags : [],
      updated_at: Number((ref && ref.updated_at) || nowTs()),
      tab_count: Array.isArray(ref && ref.tabs) ? ref.tabs.length : 0,
      artifact_count: Array.isArray(ref && ref.artifacts) ? ref.artifacts.length : 0,
      source_type: 'hyperweb_candidate',
      hyperweb_payload_version: 1,
    };
    if (full) {
      suggestion.reference_payload = {
        ...ref,
        source_peer_id: suggestion.peer_id,
        source_peer_name: suggestion.peer_name,
        source_type: 'hyperweb_candidate',
        is_temp_candidate: true,
        temp_imported_at: nowTs(),
        hyperweb_payload_version: 1,
      };
    }
    return suggestion;
  }

  async query(topic, options = {}) {
    const query = normalizeText(topic);
    if (!query) return { ok: false, message: 'query is required.', suggestions: [] };

    await this.refreshLocalPublicIndex();

    const limit = Math.max(1, Math.min(80, Number(options.limit || 25) || 25));
    const timeoutMs = Math.max(900, Math.min(12000, Number(options.timeout_ms || QUERY_TIMEOUT_MS) || QUERY_TIMEOUT_MS));

    const localSuggestions = [];
    for (const meta of this.localIndexMeta) {
      const full = this.localFullById.get(meta.id);
      if (!full) continue;
      const score = scoreMatch(full, query);
      if (score <= 0) continue;
      localSuggestions.push(this._trackSuggestion(this._buildSuggestionFromReference(full, {
        score,
        peer_id: this.peerId,
        peer_name: this.identity && this.identity.display_name ? this.identity.display_name : this.peerId,
        full: true,
      })));
    }

    const queryId = makeId('hwquery');
    const pending = {
      query,
      results: [],
      resolve: null,
      done: false,
      timer: null,
    };

    const donePromise = new Promise((resolve) => {
      pending.resolve = resolve;
    });

    pending.timer = setTimeout(() => {
      pending.done = true;
      this.pendingQueries.delete(queryId);
      pending.resolve(Array.isArray(pending.results) ? pending.results : []);
    }, timeoutMs);

    this.pendingQueries.set(queryId, pending);

    this._broadcastProtocol({
      type: 'hyperweb:query',
      query_id: queryId,
      query,
      limit,
      requester_peer_id: this.peerId,
      ts: nowTs(),
    });

    if (this.socket && this.connected) {
      this.socket.emit('hyperweb:query', {
        type: 'hyperweb:query',
        query_id: queryId,
        query,
        limit,
        requester_peer_id: this.peerId,
        ts: nowTs(),
      });
    }

    const remoteResults = await donePromise;

    const combined = [];
    const dedupe = new Set();
    const pushSuggestion = (item) => {
      if (!item) return;
      const sig = `${item.peer_id || ''}:${item.reference_id || ''}:${item.title || ''}`;
      if (dedupe.has(sig)) return;
      dedupe.add(sig);
      combined.push(item);
    };

    localSuggestions.forEach(pushSuggestion);
    remoteResults.forEach((item) => pushSuggestion(this._trackSuggestion(item)));

    combined.sort((a, b) => Number(b && b.score || 0) - Number(a && a.score || 0));
    const trimmed = combined.slice(0, limit);

    return {
      ok: true,
      query,
      suggestion_count: trimmed.length,
      local_suggestion_count: localSuggestions.length,
      remote_suggestion_count: Math.max(0, trimmed.length - localSuggestions.length),
      suggestions: trimmed,
      status: this.getStatus(),
    };
  }

  async importSuggestion(suggestionOrId, options = {}) {
    const suggestion = typeof suggestionOrId === 'string'
      ? this.suggestionCache.get(normalizeText(suggestionOrId))
      : suggestionOrId;

    if (!suggestion || typeof suggestion !== 'object') {
      return { ok: false, message: 'Hyperweb suggestion was not found.' };
    }

    if (suggestion.reference_payload && typeof suggestion.reference_payload === 'object') {
      return {
        ok: true,
        imported: {
          ...suggestion.reference_payload,
          source_peer_id: normalizeText(suggestion.peer_id || suggestion.reference_payload.source_peer_id),
          source_peer_name: normalizeText(suggestion.peer_name || suggestion.reference_payload.source_peer_name),
          source_type: 'hyperweb_candidate',
          is_temp_candidate: true,
          temp_imported_at: nowTs(),
          hyperweb_payload_version: Number(suggestion.hyperweb_payload_version || 1),
        },
      };
    }

    const peerId = normalizeText(suggestion.peer_id);
    const referenceId = normalizeText(suggestion.reference_id);
    if (!peerId || !referenceId) {
      return { ok: false, message: 'Selected suggestion does not include source peer metadata.' };
    }

    const fetchId = makeId('hwfetch');
    const timeoutMs = Math.max(1200, Math.min(12000, Number(options.timeout_ms || 6000) || 6000));

    const waitPromise = new Promise((resolve) => {
      const timer = setTimeout(() => {
        this.pendingFetches.delete(fetchId);
        resolve(null);
      }, timeoutMs);
      this.pendingFetches.set(fetchId, {
        resolve,
        timer,
        peerId,
        referenceId,
      });
    });

    this._sendProtocolToPeer(peerId, {
      type: 'hyperweb:fetch_reference',
      fetch_id: fetchId,
      reference_id: referenceId,
      requester_peer_id: this.peerId,
      ts: nowTs(),
    });

    const payload = await waitPromise;
    if (!payload || typeof payload !== 'object' || !payload.reference_payload) {
      return { ok: false, message: 'Timed out fetching Hyperweb reference payload from peer.' };
    }

    return {
      ok: true,
      imported: {
        ...payload.reference_payload,
        source_peer_id: normalizeText(payload.peer_id || suggestion.peer_id),
        source_peer_name: normalizeText(payload.peer_name || suggestion.peer_name),
        source_type: 'hyperweb_candidate',
        is_temp_candidate: true,
        temp_imported_at: nowTs(),
        hyperweb_payload_version: Number(payload.hyperweb_payload_version || 1),
      },
    };
  }

  _serializeAndSend(channel, message) {
    if (!channel || channel.readyState !== 'open') return false;
    const raw = JSON.stringify(message || {});
    if (raw.length <= CHUNK_SIZE) {
      channel.send(raw);
      return true;
    }

    const chunkId = makeId('hwchunk');
    const total = Math.ceil(raw.length / CHUNK_SIZE);
    for (let seq = 0; seq < total; seq += 1) {
      const start = seq * CHUNK_SIZE;
      const part = raw.slice(start, start + CHUNK_SIZE);
      channel.send(JSON.stringify({
        type: 'hyperweb:chunk',
        chunk_id: chunkId,
        seq,
        total,
        payload: part,
      }));
    }
    return true;
  }

  _handleIncomingRaw(peerId, rawData) {
    if (!rawData) return;
    let parsed = null;
    try {
      parsed = JSON.parse(String(rawData));
    } catch (_) {
      return;
    }

    if (!parsed || typeof parsed !== 'object') return;

    if (parsed.type === 'hyperweb:chunk') {
      const chunkId = normalizeText(parsed.chunk_id);
      const seq = Number(parsed.seq);
      const total = Number(parsed.total);
      if (!chunkId || !Number.isFinite(seq) || !Number.isFinite(total) || total <= 0) return;
      if (!this.chunkBuffers.has(chunkId)) {
        this.chunkBuffers.set(chunkId, {
          peerId,
          total,
          parts: new Array(total),
          updatedAt: nowTs(),
        });
      }
      const slot = this.chunkBuffers.get(chunkId);
      slot.parts[seq] = String(parsed.payload || '');
      slot.updatedAt = nowTs();
      const ready = slot.parts.filter((part) => typeof part === 'string').length;
      if (ready >= slot.total) {
        const merged = slot.parts.join('');
        this.chunkBuffers.delete(chunkId);
        this._handleIncomingRaw(peerId, merged);
      }
      return;
    }

    this._handleProtocolMessage(peerId, parsed);
  }

  _sendProtocolToPeer(peerId, message) {
    const state = this.peerStates.get(normalizeText(peerId));
    if (!state || !state.channel || state.channel.readyState !== 'open') return false;
    return this._serializeAndSend(state.channel, message);
  }

  _broadcastProtocol(message) {
    for (const [peerId] of this.peerStates.entries()) {
      this._sendProtocolToPeer(peerId, message);
    }
  }

  sendProtocolToPeer(peerId, message) {
    return this._sendProtocolToPeer(peerId, message);
  }

  broadcastProtocol(message) {
    this._broadcastProtocol(message);
    return { ok: true };
  }

  async _handleProtocolMessage(peerId, message) {
    const type = normalizeText(message && message.type);
    if (!type) return;
    this.emit('protocol', {
      peer_id: normalizeText(peerId),
      message,
    });

    if (type === 'hyperweb:announce_public_index') {
      const state = this._ensurePeerState(peerId);
      if (!state) return;
      state.publicIndex = Array.isArray(message.index) ? message.index : [];
      state.displayName = normalizeText(message.display_name) || state.displayName || peerId;
      state.lastSeenAt = nowTs();
      this.emit('status', this.getStatus());
      return;
    }

    if (type === 'hyperweb:query') {
      await this.refreshLocalPublicIndex();
      const q = normalizeText(message.query);
      if (!q) return;
      const limit = Math.max(1, Math.min(80, Number(message.limit || 20) || 20));

      const matches = [];
      for (const ref of this.localFullById.values()) {
        const score = scoreMatch(ref, q);
        if (score <= 0) continue;
        matches.push(this._buildSuggestionFromReference(ref, {
          score,
          peer_id: this.peerId,
          peer_name: this.identity && this.identity.display_name ? this.identity.display_name : this.peerId,
          full: false,
        }));
      }
      matches.sort((a, b) => Number(b.score || 0) - Number(a.score || 0));

      this._sendProtocolToPeer(peerId, {
        type: 'hyperweb:query_result',
        query_id: normalizeText(message.query_id),
        query: q,
        peer_id: this.peerId,
        peer_name: this.identity && this.identity.display_name ? this.identity.display_name : this.peerId,
        suggestions: matches.slice(0, limit),
        ts: nowTs(),
      });
      return;
    }

    if (type === 'hyperweb:query_result') {
      const queryId = normalizeText(message.query_id);
      const pending = this.pendingQueries.get(queryId);
      if (!pending || pending.done) return;
      const incoming = Array.isArray(message.suggestions) ? message.suggestions : [];
      incoming.forEach((item) => {
        if (!item || typeof item !== 'object') return;
        pending.results.push({
          ...item,
          peer_id: normalizeText(item.peer_id || message.peer_id || peerId),
          peer_name: normalizeText(item.peer_name || message.peer_name || peerId),
          source_type: 'hyperweb_candidate',
          hyperweb_payload_version: Number(item.hyperweb_payload_version || 1),
        });
      });
      return;
    }

    if (type === 'hyperweb:fetch_reference') {
      await this.refreshLocalPublicIndex();
      const referenceId = normalizeText(message.reference_id);
      const fetchId = normalizeText(message.fetch_id);
      if (!referenceId || !fetchId) return;
      const ref = this.localFullById.get(referenceId);
      if (!ref) return;

      this._sendProtocolToPeer(peerId, {
        type: 'hyperweb:reference_payload',
        fetch_id: fetchId,
        peer_id: this.peerId,
        peer_name: this.identity && this.identity.display_name ? this.identity.display_name : this.peerId,
        reference_id: referenceId,
        reference_payload: {
          ...ref,
          source_type: 'hyperweb_candidate',
          source_peer_id: this.peerId,
          source_peer_name: this.identity && this.identity.display_name ? this.identity.display_name : this.peerId,
          is_temp_candidate: true,
          temp_imported_at: nowTs(),
          hyperweb_payload_version: 1,
        },
        hyperweb_payload_version: 1,
        ts: nowTs(),
      });
      return;
    }

    if (type === 'hyperweb:reference_payload') {
      const fetchId = normalizeText(message.fetch_id);
      const pending = this.pendingFetches.get(fetchId);
      if (!pending) return;
      this.pendingFetches.delete(fetchId);
      if (pending.timer) clearTimeout(pending.timer);
      const suggestion = this._trackSuggestion({
        suggestion_id: makeId('hwsug'),
        peer_id: normalizeText(message.peer_id || peerId),
        peer_name: normalizeText(message.peer_name || peerId),
        reference_id: normalizeText(message.reference_id),
        reference_payload: message.reference_payload,
        source_type: 'hyperweb_candidate',
        hyperweb_payload_version: Number(message.hyperweb_payload_version || 1),
      });
      pending.resolve(suggestion);
      return;
    }

    this.emit('protocol_unhandled', {
      peer_id: normalizeText(peerId),
      message,
    });
  }

  _attachDataChannel(peerId, channel) {
    if (!channel) return;
    const state = this._ensurePeerState(peerId);
    if (!state) return;

    state.channel = channel;
    state.channelOpen = channel.readyState === 'open';

    channel.onopen = () => {
      state.channelOpen = true;
      this.emit('status', this.getStatus());
      this.announcePublicIndex().catch(() => {});
    };

    channel.onclose = () => {
      state.channelOpen = false;
      this.emit('status', this.getStatus());
    };

    channel.onerror = () => {
      state.channelOpen = false;
      this.emit('status', this.getStatus());
    };

    channel.onmessage = (event) => {
      this._handleIncomingRaw(peerId, event && event.data);
    };
  }

  async _ensureRtcConnection(peerId, options = {}) {
    const state = this._ensurePeerState(peerId);
    if (!state) return;

    if (!this.rtcFactory || !this.rtcFactory.RTCPeerConnection) {
      this._setError('WebRTC factory unavailable. Install wrtc or @roamhq/wrtc for main-process RTC support.');
      return;
    }

    if (state.pc) return;

    const { RTCPeerConnection, RTCSessionDescription, RTCIceCandidate } = this.rtcFactory;
    const pc = new RTCPeerConnection({
      iceServers: [{ urls: 'stun:stun.l.google.com:19302' }],
    });

    state.pc = pc;

    pc.onicecandidate = (event) => {
      if (!event || !event.candidate || !this.socket || !this.connected) return;
      this.socket.emit('hyperweb:signal', {
        to: peerId,
        from: this.peerId,
        signal: { candidate: event.candidate },
      });
    };

    pc.onconnectionstatechange = () => {
      const status = String(pc.connectionState || '').toLowerCase();
      if (status === 'failed' || status === 'closed' || status === 'disconnected') {
        state.channelOpen = false;
      }
      this.emit('status', this.getStatus());
    };

    pc.ondatachannel = (event) => {
      if (!event || !event.channel) return;
      this._attachDataChannel(peerId, event.channel);
    };

    if (options.initiator) {
      const channel = pc.createDataChannel('hyperweb');
      this._attachDataChannel(peerId, channel);
      const offer = await pc.createOffer();
      await pc.setLocalDescription(offer);
      if (this.socket && this.connected) {
        this.socket.emit('hyperweb:signal', {
          to: peerId,
          from: this.peerId,
          signal: {
            description: pc.localDescription,
          },
        });
      }
    }

    state.rtcSessionTypes = { RTCSessionDescription, RTCIceCandidate };
  }

  async _handleRelaySignal(payload = {}) {
    const from = normalizeText(payload.from);
    const to = normalizeText(payload.to);
    const signal = payload.signal && typeof payload.signal === 'object' ? payload.signal : {};

    if (!from || from === this.peerId) return;
    if (to && to !== this.peerId) return;

    await this._ensureRtcConnection(from, { initiator: false });
    const state = this.peerStates.get(from);
    if (!state || !state.pc) return;

    const pc = state.pc;
    const RTCSessionDescription = (state.rtcSessionTypes && state.rtcSessionTypes.RTCSessionDescription)
      || (this.rtcFactory && this.rtcFactory.RTCSessionDescription)
      || null;
    const RTCIceCandidate = (state.rtcSessionTypes && state.rtcSessionTypes.RTCIceCandidate)
      || (this.rtcFactory && this.rtcFactory.RTCIceCandidate)
      || null;

    if (signal.description) {
      const remoteDescription = RTCSessionDescription
        ? new RTCSessionDescription(signal.description)
        : signal.description;
      await pc.setRemoteDescription(remoteDescription);
      if (String(signal.description.type || '').toLowerCase() === 'offer') {
        const answer = await pc.createAnswer();
        await pc.setLocalDescription(answer);
        if (this.socket && this.connected) {
          this.socket.emit('hyperweb:signal', {
            to: from,
            from: this.peerId,
            signal: {
              description: pc.localDescription,
            },
          });
        }
      }
      return;
    }

    if (signal.candidate) {
      const candidate = RTCIceCandidate ? new RTCIceCandidate(signal.candidate) : signal.candidate;
      await pc.addIceCandidate(candidate);
    }
  }
}

module.exports = {
  HyperwebManager,
};
