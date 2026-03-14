const crypto = require('crypto');
const { EventEmitter } = require('events');

const Hyperswarm = require('hyperswarm');

const QUERY_TIMEOUT_MS = 7_000;
const HELLO_MESSAGE_TYPE = '__hyperweb_hello__';
const TOPIC_MESSAGE_TYPE = '__hyperweb_topics__';
const PUBLIC_TOPIC_DEFAULT = 'subgrapher:hyperweb:public:v1';

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

function topicBuffer(topicId = '') {
  return crypto.createHash('sha256').update(`subgrapher-hyperweb:${normalizeText(topicId)}`).digest();
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

class HyperwebManager extends EventEmitter {
  constructor(options = {}) {
    super();
    this.publicTopicId = normalizeText(options.publicTopicId || PUBLIC_TOPIC_DEFAULT) || PUBLIC_TOPIC_DEFAULT;
    this.enabled = options.enabled !== false;
    this.logger = options.logger || console;
    this.identity = null;
    this.peerId = '';

    this.swarm = null;
    this.connected = false;
    this.lastError = '';

    this.publicIndexProvider = typeof options.publicIndexProvider === 'function'
      ? options.publicIndexProvider
      : (() => []);

    this.peerStates = new Map();
    this.socketStates = new Map();
    this.joinedTopics = new Map();
    this.pendingQueries = new Map();
    this.pendingFetches = new Map();
    this.suggestionCache = new Map();

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
    this.peerId = this.identity.identity_id || this.peerId || makeId('peer');
  }

  setEnabled(enabled) {
    this.enabled = !!enabled;
  }

  setRelayUrl() {
    // relay-based transport was removed in the pure P2P refactor
  }

  setPublicIndexProvider(fn) {
    if (typeof fn === 'function') this.publicIndexProvider = fn;
  }

  getStatus() {
    const peers = Array.from(this.peerStates.values());
    return {
      ok: true,
      enabled: this.enabled,
      connected: this.connected,
      peer_id: this.peerId,
      peer_count: peers.filter((item) => this._hasOpenSocket(item)).length,
      known_peers: peers.map((item) => ({
        peer_id: item.peerId,
        display_name: item.displayName || item.peerId,
        channel_open: this._hasOpenSocket(item),
        has_index: Array.isArray(item.publicIndex) && item.publicIndex.length > 0,
        topic_ids: Array.from(item.topicIds || []),
      })),
      topic_ids: Array.from(this.joinedTopics.keys()),
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
      channel_open: this._hasOpenSocket(item),
      has_index: Array.isArray(item.publicIndex) && item.publicIndex.length > 0,
      public_index: Array.isArray(item.publicIndex) ? item.publicIndex : [],
      last_seen_at: Number(item.lastSeenAt || 0),
      topic_ids: Array.from(item.topicIds || []),
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
        sockets: new Set(),
        publicIndex: [],
        lastSeenAt: nowTs(),
        topicIds: new Set(),
      });
    }
    const state = this.peerStates.get(id);
    state.lastSeenAt = nowTs();
    return state;
  }

  _getSocketStateById(socketId) {
    return this.socketStates.get(normalizeText(socketId)) || null;
  }

  _getSocketStateFromSocket(socket) {
    for (const state of this.socketStates.values()) {
      if (state.socket === socket) return state;
    }
    return null;
  }

  _hasOpenSocket(peerState) {
    const sockets = peerState && peerState.sockets instanceof Set ? Array.from(peerState.sockets) : [];
    return sockets.some((socketId) => {
      const socketState = this._getSocketStateById(socketId);
      return !!(socketState && socketState.open && socketState.socket);
    });
  }

  _resolveTopicIds(details = {}) {
    const refs = [];
    if (Array.isArray(details.topics)) refs.push(...details.topics);
    if (details.peerInfo && Array.isArray(details.peerInfo.topics)) refs.push(...details.peerInfo.topics);
    const ids = [];
    refs.forEach((item) => {
      const hex = Buffer.isBuffer(item) ? item.toString('hex') : normalizeText(item);
      if (!hex) return;
      for (const [topicId, entry] of this.joinedTopics.entries()) {
        if (entry && entry.key && Buffer.isBuffer(entry.key) && entry.key.toString('hex') === hex) {
          ids.push(topicId);
        }
      }
    });
    return Array.from(new Set(ids));
  }

  async _ensureSwarm() {
    if (this.swarm) return this.swarm;
    this.swarm = new Hyperswarm();
    this.swarm.on('connection', (socket, details) => {
      this._handleConnection(socket, details || {});
    });
    this.swarm.on('error', (err) => {
      this._setError(err && err.message ? err.message : 'Hyperswarm error.');
    });
    return this.swarm;
  }

  _buildHelloMessage() {
    return {
      type: HELLO_MESSAGE_TYPE,
      peer_id: this.peerId,
      identity_id: this.identity && this.identity.identity_id ? this.identity.identity_id : this.peerId,
      display_name: this.identity && this.identity.display_name ? this.identity.display_name : this.peerId,
      topic_ids: Array.from(this.joinedTopics.keys()),
      ts: nowTs(),
    };
  }

  _sendRaw(socketState, message) {
    if (!socketState || !socketState.open || !socketState.socket) return false;
    try {
      socketState.socket.write(`${JSON.stringify(message || {})}\n`);
      return true;
    } catch (_) {
      return false;
    }
  }

  _handleConnection(socket, details = {}) {
    const socketId = makeId('hwsock');
    const socketState = {
      socketId,
      socket,
      open: true,
      buffer: '',
      peerId: '',
      topicIds: new Set(this._resolveTopicIds(details)),
    };
    this.socketStates.set(socketId, socketState);

    socket.on('data', (chunk) => {
      const state = this._getSocketStateById(socketId);
      if (!state) return;
      state.buffer += Buffer.isBuffer(chunk) ? chunk.toString('utf8') : String(chunk || '');
      let idx = state.buffer.indexOf('\n');
      while (idx >= 0) {
        const line = state.buffer.slice(0, idx).trim();
        state.buffer = state.buffer.slice(idx + 1);
        if (line) {
          try {
            const parsed = JSON.parse(line);
            this._handleIncomingMessage(socketId, parsed);
          } catch (_) {
            // ignore malformed packets
          }
        }
        idx = state.buffer.indexOf('\n');
      }
    });

    socket.on('error', (err) => {
      this._setError(err && err.message ? err.message : 'Peer socket error.');
    });

    socket.on('close', () => {
      this._dropSocket(socketId);
    });

    this._sendRaw(socketState, this._buildHelloMessage());
  }

  _dropSocket(socketId) {
    const state = this._getSocketStateById(socketId);
    if (!state) return;
    state.open = false;
    if (state.peerId) {
      const peerState = this.peerStates.get(state.peerId);
      if (peerState) {
        peerState.sockets.delete(socketId);
        if (!this._hasOpenSocket(peerState)) {
          peerState.publicIndex = [];
          peerState.topicIds = new Set();
        }
      }
    }
    this.socketStates.delete(socketId);
    this.emit('status', this.getStatus());
  }

  _registerPeerSocket(socketId, peerId, displayName = '', topicIds = []) {
    const socketState = this._getSocketStateById(socketId);
    if (!socketState) return null;
    socketState.peerId = peerId;
    (Array.isArray(topicIds) ? topicIds : []).forEach((topicId) => {
      const clean = normalizeText(topicId);
      if (clean) socketState.topicIds.add(clean);
    });
    const peerState = this._ensurePeerState(peerId);
    if (!peerState) return null;
    peerState.displayName = normalizeText(displayName) || peerState.displayName || peerId;
    peerState.sockets.add(socketId);
    socketState.topicIds.forEach((topicId) => peerState.topicIds.add(topicId));
    peerState.lastSeenAt = nowTs();
    return peerState;
  }

  _handleIncomingMessage(socketId, message) {
    const msg = (message && typeof message === 'object') ? message : null;
    if (!msg) return;
    const type = normalizeText(msg.type);
    const socketState = this._getSocketStateById(socketId);
    if (!socketState) return;

    if (type === HELLO_MESSAGE_TYPE) {
      const peerId = normalizeText(msg.identity_id || msg.peer_id);
      if (!peerId || peerId === this.peerId) return;
      const peerState = this._registerPeerSocket(socketId, peerId, msg.display_name, msg.topic_ids);
      if (!peerState) return;
      this.emit('peer_seen', {
        peer_id: peerId,
        display_name: peerState.displayName,
      });
      this.emit('status', this.getStatus());
      return;
    }

    if (type === TOPIC_MESSAGE_TYPE) {
      const peerId = normalizeText(msg.peer_id || socketState.peerId);
      if (!peerId) return;
      const peerState = this._registerPeerSocket(socketId, peerId, msg.display_name, msg.topic_ids);
      if (!peerState) return;
      peerState.lastSeenAt = nowTs();
      this.emit('status', this.getStatus());
      return;
    }

    const peerId = normalizeText(socketState.peerId || msg.peer_id);
    if (!peerId) return;
    this._handleProtocolMessage(peerId, msg);
  }

  async connect(identity = null) {
    if (identity) this.setIdentity(identity);
    if (!this.enabled) return { ok: true, status: this.getStatus(), message: 'Hyperweb is disabled in settings.' };
    if (!this.peerId) {
      this._setError('Hyperweb identity is missing.');
      return { ok: false, status: this.getStatus(), message: 'Hyperweb identity is missing.' };
    }

    await this.refreshLocalPublicIndex();
    await this._ensureSwarm();
    await this.joinTopic(this.publicTopicId);
    this.connected = true;
    this.lastError = '';
    await this.announcePublicIndex();
    this.emit('status', this.getStatus());
    return { ok: true, status: this.getStatus() };
  }

  async joinTopic(topicId) {
    const cleanTopicId = normalizeText(topicId);
    if (!cleanTopicId) return { ok: false, message: 'topic_id is required.' };
    await this._ensureSwarm();
    const existing = this.joinedTopics.get(cleanTopicId);
    if (existing) return { ok: true, topic_id: cleanTopicId, joined: false };
    const key = topicBuffer(cleanTopicId);
    const discovery = this.swarm.join(key, { server: true, client: true });
    if (discovery && typeof discovery.flushed === 'function') {
      await discovery.flushed();
    }
    this.joinedTopics.set(cleanTopicId, { topicId: cleanTopicId, key, discovery });
    this._broadcastTopicUpdate();
    this.emit('status', this.getStatus());
    return { ok: true, topic_id: cleanTopicId, joined: true };
  }

  async leaveTopic(topicId) {
    const cleanTopicId = normalizeText(topicId);
    const existing = this.joinedTopics.get(cleanTopicId);
    if (!existing) return { ok: true, topic_id: cleanTopicId, left: false };
    try {
      if (existing.discovery && typeof existing.discovery.destroy === 'function') {
        await existing.discovery.destroy();
      }
    } catch (_) {
      // noop
    }
    this.joinedTopics.delete(cleanTopicId);
    this._broadcastTopicUpdate();
    this.emit('status', this.getStatus());
    return { ok: true, topic_id: cleanTopicId, left: true };
  }

  disconnect() {
    for (const state of this.socketStates.values()) {
      try {
        if (state.socket) state.socket.destroy();
      } catch (_) {
        // noop
      }
    }
    this.socketStates.clear();
    this.peerStates.clear();
    const swarm = this.swarm;
    this.swarm = null;
    this.joinedTopics.clear();
    this.connected = false;
    this.lastError = '';
    if (swarm) {
      swarm.destroy().catch(() => {});
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
      identity_id: this.identity && this.identity.identity_id ? this.identity.identity_id : this.peerId,
      display_name: this.identity && this.identity.display_name ? this.identity.display_name : this.peerId,
      index: this.localIndexMeta,
      ts: nowTs(),
    };
    this.broadcastProtocol(payload);
    return { ok: true, index_count: this.localIndexMeta.length };
  }

  _broadcastTopicUpdate() {
    const message = {
      type: TOPIC_MESSAGE_TYPE,
      peer_id: this.peerId,
      display_name: this.identity && this.identity.display_name ? this.identity.display_name : this.peerId,
      topic_ids: Array.from(this.joinedTopics.keys()),
      ts: nowTs(),
    };
    for (const socketState of this.socketStates.values()) {
      this._sendRaw(socketState, message);
    }
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
    this.broadcastProtocol({
      type: 'hyperweb:query',
      query_id: queryId,
      query,
      limit,
      requester_peer_id: this.peerId,
      ts: nowTs(),
    });

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

    const peerId = normalizeText(suggestion.transport_peer_id || suggestion.peer_id);
    const referenceId = normalizeText(suggestion.reference_id);
    if (!peerId || !referenceId) {
      return { ok: false, message: 'Selected suggestion does not include source peer metadata.' };
    }

    const fetchId = makeId('hwfetch');
    const timeoutMs = Math.max(1800, Math.min(20000, Number(options.timeout_ms || 14000) || 14000));
    const waitPromise = new Promise((resolve) => {
      const timer = setTimeout(() => {
        this.pendingFetches.delete(fetchId);
        resolve(null);
      }, timeoutMs);
      this.pendingFetches.set(fetchId, {
        resolve,
        timer,
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

  _pickPeerSocket(peerId, topicId = '') {
    const peerState = this.peerStates.get(normalizeText(peerId));
    if (!peerState) return null;
    const preferredTopic = normalizeText(topicId);
    let fallback = null;
    for (const socketId of peerState.sockets) {
      const socketState = this._getSocketStateById(socketId);
      if (!socketState || !socketState.open) continue;
      if (preferredTopic && socketState.topicIds.has(preferredTopic)) return socketState;
      if (!fallback) fallback = socketState;
    }
    return fallback;
  }

  _sendProtocolToPeer(peerId, message, options = {}) {
    const socketState = this._pickPeerSocket(peerId, options.topic_id);
    if (!socketState) return false;
    return this._sendRaw(socketState, message);
  }

  _broadcastProtocol(message, options = {}) {
    const topicId = normalizeText(options.topic_id);
    const sentPeers = new Set();
    for (const socketState of this.socketStates.values()) {
      if (!socketState || !socketState.open || !socketState.peerId) continue;
      if (topicId && !socketState.topicIds.has(topicId)) continue;
      if (sentPeers.has(socketState.peerId)) continue;
      sentPeers.add(socketState.peerId);
      this._sendRaw(socketState, message);
    }
  }

  sendProtocolToPeer(peerId, message, options = {}) {
    return this._sendProtocolToPeer(peerId, message, options);
  }

  broadcastProtocol(message, options = {}) {
    this._broadcastProtocol(message, options);
    return { ok: true };
  }

  broadcastTopicProtocol(topicId, message) {
    this._broadcastProtocol(message, { topic_id: topicId });
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
}

module.exports = {
  HyperwebManager,
};
