const { app, BrowserWindow, BrowserView, Menu, ipcMain, dialog, shell, nativeImage, safeStorage } = require('electron');
const path = require('path');
const fs = require('fs');
const os = require('os');
const crypto = require('crypto');
const { execFileSync } = require('child_process');
const { pathToFileURL } = require('url');

const { handleChat, parseChatCommand, executeAgenticLoop, PROGRAMMATIC_TOOL_NAMES } = require('./runtime/agent_runtime');
const { chatWithProvider, callProviderWithTools } = require('./runtime/provider_chat');
const { scoreReferencesHybrid, buildReferenceSearchText } = require('./runtime/pathc_similarity');
const { searchLocalEvidence } = require('./runtime/local_evidence_search');
const { getPathCScopedReferences, buildPathCHarnessPayload } = require('./runtime/pathc_harness');
const { LuminoCrawler } = require('./runtime/lumino_crawler');
const { HyperwebManager } = require('./runtime/hyperweb_manager');
const { indexFolderAsContext } = require('./runtime/file_indexer');
const { PythonSandboxManager, cleanupStaleSandboxes, spawnPythonInteractiveProcess } = require('./runtime/python_sandbox');
const { installAllowedPackages } = require('./runtime/python_packages');
const { createPythonRuntimeResolver } = require('./runtime/python_runtime');
const { TrustCommonsSyncBridge, isLoopbackUrl } = require('./runtime/trustcommons_sync_bridge');
const keychain = require('./runtime/keychain');
const trustCommonsIdentity = require('./runtime/trustcommons_identity');
const { createSecureSecretStore } = require('./runtime/secure_secret_store');
const { TelegramService } = require('./runtime/telegram_service');
const { createOrchestratorJobsStore } = require('./runtime/orchestrator_jobs_store');
const { createOrchestratorScheduler } = require('./runtime/orchestrator_scheduler');
const { createPathAExecutor } = require('./runtime/lumino_path_a');
const { createPathBExecutor } = require('./runtime/lumino_path_b');
const { isPathAToolAllowed, isPathBToolAllowed } = require('./runtime/agent_toolsets');
const { createOrchestratorSessionStore } = require('./runtime/orchestrator_session_store');
const { createOrchestratorUsersStore } = require('./runtime/orchestrator_users_store');
const { createOrchestratorPreferencesStore } = require('./runtime/orchestrator_preferences_store');

const APP_NAME = 'Subgrapher';
const STORE_FILENAME = 'semantic_references.json';
const MAX_BROWSER_TABS_PER_REFERENCE = 10;
const MAX_RUNTIME_BROWSER_TABS = 120;
const MAX_CHAT_MESSAGES = 120;
const MAX_HIGHLIGHTS = 800;
const BROWSER_VIEW_PRELOAD_PATH = path.join(__dirname, 'browser_view_preload.js');
const PROVIDERS = ['openai', 'cerebras', 'google', 'anthropic', 'lmstudio'];
const APP_LOGO_PATH = path.join(__dirname, 'subgrapher_logo.jpg');
const APP_ICON_PNG_PATH = path.join(__dirname, 'assets', 'icons', 'app-icon-1024.png');
const APP_ICON_ICO_PATH = path.join(__dirname, 'assets', 'icons', 'app-icon.ico');
const APP_ICON_ICNS_PATH = path.join(__dirname, 'assets', 'icons', 'app-icon.icns');
const MAX_IMPORTED_TABS = 120;
const PUBLIC_FEED_FILENAME = 'public_references_feed.json';
const APP_SETTINGS_FILENAME = 'app_settings.json';
const GLOBAL_SKILLS_FILENAME = 'global_skills.json';
const LOCAL_SKILLS_FILENAME = 'local_skills.json';
const PRIVATE_HISTORY_FILENAME = 'private_history.json';
const HYPERWEB_PUBLIC_SNAPSHOTS_FILENAME = 'hyperweb_public_snapshots.json';
const HYPERWEB_PRIVATE_SHARES_FILENAME = 'hyperweb_private_shares.json';
const DEFAULT_HYPERWEB_RELAY_URL = 'https://relay.thetrustcommons.com';
const TRUSTCOMMONS_DOWNLOAD_URL = 'https://www.thetrustcommons.com/';
const TRUSTCOMMONS_BUNDLE_ID = 'com.trustcommons.desktop';
const TRUSTCOMMONS_SYNC_SECRET_ACCOUNT = 'local_sync_secret';
const TRUSTCOMMONS_SYNC_SECRET_SERVICE = 'com.trustcommons.local-sync';
const HYPERWEB_IDENTITY_PRIVATE_KEY_ACCOUNT = 'hyperweb_identity_private_key';
const HYPERWEB_IDENTITY_SERVICE = 'com.subgrapher.hyperweb.identity';
const HYPERWEB_IDENTITY_DER_PREFIX = 'ed25519-pkcs8-der:';
const TRUSTCOMMONS_SYNC_DEFAULT_PORT = 42631;
const TRUSTCOMMONS_SYNC_DEFAULT_PEER_URL = 'http://127.0.0.1:42641';
const TRUSTCOMMONS_SYNC_DEFAULT_INTERVAL_SEC = 8;
const HYPERWEB_SOCIAL_FILENAME = 'hyperweb_social.json';
const HYPERWEB_INVITE_PROTO = 'subgrapher';
const HYPERWEB_INVITE_ROUTE = 'hyperweb-invite';
const HYPERWEB_INVITE_EXPIRY_MS = 7 * 24 * 60 * 60 * 1000;
const HYPERWEB_MODERATION_HIDE_SCORE = -12;
const HYPERWEB_MODERATION_HIDE_MIN_DOWNVOTERS = 8;
const HYPERWEB_SNAPSHOT_DAILY_LIMIT = 5;
const HYPERWEB_SNAPSHOT_MONTHLY_LIMIT = 30;
const HYPERWEB_SNAPSHOT_RETENTION_MS = 180 * 24 * 60 * 60 * 1000;
const HYPERWEB_SEARCH_MAX_RESULTS = 80;
const HISTORY_DEFAULT_MAX_ENTRIES = 5000;
const HISTORY_RECENT_DUP_WINDOW_MS = 2 * 60 * 1000;
const HISTORY_EMBED_DIM = 96;
const SETTINGS_EDITABLE_KEYS = new Set([
  'default_search_engine',
  'lumino_last_provider',
  'lumino_last_model',
  'hyperweb_enabled',
  'hyperweb_relay_url',
  'trustcommons_sync_enabled',
  'trustcommons_sync_port',
  'trustcommons_peer_sync_url',
  'trustcommons_sync_interval_sec',
  'crawler_mode',
  'crawler_markdown_first',
  'crawler_robots_default',
  'crawler_depth_default',
  'crawler_page_cap_default',
  'agent_mode_v1_enabled',
  'trustcommons_download_url',
  'trustcommons_app_bundle_id',
  'history_enabled',
  'history_max_entries',
  'telegram_enabled',
  'telegram_allowed_chat_ids',
  'telegram_allowed_usernames',
  'telegram_poll_interval_sec',
  'lmstudio_base_url',
  'lmstudio_default_model',
  'orchestrator_web_provider',
]);
const CONTEXT_FILE_MAX_BYTES = 1024 * 1024;
const CHAT_REQUEST_TIMEOUT_MS = 60_000;
const DECISION_TRACE_MAX_STEPS = 240;
const GRAPH_MAX_NODES = 600;
const GRAPH_MAX_EDGES = 1200;
const AGENTIC_MAX_TURNS = 8;
const AGENT_MODE_SUPPORTED_PROVIDERS = ['openai', 'anthropic', 'cerebras', 'lmstudio'];
const PYTHON_EXEC_TIMEOUT_MS = 10_000;
const PACKAGED_PYTHON_IMMUTABLE_MESSAGE = 'Python package installs are disabled in installed builds. Allowlisted packages are prebundled; reinstall or update Subgrapher if packages are missing.';
const YOUTUBE_TRANSCRIPT_MAX_CHARS = 60_000;
const YOUTUBE_TRANSCRIPT_SUMMARY_MAX_CHARS = 1200;
const YOUTUBE_TRANSCRIPTS_MAX_ITEMS = 40;
const YOUTUBE_TRANSCRIPT_RETRY_COOLDOWN_MS = 6 * 60 * 60 * 1000;
const PUBLIC_REFERENCE_SUMMARY_MAX_CHARS = 520;
const PUBLIC_REFERENCE_SUMMARY_TIMEOUT_MS = 18_000;
const BROWSER_VIEW_MIN_ZOOM = 0.25;
const BROWSER_VIEW_MAX_ZOOM = 5;
const BROWSER_VIEW_DEFAULT_ZOOM = 1;
const SHORTCUT_COMMAND_ALLOWLIST = new Set([
  'web_zoom_in',
  'web_zoom_out',
  'web_zoom_reset',
  'ui_zoom_in',
  'ui_zoom_out',
  'ui_zoom_reset',
  'toggle_zen',
]);
const PROVIDER_SUMMARY_MODEL_FALLBACK = {
  openai: 'gpt-4o-mini',
  cerebras: 'llama-3.3-70b',
  anthropic: 'claude-3-7-sonnet-latest',
  google: 'gemini-2.0-flash',
  lmstudio: 'local-model',
};
const PROVIDER_PRIMARY_KEY_ID = 'primary';
const LMSTUDIO_DEFAULT_BASE_URL = 'http://127.0.0.1:1234';
const ORCHESTRATOR_WEB_PROVIDER_DEFAULT = 'ddg';
const PATH_B_GLOBAL_TOP_K = 12;
const PATH_B_LINK_VERIFY_THRESHOLD = 0.42;
const TELEGRAM_SECRET_REF_PREFIX = 'telegram_bot_token';
const LMSTUDIO_SECRET_REF_PREFIX = 'lmstudio_token';
const ORCHESTRATOR_WEB_SECRET_REF_PREFIX = 'orchestrator_web_key';
const APP_SECRET_SERVICE = 'com.subgrapher.secure-secrets';
const MEMORY_VERSION = 1;
const MEMORY_MAX_SEMANTIC = 50;
const MEMORY_MAX_DETERMINISTIC = 67;
const MEMORY_MAX_TOTAL = 117;
const MEMORY_SEMANTIC_INTERVAL_MS = 10 * 60 * 1000;
const MEMORY_SEMANTIC_COOLDOWN_MS = 6 * 60 * 60 * 1000;
const MEMORY_SEMANTIC_THRESHOLD = 0.62;
const MEMORY_SEMANTIC_FORCE_THRESHOLD = 0.8;
const MEMORY_UPDATED_LOOKBACK_MS = 24 * 60 * 60 * 1000;
const MEMORY_DETERMINISTIC_TARGETS = [
  { bucket_key: 'age_1d', target_age_ms: 24 * 60 * 60 * 1000 },
  { bucket_key: 'age_15d', target_age_ms: 15 * 24 * 60 * 60 * 1000 },
  ...Array.from({ length: 60 }).map((_, idx) => ({
    bucket_key: `month_${idx + 1}`,
    target_age_ms: (idx + 1) * 30 * 24 * 60 * 60 * 1000,
  })),
];
const MEMORY_SNAPSHOT_VOLATILE_KEYS = new Set([
  'updated_at',
  'last_used_at',
  'last_active',
  'snapshot_at',
  'visited_at',
  'last_message_at',
  'last_opened_at',
]);

let mainWindow = null;
let browserView = null;
let historyPreviewView = null;
let browserViewBounds = null;
let historyPreviewBounds = null;
let browserViewVisible = false;
let historyPreviewVisible = false;
let browserViewAudible = false;
const runtimeBrowserTabs = new Map(); // runtimeTabId -> { view, url, title, favicon, loading, audible, last_active }
let activeRuntimeBrowserTabId = null;
let markerModeEnabled = false;
let markerContext = { srId: '', artifactId: '' };
let memorySemanticTimer = null;
const activeChatRequests = new Map();
const pythonSandboxManagers = new Map();
let pythonRuntimeResolver = null;
let secureSecretStore = null;
let telegramService = null;
let orchestratorJobsStore = null;
let orchestratorScheduler = null;
let pathAExecutor = null;
let pathBExecutor = null;
let orchestratorSessionStore = null;
let orchestratorUsersStore = null;
let orchestratorPreferencesStore = null;
const pathBMetrics = {
  pathb_reuse_count: 0,
  pathb_create_count: 0,
  pathb_verified_url_count: 0,
};
const orchestratorReferenceLockMap = new Map();
const telegramPendingRegistrations = new Map();
const luminoCrawler = new LuminoCrawler({
  logger: console,
  defaultDepth: 3,
  defaultPageCap: 80,
  markdownFirst: true,
});
let trustCommonsRuntime = {
  bootstrapComplete: false,
  identity: null,
  connected: false,
  lastError: '',
  launched: false,
  launchMethod: '',
  downloadOpened: false,
  sync: {
    running: false,
    port: TRUSTCOMMONS_SYNC_DEFAULT_PORT,
    peer_url: '',
    last_sync_at: 0,
    last_sync_error: '',
  },
};
let pendingInviteToken = '';
let hyperwebSocialState = null;
const hyperwebManager = new HyperwebManager({
  relayUrl: DEFAULT_HYPERWEB_RELAY_URL,
  enabled: true,
  logger: console,
});
hyperwebManager.on('status', (status) => {
  trustCommonsRuntime.connected = !!(status && status.connected);
  if (status && status.last_error) {
    trustCommonsRuntime.lastError = String(status.last_error || '');
  }
});
hyperwebManager.on('protocol', (packet) => {
  const message = packet && packet.message && typeof packet.message === 'object' ? packet.message : {};
  const type = String(message.type || '').trim().toLowerCase();
  if (!type.startsWith('hyperweb:social_') && type !== 'hyperweb:invite_handshake') return;
  if (type === 'hyperweb:invite_handshake') {
    const payload = (message.payload && typeof message.payload === 'object') ? message.payload : {};
    const fingerprint = String(payload.fingerprint || '').trim().toUpperCase();
    if (!fingerprint) return;
    const state = ensureHyperwebSocialState();
    state.known_peers[fingerprint] = {
      fingerprint,
      alias: String(payload.alias || fingerprint),
      pubkey: String(payload.pubkey || ''),
      addresses: Array.isArray(payload.addresses) ? payload.addresses : [],
      updated_at: nowTs(),
    };
    writeHyperwebSocialState(state);
    hyperwebSocialState = state;
    return;
  }
  const event = (message.event && typeof message.event === 'object') ? message.event : null;
  if (!event) return;
  const signedPayload = {
    event_id: String(event.event_id || ''),
    type: String(event.type || ''),
    ts: Number(event.ts || 0),
    signer_pubkey: String(event.signer_pubkey || ''),
    signer_fingerprint: String(event.signer_fingerprint || '').toUpperCase(),
    payload: (event.payload && typeof event.payload === 'object') ? event.payload : {},
  };
  const signature = String(event.signature || '').trim();
  if (!signedPayload.event_id || !signedPayload.signer_pubkey || !signature) return;
  const verified = verifyHyperwebPayload(signedPayload, signature, signedPayload.signer_pubkey);
  if (!verified) return;
  appendHyperwebSocialEvent({
    ...signedPayload,
    signature,
  }, { skipBroadcast: true });
});
const trustCommonsSyncBridge = new TrustCommonsSyncBridge({
  logger: console,
  secretProvider: () => getTrustCommonsSyncSecret(),
  refsProvider: () => getSyncEligibleReferences(),
  refsConsumer: (incomingRefs, context) => mergeSyncedReferences(incomingRefs, context),
});
trustCommonsSyncBridge.on('status', (status) => {
  trustCommonsRuntime.sync = {
    running: !!(status && status.running),
    port: Number((status && status.port) || 0),
    peer_url: String((status && status.peer_url) || ''),
    last_sync_at: Number((status && status.last_sync_at) || 0),
    last_sync_error: String((status && status.last_sync_error) || ''),
  };
});

luminoCrawler.on('job_update', (event) => {
  const payload = (event && typeof event === 'object') ? event : {};
  const phase = String(payload.phase || '').trim().toLowerCase();
  if (phase === 'completed' || phase === 'stopped') {
    const job = payload.job || {};
    const srId = String((job && job.sr_id) || '').trim();
    if (srId) {
      ingestCrawlerPagesIntoReference(srId, Array.isArray(payload.result_pages) ? payload.result_pages : [], job)
        .then((result) => {
          sendBrowserEvent('browser:crawlerStream', {
            phase: 'ingested',
            job,
            ingest: result || { ok: false, message: 'Ingestion result unavailable.' },
          });
        })
        .catch((err) => {
          sendBrowserEvent('browser:crawlerStream', {
            phase: 'ingest_error',
            job,
            message: String((err && err.message) || 'Crawler ingest failed.'),
          });
        });
    }
  }
  sendBrowserEvent('browser:crawlerStream', payload);
});

function nowTs() {
  return Date.now();
}

function makeId(prefix) {
  return `${prefix}_${crypto.randomUUID()}`;
}

function deepClone(value) {
  return JSON.parse(JSON.stringify(value));
}

function memoryDefaultState() {
  return {
    enabled: false,
    checkpoints: [],
    recent_event_cursor: 0,
    last_semantic_eval_at: 0,
    last_semantic_signature: '',
    version: MEMORY_VERSION,
  };
}

function ensureReferenceMemory(ref) {
  if (!ref || typeof ref !== 'object') return memoryDefaultState();
  const existing = (ref.memory && typeof ref.memory === 'object') ? ref.memory : {};
  const normalized = {
    enabled: !!existing.enabled,
    checkpoints: Array.isArray(existing.checkpoints) ? existing.checkpoints : [],
    recent_event_cursor: Number.isFinite(Number(existing.recent_event_cursor))
      ? Math.max(0, Math.round(Number(existing.recent_event_cursor)))
      : 0,
    last_semantic_eval_at: Number(existing.last_semantic_eval_at || 0),
    last_semantic_signature: String(existing.last_semantic_signature || ''),
    version: MEMORY_VERSION,
  };
  ref.memory = normalized;
  return normalized;
}

function buildReferenceSnapshot(ref) {
  const target = ref && typeof ref === 'object' ? ref : {};
  return deepClone({
    id: String(target.id || ''),
    title: String(target.title || ''),
    title_user_edited: !!target.title_user_edited,
    intent: String(target.intent || ''),
    tags: Array.isArray(target.tags) ? target.tags : [],
    tabs: Array.isArray(target.tabs) ? target.tabs : [],
    active_tab_id: String(target.active_tab_id || ''),
    artifacts: Array.isArray(target.artifacts) ? target.artifacts : [],
    highlights: Array.isArray(target.highlights) ? target.highlights : [],
    context_files: Array.isArray(target.context_files) ? target.context_files : [],
    folder_mounts: Array.isArray(target.folder_mounts) ? target.folder_mounts : [],
    youtube_transcripts: (target.youtube_transcripts && typeof target.youtube_transcripts === 'object') ? target.youtube_transcripts : {},
    reference_graph: (target.reference_graph && typeof target.reference_graph === 'object') ? target.reference_graph : { nodes: [], edges: [] },
    agent_weights: (target.agent_weights && typeof target.agent_weights === 'object') ? target.agent_weights : {},
    decision_trace: Array.isArray(target.decision_trace) ? target.decision_trace : [],
    program: String(target.program || ''),
    skills: Array.isArray(target.skills) ? target.skills : [],
    chat_thread: (target.chat_thread && typeof target.chat_thread === 'object') ? target.chat_thread : { messages: [], last_message_at: null },
    visibility: String(target.visibility || 'private'),
    pinned_root: !!target.pinned_root,
    relation_type: String(target.relation_type || ''),
    parent_id: target.parent_id ? String(target.parent_id) : null,
    lineage: Array.isArray(target.lineage) ? target.lineage : [],
    updated_at: Number(target.updated_at || nowTs()),
    created_at: Number(target.created_at || nowTs()),
    last_used_at: Number(target.last_used_at || nowTs()),
  });
}

function buildSnapshotStats(snapshot = {}) {
  const tabs = Array.isArray(snapshot.tabs) ? snapshot.tabs : [];
  const artifacts = Array.isArray(snapshot.artifacts) ? snapshot.artifacts : [];
  const graph = (snapshot.reference_graph && typeof snapshot.reference_graph === 'object') ? snapshot.reference_graph : {};
  return {
    tab_count: tabs.length,
    artifact_count: artifacts.length,
    highlight_count: Array.isArray(snapshot.highlights) ? snapshot.highlights.length : 0,
    context_file_count: Array.isArray(snapshot.context_files) ? snapshot.context_files.length : 0,
    chat_count: Array.isArray(snapshot.chat_thread && snapshot.chat_thread.messages) ? snapshot.chat_thread.messages.length : 0,
    graph_nodes: Array.isArray(graph.nodes) ? graph.nodes.length : 0,
    graph_edges: Array.isArray(graph.edges) ? graph.edges.length : 0,
  };
}

function buildReferenceDiffSummary(prevSnapshot = null, nextSnapshot = null) {
  const prev = prevSnapshot || {};
  const next = nextSnapshot || {};
  const prevStats = buildSnapshotStats(prev);
  const nextStats = buildSnapshotStats(next);
  return {
    from_updated_at: Number(prev.updated_at || 0),
    to_updated_at: Number(next.updated_at || 0),
    tab_delta: nextStats.tab_count - prevStats.tab_count,
    artifact_delta: nextStats.artifact_count - prevStats.artifact_count,
    highlight_delta: nextStats.highlight_count - prevStats.highlight_count,
    context_file_delta: nextStats.context_file_count - prevStats.context_file_count,
    chat_delta: nextStats.chat_count - prevStats.chat_count,
    graph_node_delta: nextStats.graph_nodes - prevStats.graph_nodes,
    graph_edge_delta: nextStats.graph_edges - prevStats.graph_edges,
  };
}

function summarizeMemorySnapshot(snapshot = {}) {
  const stats = buildSnapshotStats(snapshot);
  const title = String(snapshot.title || 'Reference').trim() || 'Reference';
  return `${title} · tabs ${stats.tab_count} · artifacts ${stats.artifact_count} · notes ${stats.highlight_count}`;
}

function normalizeSnapshotForDedup(value) {
  if (Array.isArray(value)) {
    return value.map((item) => normalizeSnapshotForDedup(item));
  }
  if (!value || typeof value !== 'object') return value;
  const out = {};
  Object.keys(value).forEach((key) => {
    if (MEMORY_SNAPSHOT_VOLATILE_KEYS.has(key)) return;
    out[key] = normalizeSnapshotForDedup(value[key]);
  });
  return out;
}

function buildSnapshotHash(snapshot = {}) {
  try {
    const normalized = normalizeSnapshotForDedup(snapshot && typeof snapshot === 'object' ? snapshot : {});
    return crypto.createHash('sha1').update(JSON.stringify(normalized)).digest('hex');
  } catch (_) {
    return '';
  }
}

function getCheckpointSnapshotHash(checkpoint = {}) {
  const existing = String(checkpoint && checkpoint.snapshot_hash ? checkpoint.snapshot_hash : '').trim();
  if (existing) return existing;
  return buildSnapshotHash((checkpoint && checkpoint.snapshot) || {});
}

function memoryNormalizeText(value) {
  return String(value || '')
    .toLowerCase()
    .replace(/https?:\/\/[^\s]+/g, ' ')
    .replace(/[^a-z0-9\s]/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function buildSemanticSignatureFromSnapshot(snapshot = {}) {
  const tabs = Array.isArray(snapshot.tabs) ? snapshot.tabs : [];
  const artifacts = Array.isArray(snapshot.artifacts) ? snapshot.artifacts : [];
  const highlights = Array.isArray(snapshot.highlights) ? snapshot.highlights : [];
  const program = String(snapshot.program || '');
  const parts = [
    String(snapshot.title || ''),
    String(snapshot.intent || ''),
    Array.isArray(snapshot.tags) ? snapshot.tags.join(' ') : '',
    tabs.map((tab) => `${tab && tab.title ? tab.title : ''} ${tab && tab.url ? tab.url : ''}`).join(' '),
    artifacts.map((artifact) => `${artifact && artifact.title ? artifact.title : ''} ${artifact && artifact.content ? String(artifact.content).slice(0, 2200) : ''}`).join(' '),
    highlights.map((item) => String(item && item.text || '')).join(' '),
    program,
  ];
  return memoryNormalizeText(parts.join(' '));
}

function tokenizeSignature(signature = '') {
  return new Set(String(signature || '').split(' ').filter(Boolean).slice(0, 1200));
}

function jaccardDistance(aSig = '', bSig = '') {
  const a = tokenizeSignature(aSig);
  const b = tokenizeSignature(bSig);
  if (!a.size && !b.size) return 0;
  const [small, big] = a.size <= b.size ? [a, b] : [b, a];
  let intersection = 0;
  for (const token of small) {
    if (big.has(token)) intersection += 1;
  }
  const union = a.size + b.size - intersection;
  if (union <= 0) return 0;
  return Math.max(0, Math.min(1, 1 - (intersection / union)));
}

function structuralDeltaScore(prevSnapshot = null, nextSnapshot = null) {
  const prev = buildSnapshotStats(prevSnapshot || {});
  const next = buildSnapshotStats(nextSnapshot || {});
  const weighted = (
    Math.abs(next.tab_count - prev.tab_count) * 0.9
    + Math.abs(next.artifact_count - prev.artifact_count) * 1.1
    + Math.abs(next.highlight_count - prev.highlight_count) * 0.5
    + Math.abs(next.context_file_count - prev.context_file_count) * 0.7
    + Math.abs(next.chat_count - prev.chat_count) * 0.4
    + Math.abs(next.graph_nodes - prev.graph_nodes) * 0.5
    + Math.abs(next.graph_edges - prev.graph_edges) * 0.4
  );
  return Math.max(0, Math.min(1, weighted / 40));
}

function getLatestCheckpoint(memory, kind = '') {
  const list = Array.isArray(memory && memory.checkpoints) ? memory.checkpoints : [];
  const targetKind = String(kind || '').trim().toLowerCase();
  const filtered = targetKind ? list.filter((item) => String((item && item.kind) || '').toLowerCase() === targetKind) : list;
  if (!filtered.length) return null;
  return filtered.reduce((best, item) => {
    if (!best) return item;
    return Number(item.created_at || 0) > Number(best.created_at || 0) ? item : best;
  }, null);
}

function buildDeterministicCheckpoint(ref, memory, bucketKey, options = {}) {
  const now = nowTs();
  const snapshot = (options && options.snapshot && typeof options.snapshot === 'object')
    ? deepClone(options.snapshot)
    : buildReferenceSnapshot(ref);
  const snapshotHash = String(options && options.snapshot_hash ? options.snapshot_hash : '').trim() || buildSnapshotHash(snapshot);
  const seedMetadata = (options && typeof options === 'object') ? { ...options } : {};
  delete seedMetadata.snapshot;
  delete seedMetadata.snapshot_hash;
  const prev = getLatestCheckpoint(memory, '');
  return {
    id: makeId('mem'),
    kind: 'periodic',
    bucket_key: String(bucketKey || ''),
    created_at: now,
    source_reference_id: String(ref.id || ''),
    source_reference_updated_at: Number(ref.updated_at || now),
    summary: summarizeMemorySnapshot(snapshot),
    semantic_score: null,
    structural_delta: buildReferenceDiffSummary(prev && prev.snapshot ? prev.snapshot : null, snapshot),
    snapshot_hash: snapshotHash,
    snapshot,
    diff_from_prev: buildReferenceDiffSummary(prev && prev.snapshot ? prev.snapshot : null, snapshot),
    seed_metadata: seedMetadata,
  };
}

function upsertDeterministicBucket(memory, checkpoint, targetAgeMs = null) {
  memory.checkpoints = Array.isArray(memory.checkpoints) ? memory.checkpoints : [];
  const key = String((checkpoint && checkpoint.bucket_key) || '').trim();
  if (!key) return;
  const idx = memory.checkpoints.findIndex((item) => item && item.kind === 'periodic' && String(item.bucket_key || '') === key);
  if (idx < 0) {
    memory.checkpoints.push(checkpoint);
    return;
  }
  const current = memory.checkpoints[idx];
  if (!Number.isFinite(Number(targetAgeMs))) {
    memory.checkpoints[idx] = checkpoint;
    return;
  }
  const now = nowTs();
  const currentCloseness = Math.abs((now - Number(current.created_at || now)) - targetAgeMs);
  const nextCloseness = Math.abs((now - Number(checkpoint.created_at || now)) - targetAgeMs);
  if (nextCloseness <= currentCloseness) {
    memory.checkpoints[idx] = checkpoint;
  }
}

function compactMemoryCheckpoints(memory) {
  memory.checkpoints = Array.isArray(memory.checkpoints) ? memory.checkpoints : [];
  const periodic = memory.checkpoints
    .filter((item) => item && item.kind === 'periodic')
    .sort((a, b) => Number(b.created_at || 0) - Number(a.created_at || 0))
    .slice(0, MEMORY_MAX_DETERMINISTIC);
  const semantic = memory.checkpoints
    .filter((item) => item && item.kind === 'semantic')
    .sort((a, b) => Number(b.created_at || 0) - Number(a.created_at || 0))
    .slice(0, MEMORY_MAX_SEMANTIC);
  memory.checkpoints = [...periodic, ...semantic]
    .sort((a, b) => Number(b.created_at || 0) - Number(a.created_at || 0))
    .slice(0, MEMORY_MAX_TOTAL);
}

function capturePeriodicMemoryCheckpoints(ref) {
  const memory = ensureReferenceMemory(ref);
  if (!memory.enabled) return;
  const snapshot = buildReferenceSnapshot(ref);
  const snapshotHash = buildSnapshotHash(snapshot);
  const existingForUpdatedAt = (memory.checkpoints || []).some((item) => (
    item
    && item.kind === 'periodic'
    && Number(item.source_reference_updated_at || 0) === Number(ref.updated_at || 0)
  ));
  if (existingForUpdatedAt) return;
  const latestPeriodic = getLatestCheckpoint(memory, 'periodic');
  if (latestPeriodic && getCheckpointSnapshotHash(latestPeriodic) === snapshotHash) return;

  const cursor = Number(memory.recent_event_cursor || 0) % 5;
  memory.recent_event_cursor = (cursor + 1) % 5;
  const recentKey = `recent_change_${cursor}`;
  upsertDeterministicBucket(
    memory,
    buildDeterministicCheckpoint(ref, memory, recentKey, { target_age_ms: 0, snapshot, snapshot_hash: snapshotHash }),
    null
  );

  MEMORY_DETERMINISTIC_TARGETS.forEach((target) => {
    upsertDeterministicBucket(
      memory,
      buildDeterministicCheckpoint(ref, memory, target.bucket_key, {
        target_age_ms: target.target_age_ms,
        snapshot,
        snapshot_hash: snapshotHash,
      }),
      target.target_age_ms
    );
  });
  compactMemoryCheckpoints(memory);
}

function maybeCaptureSemanticMemoryCheckpoint(ref) {
  const memory = ensureReferenceMemory(ref);
  if (!memory.enabled) return false;
  const now = nowTs();
  const snapshot = buildReferenceSnapshot(ref);
  const snapshotHash = buildSnapshotHash(snapshot);
  const signature = buildSemanticSignatureFromSnapshot(snapshot);
  const prevSignature = String(memory.last_semantic_signature || '').trim();
  memory.last_semantic_eval_at = now;
  if (!prevSignature) {
    memory.last_semantic_signature = signature;
    return true;
  }
  const lastSemantic = getLatestCheckpoint(memory, 'semantic');
  const semanticDistance = jaccardDistance(prevSignature, signature);
  const structureDistance = structuralDeltaScore(lastSemantic && lastSemantic.snapshot ? lastSemantic.snapshot : null, snapshot);
  const score = Math.max(0, Math.min(1, (semanticDistance * 0.7) + (structureDistance * 0.3)));
  const sinceLast = lastSemantic ? (now - Number(lastSemantic.created_at || 0)) : Number.POSITIVE_INFINITY;
  const shouldCapture = score >= MEMORY_SEMANTIC_FORCE_THRESHOLD
    || (score >= MEMORY_SEMANTIC_THRESHOLD && sinceLast >= MEMORY_SEMANTIC_COOLDOWN_MS);
  if (!shouldCapture) return false;

  const checkpoint = {
    id: makeId('mem'),
    kind: 'semantic',
    bucket_key: `semantic_${String(now)}`,
    created_at: now,
    source_reference_id: String(ref.id || ''),
    source_reference_updated_at: Number(ref.updated_at || now),
    summary: summarizeMemorySnapshot(snapshot),
    semantic_score: Number(score.toFixed(4)),
    structural_delta: buildReferenceDiffSummary(lastSemantic && lastSemantic.snapshot ? lastSemantic.snapshot : null, snapshot),
    snapshot_hash: snapshotHash,
    snapshot,
    diff_from_prev: buildReferenceDiffSummary(lastSemantic && lastSemantic.snapshot ? lastSemantic.snapshot : null, snapshot),
    seed_metadata: {
      semantic_distance: Number(semanticDistance.toFixed(4)),
      structural_distance: Number(structureDistance.toFixed(4)),
      threshold: MEMORY_SEMANTIC_THRESHOLD,
    },
  };
  memory.checkpoints = Array.isArray(memory.checkpoints) ? memory.checkpoints : [];
  memory.checkpoints.push(checkpoint);
  memory.last_semantic_signature = signature;
  compactMemoryCheckpoints(memory);
  return true;
}

function buildMemoryCheckpointMetadata(checkpoint = {}) {
  const snapshot = (checkpoint.snapshot && typeof checkpoint.snapshot === 'object') ? checkpoint.snapshot : {};
  const stats = buildSnapshotStats(snapshot);
  return {
    id: String(checkpoint.id || ''),
    kind: String(checkpoint.kind || 'periodic'),
    bucket_key: String(checkpoint.bucket_key || ''),
    created_at: Number(checkpoint.created_at || 0),
    source_reference_id: String(checkpoint.source_reference_id || ''),
    source_reference_updated_at: Number(checkpoint.source_reference_updated_at || 0),
    summary: String(checkpoint.summary || ''),
    semantic_score: checkpoint.semantic_score == null ? null : Number(checkpoint.semantic_score),
    snapshot_hash: String(checkpoint.snapshot_hash || '') || getCheckpointSnapshotHash(checkpoint),
    diff_from_prev: (checkpoint.diff_from_prev && typeof checkpoint.diff_from_prev === 'object') ? checkpoint.diff_from_prev : {},
    stats,
  };
}

function normalizeUrl(raw) {
  const input = String(raw || '').trim();
  if (!input) return 'about:blank';
  if (/^about:blank$/i.test(input)) return 'about:blank';
  if (/^https?:\/\//i.test(input)) return input;
  if (/^[a-zA-Z0-9-]+\.[a-zA-Z]{2,}/.test(input)) return `https://${input}`;
  return buildSearchUrl(input, getDefaultSearchEngine());
}

function normalizeUrlForMatch(raw) {
  const input = String(raw || '').trim();
  if (!input) return '';
  try {
    const parsed = new URL(input);
    let pathname = parsed.pathname || '/';
    if (pathname.length > 1 && pathname.endsWith('/')) pathname = pathname.slice(0, -1);
    return `${parsed.protocol}//${parsed.host}${pathname}${parsed.search}`;
  } catch (_) {
    return input.toLowerCase();
  }
}

function makeTimeoutSignal(timeoutMs = 12000) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(new Error('Request timeout')), timeoutMs);
  return { controller, timer };
}

async function fetchJsonWithTimeout(url, options = {}, timeoutMs = 12000) {
  const { controller, timer } = makeTimeoutSignal(timeoutMs);
  try {
    const res = await fetch(url, { ...options, signal: controller.signal });
    const text = await res.text();
    let json = null;
    try {
      json = text ? JSON.parse(text) : null;
    } catch (_) {
      json = null;
    }
    return { ok: res.ok, status: res.status, json, raw: text };
  } finally {
    clearTimeout(timer);
  }
}

async function fetchTextWithTimeout(url, options = {}, timeoutMs = 12000) {
  const { controller, timer } = makeTimeoutSignal(timeoutMs);
  try {
    const res = await fetch(url, { ...options, signal: controller.signal });
    const text = await res.text();
    return { ok: res.ok, status: res.status, text };
  } finally {
    clearTimeout(timer);
  }
}

function decodeXmlEntities(value) {
  return String(value || '').replace(/&(#x?[0-9a-fA-F]+|amp|lt|gt|quot|apos);/g, (_match, entity) => {
    const lower = String(entity || '').toLowerCase();
    if (lower === 'amp') return '&';
    if (lower === 'lt') return '<';
    if (lower === 'gt') return '>';
    if (lower === 'quot') return '"';
    if (lower === 'apos') return '\'';
    if (lower.startsWith('#x')) {
      const code = Number.parseInt(lower.slice(2), 16);
      return Number.isFinite(code) ? String.fromCodePoint(code) : '';
    }
    if (lower.startsWith('#')) {
      const code = Number.parseInt(lower.slice(1), 10);
      return Number.isFinite(code) ? String.fromCodePoint(code) : '';
    }
    return '';
  });
}

function parseXmlAttributes(tagText) {
  const out = {};
  const raw = String(tagText || '');
  const attrRegex = /([a-zA-Z0-9_:-]+)\s*=\s*"([^"]*)"/g;
  let match = attrRegex.exec(raw);
  while (match) {
    const key = String(match[1] || '').trim();
    if (key) out[key] = decodeXmlEntities(match[2] || '');
    match = attrRegex.exec(raw);
  }
  return out;
}

function parseChromeBookmarkNode(node, items) {
  if (!node || typeof node !== 'object') return;
  const type = String(node.type || '').toLowerCase();
  if (type === 'url') {
    const url = normalizeUrl(node.url || '');
    if (!/^https?:\/\//i.test(url)) return;
    items.push({
      url,
      title: String(node.name || url).trim().slice(0, 180),
    });
    return;
  }
  const children = Array.isArray(node.children) ? node.children : [];
  children.forEach((child) => parseChromeBookmarkNode(child, items));
}

function parseChromeBookmarks(filePath) {
  const raw = fs.readFileSync(filePath, 'utf8');
  const parsed = JSON.parse(raw);
  const roots = (parsed && parsed.roots && typeof parsed.roots === 'object') ? parsed.roots : {};
  const items = [];
  Object.values(roots).forEach((rootNode) => parseChromeBookmarkNode(rootNode, items));
  return items;
}

function collectSafariBookmarkItems(node, items) {
  if (!node || typeof node !== 'object') return;
  if (Array.isArray(node)) {
    node.forEach((child) => collectSafariBookmarkItems(child, items));
    return;
  }

  const maybeUrl = String(node.URLString || node.url || '').trim();
  if (maybeUrl) {
    const normalized = normalizeUrl(maybeUrl);
    if (/^https?:\/\//i.test(normalized)) {
      const title = String(
        (node.URIDictionary && node.URIDictionary.title)
        || node.Title
        || node.title
        || normalized
      ).trim();
      items.push({
        url: normalized,
        title: title.slice(0, 180),
      });
    }
  }

  Object.values(node).forEach((value) => collectSafariBookmarkItems(value, items));
}

function parseSafariBookmarks(filePath) {
  const exported = execFileSync('plutil', ['-convert', 'json', '-o', '-', filePath], { encoding: 'utf8' });
  const parsed = JSON.parse(exported);
  const items = [];
  collectSafariBookmarkItems(parsed, items);
  return items;
}

function parseSqliteLineOutputRows(raw = '') {
  const text = String(raw || '').trim();
  if (!text) return [];
  const blocks = text.split(/\n\s*\n/g);
  const rows = [];
  for (const block of blocks) {
    const row = {};
    const lines = String(block || '').split('\n');
    for (const line of lines) {
      const idx = line.indexOf('=');
      if (idx <= 0) continue;
      const key = String(line.slice(0, idx) || '').trim();
      const value = String(line.slice(idx + 1) || '').trim();
      if (key) row[key] = value;
    }
    if (Object.keys(row).length > 0) rows.push(row);
  }
  return rows;
}

function querySqliteRowsSnapshot(dbPath, sql) {
  const targetDb = String(dbPath || '').trim();
  if (!targetDb || !fs.existsSync(targetDb)) return [];
  const query = String(sql || '').trim();
  if (!query) return [];
  let tmpDir = '';
  try {
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'subgrapher-sqlite-'));
    const snapshotPath = path.join(tmpDir, 'snapshot.db');
    fs.copyFileSync(targetDb, snapshotPath);
    const output = execFileSync('sqlite3', ['-line', snapshotPath, query], { encoding: 'utf8' });
    return parseSqliteLineOutputRows(output);
  } catch (_) {
    return [];
  } finally {
    if (tmpDir) {
      try {
        fs.rmSync(tmpDir, { recursive: true, force: true });
      } catch (_) {
        // noop
      }
    }
  }
}

function parseChromeHistory(dbPath) {
  const rows = querySqliteRowsSnapshot(
    dbPath,
    "SELECT url, title, last_visit_time FROM urls WHERE url LIKE 'http%' ORDER BY last_visit_time DESC LIMIT 500;",
  );
  return rows.map((row) => {
    const url = normalizeUrl(row.url || '');
    const title = String(row.title || url).trim().slice(0, 180);
    return { url, title };
  }).filter((item) => /^https?:\/\//i.test(item.url));
}

function parseSafariHistory(dbPath) {
  const rows = querySqliteRowsSnapshot(
    dbPath,
    "SELECT hi.url AS url, COALESCE(hi.title, '') AS title, hv.visit_time AS visit_time FROM history_items hi JOIN history_visits hv ON hv.history_item = hi.id WHERE hi.url LIKE 'http%' ORDER BY hv.visit_time DESC LIMIT 500;",
  );
  return rows.map((row) => {
    const url = normalizeUrl(row.url || '');
    const title = String(row.title || url).trim().slice(0, 180);
    return { url, title };
  }).filter((item) => /^https?:\/\//i.test(item.url));
}

function getBrowserImportPaths(source) {
  const home = os.homedir();
  const target = String(source || '').trim().toLowerCase();
  if (target === 'chrome') {
    return {
      bookmarks: [
        path.join(home, 'Library', 'Application Support', 'Google', 'Chrome', 'Default', 'Bookmarks'),
        path.join(home, 'Library', 'Application Support', 'Google', 'Chrome', 'Profile 1', 'Bookmarks'),
      ],
      history: [
        path.join(home, 'Library', 'Application Support', 'Google', 'Chrome', 'Default', 'History'),
        path.join(home, 'Library', 'Application Support', 'Google', 'Chrome', 'Profile 1', 'History'),
      ],
    };
  }
  if (target === 'safari') {
    return {
      bookmarks: [
        path.join(home, 'Library', 'Safari', 'Bookmarks.plist'),
      ],
      history: [
        path.join(home, 'Library', 'Safari', 'History.db'),
      ],
    };
  }
  return { bookmarks: [], history: [] };
}

function importBrowserDataToReference(source) {
  const target = String(source || '').trim().toLowerCase();
  const candidates = getBrowserImportPaths(target);
  let bookmarkItems = [];
  let historyItems = [];
  let bookmarkSourcePath = '';
  let historySourcePath = '';

  for (const filePath of (Array.isArray(candidates.bookmarks) ? candidates.bookmarks : [])) {
    if (!filePath || !fs.existsSync(filePath)) continue;
    try {
      bookmarkItems = target === 'chrome' ? parseChromeBookmarks(filePath) : parseSafariBookmarks(filePath);
      bookmarkSourcePath = filePath;
      if (bookmarkItems.length > 0) break;
    } catch (_) {
      // Try next candidate.
    }
  }

  for (const filePath of (Array.isArray(candidates.history) ? candidates.history : [])) {
    if (!filePath || !fs.existsSync(filePath)) continue;
    try {
      historyItems = target === 'chrome' ? parseChromeHistory(filePath) : parseSafariHistory(filePath);
      historySourcePath = filePath;
      if (historyItems.length > 0) break;
    } catch (_) {
      // Try next candidate.
    }
  }

  if (!bookmarkSourcePath && !historySourcePath) {
    return { ok: false, message: `No ${target} bookmark/history data file found on this machine.` };
  }

  const collected = historyItems.concat(bookmarkItems);
  const dedupe = new Set();
  const items = collected
    .filter((item) => item && item.url)
    .map((item) => ({
      url: normalizeUrl(item.url),
      title: String(item.title || item.url).trim().slice(0, 180),
    }))
    .filter((item) => {
      if (!/^https?:\/\//i.test(item.url)) return false;
      if (dedupe.has(item.url)) return false;
      dedupe.add(item.url);
      return true;
    })
    .slice(0, MAX_IMPORTED_TABS);

  if (items.length === 0) {
    return { ok: false, message: `${target} data files were found but no importable web URLs were detected.` };
  }

  const refs = getReferences();
  const importedRef = createReferenceBase({
    title: `${target[0].toUpperCase()}${target.slice(1)} Import`,
    intent: `Imported browser data from ${target}`,
    relation_type: 'root',
    current_tab: items[0],
  });
  importedRef.tabs = items.map((item) => createWebTab(item));
  importedRef.active_tab_id = importedRef.tabs[0] ? importedRef.tabs[0].id : null;
  importedRef.updated_at = nowTs();

  refs.unshift(importedRef);
  setReferences(refs);
  importedRef.tabs.forEach((tab) => {
    captureCommittedHistoryFromTabSync(tab, {
      committed_at: nowTs(),
      source_sr_id: importedRef.id,
      source_tab_id: String((tab && tab.id) || ''),
      content_excerpt: String((tab && tab.title) || ''),
    });
  });

  return {
    ok: true,
    imported_count: importedRef.tabs.length,
    imported_breakdown: {
      history_count: historyItems.length,
      bookmark_count: bookmarkItems.length,
    },
    unsupported: {
      cookies: true,
      passwords: true,
    },
    reference: importedRef,
    source: target,
    message: `Imported ${importedRef.tabs.length} URL(s) from ${target} history/bookmarks. Cookies and passwords are not imported.`,
  };
}

function setDockIconIfAvailable() {
  if (process.platform !== 'darwin') return;
  if (!app.dock || typeof app.dock.setIcon !== 'function') return;
  const dockIconPath = fs.existsSync(APP_ICON_ICNS_PATH)
    ? APP_ICON_ICNS_PATH
    : fs.existsSync(APP_ICON_PNG_PATH)
      ? APP_ICON_PNG_PATH
    : APP_LOGO_PATH;
  if (!fs.existsSync(dockIconPath)) return;
  try {
    const image = nativeImage.createFromPath(dockIconPath);
    if (!image || image.isEmpty()) return;
    app.dock.setIcon(image);
  } catch (_) {
    // noop
  }
}

function resolveWindowIconPath() {
  if (process.platform === 'darwin') return undefined;
  if (process.platform === 'win32' && fs.existsSync(APP_ICON_ICO_PATH)) return APP_ICON_ICO_PATH;
  if (fs.existsSync(APP_ICON_PNG_PATH)) return APP_ICON_PNG_PATH;
  if (fs.existsSync(APP_LOGO_PATH)) return APP_LOGO_PATH;
  return undefined;
}

function openDefaultBrowserSettings() {
  if (process.platform === 'darwin') {
    shell.openExternal('x-apple.systempreferences:com.apple.preference.general');
    return { ok: true, opened: true, message: 'Opened macOS settings for default browser selection.' };
  }
  shell.openExternal('https://support.google.com/chrome/answer/95417');
  return { ok: true, opened: true, message: 'Opened default browser help instructions.' };
}

function requestDefaultBrowser() {
  let attempted = false;
  let success = false;
  try {
    attempted = true;
    const httpSet = app.setAsDefaultProtocolClient('http');
    const httpsSet = app.setAsDefaultProtocolClient('https');
    success = !!(httpSet && httpsSet);
  } catch (_) {
    success = false;
  }
  const settings = openDefaultBrowserSettings();
  return {
    ok: true,
    attempted_programmatic_set: attempted,
    programmatic_success: success,
    settings_opened: !!(settings && settings.opened),
    message: success
      ? 'Requested default browser registration and opened system settings to confirm.'
      : 'Opened system settings to choose Subgrapher as default browser.',
  };
}

async function fetchProviderModels(provider, apiKey) {
  const target = String(provider || '').trim().toLowerCase();
  const key = String(apiKey || '').trim();
  const settings = readSettings();
  if (!PROVIDERS.includes(target)) {
    return { ok: false, message: 'Unsupported provider.' };
  }
  if (target !== 'lmstudio' && !key) {
    return { ok: false, message: 'API key is not configured for this provider.' };
  }

  if (target === 'openai' || target === 'cerebras') {
    const base = target === 'openai' ? 'https://api.openai.com' : 'https://api.cerebras.ai';
    const result = await fetchJsonWithTimeout(`${base}/v1/models`, {
      method: 'GET',
      headers: {
        Authorization: `Bearer ${key}`,
      },
    });
    if (!result.ok) {
      return { ok: false, message: `Model list request failed (${result.status}).`, details: result.raw || '' };
    }
    const models = Array.isArray(result.json && result.json.data)
      ? result.json.data.map((item) => String((item && item.id) || '').trim()).filter(Boolean)
      : [];
    return { ok: true, provider: target, models: models.sort() };
  }

  if (target === 'lmstudio') {
    const baseUrl = normalizeHttpBaseUrl(
      String((settings && settings.lmstudio_base_url) || LMSTUDIO_DEFAULT_BASE_URL),
      LMSTUDIO_DEFAULT_BASE_URL,
    );
    const headers = {};
    if (key) headers.Authorization = `Bearer ${key}`;
    const result = await fetchJsonWithTimeout(`${baseUrl}/v1/models`, {
      method: 'GET',
      headers,
    });
    if (!result || !result.ok) {
      return {
        ok: false,
        message: `LM Studio model list request failed (${(result && result.status) || 'unknown'}).`,
        details: (result && result.raw) || '',
      };
    }
    const models = Array.isArray(result.json && result.json.data)
      ? result.json.data.map((item) => String((item && item.id) || '').trim()).filter(Boolean)
      : [];
    return { ok: true, provider: target, models: models.sort() };
  }

  if (target === 'google') {
    const result = await fetchJsonWithTimeout(`https://generativelanguage.googleapis.com/v1beta/models?key=${encodeURIComponent(key)}`, {
      method: 'GET',
      headers: { 'Content-Type': 'application/json' },
    });
    if (!result.ok) {
      return { ok: false, message: `Google model list request failed (${result.status}).`, details: result.raw || '' };
    }
    const models = Array.isArray(result.json && result.json.models)
      ? result.json.models
        .map((item) => String((item && item.name) || '').trim())
        .map((name) => name.replace(/^models\//, ''))
        .filter(Boolean)
      : [];
    return { ok: true, provider: target, models: models.sort() };
  }

  // anthropic
  const anthResult = await fetchJsonWithTimeout('https://api.anthropic.com/v1/models', {
    method: 'GET',
    headers: {
      'x-api-key': key,
      'anthropic-version': '2023-06-01',
      'content-type': 'application/json',
    },
  });
  if (anthResult.ok) {
    const models = Array.isArray(anthResult.json && anthResult.json.data)
      ? anthResult.json.data.map((item) => String((item && item.id) || '').trim()).filter(Boolean)
      : [];
    if (models.length > 0) {
      return { ok: true, provider: target, models: models.sort() };
    }
  }

  return {
    ok: true,
    provider: target,
    models: ['claude-opus-4-1', 'claude-sonnet-4', 'claude-3-7-sonnet-latest'],
    fallback: true,
    message: 'Live model discovery unavailable for Anthropic; showing recommended models.',
  };
}

function getStorePath() {
  return path.join(app.getPath('userData'), STORE_FILENAME);
}

function getSettingsPath() {
  return path.join(app.getPath('userData'), APP_SETTINGS_FILENAME);
}

function getGlobalSkillsPath() {
  return path.join(app.getPath('userData'), GLOBAL_SKILLS_FILENAME);
}

function getLocalSkillsPath() {
  return path.join(app.getPath('userData'), LOCAL_SKILLS_FILENAME);
}

function sanitizeSkillDescriptor(value) {
  const input = (value && typeof value === 'object') ? value : {};
  const id = String(input.id || '').trim();
  const scope = String(input.scope || 'local').trim().toLowerCase() === 'global' ? 'global' : 'local';
  const name = String(input.name || '').trim().slice(0, 120);
  if (!id || !name) return null;
  return { id, scope, name };
}

function sanitizeSkillObject(value, scope = 'local') {
  const input = (value && typeof value === 'object') ? value : {};
  const normalizedScope = String(scope || input.scope || 'local').trim().toLowerCase() === 'global' ? 'global' : 'local';
  const now = nowTs();
  const id = String(input.id || makeId('skill')).trim();
  const name = String(input.name || '').trim().slice(0, 120);
  const code = String(input.code || '');
  if (!id || !name || !code.trim()) return null;
  return {
    id,
    name,
    description: String(input.description || '').trim().slice(0, 500),
    code,
    created_at: Number(input.created_at || now),
    updated_at: Number(input.updated_at || now),
    owner_reference_id: input.owner_reference_id ? String(input.owner_reference_id) : null,
    scope: normalizedScope,
  };
}

function readSkillStore(scope = 'local') {
  const normalizedScope = String(scope || '').trim().toLowerCase() === 'global' ? 'global' : 'local';
  const filePath = normalizedScope === 'global' ? getGlobalSkillsPath() : getLocalSkillsPath();
  if (!fs.existsSync(filePath)) return [];
  try {
    const raw = fs.readFileSync(filePath, 'utf8');
    const parsed = JSON.parse(raw);
    if (!Array.isArray(parsed)) return [];
    return parsed
      .map((item) => sanitizeSkillObject(item, normalizedScope))
      .filter(Boolean);
  } catch (_) {
    return [];
  }
}

function writeSkillStore(scope = 'local', list = []) {
  const normalizedScope = String(scope || '').trim().toLowerCase() === 'global' ? 'global' : 'local';
  const filePath = normalizedScope === 'global' ? getGlobalSkillsPath() : getLocalSkillsPath();
  const skills = Array.isArray(list)
    ? list.map((item) => sanitizeSkillObject(item, normalizedScope)).filter(Boolean)
    : [];
  fs.writeFileSync(filePath, JSON.stringify(skills, null, 2), 'utf8');
  return skills;
}

function getPythonRuntimeResolver() {
  if (pythonRuntimeResolver) return pythonRuntimeResolver;
  pythonRuntimeResolver = createPythonRuntimeResolver({
    app,
    projectRoot: __dirname,
  });
  return pythonRuntimeResolver;
}

async function getPythonSandboxManagerForRole(runtimeRole = 'tool') {
  const role = String(runtimeRole || '').trim().toLowerCase() === 'viz' ? 'viz' : 'tool';
  const runtime = await getPythonRuntimeResolver().resolve(role);
  const pythonBin = String((runtime && runtime.python_bin) || '').trim() || 'python3';
  const cacheKey = `${role}:${pythonBin}`;
  if (pythonSandboxManagers.has(cacheKey)) {
    return { manager: pythonSandboxManagers.get(cacheKey), runtime };
  }
  const manager = new PythonSandboxManager({
    basePath: app.getPath('userData'),
    pythonBin,
    maxPerReference: 2,
    maxGlobal: 6,
    defaultTimeoutMs: PYTHON_EXEC_TIMEOUT_MS,
    maxQueue: 160,
  });
  pythonSandboxManagers.set(cacheKey, manager);
  return { manager, runtime };
}

function getVizCacheDir() {
  const outDir = path.join(app.getPath('userData'), 'viz_cache');
  fs.mkdirSync(outDir, { recursive: true });
  return outDir;
}

function persistVizPngFromBase64(tabId, pngBase64) {
  const cleanTabId = String(tabId || makeId('tab')).replace(/[^a-zA-Z0-9_-]/g, '_');
  const base64 = String(pngBase64 || '').trim();
  if (!base64) return '';
  const outPath = path.join(getVizCacheDir(), `${cleanTabId}.png`);
  try {
    fs.writeFileSync(outPath, Buffer.from(base64, 'base64'));
    return outPath;
  } catch (_) {
    return '';
  }
}

function isPathWithinDir(candidatePath, baseDir) {
  const candidate = path.resolve(String(candidatePath || ''));
  const base = path.resolve(String(baseDir || ''));
  const rel = path.relative(base, candidate);
  return rel === '' || (!!rel && !rel.startsWith('..') && !path.isAbsolute(rel));
}

function findMostRecentPythonVizPngPath(ref) {
  const tabs = Array.isArray(ref && ref.tabs) ? ref.tabs : [];
  const candidates = tabs
    .filter((tab) => (
      String((tab && tab.tab_kind) || '').trim().toLowerCase() === 'viz'
      && String((tab && tab.viz_source) || '').trim().toLowerCase() === 'python_sandbox'
      && String((tab && tab.viz_png_path) || '').trim()
    ))
    .sort((a, b) => Number((b && b.snapshot_at) || 0) - Number((a && a.snapshot_at) || 0));

  for (const tab of candidates) {
    const pngPath = path.resolve(String((tab && tab.viz_png_path) || '').trim());
    if (!pngPath) continue;
    if (fs.existsSync(pngPath)) return pngPath;
  }
  return '';
}

function resolveArtifactAssetUri(srId, uri) {
  const refId = String(srId || '').trim();
  const rawUri = String(uri || '').trim();
  if (!refId || !rawUri) {
    return { ok: false, resolved_url: '', reason: 'invalid_input' };
  }

  const refs = getReferences();
  const idx = findReferenceIndex(refs, refId);
  if (idx < 0) {
    return { ok: false, resolved_url: '', reason: 'reference_not_found' };
  }
  const ref = refs[idx];

  if (/^https?:\/\//i.test(rawUri)) {
    return { ok: true, resolved_url: rawUri, reason: 'remote_url' };
  }

  if (/^sandbox:\/\//i.test(rawUri)) {
    let parsed;
    try {
      parsed = new URL(rawUri);
    } catch (_) {
      return { ok: false, resolved_url: '', reason: 'invalid_uri' };
    }
    const sandboxSrId = String((parsed && parsed.hostname) || '').trim();
    const sandboxPath = String((parsed && parsed.pathname) || '').trim();
    if (!sandboxSrId || sandboxSrId !== refId) {
      return { ok: false, resolved_url: '', reason: 'sr_mismatch' };
    }
    if (sandboxPath !== '/output.png') {
      return { ok: false, resolved_url: '', reason: 'unsupported_sandbox_asset' };
    }
    const resolvedPath = findMostRecentPythonVizPngPath(ref);
    if (!resolvedPath) {
      return { ok: false, resolved_url: '', reason: 'missing_file' };
    }
    return {
      ok: true,
      resolved_url: pathToFileURL(resolvedPath).toString(),
      reason: 'sandbox_mapped_to_viz_cache',
    };
  }

  if (/^file:\/\//i.test(rawUri)) {
    let localPath = '';
    try {
      const parsed = new URL(rawUri);
      if (parsed.protocol !== 'file:') {
        return { ok: false, resolved_url: '', reason: 'invalid_uri' };
      }
      localPath = decodeURIComponent(parsed.pathname || '');
      if (process.platform === 'win32') localPath = localPath.replace(/^\/([a-zA-Z]:)/, '$1');
      if (!localPath) return { ok: false, resolved_url: '', reason: 'invalid_uri' };
    } catch (_) {
      return { ok: false, resolved_url: '', reason: 'invalid_uri' };
    }

    const resolvedPath = path.resolve(localPath);
    if (!fs.existsSync(resolvedPath)) {
      return { ok: false, resolved_url: '', reason: 'missing_file' };
    }

    const vizCacheDir = getVizCacheDir();
    const fromVizCache = isPathWithinDir(resolvedPath, vizCacheDir);
    const artifactBlob = (Array.isArray(ref.artifacts) ? ref.artifacts : [])
      .map((artifact) => String((artifact && artifact.content) || ''))
      .join('\n');
    const explicitlyReferenced = artifactBlob.includes(rawUri);

    if (!fromVizCache && !explicitlyReferenced) {
      return { ok: false, resolved_url: '', reason: 'forbidden_local_path' };
    }
    return {
      ok: true,
      resolved_url: pathToFileURL(resolvedPath).toString(),
      reason: fromVizCache ? 'viz_cache' : 'artifact_file_link',
    };
  }

  return { ok: false, resolved_url: '', reason: 'unsupported_scheme' };
}

function filePathFromFileUrl(rawUrl) {
  let localPath = '';
  const parsed = new URL(String(rawUrl || '').trim());
  if (parsed.protocol !== 'file:') return '';
  localPath = decodeURIComponent(parsed.pathname || '');
  if (process.platform === 'win32') localPath = localPath.replace(/^\/([a-zA-Z]:)/, '$1');
  if (!localPath) return '';
  return path.resolve(localPath);
}

function sanitizeFilename(name, fallback = 'artifact-image.png') {
  const raw = String(name || '').trim();
  if (!raw) return fallback;
  const safe = raw.replace(/[^a-zA-Z0-9._-]/g, '_').replace(/^_+/, '').slice(0, 160);
  if (!safe) return fallback;
  if (safe.includes('.')) return safe;
  return `${safe}.png`;
}

async function saveArtifactImageForReference(srId, sourceUrl, suggestedName) {
  const refId = String(srId || '').trim();
  const src = String(sourceUrl || '').trim();
  if (!refId || !src) return { ok: false, message: 'srId and sourceUrl are required.' };

  const resolved = resolveArtifactAssetUri(refId, src);
  if (!resolved.ok || !resolved.resolved_url) {
    return {
      ok: false,
      message: `Image URL could not be resolved: ${String(resolved.reason || 'unknown_error')}.`,
    };
  }

  const outName = sanitizeFilename(suggestedName, 'artifact-image.png');
  const pick = await dialog.showSaveDialog(mainWindow, {
    title: 'Save Artifact Image',
    defaultPath: path.join(os.homedir(), 'Downloads', outName),
    filters: [
      { name: 'PNG Image', extensions: ['png'] },
      { name: 'All Files', extensions: ['*'] },
    ],
  });
  if (!pick || pick.canceled || !pick.filePath) return { ok: false, message: 'Save canceled.' };

  const destPath = String(pick.filePath || '').trim();
  if (!destPath) return { ok: false, message: 'Invalid destination path.' };

  try {
    if (/^file:\/\//i.test(resolved.resolved_url)) {
      const sourcePath = filePathFromFileUrl(resolved.resolved_url);
      if (!sourcePath || !fs.existsSync(sourcePath)) {
        return { ok: false, message: 'Resolved source file is missing.' };
      }
      fs.copyFileSync(sourcePath, destPath);
      return { ok: true, saved_path: destPath };
    }

    if (/^https?:\/\//i.test(resolved.resolved_url)) {
      if (typeof fetch !== 'function') {
        return { ok: false, message: 'Remote image download is unavailable in this runtime.' };
      }
      const response = await fetch(resolved.resolved_url);
      if (!response || !response.ok) {
        return { ok: false, message: `Remote image request failed (${response ? response.status : 'unknown'}).` };
      }
      const arr = await response.arrayBuffer();
      fs.writeFileSync(destPath, Buffer.from(arr));
      return { ok: true, saved_path: destPath };
    }

    return { ok: false, message: 'Unsupported resolved image URL.' };
  } catch (err) {
    return { ok: false, message: String((err && err.message) || 'Unable to save image.') };
  }
}

function removeSkillDescriptorFromReference(ref, skillId, scope = 'local') {
  if (!ref || typeof ref !== 'object') return false;
  const targetId = String(skillId || '').trim();
  const targetScope = String(scope || 'local').trim().toLowerCase() === 'global' ? 'global' : 'local';
  const prev = Array.isArray(ref.skills) ? ref.skills : [];
  const next = prev.filter((item) => {
    const descriptor = sanitizeSkillDescriptor(item);
    if (!descriptor) return false;
    return !(descriptor.id === targetId && descriptor.scope === targetScope);
  });
  ref.skills = next;
  return next.length !== prev.length;
}

function referenceHasSkillDescriptor(ref, skillId, scope = 'local') {
  if (!ref || typeof ref !== 'object') return false;
  const targetId = String(skillId || '').trim();
  const targetScope = String(scope || 'local').trim().toLowerCase() === 'global' ? 'global' : 'local';
  const descriptors = Array.isArray(ref.skills) ? ref.skills : [];
  return descriptors.some((item) => {
    const descriptor = sanitizeSkillDescriptor(item);
    if (!descriptor) return false;
    return descriptor.id === targetId && descriptor.scope === targetScope;
  });
}

function attachSkillDescriptorToReference(ref, skill) {
  if (!ref || typeof ref !== 'object') return false;
  const normalized = sanitizeSkillObject(skill, (skill && skill.scope) || 'local');
  if (!normalized) return false;
  ref.skills = Array.isArray(ref.skills) ? ref.skills : [];
  const existing = ref.skills.find((item) => {
    const descriptor = sanitizeSkillDescriptor(item);
    if (!descriptor) return false;
    return descriptor.id === normalized.id && descriptor.scope === normalized.scope;
  });
  if (existing) {
    existing.name = normalized.name;
    return false;
  }
  ref.skills.push({
    id: normalized.id,
    scope: normalized.scope,
    name: normalized.name,
  });
  return true;
}

function isSkillLinkedAnywhere(refs, skillId, scope = 'local') {
  const targetId = String(skillId || '').trim();
  const targetScope = String(scope || 'local').trim().toLowerCase() === 'global' ? 'global' : 'local';
  return (Array.isArray(refs) ? refs : []).some((ref) => referenceHasSkillDescriptor(ref, targetId, targetScope));
}

function listSkillsForReference(ref) {
  const localSkills = readSkillStore('local');
  const globalSkills = readSkillStore('global');
  const descriptors = Array.isArray(ref && ref.skills) ? ref.skills.map((item) => sanitizeSkillDescriptor(item)).filter(Boolean) : [];
  const linked = descriptors.map((descriptor) => {
    const pool = descriptor.scope === 'global' ? globalSkills : localSkills;
    const found = pool.find((skill) => String((skill && skill.id) || '') === descriptor.id);
    if (!found) return null;
    return {
      id: found.id,
      scope: found.scope,
      name: found.name,
      description: found.description,
      code: found.code,
      created_at: found.created_at,
      updated_at: found.updated_at,
      owner_reference_id: found.owner_reference_id,
    };
  }).filter(Boolean);
  return {
    local_skills: localSkills,
    global_skills: globalSkills,
    linked_skills: linked,
  };
}

function findSkillByName(ref, name, scope = '') {
  const targetName = String(name || '').trim().toLowerCase();
  if (!targetName) return null;
  const scopeFilter = String(scope || '').trim().toLowerCase();
  const pools = [];
  if (!scopeFilter || scopeFilter === 'local') {
    pools.push(...readSkillStore('local'));
  }
  if (!scopeFilter || scopeFilter === 'global') {
    pools.push(...readSkillStore('global'));
  }
  const descriptors = Array.isArray(ref && ref.skills)
    ? ref.skills.map((item) => sanitizeSkillDescriptor(item)).filter(Boolean)
    : [];
  const linkedIds = new Set(descriptors.map((descriptor) => `${descriptor.scope}:${descriptor.id}`));
  const found = pools.find((skill) => {
    if (!skill || !skill.name) return false;
    const key = `${skill.scope}:${skill.id}`;
    if (linkedIds.size > 0 && !linkedIds.has(key)) return false;
    return String(skill.name || '').trim().toLowerCase() === targetName;
  });
  return found || null;
}

function normalizeProviderKeyId(value) {
  const raw = String(value || '').trim().toLowerCase();
  if (!raw) return '';
  const cleaned = raw
    .replace(/[^a-z0-9._-]+/g, '-')
    .replace(/^-+/g, '')
    .replace(/-+$/g, '')
    .slice(0, 80);
  return cleaned;
}

function defaultProviderKeyProfiles() {
  const profiles = {};
  PROVIDERS.forEach((provider) => {
    profiles[provider] = { primary_key_id: '', keys: [] };
  });
  return profiles;
}

function normalizeProviderKeyEntry(value) {
  const target = (value && typeof value === 'object') ? value : {};
  const keyId = normalizeProviderKeyId(target.key_id);
  if (!keyId) return null;
  const createdAtRaw = Number(target.created_at || 0);
  const updatedAtRaw = Number(target.updated_at || 0);
  const createdAt = Number.isFinite(createdAtRaw) && createdAtRaw > 0 ? Math.round(createdAtRaw) : 0;
  const updatedAt = Number.isFinite(updatedAtRaw) && updatedAtRaw > 0 ? Math.round(updatedAtRaw) : createdAt;
  return {
    key_id: keyId,
    label: String(target.label || '').trim() || keyId,
    created_at: createdAt,
    updated_at: updatedAt,
  };
}

function normalizeProviderKeyProfile(value) {
  const target = (value && typeof value === 'object') ? value : {};
  const seen = new Set();
  const keys = [];
  (Array.isArray(target.keys) ? target.keys : []).forEach((entry) => {
    const normalized = normalizeProviderKeyEntry(entry);
    if (!normalized || seen.has(normalized.key_id)) return;
    seen.add(normalized.key_id);
    keys.push(normalized);
  });
  const requestedPrimary = normalizeProviderKeyId(target.primary_key_id);
  const primary = keys.some((entry) => entry.key_id === requestedPrimary) ? requestedPrimary : '';
  return {
    primary_key_id: primary,
    keys,
  };
}

function normalizeProviderKeyProfiles(value) {
  const base = defaultProviderKeyProfiles();
  const src = (value && typeof value === 'object') ? value : {};
  PROVIDERS.forEach((provider) => {
    base[provider] = normalizeProviderKeyProfile(src[provider]);
  });
  return base;
}

function generateProviderKeyId() {
  const entropy = String(crypto.randomUUID()).split('-')[0];
  return normalizeProviderKeyId(`key-${nowTs().toString(36)}-${entropy}`);
}

function getProviderProfile(settings, provider) {
  const normalizedProvider = String(provider || '').trim().toLowerCase();
  const profiles = normalizeProviderKeyProfiles(settings && settings.provider_key_profiles);
  return profiles[normalizedProvider] || { primary_key_id: '', keys: [] };
}

function pickProviderAutoPrimary(profile) {
  const entries = Array.isArray(profile && profile.keys) ? profile.keys.slice() : [];
  if (!entries.length) return '';
  entries.sort((a, b) => {
    const bUpdated = Number(b && b.updated_at || 0);
    const aUpdated = Number(a && a.updated_at || 0);
    if (bUpdated !== aUpdated) return bUpdated - aUpdated;
    const bCreated = Number(b && b.created_at || 0);
    const aCreated = Number(a && a.created_at || 0);
    return bCreated - aCreated;
  });
  return String((entries[0] && entries[0].key_id) || '').trim();
}

function ensureProviderProfileMigrated(provider) {
  const normalizedProvider = String(provider || '').trim().toLowerCase();
  if (!PROVIDERS.includes(normalizedProvider)) {
    return { ok: false, message: 'Unsupported provider.' };
  }
  const current = readSettings();
  const profiles = normalizeProviderKeyProfiles(current.provider_key_profiles);
  const profile = profiles[normalizedProvider] || { primary_key_id: '', keys: [] };
  if (Array.isArray(profile.keys) && profile.keys.length > 0) {
    return { ok: true, migrated: false, settings: current, profiles };
  }
  const legacy = keychain.getLegacyProviderKey(normalizedProvider);
  if (!legacy || !legacy.ok || !legacy.apiKey) {
    return { ok: true, migrated: false, settings: current, profiles };
  }
  const copyRes = keychain.setProviderKey(normalizedProvider, PROVIDER_PRIMARY_KEY_ID, legacy.apiKey);
  if (!copyRes || !copyRes.ok) {
    return { ok: false, message: (copyRes && copyRes.message) || 'Unable to migrate legacy provider key.' };
  }
  const ts = nowTs();
  profiles[normalizedProvider] = {
    primary_key_id: PROVIDER_PRIMARY_KEY_ID,
    keys: [{
      key_id: PROVIDER_PRIMARY_KEY_ID,
      label: 'Primary (migrated)',
      created_at: ts,
      updated_at: ts,
    }],
  };
  const updated = writeSettings({ provider_key_profiles: profiles });
  return {
    ok: true,
    migrated: true,
    settings: updated,
    profiles: normalizeProviderKeyProfiles(updated.provider_key_profiles),
  };
}

function ensureAllProviderProfilesMigrated() {
  let latest = readSettings();
  for (const provider of PROVIDERS) {
    const res = ensureProviderProfileMigrated(provider);
    if (!res || !res.ok) return res || { ok: false, message: 'Provider key migration failed.' };
    latest = res.settings || latest;
  }
  return { ok: true, settings: latest };
}

function resolveProviderApiKey(provider, keyId = '') {
  const normalizedProvider = String(provider || '').trim().toLowerCase();
  if (!PROVIDERS.includes(normalizedProvider)) {
    return { ok: false, message: 'Unsupported provider.' };
  }
  const migration = ensureProviderProfileMigrated(normalizedProvider);
  if (!migration || !migration.ok) {
    return { ok: false, message: (migration && migration.message) || 'Unable to resolve provider key.' };
  }
  const settings = migration.settings || readSettings();
  const profile = getProviderProfile(settings, normalizedProvider);
  const requestedKeyId = normalizeProviderKeyId(keyId);
  if (requestedKeyId) {
    const keyEntry = (profile.keys || []).find((entry) => String((entry && entry.key_id) || '') === requestedKeyId);
    if (!keyEntry) {
      return {
        ok: false,
        message: `Key "${requestedKeyId}" was not found for ${normalizedProvider}.`,
      };
    }
    const keyRes = keychain.getProviderKey(normalizedProvider, requestedKeyId);
    if (!keyRes || !keyRes.ok || !keyRes.apiKey) {
      return {
        ok: false,
        message: `API key "${requestedKeyId}" is not configured for ${normalizedProvider}.`,
      };
    }
    return {
      ok: true,
      provider: normalizedProvider,
      key_id: requestedKeyId,
      label: String(keyEntry.label || requestedKeyId),
      apiKey: String(keyRes.apiKey || ''),
      source: 'profile',
    };
  }

  let targetKeyId = normalizeProviderKeyId(profile.primary_key_id);
  if (!targetKeyId && Array.isArray(profile.keys) && profile.keys.length > 0) {
    targetKeyId = pickProviderAutoPrimary(profile);
  }
  if (targetKeyId) {
    const keyEntry = (profile.keys || []).find((entry) => String((entry && entry.key_id) || '') === targetKeyId);
    const keyRes = keychain.getProviderKey(normalizedProvider, targetKeyId);
    if (keyRes && keyRes.ok && keyRes.apiKey) {
      return {
        ok: true,
        provider: normalizedProvider,
        key_id: targetKeyId,
        label: String((keyEntry && keyEntry.label) || targetKeyId),
        apiKey: String(keyRes.apiKey || ''),
        source: 'profile',
      };
    }
    return {
      ok: false,
      message: `Primary API key is missing for ${normalizedProvider}. Configure keys in Settings.`,
    };
  }

  const legacy = keychain.getLegacyProviderKey(normalizedProvider);
  if (legacy && legacy.ok && legacy.apiKey) {
    return {
      ok: true,
      provider: normalizedProvider,
      key_id: '',
      label: 'Legacy',
      apiKey: String(legacy.apiKey || ''),
      source: 'legacy',
    };
  }
  return {
    ok: false,
    message: `No API key configured for ${normalizedProvider}. Add a key in Settings.`,
  };
}

function buildProviderKeysState() {
  const migration = ensureAllProviderProfilesMigrated();
  if (!migration || !migration.ok) {
    return {
      ok: false,
      message: (migration && migration.message) || 'Unable to read provider keys.',
      providers: [],
    };
  }
  const settings = migration.settings || readSettings();
  const profiles = normalizeProviderKeyProfiles(settings.provider_key_profiles);
  const providers = PROVIDERS.map((provider) => {
    const profile = profiles[provider] || { primary_key_id: '', keys: [] };
    const keys = (Array.isArray(profile.keys) ? profile.keys : []).map((entry) => {
      const keyId = String((entry && entry.key_id) || '');
      return {
        key_id: keyId,
        label: String((entry && entry.label) || keyId),
        created_at: Number((entry && entry.created_at) || 0),
        updated_at: Number((entry && entry.updated_at) || 0),
        configured: !!(keyId && keychain.hasProviderKey(provider, keyId)),
      };
    });
    const resolved = resolveProviderApiKey(provider, profile.primary_key_id || '');
    return {
      provider,
      primary_key_id: String(profile.primary_key_id || ''),
      keys,
      configured: !!(resolved && resolved.ok),
      legacy_configured: keychain.hasLegacyProviderKey(provider),
    };
  });
  return { ok: true, providers };
}

function ensureUniqueProviderKeyLabel(profile, keyId, label) {
  const targetLabel = String(label || '').trim().toLowerCase();
  if (!targetLabel) return { ok: false, message: 'Key label is required.' };
  const duplicate = (Array.isArray(profile && profile.keys) ? profile.keys : []).find((entry) => {
    const entryId = String((entry && entry.key_id) || '');
    const entryLabel = String((entry && entry.label) || '').trim().toLowerCase();
    return entryId !== keyId && entryLabel === targetLabel;
  });
  if (duplicate) return { ok: false, message: `Duplicate key label "${label}" for this provider.` };
  return { ok: true };
}

function upsertProviderKeyProfile(payload = {}) {
  const provider = String(payload.provider || '').trim().toLowerCase();
  const apiKey = String(payload.apiKey || '');
  const setPrimary = !!(payload.set_primary || payload.setPrimary);
  if (!PROVIDERS.includes(provider)) return { ok: false, message: 'Unsupported provider.' };
  if (!apiKey) return { ok: false, message: 'API key is required.' };

  const migration = ensureProviderProfileMigrated(provider);
  if (!migration || !migration.ok) {
    return { ok: false, message: (migration && migration.message) || 'Unable to update provider key.' };
  }
  const settings = migration.settings || readSettings();
  const profiles = normalizeProviderKeyProfiles(settings.provider_key_profiles);
  const profile = profiles[provider] || { primary_key_id: '', keys: [] };

  let keyId = normalizeProviderKeyId(payload.key_id || payload.keyId);
  if (!keyId) keyId = generateProviderKeyId();
  let label = String(payload.label || '').trim();
  if (!label) label = keyId === PROVIDER_PRIMARY_KEY_ID ? 'Primary' : `Key ${profile.keys.length + 1}`;
  const unique = ensureUniqueProviderKeyLabel(profile, keyId, label);
  if (!unique.ok) return unique;

  const storeRes = keychain.setProviderKey(provider, keyId, apiKey);
  if (!storeRes || !storeRes.ok) {
    return { ok: false, message: (storeRes && storeRes.message) || 'Unable to store provider key.' };
  }

  const ts = nowTs();
  const nextKeys = Array.isArray(profile.keys) ? profile.keys.slice() : [];
  const existingIdx = nextKeys.findIndex((entry) => String((entry && entry.key_id) || '') === keyId);
  if (existingIdx >= 0) {
    const existing = nextKeys[existingIdx] || {};
    nextKeys[existingIdx] = {
      key_id: keyId,
      label,
      created_at: Number(existing.created_at || ts) || ts,
      updated_at: ts,
    };
  } else {
    nextKeys.push({
      key_id: keyId,
      label,
      created_at: ts,
      updated_at: ts,
    });
  }
  const nextProfile = normalizeProviderKeyProfile({
    primary_key_id: (setPrimary || !profile.primary_key_id) ? keyId : profile.primary_key_id,
    keys: nextKeys,
  });
  profiles[provider] = nextProfile;
  const updated = writeSettings({ provider_key_profiles: profiles });
  return {
    ok: true,
    provider,
    key_id: keyId,
    primary_key_id: String(nextProfile.primary_key_id || ''),
    profile: getProviderProfile(updated, provider),
    providers: buildProviderKeysState().providers,
  };
}

function deleteProviderKeyProfileEntry(payload = {}) {
  const provider = String(payload.provider || '').trim().toLowerCase();
  let keyId = normalizeProviderKeyId(payload.key_id || payload.keyId || '');
  if (!PROVIDERS.includes(provider)) return { ok: false, message: 'Unsupported provider.' };

  const migration = ensureProviderProfileMigrated(provider);
  if (!migration || !migration.ok) {
    return { ok: false, message: (migration && migration.message) || 'Unable to delete provider key.' };
  }
  const settings = migration.settings || readSettings();
  const profiles = normalizeProviderKeyProfiles(settings.provider_key_profiles);
  const profile = profiles[provider] || { primary_key_id: '', keys: [] };

  if (!keyId) keyId = normalizeProviderKeyId(profile.primary_key_id) || pickProviderAutoPrimary(profile);
  if (!keyId && (!Array.isArray(profile.keys) || profile.keys.length === 0)) {
    const legacyRes = keychain.deleteLegacyProviderKey(provider);
    return {
      ok: !!(legacyRes && legacyRes.ok),
      message: legacyRes && legacyRes.ok ? '' : (legacyRes && legacyRes.message) || 'Unable to delete provider key.',
      provider,
      providers: buildProviderKeysState().providers,
    };
  }
  if (!keyId) return { ok: false, message: 'No key selected.' };

  const nextKeys = (Array.isArray(profile.keys) ? profile.keys : [])
    .filter((entry) => String((entry && entry.key_id) || '') !== keyId);
  if (nextKeys.length === (Array.isArray(profile.keys) ? profile.keys.length : 0)) {
    return { ok: false, message: `Key "${keyId}" was not found for ${provider}.` };
  }

  const deleteRes = keychain.deleteProviderKey(provider, keyId);
  if (!deleteRes || !deleteRes.ok) {
    return { ok: false, message: (deleteRes && deleteRes.message) || 'Unable to delete provider key.' };
  }

  let nextPrimary = normalizeProviderKeyId(profile.primary_key_id);
  if (nextPrimary === keyId) nextPrimary = pickProviderAutoPrimary({ keys: nextKeys });
  if (nextPrimary && !nextKeys.some((entry) => String((entry && entry.key_id) || '') === nextPrimary)) {
    nextPrimary = pickProviderAutoPrimary({ keys: nextKeys });
  }
  profiles[provider] = normalizeProviderKeyProfile({
    primary_key_id: nextPrimary,
    keys: nextKeys,
  });
  const updated = writeSettings({ provider_key_profiles: profiles });
  return {
    ok: true,
    provider,
    primary_key_id: String(profiles[provider].primary_key_id || ''),
    profile: getProviderProfile(updated, provider),
    providers: buildProviderKeysState().providers,
  };
}

function setProviderPrimaryKeyProfile(payload = {}) {
  const provider = String(payload.provider || '').trim().toLowerCase();
  const keyId = normalizeProviderKeyId(payload.key_id || payload.keyId || '');
  if (!PROVIDERS.includes(provider)) return { ok: false, message: 'Unsupported provider.' };
  if (!keyId) return { ok: false, message: 'key_id is required.' };

  const migration = ensureProviderProfileMigrated(provider);
  if (!migration || !migration.ok) {
    return { ok: false, message: (migration && migration.message) || 'Unable to set primary key.' };
  }
  const settings = migration.settings || readSettings();
  const profiles = normalizeProviderKeyProfiles(settings.provider_key_profiles);
  const profile = profiles[provider] || { primary_key_id: '', keys: [] };
  const exists = (Array.isArray(profile.keys) ? profile.keys : [])
    .some((entry) => String((entry && entry.key_id) || '') === keyId);
  if (!exists) return { ok: false, message: `Key "${keyId}" was not found for ${provider}.` };

  profiles[provider] = normalizeProviderKeyProfile({
    primary_key_id: keyId,
    keys: profile.keys,
  });
  const updated = writeSettings({ provider_key_profiles: profiles });
  return {
    ok: true,
    provider,
    primary_key_id: keyId,
    profile: getProviderProfile(updated, provider),
    providers: buildProviderKeysState().providers,
  };
}

function upsertProviderPrimaryCompatibility(provider, apiKey) {
  const migration = ensureProviderProfileMigrated(provider);
  if (!migration || !migration.ok) {
    return { ok: false, message: (migration && migration.message) || 'Unable to set provider key.' };
  }
  const settings = migration.settings || readSettings();
  const profile = getProviderProfile(settings, provider);
  const targetKeyId = normalizeProviderKeyId(profile.primary_key_id)
    || String(((Array.isArray(profile.keys) && profile.keys[0]) || {}).key_id || PROVIDER_PRIMARY_KEY_ID);
  const existing = (Array.isArray(profile.keys) ? profile.keys : [])
    .find((entry) => String((entry && entry.key_id) || '') === targetKeyId);
  const label = String((existing && existing.label) || (targetKeyId === PROVIDER_PRIMARY_KEY_ID ? 'Primary' : targetKeyId));
  return upsertProviderKeyProfile({
    provider,
    key_id: targetKeyId,
    label,
    apiKey,
    set_primary: true,
  });
}

function getDefaultSettings() {
  return {
    default_search_engine: 'ddg',
    trustcommons_bootstrap_complete: false,
    trustcommons_identity_id: '',
    trustcommons_display_name: '',
    trustcommons_bootstrap_at: 0,
    trustcommons_download_url: TRUSTCOMMONS_DOWNLOAD_URL,
    trustcommons_app_bundle_id: TRUSTCOMMONS_BUNDLE_ID,
    trustcommons_sync_enabled: true,
    trustcommons_sync_port: TRUSTCOMMONS_SYNC_DEFAULT_PORT,
    trustcommons_peer_sync_url: TRUSTCOMMONS_SYNC_DEFAULT_PEER_URL,
    trustcommons_sync_interval_sec: TRUSTCOMMONS_SYNC_DEFAULT_INTERVAL_SEC,
    hyperweb_relay_url: DEFAULT_HYPERWEB_RELAY_URL,
    hyperweb_enabled: true,
    crawler_mode: 'broad',
    crawler_markdown_first: true,
    crawler_robots_default: 'respect',
    crawler_depth_default: 3,
    crawler_page_cap_default: 80,
    agent_mode_v1_enabled: false,
    lumino_last_provider: 'openai',
    lumino_last_model: '',
    provider_key_profiles: defaultProviderKeyProfiles(),
    history_enabled: true,
    history_max_entries: HISTORY_DEFAULT_MAX_ENTRIES,
    telegram_enabled: false,
    telegram_allowed_chat_ids: [],
    telegram_allowed_usernames: [],
    telegram_poll_interval_sec: 2,
    telegram_bot_token_ref: '',
    lmstudio_base_url: LMSTUDIO_DEFAULT_BASE_URL,
    lmstudio_default_model: '',
    lmstudio_token_ref: '',
    orchestrator_web_provider: ORCHESTRATOR_WEB_PROVIDER_DEFAULT,
    orchestrator_web_provider_key_ref: '',
  };
}

function readSettings() {
  const defaults = getDefaultSettings();
  const settingsPath = getSettingsPath();
  if (!fs.existsSync(settingsPath)) {
    return defaults;
  }
  try {
    const raw = fs.readFileSync(settingsPath, 'utf8');
    const parsed = JSON.parse(raw);
    const engine = String((parsed && parsed.default_search_engine) || defaults.default_search_engine).trim().toLowerCase();
    const relay = String((parsed && parsed.hyperweb_relay_url) || defaults.hyperweb_relay_url).trim();
    const peerSyncUrl = String((parsed && parsed.trustcommons_peer_sync_url) || defaults.trustcommons_peer_sync_url).trim();
    const downloadUrl = String((parsed && parsed.trustcommons_download_url) || defaults.trustcommons_download_url).trim();
    const appBundle = String((parsed && parsed.trustcommons_app_bundle_id) || defaults.trustcommons_app_bundle_id).trim();
    const syncPort = Number((parsed && parsed.trustcommons_sync_port) || defaults.trustcommons_sync_port);
    const syncIntervalSec = Number((parsed && parsed.trustcommons_sync_interval_sec) || defaults.trustcommons_sync_interval_sec);
    const savedProvider = String((parsed && parsed.lumino_last_provider) || defaults.lumino_last_provider).trim().toLowerCase();
    const savedModel = String((parsed && parsed.lumino_last_model) || defaults.lumino_last_model).trim();
    const providerKeyProfiles = normalizeProviderKeyProfiles(parsed && parsed.provider_key_profiles);
    const telegramAllowedChatIds = Array.isArray(parsed && parsed.telegram_allowed_chat_ids)
      ? parsed.telegram_allowed_chat_ids.map((item) => String(item || '').trim()).filter(Boolean)
      : [];
    const telegramAllowedUsernames = Array.isArray(parsed && parsed.telegram_allowed_usernames)
      ? parsed.telegram_allowed_usernames.map((item) => String(item || '').trim().toLowerCase().replace(/^@/, '')).filter(Boolean)
      : [];
    const telegramPollIntervalSec = Number((parsed && parsed.telegram_poll_interval_sec) || defaults.telegram_poll_interval_sec);
    const lmstudioBaseUrl = String((parsed && parsed.lmstudio_base_url) || defaults.lmstudio_base_url).trim();
    const lmstudioDefaultModel = String((parsed && parsed.lmstudio_default_model) || defaults.lmstudio_default_model).trim();
    const orchestratorWebProvider = String((parsed && parsed.orchestrator_web_provider) || defaults.orchestrator_web_provider).trim().toLowerCase();
    return {
      default_search_engine: ['google', 'bing', 'ddg'].includes(engine) ? engine : 'ddg',
      trustcommons_bootstrap_complete: !!(parsed && parsed.trustcommons_bootstrap_complete),
      trustcommons_identity_id: String((parsed && parsed.trustcommons_identity_id) || '').trim(),
      trustcommons_display_name: String((parsed && parsed.trustcommons_display_name) || '').trim(),
      trustcommons_bootstrap_at: Number((parsed && parsed.trustcommons_bootstrap_at) || 0),
      trustcommons_download_url: downloadUrl || defaults.trustcommons_download_url,
      trustcommons_app_bundle_id: appBundle || defaults.trustcommons_app_bundle_id,
      trustcommons_sync_enabled: parsed && Object.prototype.hasOwnProperty.call(parsed, 'trustcommons_sync_enabled')
        ? !!parsed.trustcommons_sync_enabled
        : defaults.trustcommons_sync_enabled,
      trustcommons_sync_port: Number.isFinite(syncPort) && syncPort >= 1024 && syncPort <= 65535
        ? Math.round(syncPort)
        : defaults.trustcommons_sync_port,
      trustcommons_peer_sync_url: isLoopbackUrl(peerSyncUrl) ? peerSyncUrl : '',
      trustcommons_sync_interval_sec: Number.isFinite(syncIntervalSec) && syncIntervalSec >= 2 && syncIntervalSec <= 60
        ? Math.round(syncIntervalSec)
        : defaults.trustcommons_sync_interval_sec,
      hyperweb_relay_url: relay || defaults.hyperweb_relay_url,
      hyperweb_enabled: parsed && Object.prototype.hasOwnProperty.call(parsed, 'hyperweb_enabled')
        ? !!parsed.hyperweb_enabled
        : defaults.hyperweb_enabled,
      crawler_mode: String((parsed && parsed.crawler_mode) || defaults.crawler_mode).trim().toLowerCase() === 'safe' ? 'safe' : 'broad',
      crawler_markdown_first: parsed && Object.prototype.hasOwnProperty.call(parsed, 'crawler_markdown_first')
        ? !!parsed.crawler_markdown_first
        : defaults.crawler_markdown_first,
      crawler_robots_default: String((parsed && parsed.crawler_robots_default) || defaults.crawler_robots_default).trim().toLowerCase() === 'ignore'
        ? 'ignore'
        : 'respect',
      crawler_depth_default: Number.isFinite(Number(parsed && parsed.crawler_depth_default))
        ? Math.max(1, Math.min(6, Math.round(Number(parsed.crawler_depth_default))))
        : defaults.crawler_depth_default,
      crawler_page_cap_default: Number.isFinite(Number(parsed && parsed.crawler_page_cap_default))
        ? Math.max(5, Math.min(300, Math.round(Number(parsed.crawler_page_cap_default))))
        : defaults.crawler_page_cap_default,
      agent_mode_v1_enabled: !!(parsed && parsed.agent_mode_v1_enabled),
      lumino_last_provider: PROVIDERS.includes(savedProvider) ? savedProvider : defaults.lumino_last_provider,
      lumino_last_model: savedModel,
      provider_key_profiles: providerKeyProfiles,
      history_enabled: parsed && Object.prototype.hasOwnProperty.call(parsed, 'history_enabled')
        ? !!parsed.history_enabled
        : defaults.history_enabled,
      history_max_entries: Number.isFinite(Number(parsed && parsed.history_max_entries))
        ? Math.max(500, Math.min(10000, Math.round(Number(parsed.history_max_entries))))
        : defaults.history_max_entries,
      telegram_enabled: parsed && Object.prototype.hasOwnProperty.call(parsed, 'telegram_enabled')
        ? !!parsed.telegram_enabled
        : defaults.telegram_enabled,
      telegram_allowed_chat_ids: telegramAllowedChatIds,
      telegram_allowed_usernames: telegramAllowedUsernames,
      telegram_poll_interval_sec: Number.isFinite(telegramPollIntervalSec)
        ? Math.max(1, Math.min(30, Math.round(telegramPollIntervalSec)))
        : defaults.telegram_poll_interval_sec,
      telegram_bot_token_ref: String((parsed && parsed.telegram_bot_token_ref) || defaults.telegram_bot_token_ref).trim(),
      lmstudio_base_url: lmstudioBaseUrl || LMSTUDIO_DEFAULT_BASE_URL,
      lmstudio_default_model: lmstudioDefaultModel,
      lmstudio_token_ref: String((parsed && parsed.lmstudio_token_ref) || defaults.lmstudio_token_ref).trim(),
      orchestrator_web_provider: ['ddg', 'serpapi'].includes(orchestratorWebProvider)
        ? orchestratorWebProvider
        : defaults.orchestrator_web_provider,
      orchestrator_web_provider_key_ref: String((parsed && parsed.orchestrator_web_provider_key_ref) || defaults.orchestrator_web_provider_key_ref).trim(),
    };
  } catch (_) {
    return defaults;
  }
}

function writeSettings(next) {
  const current = readSettings();
  const input = (next && typeof next === 'object') ? next : {};
  const settingsPath = getSettingsPath();
  const requestedEngine = String(
    Object.prototype.hasOwnProperty.call(input, 'default_search_engine')
      ? input.default_search_engine
      : current.default_search_engine
  ).trim().toLowerCase();
  const requestedRelay = String(
    Object.prototype.hasOwnProperty.call(input, 'hyperweb_relay_url')
      ? input.hyperweb_relay_url
      : current.hyperweb_relay_url
  ).trim();
  const requestedPeerSyncUrl = String(
    Object.prototype.hasOwnProperty.call(input, 'trustcommons_peer_sync_url')
      ? input.trustcommons_peer_sync_url
      : current.trustcommons_peer_sync_url
  ).trim();
  const requestedDownloadUrl = String(
    Object.prototype.hasOwnProperty.call(input, 'trustcommons_download_url')
      ? input.trustcommons_download_url
      : current.trustcommons_download_url
  ).trim();
  const requestedAppBundle = String(
    Object.prototype.hasOwnProperty.call(input, 'trustcommons_app_bundle_id')
      ? input.trustcommons_app_bundle_id
      : current.trustcommons_app_bundle_id
  ).trim();
  const requestedSyncPort = Number(
    Object.prototype.hasOwnProperty.call(input, 'trustcommons_sync_port')
      ? input.trustcommons_sync_port
      : current.trustcommons_sync_port
  );
  const requestedSyncIntervalSec = Number(
    Object.prototype.hasOwnProperty.call(input, 'trustcommons_sync_interval_sec')
      ? input.trustcommons_sync_interval_sec
      : current.trustcommons_sync_interval_sec
  );
  const requestedLastProvider = String(
    Object.prototype.hasOwnProperty.call(input, 'lumino_last_provider')
      ? input.lumino_last_provider
      : current.lumino_last_provider
  ).trim().toLowerCase();
  const requestedLastModel = String(
    Object.prototype.hasOwnProperty.call(input, 'lumino_last_model')
      ? input.lumino_last_model
      : current.lumino_last_model
  ).trim();
  const requestedHistoryMaxEntries = Number(
    Object.prototype.hasOwnProperty.call(input, 'history_max_entries')
      ? input.history_max_entries
      : current.history_max_entries
  );
  const requestedProviderKeyProfiles = normalizeProviderKeyProfiles(
    Object.prototype.hasOwnProperty.call(input, 'provider_key_profiles')
      ? input.provider_key_profiles
      : current.provider_key_profiles
  );
  const requestedTelegramAllowedChatIds = Array.isArray(
    Object.prototype.hasOwnProperty.call(input, 'telegram_allowed_chat_ids')
      ? input.telegram_allowed_chat_ids
      : current.telegram_allowed_chat_ids
  )
    ? (
      Object.prototype.hasOwnProperty.call(input, 'telegram_allowed_chat_ids')
        ? input.telegram_allowed_chat_ids
        : current.telegram_allowed_chat_ids
    ).map((item) => String(item || '').trim()).filter(Boolean)
    : [];
  const requestedTelegramAllowedUsernames = Array.isArray(
    Object.prototype.hasOwnProperty.call(input, 'telegram_allowed_usernames')
      ? input.telegram_allowed_usernames
      : current.telegram_allowed_usernames
  )
    ? (
      Object.prototype.hasOwnProperty.call(input, 'telegram_allowed_usernames')
        ? input.telegram_allowed_usernames
        : current.telegram_allowed_usernames
    ).map((item) => String(item || '').trim().toLowerCase().replace(/^@/, '')).filter(Boolean)
    : [];
  const requestedTelegramPollIntervalSec = Number(
    Object.prototype.hasOwnProperty.call(input, 'telegram_poll_interval_sec')
      ? input.telegram_poll_interval_sec
      : current.telegram_poll_interval_sec
  );
  const requestedLmstudioBaseUrl = String(
    Object.prototype.hasOwnProperty.call(input, 'lmstudio_base_url')
      ? input.lmstudio_base_url
      : current.lmstudio_base_url
  ).trim();
  const requestedLmstudioDefaultModel = String(
    Object.prototype.hasOwnProperty.call(input, 'lmstudio_default_model')
      ? input.lmstudio_default_model
      : current.lmstudio_default_model
  ).trim();
  const requestedOrchestratorWebProvider = String(
    Object.prototype.hasOwnProperty.call(input, 'orchestrator_web_provider')
      ? input.orchestrator_web_provider
      : current.orchestrator_web_provider
  ).trim().toLowerCase();
  const settings = {
    default_search_engine: ['google', 'bing', 'ddg'].includes(requestedEngine)
      ? requestedEngine
      : 'ddg',
    trustcommons_bootstrap_complete: Object.prototype.hasOwnProperty.call(input, 'trustcommons_bootstrap_complete')
      ? !!input.trustcommons_bootstrap_complete
      : !!current.trustcommons_bootstrap_complete,
    trustcommons_identity_id: String(
      Object.prototype.hasOwnProperty.call(input, 'trustcommons_identity_id')
        ? input.trustcommons_identity_id
        : current.trustcommons_identity_id
    ).trim(),
    trustcommons_display_name: String(
      Object.prototype.hasOwnProperty.call(input, 'trustcommons_display_name')
        ? input.trustcommons_display_name
        : current.trustcommons_display_name
    ).trim(),
    trustcommons_bootstrap_at: Number(
      Object.prototype.hasOwnProperty.call(input, 'trustcommons_bootstrap_at')
        ? input.trustcommons_bootstrap_at
        : current.trustcommons_bootstrap_at
    ) || 0,
    trustcommons_download_url: requestedDownloadUrl || TRUSTCOMMONS_DOWNLOAD_URL,
    trustcommons_app_bundle_id: requestedAppBundle || TRUSTCOMMONS_BUNDLE_ID,
    trustcommons_sync_enabled: Object.prototype.hasOwnProperty.call(input, 'trustcommons_sync_enabled')
      ? !!input.trustcommons_sync_enabled
      : !!current.trustcommons_sync_enabled,
    trustcommons_sync_port: Number.isFinite(requestedSyncPort) && requestedSyncPort >= 1024 && requestedSyncPort <= 65535
      ? Math.round(requestedSyncPort)
      : TRUSTCOMMONS_SYNC_DEFAULT_PORT,
    trustcommons_peer_sync_url: isLoopbackUrl(requestedPeerSyncUrl) ? requestedPeerSyncUrl : '',
    trustcommons_sync_interval_sec: Number.isFinite(requestedSyncIntervalSec) && requestedSyncIntervalSec >= 2 && requestedSyncIntervalSec <= 60
      ? Math.round(requestedSyncIntervalSec)
      : TRUSTCOMMONS_SYNC_DEFAULT_INTERVAL_SEC,
    hyperweb_relay_url: requestedRelay || DEFAULT_HYPERWEB_RELAY_URL,
    hyperweb_enabled: Object.prototype.hasOwnProperty.call(input, 'hyperweb_enabled')
      ? !!input.hyperweb_enabled
      : !!current.hyperweb_enabled,
    crawler_mode: String(
      Object.prototype.hasOwnProperty.call(input, 'crawler_mode')
        ? input.crawler_mode
        : current.crawler_mode
    ).trim().toLowerCase() === 'safe' ? 'safe' : 'broad',
    crawler_markdown_first: Object.prototype.hasOwnProperty.call(input, 'crawler_markdown_first')
      ? !!input.crawler_markdown_first
      : !!current.crawler_markdown_first,
    crawler_robots_default: String(
      Object.prototype.hasOwnProperty.call(input, 'crawler_robots_default')
        ? input.crawler_robots_default
        : current.crawler_robots_default
    ).trim().toLowerCase() === 'ignore' ? 'ignore' : 'respect',
    crawler_depth_default: Math.max(1, Math.min(6, Number(
      Object.prototype.hasOwnProperty.call(input, 'crawler_depth_default')
        ? input.crawler_depth_default
        : current.crawler_depth_default
    ) || 3)),
    crawler_page_cap_default: Math.max(5, Math.min(300, Number(
      Object.prototype.hasOwnProperty.call(input, 'crawler_page_cap_default')
        ? input.crawler_page_cap_default
        : current.crawler_page_cap_default
    ) || 80)),
    agent_mode_v1_enabled: Object.prototype.hasOwnProperty.call(input, 'agent_mode_v1_enabled')
      ? !!input.agent_mode_v1_enabled
      : !!current.agent_mode_v1_enabled,
    lumino_last_provider: PROVIDERS.includes(requestedLastProvider) ? requestedLastProvider : 'openai',
    lumino_last_model: requestedLastModel,
    provider_key_profiles: requestedProviderKeyProfiles,
    history_enabled: Object.prototype.hasOwnProperty.call(input, 'history_enabled')
      ? !!input.history_enabled
      : !!current.history_enabled,
    history_max_entries: Number.isFinite(requestedHistoryMaxEntries)
      ? Math.max(500, Math.min(10000, Math.round(requestedHistoryMaxEntries)))
      : HISTORY_DEFAULT_MAX_ENTRIES,
    telegram_enabled: Object.prototype.hasOwnProperty.call(input, 'telegram_enabled')
      ? !!input.telegram_enabled
      : !!current.telegram_enabled,
    telegram_allowed_chat_ids: requestedTelegramAllowedChatIds,
    telegram_allowed_usernames: requestedTelegramAllowedUsernames,
    telegram_poll_interval_sec: Number.isFinite(requestedTelegramPollIntervalSec)
      ? Math.max(1, Math.min(30, Math.round(requestedTelegramPollIntervalSec)))
      : 2,
    telegram_bot_token_ref: String(
      Object.prototype.hasOwnProperty.call(input, 'telegram_bot_token_ref')
        ? input.telegram_bot_token_ref
        : current.telegram_bot_token_ref
    ).trim(),
    lmstudio_base_url: requestedLmstudioBaseUrl || LMSTUDIO_DEFAULT_BASE_URL,
    lmstudio_default_model: requestedLmstudioDefaultModel,
    lmstudio_token_ref: String(
      Object.prototype.hasOwnProperty.call(input, 'lmstudio_token_ref')
        ? input.lmstudio_token_ref
        : current.lmstudio_token_ref
    ).trim(),
    orchestrator_web_provider: ['ddg', 'serpapi'].includes(requestedOrchestratorWebProvider)
      ? requestedOrchestratorWebProvider
      : ORCHESTRATOR_WEB_PROVIDER_DEFAULT,
    orchestrator_web_provider_key_ref: String(
      Object.prototype.hasOwnProperty.call(input, 'orchestrator_web_provider_key_ref')
        ? input.orchestrator_web_provider_key_ref
        : current.orchestrator_web_provider_key_ref
    ).trim(),
  };
  fs.writeFileSync(settingsPath, JSON.stringify(settings, null, 2), 'utf8');
  return settings;
}

function normalizeHttpBaseUrl(value, fallback = '') {
  const raw = String(value || '').trim();
  const candidate = raw || String(fallback || '').trim();
  if (!candidate) return '';
  try {
    const parsed = new URL(candidate);
    if (!/^https?:$/i.test(parsed.protocol)) return '';
    parsed.hash = '';
    parsed.search = '';
    return parsed.toString().replace(/\/+$/, '');
  } catch (_) {
    return '';
  }
}

function normalizeAllowedChatIds(value) {
  const list = Array.isArray(value) ? value : [];
  return list.map((item) => String(item || '').trim()).filter(Boolean);
}

function normalizeAllowedUsernames(value) {
  const list = Array.isArray(value) ? value : [];
  return list.map((item) => String(item || '').trim().toLowerCase().replace(/^@/, '')).filter(Boolean);
}

function getSecureSecretStore() {
  if (!secureSecretStore) {
    secureSecretStore = createSecureSecretStore({
      userDataPath: app.getPath('userData'),
      safeStorage,
      keychain,
      service: APP_SECRET_SERVICE,
      logger: console,
    });
  }
  return secureSecretStore;
}

function getSecretValueByRef(ref) {
  const cleanRef = String(ref || '').trim();
  if (!cleanRef) return { ok: false, message: 'Secret ref is missing.' };
  return getSecureSecretStore().getSecret(cleanRef);
}

function setSecretValueByRef(ref, secret, prefix = 'sec') {
  const cleanSecret = String(secret || '');
  if (!cleanSecret) return { ok: false, message: 'Secret value is required.' };
  const store = getSecureSecretStore();
  const cleanRef = String(ref || '').trim() || store.createRef(prefix);
  const setRes = store.setSecret(cleanRef, cleanSecret);
  if (!setRes || !setRes.ok) return setRes;
  return { ok: true, ref: cleanRef };
}

function clearSecretValueByRef(ref) {
  const cleanRef = String(ref || '').trim();
  if (!cleanRef) return { ok: true, missing: true };
  return getSecureSecretStore().clearSecret(cleanRef);
}

function resolveLmstudioToken(settings = readSettings()) {
  const tokenRef = String((settings && settings.lmstudio_token_ref) || '').trim();
  if (!tokenRef) return { ok: true, apiKey: '', source: 'none', key_id: '' };
  const secRes = getSecretValueByRef(tokenRef);
  if (!secRes || !secRes.ok) return { ok: false, message: (secRes && secRes.message) || 'LM Studio token is not configured.' };
  return {
    ok: true,
    apiKey: String(secRes.secret || ''),
    source: 'secret_ref',
    key_id: tokenRef,
  };
}

function getTelegramToken(settings = readSettings()) {
  const tokenRef = String((settings && settings.telegram_bot_token_ref) || '').trim();
  if (!tokenRef) return { ok: false, message: 'Telegram token is not configured.' };
  return getSecretValueByRef(tokenRef);
}

function parseTelegramCommandArgs(text = '') {
  const raw = String(text || '').trim();
  if (!raw) return {};
  const out = {};
  raw.split(/\s+/).forEach((segment) => {
    const idx = segment.indexOf('=');
    if (idx <= 0) return;
    const key = String(segment.slice(0, idx) || '').trim().toLowerCase();
    const value = String(segment.slice(idx + 1) || '').trim();
    if (!key) return;
    out[key] = value;
  });
  return out;
}

function getOrchestratorSessionStore() {
  if (!orchestratorSessionStore) {
    orchestratorSessionStore = createOrchestratorSessionStore({
      userDataPath: app.getPath('userData'),
    });
  }
  return orchestratorSessionStore;
}

function getOrchestratorUsersStore() {
  if (!orchestratorUsersStore) {
    orchestratorUsersStore = createOrchestratorUsersStore({
      userDataPath: app.getPath('userData'),
    });
  }
  return orchestratorUsersStore;
}

function getOrchestratorPreferencesStore() {
  if (!orchestratorPreferencesStore) {
    orchestratorPreferencesStore = createOrchestratorPreferencesStore({
      userDataPath: app.getPath('userData'),
    });
  }
  return orchestratorPreferencesStore;
}

function getOrchestratorJobsStore() {
  if (!orchestratorJobsStore) {
    orchestratorJobsStore = createOrchestratorJobsStore({
      userDataPath: app.getPath('userData'),
    });
  }
  return orchestratorJobsStore;
}

function resolveRuntimeProviderAndModel(input = {}, settings = readSettings()) {
  const src = (input && typeof input === 'object') ? input : {};
  const preferredProvider = String(src.provider || settings.lumino_last_provider || 'openai').trim().toLowerCase();
  const provider = PROVIDERS.includes(preferredProvider) ? preferredProvider : 'openai';
  const preferredModel = String(
    src.model
    || settings.lumino_last_model
    || (provider === 'lmstudio' ? settings.lmstudio_default_model : '')
    || PROVIDER_SUMMARY_MODEL_FALLBACK[provider]
    || ''
  ).trim();
  return {
    provider,
    model: preferredModel,
  };
}

function resolveProviderRuntimeCredentials(provider, settings = readSettings(), keyId = '') {
  const target = String(provider || '').trim().toLowerCase();
  if (!PROVIDERS.includes(target)) {
    return { ok: false, message: 'Unsupported provider.' };
  }

  if (target === 'lmstudio') {
    const lmstudioTokenRes = resolveLmstudioToken(settings);
    let apiKey = '';
    let key_id = String((settings && settings.lmstudio_token_ref) || '').trim();
    if (lmstudioTokenRes && lmstudioTokenRes.ok && lmstudioTokenRes.apiKey) {
      apiKey = String(lmstudioTokenRes.apiKey || '');
    } else {
      const fallbackKey = resolveProviderApiKey(target, keyId);
      if (fallbackKey && fallbackKey.ok && fallbackKey.apiKey) {
        apiKey = String(fallbackKey.apiKey || '');
        key_id = String(fallbackKey.key_id || '');
      }
    }
    return {
      ok: true,
      provider: target,
      apiKey,
      key_id: key_id || '',
      base_url: normalizeHttpBaseUrl(
        String((settings && settings.lmstudio_base_url) || LMSTUDIO_DEFAULT_BASE_URL),
        LMSTUDIO_DEFAULT_BASE_URL,
      ),
    };
  }

  const keyRes = resolveProviderApiKey(target, keyId);
  if (!keyRes || !keyRes.ok || !keyRes.apiKey) {
    return {
      ok: false,
      message: (keyRes && keyRes.message) || `API key is not configured for ${target}.`,
    };
  }
  return {
    ok: true,
    provider: target,
    apiKey: String(keyRes.apiKey || ''),
    key_id: String(keyRes.key_id || ''),
    base_url: '',
  };
}

function decodeHtmlEntities(text = '') {
  return String(text || '')
    .replace(/&amp;/g, '&')
    .replace(/&lt;/g, '<')
    .replace(/&gt;/g, '>')
    .replace(/&quot;/g, '"')
    .replace(/&#39;/g, "'");
}

function dedupeWebResults(rows = []) {
  const dedup = [];
  const seen = new Set();
  (Array.isArray(rows) ? rows : []).forEach((item) => {
    const url = String((item && item.url) || '').trim();
    const title = String((item && item.title) || '').trim();
    if (!url || !title) return;
    const key = url.toLowerCase();
    if (seen.has(key)) return;
    seen.add(key);
    dedup.push({
      title: title.slice(0, 240),
      url,
      snippet: String((item && item.snippet) || '').trim().slice(0, 500),
    });
  });
  return dedup;
}

function parseDdgHtmlSearchResults(html = '') {
  const raw = String(html || '');
  if (!raw) return [];
  const rows = [];
  const re = /<a[^>]+class="[^"]*result__a[^"]*"[^>]+href="([^"]+)"[^>]*>([\s\S]*?)<\/a>/gi;
  let match;
  while ((match = re.exec(raw)) !== null) {
    const href = decodeHtmlEntities(String(match[1] || '').trim());
    const title = decodeHtmlEntities(String(match[2] || '').replace(/<[^>]+>/g, ' ').replace(/\s+/g, ' ').trim());
    if (!href || !title) continue;
    let url = href;
    if (href.startsWith('//')) url = `https:${href}`;
    if (href.startsWith('/l/?kh=')) {
      const m = href.match(/[?&]uddg=([^&]+)/i);
      if (m && m[1]) {
        try {
          url = decodeURIComponent(m[1]);
        } catch (_) {
          url = m[1];
        }
      }
    }
    if (!/^https?:\/\//i.test(url)) continue;
    rows.push({
      title,
      url,
      snippet: '',
    });
    if (rows.length >= 30) break;
  }
  return rows;
}

async function runOrchestratorWebSearch(params = {}) {
  const query = String((params && params.query) || '').trim();
  if (!query) return { ok: false, message: 'query is required.', results: [] };
  const maxResults = Math.max(1, Math.min(20, Number((params && params.max_results) || 8)));
  const settings = readSettings();
  const providerHint = String((params && params.provider_hint) || '').trim().toLowerCase();
  const configuredProvider = String((settings && settings.orchestrator_web_provider) || ORCHESTRATOR_WEB_PROVIDER_DEFAULT)
    .trim()
    .toLowerCase();
  const provider = ['ddg', 'serpapi'].includes(providerHint) ? providerHint : configuredProvider;

  if (provider === 'serpapi') {
    const ref = String((settings && settings.orchestrator_web_provider_key_ref) || '').trim();
    const keyRes = ref ? getSecretValueByRef(ref) : { ok: false, message: 'No SERPAPI key configured.' };
    const apiKey = keyRes && keyRes.ok ? String(keyRes.secret || '').trim() : '';
    if (apiKey) {
      try {
        const endpoint = `https://serpapi.com/search.json?engine=google&q=${encodeURIComponent(query)}&num=${maxResults}&api_key=${encodeURIComponent(apiKey)}`;
        const res = await fetchJsonWithTimeout(endpoint, {}, Math.max(6_000, Number(params.timeout_ms || 12_000)));
        if (res && res.ok) {
          const rows = Array.isArray(res.json && res.json.organic_results) ? res.json.organic_results : [];
          const results = rows.slice(0, maxResults).map((item) => ({
            title: String((item && item.title) || '').trim().slice(0, 240),
            url: String((item && item.link) || '').trim(),
            snippet: String((item && item.snippet) || '').trim().slice(0, 500),
          })).filter((item) => item.url);
          if (results.length > 0) {
            return {
              ok: true,
              provider: 'serpapi',
              query,
              results,
            };
          }
        }
      } catch (_) {
        // fall through to DDG
      }
    }
  }

  const ddgUrl = `https://api.duckduckgo.com/?q=${encodeURIComponent(query)}&format=json&no_redirect=1&no_html=1`;
  const fetchRes = await fetchJsonWithTimeout(ddgUrl, {}, Math.max(5_000, Number(params.timeout_ms || 10_000)));
  const rows = [];
  if (fetchRes && fetchRes.ok && fetchRes.json) {
    const body = fetchRes.json || {};
    if (body.AbstractText && body.AbstractURL) {
      rows.push({
        title: String(body.Heading || query).trim().slice(0, 240),
        url: String(body.AbstractURL || '').trim(),
        snippet: String(body.AbstractText || '').trim().slice(0, 500),
      });
    }
    const topics = Array.isArray(body.RelatedTopics) ? body.RelatedTopics : [];
    topics.slice(0, Math.max(maxResults * 3, 18)).forEach((topic) => {
      if (topic && topic.FirstURL && topic.Text) {
        rows.push({
          title: String(topic.Text || '').trim().slice(0, 240),
          url: String(topic.FirstURL || '').trim(),
          snippet: String(topic.Text || '').trim().slice(0, 500),
        });
        return;
      }
      const nested = Array.isArray(topic && topic.Topics) ? topic.Topics : [];
      nested.forEach((item) => {
        if (item && item.FirstURL && item.Text) {
          rows.push({
            title: String(item.Text || '').trim().slice(0, 240),
            url: String(item.FirstURL || '').trim(),
            snippet: String(item.Text || '').trim().slice(0, 500),
          });
        }
      });
    });
  }

  // Strong fallback: parse DuckDuckGo HTML SERP when instant answer returns too few links.
  const normalizedInstant = dedupeWebResults(rows);
  if (normalizedInstant.length < Math.min(5, maxResults)) {
    try {
      const ddgHtmlUrl = `https://duckduckgo.com/html/?q=${encodeURIComponent(query)}`;
      const htmlRes = await fetchTextWithTimeout(ddgHtmlUrl, {
        headers: {
          'user-agent': 'Subgrapher/1.0 (+https://subgrapher.local)',
        },
      }, Math.max(6_000, Number(params.timeout_ms || 12_000)));
      if (htmlRes && htmlRes.ok) {
        const parsed = parseDdgHtmlSearchResults(String(htmlRes.text || ''));
        parsed.forEach((item) => rows.push(item));
      }
    } catch (_) {
      // Keep graceful fallback.
    }
  }

  const normalized = dedupeWebResults(rows).slice(0, maxResults);
  if (normalized.length === 0) {
    return {
      ok: false,
      provider: 'ddg',
      query,
      results: [],
      message: 'Web search returned no results.',
    };
  }

  return {
    ok: true,
    provider: 'ddg',
    query,
    results: normalized,
  };
}

function getPathAExecutor() {
  if (!pathAExecutor) {
    pathAExecutor = createPathAExecutor({
      executeLegacyLuminoChat: executeLuminoChat,
    });
  }
  return pathAExecutor;
}

async function executePathAChat(input = {}, options = {}) {
  return getPathAExecutor().executePathAChat(input, options);
}

async function invokePathAFromPathB(input = {}, options = {}) {
  const settings = readSettings();
  const runtime = resolveRuntimeProviderAndModel(input, settings);
  return executePathAChat({
    ...(input && typeof input === 'object' ? input : {}),
    provider: runtime.provider,
    model: runtime.model,
  }, {
    ...(options && typeof options === 'object' ? options : {}),
    lane: 'path_a',
  });
}

function upsertPathBUserPreferences(input = {}) {
  const payload = (input && typeof input === 'object') ? input : {};
  const userScope = String(payload.user_scope || payload.chat_id || payload.username || '').trim().toLowerCase();
  if (!userScope) return { ok: false, message: 'user_scope is required.' };
  const patch = (payload.patch && typeof payload.patch === 'object') ? payload.patch : {};
  return getOrchestratorPreferencesStore().upsert(userScope, patch, {
    chat_id: String(payload.chat_id || '').trim(),
    username: normalizeTelegramUsername(String(payload.username || '').trim()),
  });
}

function isPathBToolLoopEnabled() {
  const raw = String(process.env.SUBGRAPHER_PATH_B_TOOL_LOOP || '1').trim().toLowerCase();
  return !['0', 'false', 'off', 'no'].includes(raw);
}

function logPathBToolEvent(toolName = '', payload = {}) {
  try {
    console.info('[path_b_tool]', JSON.stringify({
      tool: String(toolName || '').trim(),
      ...((payload && typeof payload === 'object') ? payload : {}),
    }));
  } catch (_) {
    // Best-effort logging only.
  }
}

function getPathBSearchableReferences() {
  const refs = getReferences();
  return (Array.isArray(refs) ? refs : []).filter((ref) => ref && !ref.is_public_candidate);
}

async function pathBGlobalReferenceSearch(params = {}) {
  const query = String((params && params.query) || '').trim();
  if (!query) return { ok: false, message: 'query is required.', results: [] };
  const topK = Math.max(1, Math.min(60, Number((params && params.top_k) || PATH_B_GLOBAL_TOP_K)));
  const refs = getPathBSearchableReferences();
  const result = scoreReferencesHybrid(query, refs, { topK });
  const byId = new Map();
  refs.forEach((ref) => {
    const id = String((ref && ref.id) || '').trim();
    if (id) byId.set(id, ref);
  });
  const rows = Array.isArray(result && result.results) ? result.results : [];
  return {
    ok: !!(result && result.ok),
    method: String((result && result.method) || 'hybrid:local-hash-embedding-v1'),
    query,
    results: rows.map((row) => {
      const srId = String((row && row.sr_id) || '').trim();
      const ref = byId.get(srId) || {};
      return {
        ...row,
        title: String(ref.title || '').trim(),
        intent: String(ref.intent || '').trim(),
        updated_at: Number(ref.updated_at || 0),
        artifact_count: Array.isArray(ref.artifacts) ? ref.artifacts.length : 0,
        tab_count: Array.isArray(ref.tabs) ? ref.tabs.length : 0,
      };
    }),
  };
}

function buildPathBReferenceSnapshot(ref = {}) {
  const artifacts = Array.isArray(ref.artifacts) ? ref.artifacts : [];
  const tabs = Array.isArray(ref.tabs) ? ref.tabs : [];
  const activeTabId = String(ref.active_tab_id || '').trim();
  const activeTab = tabs.find((tab) => String((tab && tab.id) || '').trim() === activeTabId) || tabs[0] || null;
  const recentArtifacts = artifacts
    .filter((artifact) => !isMemoryArtifact(artifact))
    .sort((a, b) => Number((b && b.updated_at) || 0) - Number((a && a.updated_at) || 0))
    .slice(0, 6)
    .map((artifact) => ({
      id: String((artifact && artifact.id) || '').trim(),
      title: String((artifact && artifact.title) || '').trim(),
      updated_at: Number((artifact && artifact.updated_at) || 0),
      type: normalizeArtifactType((artifact && artifact.type) || 'markdown'),
    }));
  return {
    sr_id: String(ref.id || '').trim(),
    title: String(ref.title || '').trim(),
    intent: String(ref.intent || '').trim(),
    updated_at: Number(ref.updated_at || 0),
    tab_count: tabs.length,
    artifact_count: artifacts.length,
    active_tab: activeTab
      ? {
        id: String((activeTab && activeTab.id) || '').trim(),
        title: String((activeTab && activeTab.title) || '').trim(),
        url: String((activeTab && activeTab.url) || '').trim(),
      }
      : null,
    recent_artifacts: recentArtifacts,
    search_text_preview: trimForPrompt(buildReferenceSearchText(ref), 1600),
  };
}

async function pathBReadReferenceSnapshot(params = {}) {
  const srId = String((params && params.sr_id) || '').trim();
  if (!srId) return { ok: false, message: 'sr_id is required.' };
  const refs = getReferences();
  const idx = findReferenceIndex(refs, srId);
  if (idx < 0) return { ok: false, message: 'Reference not found.' };
  return {
    ok: true,
    sr_id: srId,
    snapshot: buildPathBReferenceSnapshot(refs[idx]),
  };
}

async function createReferenceForTopic(params = {}) {
  const topic = String((params && params.topic) || '').trim();
  if (!topic) return { ok: false, message: 'topic is required.' };
  const source = String((params && params.source) || 'orchestrator').trim().toLowerCase();
  const dayBucket = String((params && params.day_bucket) || new Date().toISOString().slice(0, 10)).trim();
  const userScope = String((params && params.user_scope) || 'default').trim().toLowerCase();
  const key = String((params && params.idempotency_key) || '').trim() || computeReferenceTopicKey({
    topic,
    source,
    day_bucket: dayBucket,
    user_scope: userScope,
  });
  const refs = getReferences();
  const created = createReferenceBase({
    title: deriveReferenceTitleFromTopic(topic),
    intent: topic,
    relation_type: 'root',
    current_tab: {
      url: getDefaultSearchHomeUrl(),
      title: getDefaultSearchHomeTitle(),
    },
    agent_meta: {
      created_by: 'lumino_b',
      path: 'path_b',
      source,
      run_id: String((params && params.run_id) || '').trim(),
      job_id: String((params && params.job_id) || '').trim(),
      idempotency_key: key,
      status: 'active',
      created_at: nowTs(),
      updated_at: nowTs(),
    },
  });
  refs.unshift(created);
  setReferences(refs);
  pathBMetrics.pathb_create_count += 1;
  logPathBToolEvent('create_reference', {
    run_id: String((params && params.run_id) || '').trim(),
    chosen_mode: 'created',
    chosen_sr_id: String((created && created.id) || '').trim(),
    pathb_create_count: pathBMetrics.pathb_create_count,
  });
  return {
    ok: true,
    sr_id: String((created && created.id) || '').trim(),
    created: true,
    idempotency_key: key,
    reference: created,
  };
}

async function reuseReferenceById(params = {}) {
  const srId = String((params && params.sr_id) || '').trim();
  if (!srId) return { ok: false, message: 'sr_id is required.' };
  const refs = getReferences();
  const idx = findReferenceIndex(refs, srId);
  if (idx < 0) return { ok: false, message: 'Reference not found.' };
  const patch = (params && params.meta_patch && typeof params.meta_patch === 'object')
    ? params.meta_patch
    : {};
  const currentMeta = normalizeReferenceAgentMeta(refs[idx].agent_meta) || {};
  refs[idx].agent_meta = normalizeReferenceAgentMeta({
    ...currentMeta,
    ...patch,
    created_by: patch.created_by || currentMeta.created_by || 'lumino_b',
    path: patch.path || currentMeta.path || 'path_b',
    source: patch.source || currentMeta.source || 'orchestrator',
    status: patch.status || currentMeta.status || 'active',
    created_at: Number(currentMeta.created_at || nowTs()),
    updated_at: nowTs(),
  });
  refs[idx].updated_at = nowTs();
  setReferences(refs);
  return {
    ok: true,
    sr_id: srId,
    created: false,
    reference: refs[idx],
  };
}

async function pathBSelectReference(params = {}) {
  const srId = String((params && params.sr_id) || '').trim();
  if (!srId) return { ok: false, message: 'sr_id is required.' };
  const source = String((params && params.source) || 'orchestrator').trim().toLowerCase();
  const res = await reuseReferenceById({
    sr_id: srId,
    meta_patch: {
      created_by: 'lumino_b',
      path: 'path_b',
      source,
      run_id: String((params && params.run_id) || '').trim(),
      job_id: String((params && params.job_id) || '').trim(),
      idempotency_key: String((params && params.idempotency_key) || '').trim(),
      status: 'active',
      updated_at: nowTs(),
    },
  });
  if (res && res.ok) {
    pathBMetrics.pathb_reuse_count += 1;
    logPathBToolEvent('select_reference', {
      run_id: String((params && params.run_id) || '').trim(),
      chosen_mode: 'reused',
      chosen_sr_id: srId,
      pathb_reuse_count: pathBMetrics.pathb_reuse_count,
    });
  }
  return res;
}

function normalizePathBHttpUrl(raw = '') {
  try {
    const parsed = new URL(String(raw || '').trim());
    if (parsed.protocol !== 'http:' && parsed.protocol !== 'https:') return '';
    parsed.hash = '';
    return parsed.toString();
  } catch (_) {
    return '';
  }
}

function extractPathBMetaContent(html = '', key = '') {
  const safeKey = String(key || '').trim().toLowerCase();
  if (!safeKey) return '';
  const directProperty = new RegExp(`<meta[^>]+property=["']${safeKey}["'][^>]+content=["']([\\s\\S]*?)["'][^>]*>`, 'i');
  const reverseProperty = new RegExp(`<meta[^>]+content=["']([\\s\\S]*?)["'][^>]+property=["']${safeKey}["'][^>]*>`, 'i');
  const directName = new RegExp(`<meta[^>]+name=["']${safeKey}["'][^>]+content=["']([\\s\\S]*?)["'][^>]*>`, 'i');
  const reverseName = new RegExp(`<meta[^>]+content=["']([\\s\\S]*?)["'][^>]+name=["']${safeKey}["'][^>]*>`, 'i');
  const match = String(html || '').match(directProperty)
    || String(html || '').match(reverseProperty)
    || String(html || '').match(directName)
    || String(html || '').match(reverseName);
  return match ? decodeHtmlEntities(String(match[1] || '').replace(/\s+/g, ' ').trim()) : '';
}

function extractPathBPageTextChunk(html = '', maxLen = 900) {
  const noScripts = String(html || '')
    .replace(/<script[\s\S]*?<\/script>/gi, ' ')
    .replace(/<style[\s\S]*?<\/style>/gi, ' ')
    .replace(/<noscript[\s\S]*?<\/noscript>/gi, ' ')
    .replace(/<template[\s\S]*?<\/template>/gi, ' ')
    .replace(/<!--[\s\S]*?-->/g, ' ');
  const text = decodeHtmlEntities(noScripts.replace(/<[^>]+>/g, ' ').replace(/\s+/g, ' ').trim());
  if (!text) return '';
  return text.slice(0, Math.max(120, maxLen));
}

function extractPathBPageMetadata(html = '', fallbackUrl = '') {
  const titleMatch = String(html || '').match(/<title[^>]*>([\s\S]*?)<\/title>/i);
  const title = titleMatch
    ? decodeHtmlEntities(String(titleMatch[1] || '').replace(/<[^>]+>/g, ' ').replace(/\s+/g, ' ').trim())
    : '';
  const ogTitle = extractPathBMetaContent(html, 'og:title');
  const textChunk = extractPathBPageTextChunk(html, 1200);
  const finalTitle = String(ogTitle || title || fallbackUrl || '').trim();
  return {
    title: finalTitle,
    og_title: ogTitle,
    text_chunk: textChunk,
  };
}

async function pathBVerifyLink(params = {}) {
  const rawUrl = String((params && params.url) || '').trim();
  const query = String((params && params.query) || '').trim();
  const intentContext = String((params && params.intent_context) || '').trim();
  if (!rawUrl) return { ok: false, accepted: false, message: 'url is required.' };
  if (!query) return { ok: false, accepted: false, message: 'query is required.' };
  const normalizedInput = normalizePathBHttpUrl(rawUrl);
  if (!normalizedInput) {
    return { ok: false, accepted: false, message: 'Only http(s) URLs are supported.' };
  }

  const { controller, timer } = makeTimeoutSignal(14_000);
  try {
    const response = await fetch(normalizedInput, {
      signal: controller.signal,
      redirect: 'follow',
      headers: {
        'user-agent': 'Subgrapher/1.0 (+https://subgrapher.local)',
        accept: 'text/html, text/plain;q=0.9, */*;q=0.1',
      },
    });
    const finalUrl = normalizePathBHttpUrl(String(response.url || normalizedInput));
    if (!response.ok) {
      return {
        ok: true,
        accepted: false,
        url: finalUrl || normalizedInput,
        status: Number(response.status || 0),
        relevance_score: 0,
        reason: `HTTP ${Number(response.status || 0)} while verifying page.`,
      };
    }

    const contentType = String((response.headers && response.headers.get('content-type')) || '').toLowerCase();
    const body = await response.text();
    const html = String(body || '').slice(0, 400_000);
    const meta = extractPathBPageMetadata(html, finalUrl || normalizedInput);
    const textChunk = String(meta.text_chunk || '').trim();
    const readable = (
      textChunk.length >= 120
      && (
        contentType.includes('text/html')
        || contentType.includes('text/plain')
        || contentType.includes('application/xhtml+xml')
        || !contentType
      )
    );
    const scoringQuery = [query, intentContext].filter(Boolean).join(' ').trim() || query;
    const pseudoReference = {
      id: 'pathb_url_probe',
      title: String(meta.title || finalUrl || normalizedInput).trim(),
      intent: scoringQuery,
      tabs: [{
        title: String(meta.title || finalUrl || normalizedInput).trim(),
        url: finalUrl || normalizedInput,
        excerpt: textChunk,
      }],
      artifacts: [{
        id: 'probe',
        title: String(meta.title || 'Page').trim(),
        content: textChunk,
      }],
    };
    const scoreRes = scoreReferencesHybrid(scoringQuery, [pseudoReference], { topK: 1 });
    const relevance = Number((((scoreRes && scoreRes.results) || [])[0] || {}).score || 0);
    const accepted = readable && relevance >= PATH_B_LINK_VERIFY_THRESHOLD;
    if (accepted) {
      pathBMetrics.pathb_verified_url_count += 1;
    }
    logPathBToolEvent('verify_link', {
      url: finalUrl || normalizedInput,
      accepted,
      relevance_score: Number(relevance.toFixed(4)),
      pathb_verified_url_count: pathBMetrics.pathb_verified_url_count,
    });
    return {
      ok: true,
      accepted,
      url: finalUrl || normalizedInput,
      url_norm: normalizeUrlForMatch(finalUrl || normalizedInput),
      title: String(meta.title || finalUrl || normalizedInput).trim(),
      relevance_score: Number(relevance.toFixed(4)),
      reason: accepted
        ? `Readable content and relevance ${relevance.toFixed(4)} >= ${PATH_B_LINK_VERIFY_THRESHOLD.toFixed(2)}.`
        : (!readable
          ? 'Page content was not readable enough for verification.'
          : `Relevance ${relevance.toFixed(4)} is below ${PATH_B_LINK_VERIFY_THRESHOLD.toFixed(2)}.`),
      status: Number(response.status || 0),
      content_type: contentType,
    };
  } catch (err) {
    logPathBToolEvent('verify_link', {
      url: normalizedInput,
      accepted: false,
      error: String((err && err.message) || 'unknown error'),
    });
    return {
      ok: false,
      accepted: false,
      url: normalizedInput,
      relevance_score: 0,
      message: `Link verification failed: ${String((err && err.message) || 'unknown error')}`,
    };
  } finally {
    clearTimeout(timer);
  }
}

async function pathBAddTab(params = {}) {
  const srId = String((params && params.sr_id) || '').trim();
  const rawUrl = String((params && params.url) || '').trim();
  const runId = String((params && params.run_id) || '').trim();
  if (!srId) return { ok: false, message: 'sr_id is required.' };
  const normalizedUrl = normalizePathBHttpUrl(rawUrl);
  if (!normalizedUrl) return { ok: false, message: 'A valid http(s) url is required.' };
  const refs = getReferences();
  const idx = findReferenceIndex(refs, srId);
  if (idx < 0) return { ok: false, message: 'Reference not found.' };
  refs[idx].tabs = Array.isArray(refs[idx].tabs) ? refs[idx].tabs : [];
  const urlNorm = normalizeUrlForMatch(normalizedUrl);
  const existing = refs[idx].tabs.find((tab) => normalizeUrlForMatch((tab && tab.url) || '') === urlNorm);
  if (existing) {
    refs[idx].active_tab_id = String((existing && existing.id) || '').trim() || refs[idx].active_tab_id;
    refs[idx].updated_at = nowTs();
    setReferences(refs);
    logPathBToolEvent('add_tab', {
      run_id: runId,
      chosen_sr_id: srId,
      deduped: true,
      url: normalizedUrl,
    });
    return {
      ok: true,
      sr_id: srId,
      url: normalizedUrl,
      deduped: true,
      added: false,
    };
  }
  const webCount = refs[idx].tabs.filter((tab) => String((tab && tab.tab_kind) || 'web').trim().toLowerCase() === 'web').length;
  if (webCount >= MAX_BROWSER_TABS_PER_REFERENCE) {
    return { ok: false, message: `Maximum web tabs reached (${MAX_BROWSER_TABS_PER_REFERENCE}).` };
  }
  const nextTab = createWebTab({
    url: normalizedUrl,
    title: String((params && params.title) || normalizedUrl).trim().slice(0, 180) || normalizedUrl,
  });
  insertWebTabAdjacent(refs[idx], nextTab, String((params && params.insert_after_tab_id) || '').trim());
  refs[idx].active_tab_id = String((nextTab && nextTab.id) || '').trim();
  maybeAutoRetitleReferenceFromActiveTab(refs[idx]);
  refs[idx].updated_at = nowTs();
  setReferences(refs);
  logPathBToolEvent('add_tab', {
    run_id: runId,
    chosen_sr_id: srId,
    deduped: false,
    url: normalizedUrl,
  });
  return {
    ok: true,
    sr_id: srId,
    url: normalizedUrl,
    tab_id: String((nextTab && nextTab.id) || '').trim(),
    added: true,
    deduped: false,
  };
}

function isPathBSystemIntakeArtifact(artifact) {
  const title = String((artifact && artifact.title) || '').trim().toLowerCase();
  if (!title) return false;
  return (
    title.includes('path b intake')
    || title.includes('intake context')
    || title.includes('system intake')
  );
}

function isSubstantiveArtifactForPathB(artifact) {
  if (!artifact || typeof artifact !== 'object') return false;
  if (isMemoryArtifact(artifact)) return false;
  if (isPathBSystemIntakeArtifact(artifact)) return false;
  const content = String((artifact && artifact.content) || '').replace(/\s+/g, ' ').trim();
  return content.length >= 80;
}

function extractPathBArtifactSummary(artifact = {}, maxLen = 1200) {
  const content = String((artifact && artifact.content) || '').trim();
  if (!content) return '';
  const lines = content.split('\n');
  const markerIdx = lines.findIndex((line) => /telegram[- ]ready summary/i.test(String(line || '').trim()));
  if (markerIdx >= 0) {
    const preferred = lines.slice(markerIdx + 1, markerIdx + 16).join(' ').replace(/\s+/g, ' ').trim();
    if (preferred) return preferred.slice(0, maxLen);
  }
  const compact = content
    .replace(/```[\s\S]*?```/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
  return compact.slice(0, maxLen);
}

async function pathBReadResearchArtifact(params = {}) {
  const srId = String((params && params.sr_id) || '').trim();
  if (!srId) return { ok: false, message: 'sr_id is required.' };
  const delegated = (params && params.delegated_response && typeof params.delegated_response === 'object')
    ? params.delegated_response
    : {};

  const pendingArtifacts = Array.isArray(delegated.pending_artifacts) ? delegated.pending_artifacts : [];
  const pendingCandidate = pendingArtifacts
    .filter((artifact) => String((artifact && artifact.reference_id) || '').trim() === srId)
    .filter((artifact) => isSubstantiveArtifactForPathB(artifact))
    .sort((a, b) => Number((b && b.updated_at) || 0) - Number((a && a.updated_at) || 0))[0] || null;
  if (pendingCandidate) {
    return {
      ok: true,
      sr_id: srId,
      source: 'path_a_pending_artifact',
      artifact: pendingCandidate,
      artifact_id_used_for_summary: String((pendingCandidate && pendingCandidate.id) || '').trim(),
      summary: extractPathBArtifactSummary(pendingCandidate),
    };
  }

  const refs = getReferences();
  const idx = findReferenceIndex(refs, srId);
  if (idx >= 0) {
    const refArtifacts = Array.isArray(refs[idx].artifacts) ? refs[idx].artifacts : [];
    const best = refArtifacts
      .filter((artifact) => isSubstantiveArtifactForPathB(artifact))
      .sort((a, b) => Number((b && b.updated_at) || 0) - Number((a && a.updated_at) || 0))[0] || null;
    if (best) {
      return {
        ok: true,
        sr_id: srId,
        source: 'reference_latest_artifact',
        artifact: best,
        artifact_id_used_for_summary: String((best && best.id) || '').trim(),
        summary: extractPathBArtifactSummary(best),
      };
    }
  }

  const fallbackText = String(
    delegated.message
    || delegated.final_message
    || delegated.text
    || ''
  ).trim();
  if (fallbackText) {
    return {
      ok: true,
      sr_id: srId,
      source: 'path_a_text_fallback',
      artifact: null,
      artifact_id_used_for_summary: '',
      summary: trimForPrompt(fallbackText, 1200),
    };
  }

  return {
    ok: false,
    sr_id: srId,
    message: 'No substantive non-memory artifact was found.',
    artifact: null,
    artifact_id_used_for_summary: '',
    summary: '',
  };
}

async function pathBDelegatePathA(params = {}, options = {}) {
  const srId = String((params && params.sr_id) || '').trim();
  const workerPrompt = String((params && params.worker_prompt) || '').trim();
  if (!srId) return { ok: false, message: 'sr_id is required.' };
  if (!workerPrompt) return { ok: false, message: 'worker_prompt is required.' };

  const runId = String((params && params.run_id) || '').trim();
  const source = String((params && params.source) || 'telegram').trim().toLowerCase();
  const jobId = String((params && params.job_id) || '').trim();
  const idempotencyKey = String((params && params.idempotency_key) || '').trim();
  const invokeRes = await invokePathAFromPathB({
    ...(params && typeof params === 'object' ? params : {}),
    sr_id: srId,
    message: workerPrompt,
  }, options);
  const applyRes = await applyPendingAgentUpdatesInMain(invokeRes, srId, {
    lane: 'path_b',
    run_id: runId,
    source,
    job_id: jobId,
    idempotency_key: idempotencyKey,
  });
  logPathBToolEvent('delegate_path_a', {
    run_id: runId,
    chosen_sr_id: srId,
    pending_changed: !!(applyRes && applyRes.changed),
  });
  return {
    ...(invokeRes && typeof invokeRes === 'object' ? invokeRes : {}),
    pending_apply: applyRes || null,
  };
}

function getPathBExecutor() {
  if (!pathBExecutor) {
    const enabled = isPathBToolLoopEnabled();
    if (!enabled) {
      console.warn('[path_b] Tool-loop mode disabled via SUBGRAPHER_PATH_B_TOOL_LOOP env flag. Using legacy fallback flow.');
    }
    pathBExecutor = createPathBExecutor({
      enabled,
      globalReferenceSearch: pathBGlobalReferenceSearch,
      readReferenceSnapshot: pathBReadReferenceSnapshot,
      selectReference: pathBSelectReference,
      createReference: createReferenceForTopic,
      webSearch: runOrchestratorWebSearch,
      verifyLink: pathBVerifyLink,
      addTab: pathBAddTab,
      delegatePathA: pathBDelegatePathA,
      readResearchArtifact: pathBReadResearchArtifact,
      upsertUserPreferences: upsertPathBUserPreferences,
      executeAgenticLoop,
      callProviderWithTools,
      resolveRuntimeCredentials: (provider, keyId = '') => resolveProviderRuntimeCredentials(provider, readSettings(), keyId),
      sessionStore: getOrchestratorSessionStore(),
      logger: console,
    });
  }
  return pathBExecutor;
}

async function executePathBTask(input = {}, options = {}) {
  return getPathBExecutor().executePathBTask(input, options);
}

function normalizeTelegramUsername(value = '') {
  return String(value || '').trim().toLowerCase().replace(/^@/, '');
}

function isValidTelegramUserId(value = '') {
  const id = String(value || '').trim();
  if (!id) return false;
  if (id.length < 3 || id.length > 30) return false;
  return /^[a-zA-Z0-9_-]+$/.test(id);
}

function isTelegramRegistrationPending(chatId = '') {
  const key = String(chatId || '').trim();
  if (!key) return false;
  return telegramPendingRegistrations.has(key);
}

function setTelegramRegistrationPending(chatId = '', pending = true) {
  const key = String(chatId || '').trim();
  if (!key) return;
  if (pending) {
    telegramPendingRegistrations.set(key, nowTs());
    return;
  }
  telegramPendingRegistrations.delete(key);
}

function buildTelegramCommandMenu() {
  return [
    '/clear - Clear conversation context',
    '/stats - View your usage stats',
    '/help - View full command list',
  ].join('\n');
}

function buildTelegramWelcomeBackText(user = {}) {
  const userId = String((user && user.user_id) || 'user').trim() || 'user';
  const promptsToday = Math.max(0, Number((user && user.prompts_today) || 0));
  const promptsTotal = Math.max(0, Number((user && user.prompts_total) || 0));
  return [
    `Welcome back, ${userId}!`,
    '',
    'Stats:',
    `- Prompts today: ${promptsToday}`,
    `- Total prompts: ${promptsTotal}`,
    '',
    `Commands: ${buildTelegramCommandMenu()}`,
  ].join('\n');
}

function buildTelegramRegistrationSuccessText(userId = '') {
  const id = String(userId || '').trim() || 'user';
  return [
    `Account registered as: ${id}`,
    '',
    'You can now chat with Lumino!',
    '',
    'Commands:',
    buildTelegramCommandMenu(),
  ].join('\n');
}

function buildTelegramNotRegisteredText() {
  return [
    "You're not registered yet!",
    '',
    'Type /hello to create an account.',
  ].join('\n');
}

function splitTelegramCommand(text = '') {
  const raw = String(text || '').trim();
  if (!raw.startsWith('/')) return { command: '', argsRaw: raw };
  const firstSpace = raw.indexOf(' ');
  const commandPart = firstSpace >= 0 ? raw.slice(0, firstSpace) : raw;
  const argsRaw = firstSpace >= 0 ? raw.slice(firstSpace + 1).trim() : '';
  const slashPart = commandPart.includes('@') ? commandPart.slice(0, commandPart.indexOf('@')) : commandPart;
  return {
    command: String(slashPart || '').trim().toLowerCase(),
    argsRaw,
  };
}

function splitLongTelegramText(text = '', maxLen = 3900) {
  const input = String(text || '').trim();
  if (!input) return [];
  if (input.length <= maxLen) return [input];
  const chunks = [];
  let cursor = 0;
  while (cursor < input.length) {
    const end = Math.min(input.length, cursor + maxLen);
    chunks.push(input.slice(cursor, end));
    cursor = end;
  }
  return chunks;
}

async function sendTelegramText(chatId, text, options = {}) {
  const targetChatId = String(chatId || '').trim();
  if (!targetChatId || !telegramService) return { ok: false, message: 'Telegram service unavailable.' };
  const chunks = splitLongTelegramText(text, 3900);
  if (chunks.length === 0) return { ok: false, message: 'Message is empty.' };
  let last = { ok: true };
  for (const chunk of chunks) {
    last = await telegramService.sendMessage(targetChatId, chunk, {
      disable_web_page_preview: options.disable_web_page_preview !== false,
      parse_mode: options.parse_mode || '',
    });
    if (!last || !last.ok) return last || { ok: false, message: 'Unable to send Telegram message.' };
  }
  return { ok: true };
}

function parseBoolLike(value, fallback = false) {
  const raw = String(value || '').trim().toLowerCase();
  if (!raw) return !!fallback;
  if (['1', 'true', 'yes', 'y', 'on'].includes(raw)) return true;
  if (['0', 'false', 'no', 'n', 'off'].includes(raw)) return false;
  return !!fallback;
}

function formatOrchestratorJobsList(jobs = []) {
  const list = Array.isArray(jobs) ? jobs : [];
  if (list.length === 0) return 'No scheduled jobs found.';
  return list.slice(0, 40).map((job) => {
    const id = String((job && job.id) || '').trim();
    const name = String((job && job.name) || 'Untitled').trim();
    const status = String((job && job.status) || 'active').trim().toLowerCase();
    const type = String((job && job.schedule_type) || 'daily').trim();
    const time = String((job && job.time) || '').trim();
    const day = String((job && job.day) || '').trim();
    const date = String((job && job.once_date) || '').trim();
    const tz = String((job && job.timezone) || '').trim();
    const scheduleText = type === 'weekly'
      ? `${type} ${day || '(day?)'} ${time || '(time?)'} ${tz || ''}`.trim()
      : (
        type === 'once'
          ? `${type} ${date || '(date?)'} ${time || '(time?)'} ${tz || ''}`.trim()
          : `${type} ${time || '(time?)'} ${tz || ''}`.trim()
      );
    const nextRun = Number((job && job.next_run_at) || 0);
    const nextText = nextRun > 0 ? new Date(nextRun).toLocaleString('en-US') : 'n/a';
    return `- ${id}\n  ${name}\n  ${scheduleText}\n  status=${status}, next=${nextText}`;
  }).join('\n');
}

function buildTelegramHelpText() {
  return [
    'Subgrapher Orchestrator Commands:',
    '/hello - start registration or show your account',
    '/help - show this help',
    '/clear - clear Path B context for this chat',
    '/stats - show your usage stats',
    '/context_clear - clear Path B context for this chat',
    '/jobs - list scheduled jobs',
    '/jobs_running - list scheduled jobs',
    '/cron - list scheduled jobs',
    '/job_create name=... task=... type=daily|weekly|once time=HH:MM day=monday once_date=YYYY-MM-DD timezone=Area/City notify=true|false',
    '/job_edit <id> name=... task=... type=... time=... day=... once_date=... timezone=... notify=true|false',
    '/job_pause <id>',
    '/job_resume <id>',
    '/job_delete <id>',
    '',
    'Any normal text message runs Path B orchestrator.',
  ].join('\n');
}

function extractTelegramTextMessage(message = {}) {
  return String((message && message.text) || '').trim();
}

async function handleTelegramCommand(command, argsRaw, context = {}) {
  const chatId = String(context.chat_id || '').trim();
  const username = String(context.username || '').trim();
  const usersStore = getOrchestratorUsersStore();
  const jobsStore = getOrchestratorJobsStore();
  const registered = usersStore.getByChatId(chatId);

  if (command === '/help') {
    return sendTelegramText(chatId, buildTelegramHelpText());
  }

  if (command === '/hello') {
    if (registered) {
      usersStore.register({
        chat_id: chatId,
        telegram_username: normalizeTelegramUsername(username),
        user_id: String(registered.user_id || '').trim(),
      });
      setTelegramRegistrationPending(chatId, false);
      return sendTelegramText(chatId, buildTelegramWelcomeBackText(registered));
    }
    setTelegramRegistrationPending(chatId, true);
    return sendTelegramText(chatId, [
      'Welcome to Lumino Bot!',
      '',
      'Please type a unique User ID to register your account.',
      '(This will be your identity - choose something memorable)',
    ].join('\n'));
  }

  if (!registered) {
    return sendTelegramText(chatId, buildTelegramNotRegisteredText());
  }

  if (command === '/clear' || command === '/context_clear') {
    const clearRes = getPathBExecutor().clearConversation(chatId);
    if (!clearRes || !clearRes.ok) {
      return sendTelegramText(chatId, 'Unable to clear context.');
    }
    return sendTelegramText(chatId, 'Path B context cleared for this chat.');
  }

  if (command === '/stats') {
    return sendTelegramText(chatId, [
      `Stats for ${String(registered.user_id || 'user')}:`,
      '',
      `- Registered: ${new Date(Number(registered.created_at || nowTs())).toISOString().slice(0, 10)}`,
      `- Prompts today: ${Math.max(0, Number(registered.prompts_today || 0))}`,
      `- Total prompts: ${Math.max(0, Number(registered.prompts_total || 0))}`,
      `- Tokens used: ${Math.max(0, Number(registered.tokens_total || 0))}`,
    ].join('\n'));
  }

  if (command === '/jobs' || command === '/jobs_running' || command === '/cron') {
    const listRes = jobsStore.listJobs({ include_deleted: false, created_by_chat_id: chatId });
    if (!listRes || !listRes.ok) {
      return sendTelegramText(chatId, `Unable to list jobs: ${(listRes && listRes.message) || 'unknown error'}`);
    }
    return sendTelegramText(chatId, formatOrchestratorJobsList(listRes.jobs));
  }

  if (command === '/job_create') {
    const args = parseTelegramCommandArgs(argsRaw);
    const scheduleType = String(args.type || args.schedule_type || args.schedule || 'daily').trim().toLowerCase();
    const payload = {
      name: String(args.name || '').trim() || `Job ${new Date().toLocaleString('en-US')}`,
      task: String(args.task || args.query || argsRaw || '').trim(),
      schedule_type: ['once', 'daily', 'weekly'].includes(scheduleType) ? scheduleType : 'daily',
      time: String(args.time || '09:00').trim(),
      day: String(args.day || '').trim(),
      once_date: String(args.once_date || args.date || '').trim(),
      timezone: String(args.timezone || Intl.DateTimeFormat().resolvedOptions().timeZone || 'UTC').trim(),
      notify_telegram: parseBoolLike(args.notify, true),
      status: 'active',
      created_by_chat_id: chatId,
    };
    const createRes = jobsStore.createJob(payload);
    if (!createRes || !createRes.ok) {
      return sendTelegramText(chatId, `Unable to create job: ${(createRes && createRes.message) || 'unknown error'}`);
    }
    const job = createRes.job || {};
    return sendTelegramText(chatId, `Created job ${job.id || ''}\n${job.name || ''}`);
  }

  if (command === '/job_edit') {
    const firstSpace = argsRaw.indexOf(' ');
    const jobId = String(firstSpace >= 0 ? argsRaw.slice(0, firstSpace) : argsRaw).trim();
    const rest = String(firstSpace >= 0 ? argsRaw.slice(firstSpace + 1) : '').trim();
    if (!jobId) {
      return sendTelegramText(chatId, 'Usage: /job_edit <id> key=value ...');
    }
    const existing = jobsStore.getJob(jobId);
    if (!existing || !existing.ok || !existing.job) {
      return sendTelegramText(chatId, `Unable to edit job: ${(existing && existing.message) || 'Job not found.'}`);
    }
    if (String((existing.job && existing.job.created_by_chat_id) || '').trim() !== chatId) {
      return sendTelegramText(chatId, 'You can edit only jobs created by this chat.');
    }
    const args = parseTelegramCommandArgs(rest);
    const patch = {};
    if (Object.prototype.hasOwnProperty.call(args, 'name')) patch.name = String(args.name || '').trim();
    if (Object.prototype.hasOwnProperty.call(args, 'task')) patch.task = String(args.task || '').trim();
    if (Object.prototype.hasOwnProperty.call(args, 'type') || Object.prototype.hasOwnProperty.call(args, 'schedule_type')) {
      const scheduleType = String(args.type || args.schedule_type || '').trim().toLowerCase();
      patch.schedule_type = ['once', 'daily', 'weekly'].includes(scheduleType) ? scheduleType : 'daily';
    }
    if (Object.prototype.hasOwnProperty.call(args, 'time')) patch.time = String(args.time || '').trim();
    if (Object.prototype.hasOwnProperty.call(args, 'day')) patch.day = String(args.day || '').trim();
    if (Object.prototype.hasOwnProperty.call(args, 'once_date') || Object.prototype.hasOwnProperty.call(args, 'date')) {
      patch.once_date = String(args.once_date || args.date || '').trim();
    }
    if (Object.prototype.hasOwnProperty.call(args, 'timezone')) patch.timezone = String(args.timezone || '').trim();
    if (Object.prototype.hasOwnProperty.call(args, 'notify')) patch.notify_telegram = parseBoolLike(args.notify, true);
    const editRes = jobsStore.editJob(jobId, patch);
    if (!editRes || !editRes.ok) {
      return sendTelegramText(chatId, `Unable to edit job: ${(editRes && editRes.message) || 'unknown error'}`);
    }
    const job = editRes.job || {};
    return sendTelegramText(chatId, `Updated job ${job.id || ''}\n${job.name || ''}`);
  }

  if (command === '/job_pause' || command === '/job_resume' || command === '/job_delete') {
    const jobId = String(argsRaw || '').trim();
    if (!jobId) {
      return sendTelegramText(chatId, `Usage: ${command} <id>`);
    }
    const existing = jobsStore.getJob(jobId);
    if (!existing || !existing.ok || !existing.job) {
      return sendTelegramText(chatId, `Unable to update job: ${(existing && existing.message) || 'Job not found.'}`);
    }
    if (String((existing.job && existing.job.created_by_chat_id) || '').trim() !== chatId) {
      return sendTelegramText(chatId, 'You can update only jobs created by this chat.');
    }
    const op = command === '/job_pause'
      ? jobsStore.pauseJob(jobId)
      : (
        command === '/job_resume'
          ? jobsStore.resumeJob(jobId)
          : jobsStore.deleteJob(jobId)
      );
    if (!op || !op.ok) {
      return sendTelegramText(chatId, `Unable to update job: ${(op && op.message) || 'unknown error'}`);
    }
    return sendTelegramText(chatId, `${command.replace('/job_', '').toUpperCase()} ${jobId}`);
  }

  return sendTelegramText(chatId, 'Unknown command. Use /help.');
}

async function handleTelegramInboundMessage(message = {}) {
  const chatId = String((message && message.chat && message.chat.id) || '').trim();
  const username = normalizeTelegramUsername((message && message.from && message.from.username) || '');
  const text = extractTelegramTextMessage(message);
  if (!chatId || !text) return;

  const parsed = splitTelegramCommand(text);
  if (parsed.command) {
    await handleTelegramCommand(parsed.command, parsed.argsRaw, {
      chat_id: chatId,
      username,
      message,
    });
    return;
  }

  const usersStore = getOrchestratorUsersStore();
  if (isTelegramRegistrationPending(chatId)) {
    const requestedUserId = String(text || '').trim();
    if (requestedUserId.length < 3) {
      await sendTelegramText(chatId, 'User ID must be at least 3 characters. Try again:');
      return;
    }
    if (requestedUserId.length > 30) {
      await sendTelegramText(chatId, 'User ID must be under 30 characters. Try again:');
      return;
    }
    if (!isValidTelegramUserId(requestedUserId)) {
      await sendTelegramText(chatId, 'User ID can only contain letters, numbers, - and _. Try again:');
      return;
    }
    const regRes = usersStore.register({
      chat_id: chatId,
      telegram_username: normalizeTelegramUsername(username),
      user_id: requestedUserId,
    });
    setTelegramRegistrationPending(chatId, false);
    if (!regRes || !regRes.ok) {
      await sendTelegramText(chatId, `Registration failed: ${(regRes && regRes.message) || 'unknown error'}`);
      return;
    }
    await sendTelegramText(chatId, buildTelegramRegistrationSuccessText(requestedUserId));
    return;
  }

  const user = usersStore.getByChatId(chatId);
  if (!user) {
    await sendTelegramText(chatId, buildTelegramNotRegisteredText());
    return;
  }

  usersStore.register({
    chat_id: chatId,
    telegram_username: normalizeTelegramUsername(username),
    user_id: String(user.user_id || '').trim(),
  });

  const settings = readSettings();
  const runtime = resolveRuntimeProviderAndModel({}, settings);
  let pathBRes = null;
  try {
    pathBRes = await executePathBTask({
      message: text,
      source: 'telegram',
      chat_id: chatId,
      username,
      user_scope: String(user.user_id || chatId),
      session_key: chatId,
      provider: runtime.provider,
      model: runtime.model,
      web_provider: String(settings.orchestrator_web_provider || ORCHESTRATOR_WEB_PROVIDER_DEFAULT),
    }, {
      timeoutMs: 120_000,
    });
  } catch (err) {
    await sendTelegramText(chatId, `Task failed: ${String((err && err.message) || 'unknown error')}`);
    return;
  }

  if (!pathBRes || !pathBRes.ok) {
    await sendTelegramText(chatId, `Task failed: ${(pathBRes && pathBRes.message) || 'unknown error'}`);
    return;
  }

  const summary = String(pathBRes.telegram_summary || pathBRes.message || '').trim();
  const followUp = String(pathBRes.follow_up_question || '').trim();
  await sendTelegramText(chatId, [summary, followUp].filter(Boolean).join('\n\n'));
  usersStore.incrementUsage(chatId, {
    tokens: String(text || '').length + String(summary || '').length,
  });
}

async function runScheduledOrchestratorJob(job = {}) {
  const chatId = String((job && job.created_by_chat_id) || '').trim();
  const task = String((job && job.task) || '').trim();
  if (!task) return { ok: false, message: 'Job task is required.' };
  const settings = readSettings();
  const runtime = resolveRuntimeProviderAndModel({}, settings);
  const runId = makeId('jobrun');
  let pathBRes = null;
  try {
    pathBRes = await executePathBTask({
      message: task,
      source: 'scheduler',
      chat_id: chatId,
      user_scope: chatId || 'scheduler',
      session_key: chatId || `job_${String(job.id || '')}`,
      provider: runtime.provider,
      model: runtime.model,
      run_id: runId,
      job_id: String((job && job.id) || ''),
      web_provider: String(settings.orchestrator_web_provider || ORCHESTRATOR_WEB_PROVIDER_DEFAULT),
    }, {
      timeoutMs: 150_000,
    });
  } catch (err) {
    return {
      ok: false,
      message: String((err && err.message) || 'Scheduled job failed.'),
    };
  }
  if (!pathBRes || !pathBRes.ok) {
    return {
      ok: false,
      message: String((pathBRes && pathBRes.message) || 'Scheduled job failed.'),
    };
  }
  if (job.notify_telegram && chatId && telegramService) {
    await sendTelegramText(chatId, `Scheduled Task: ${String(job.name || 'Job')}\n\n${String(pathBRes.telegram_summary || pathBRes.message || '')}`);
  }
  return {
    ok: true,
    message: String(pathBRes.message || ''),
    result: pathBRes,
  };
}

async function ensureTelegramRuntime(settings = readSettings()) {
  if (!settings || !settings.telegram_enabled) {
    if (telegramService) await telegramService.stop();
    return { ok: true, enabled: false, running: false };
  }
  const tokenRes = getTelegramToken(settings);
  const token = tokenRes && tokenRes.ok ? String(tokenRes.secret || '').trim() : '';
  if (!token) {
    if (telegramService) await telegramService.stop();
    return { ok: false, enabled: true, running: false, message: 'Telegram token is not configured.' };
  }
  if (!telegramService) {
    telegramService = new TelegramService({
      logger: console,
      tokenProvider: () => {
        const sec = getTelegramToken(readSettings());
        return sec && sec.ok ? String(sec.secret || '').trim() : '';
      },
      onMessage: handleTelegramInboundMessage,
      pollIntervalSec: Number(settings.telegram_poll_interval_sec || 2),
    });
  }
  telegramService.setPollIntervalSec(Number(settings.telegram_poll_interval_sec || 2));
  const startRes = telegramService.start();
  return {
    ok: !!(startRes && startRes.ok),
    enabled: true,
    running: true,
    ...(telegramService.status ? telegramService.status() : {}),
  };
}

function ensureOrchestratorSchedulerRuntime() {
  if (!orchestratorScheduler) {
    orchestratorScheduler = createOrchestratorScheduler({
      jobsStore: getOrchestratorJobsStore(),
      runJob: runScheduledOrchestratorJob,
      logger: console,
      tickMs: 10_000,
    });
  }
  orchestratorScheduler.start();
  return orchestratorScheduler.status();
}

function getDefaultSearchEngine() {
  return readSettings().default_search_engine || 'ddg';
}

function buildSearchUrl(queryText, engine) {
  const q = String(queryText || '').trim();
  const encoded = encodeURIComponent(q);
  const target = String(engine || 'ddg').trim().toLowerCase();
  if (target === 'google') return `https://www.google.com/search?q=${encoded}`;
  if (target === 'bing') return `https://www.bing.com/search?q=${encoded}`;
  return `https://duckduckgo.com/?q=${encoded}`;
}

function getDefaultSearchHomeUrl(engine = getDefaultSearchEngine()) {
  const target = String(engine || 'ddg').trim().toLowerCase();
  if (target === 'google') return 'https://www.google.com/';
  if (target === 'bing') return 'https://www.bing.com/';
  return 'https://duckduckgo.com/';
}

function getDefaultSearchHomeTitle(engine = getDefaultSearchEngine()) {
  const target = String(engine || 'ddg').trim().toLowerCase();
  if (target === 'google') return 'Google';
  if (target === 'bing') return 'Bing';
  return 'DuckDuckGo';
}

function deriveReferenceTitleFromTab(tab = null) {
  const tabTitle = String((tab && tab.title) || '').trim();
  if (tabTitle) return tabTitle.slice(0, 120);
  const tabUrl = String((tab && tab.url) || '').trim();
  if (tabUrl) {
    try {
      const host = new URL(tabUrl).hostname.replace(/^www\./i, '').trim();
      if (host) return `${host} snapshot`.slice(0, 120);
    } catch (_) {
      // noop
    }
    return `${tabUrl.slice(0, 40)} snapshot`.slice(0, 120);
  }
  return `Reference ${new Date(nowTs()).toLocaleString('en-US')}`.slice(0, 120);
}

function maybeAutoRetitleReferenceFromActiveTab(ref) {
  if (!ref || typeof ref !== 'object') return false;
  if (ref.title_user_edited) return false;
  const tabs = Array.isArray(ref.tabs) ? ref.tabs : [];
  const activeTabId = String(ref.active_tab_id || '').trim();
  const activeTab = tabs.find((tab) => String((tab && tab.id) || '') === activeTabId) || tabs[0] || null;
  const nextTitle = deriveReferenceTitleFromTab(activeTab);
  if (!nextTitle) return false;
  if (String(ref.title || '') === nextTitle) return false;
  ref.title = nextTitle;
  return true;
}

function readReferencesRaw() {
  const storePath = getStorePath();
  if (!fs.existsSync(storePath)) return [];
  try {
    const raw = fs.readFileSync(storePath, 'utf8');
    const parsed = JSON.parse(raw);
    return Array.isArray(parsed) ? parsed : [];
  } catch (_) {
    return [];
  }
}

function writeReferencesRaw(refs) {
  const storePath = getStorePath();
  const payload = JSON.stringify(Array.isArray(refs) ? refs : [], null, 2);
  fs.writeFileSync(storePath, payload, 'utf8');
}

function getPublicFeedPath() {
  return path.join(app.getPath('userData'), PUBLIC_FEED_FILENAME);
}

function readPublicFeed() {
  const feedPath = getPublicFeedPath();
  if (!fs.existsSync(feedPath)) return [];
  try {
    const raw = fs.readFileSync(feedPath, 'utf8');
    const parsed = JSON.parse(raw);
    return Array.isArray(parsed) ? parsed : [];
  } catch (_) {
    return [];
  }
}

function writePublicFeed(feed) {
  const feedPath = getPublicFeedPath();
  const payload = JSON.stringify(Array.isArray(feed) ? feed : [], null, 2);
  fs.writeFileSync(feedPath, payload, 'utf8');
}

function isTtcHyperwebAuthenticated() {
  const settings = readSettings();
  return !!(
    settings
    && settings.trustcommons_bootstrap_complete
    && String(settings.trustcommons_identity_id || '').trim()
  );
}

function requireTtcHyperwebAuth() {
  if (isTtcHyperwebAuthenticated()) return { ok: true };
  return { ok: false, message: 'TTC authentication required for Hyperweb actions.' };
}

function getHyperwebPublicSnapshotsPath() {
  return path.join(app.getPath('userData'), HYPERWEB_PUBLIC_SNAPSHOTS_FILENAME);
}

function createDefaultHyperwebPublicSnapshotsState() {
  return {
    version: 1,
    snapshots: [],
    publish_log: [],
  };
}

function readHyperwebPublicSnapshotsState() {
  const filePath = getHyperwebPublicSnapshotsPath();
  if (!fs.existsSync(filePath)) return createDefaultHyperwebPublicSnapshotsState();
  try {
    const parsed = JSON.parse(fs.readFileSync(filePath, 'utf8'));
    return {
      ...createDefaultHyperwebPublicSnapshotsState(),
      ...(parsed && typeof parsed === 'object' ? parsed : {}),
    };
  } catch (_) {
    return createDefaultHyperwebPublicSnapshotsState();
  }
}

function writeHyperwebPublicSnapshotsState(state) {
  const next = {
    ...createDefaultHyperwebPublicSnapshotsState(),
    ...((state && typeof state === 'object') ? state : {}),
  };
  fs.writeFileSync(getHyperwebPublicSnapshotsPath(), JSON.stringify(next, null, 2), 'utf8');
}

function pruneHyperwebPublicSnapshotsState(state) {
  const next = (state && typeof state === 'object') ? state : createDefaultHyperwebPublicSnapshotsState();
  const cutoff = nowTs() - HYPERWEB_SNAPSHOT_RETENTION_MS;
  next.snapshots = (Array.isArray(next.snapshots) ? next.snapshots : []).filter((item) => {
    const ts = Number((item && item.published_at) || 0);
    return ts > 0 && ts >= cutoff;
  });
  next.publish_log = (Array.isArray(next.publish_log) ? next.publish_log : []).filter((item) => {
    const ts = Number((item && item.ts) || 0);
    return ts > 0 && ts >= cutoff;
  });
  return next;
}

function syncPublicFeedWithSnapshots() {
  const state = pruneHyperwebPublicSnapshotsState(readHyperwebPublicSnapshotsState());
  writeHyperwebPublicSnapshotsState(state);
  const publicRefs = state.snapshots
    .filter((item) => String((item && item.status) || 'visible') !== 'hidden')
    .map((item) => {
      const payload = (item && item.reference_payload && typeof item.reference_payload === 'object')
        ? item.reference_payload
        : {};
      return {
        ...payload,
        snapshot_id: String((item && item.snapshot_id) || ''),
        status: String((item && item.status) || 'visible'),
        author_fingerprint: String((item && item.author_fingerprint) || ''),
        author_alias: String((item && item.author_alias) || ''),
        published_at: Number((item && item.published_at) || 0),
        updated_at: Number((item && item.updated_at) || 0),
      };
    });
  writePublicFeed(publicRefs);
}

function getHyperwebPrivateSharesPath() {
  return path.join(app.getPath('userData'), HYPERWEB_PRIVATE_SHARES_FILENAME);
}

function createDefaultHyperwebPrivateSharesState() {
  return {
    version: 1,
    shares: [],
    rooms: [],
  };
}

function readHyperwebPrivateSharesState() {
  const filePath = getHyperwebPrivateSharesPath();
  if (!fs.existsSync(filePath)) return createDefaultHyperwebPrivateSharesState();
  try {
    const parsed = JSON.parse(fs.readFileSync(filePath, 'utf8'));
    return {
      ...createDefaultHyperwebPrivateSharesState(),
      ...(parsed && typeof parsed === 'object' ? parsed : {}),
    };
  } catch (_) {
    return createDefaultHyperwebPrivateSharesState();
  }
}

function writeHyperwebPrivateSharesState(state) {
  const next = {
    ...createDefaultHyperwebPrivateSharesState(),
    ...((state && typeof state === 'object') ? state : {}),
  };
  fs.writeFileSync(getHyperwebPrivateSharesPath(), JSON.stringify(next, null, 2), 'utf8');
}

function getPrivateHistoryPath() {
  return path.join(app.getPath('userData'), PRIVATE_HISTORY_FILENAME);
}

function createDefaultPrivateHistoryState() {
  return {
    version: 1,
    entries: [],
  };
}

function readPrivateHistoryState() {
  const filePath = getPrivateHistoryPath();
  if (!fs.existsSync(filePath)) return createDefaultPrivateHistoryState();
  try {
    const parsed = JSON.parse(fs.readFileSync(filePath, 'utf8'));
    const entries = Array.isArray(parsed && parsed.entries) ? parsed.entries : [];
    return {
      version: Number((parsed && parsed.version) || 1),
      entries,
    };
  } catch (_) {
    return createDefaultPrivateHistoryState();
  }
}

function writePrivateHistoryState(state) {
  const next = (state && typeof state === 'object') ? state : createDefaultPrivateHistoryState();
  const entries = Array.isArray(next.entries) ? next.entries : [];
  const payload = JSON.stringify({ version: 1, entries }, null, 2);
  fs.writeFileSync(getPrivateHistoryPath(), payload, 'utf8');
  return { version: 1, entries };
}

function normalizeHistoryText(value) {
  return String(value || '')
    .toLowerCase()
    .replace(/https?:\/\/[^\s]+/g, ' ')
    .replace(/[^a-z0-9\s]/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function tokenizeHistoryText(value) {
  const tokens = normalizeHistoryText(value).split(' ').filter(Boolean);
  const counts = new Map();
  tokens.forEach((token) => {
    if (token.length < 3) return;
    if (/^\d+$/.test(token)) return;
    counts.set(token, (counts.get(token) || 0) + 1);
  });
  return Array.from(counts.entries())
    .sort((a, b) => (b[1] - a[1]) || a[0].localeCompare(b[0]))
    .slice(0, 24)
    .map((item) => item[0]);
}

function hashTokenToHistoryVectorIndex(token) {
  const hash = crypto.createHash('sha256').update(String(token || '')).digest();
  return hash.readUInt16BE(0) % HISTORY_EMBED_DIM;
}

function buildHistoryEmbedding(text) {
  const vec = new Array(HISTORY_EMBED_DIM).fill(0);
  const normalized = normalizeHistoryText(text);
  if (!normalized) return vec;
  const tokens = normalized.split(' ').filter(Boolean);
  tokens.forEach((token) => {
    const idx = hashTokenToHistoryVectorIndex(token);
    vec[idx] += 1;
  });
  let norm = 0;
  for (let i = 0; i < vec.length; i += 1) norm += vec[i] * vec[i];
  norm = Math.sqrt(norm);
  if (norm > 0) {
    for (let i = 0; i < vec.length; i += 1) {
      vec[i] = Number((vec[i] / norm).toFixed(6));
    }
  }
  return vec;
}

function cosineSimilarityArray(vecA, vecB) {
  const a = Array.isArray(vecA) ? vecA : [];
  const b = Array.isArray(vecB) ? vecB : [];
  if (!a.length || a.length !== b.length) return 0;
  let dot = 0;
  for (let i = 0; i < a.length; i += 1) {
    const av = Number(a[i] || 0);
    const bv = Number(b[i] || 0);
    dot += av * bv;
  }
  if (!Number.isFinite(dot)) return 0;
  return Math.max(0, Math.min(1, dot));
}

function extractHostFromUrl(rawUrl) {
  try {
    return String(new URL(String(rawUrl || '')).hostname || '').trim().toLowerCase();
  } catch (_) {
    return '';
  }
}

function buildHistorySearchBlob(entry) {
  const item = (entry && typeof entry === 'object') ? entry : {};
  const tokens = Array.isArray(item.semantic_tokens) ? item.semantic_tokens.join(' ') : '';
  return [
    String(item.url || ''),
    String(item.title || ''),
    String(item.url_host || ''),
    String(item.content_excerpt || ''),
    tokens,
  ].join(' ').replace(/\s+/g, ' ').trim().toLowerCase();
}

function sanitizeHistoryEntry(entry = {}) {
  const item = (entry && typeof entry === 'object') ? entry : {};
  const rawUrl = normalizeUrl(String(item.url || '').trim());
  if (!/^https?:\/\//i.test(rawUrl)) return null;
  const embedding = Array.isArray(item.embedding) ? item.embedding.slice(0, HISTORY_EMBED_DIM) : [];
  const paddedEmbedding = new Array(HISTORY_EMBED_DIM).fill(0);
  embedding.forEach((value, idx) => {
    if (idx >= HISTORY_EMBED_DIM) return;
    const num = Number(value || 0);
    paddedEmbedding[idx] = Number.isFinite(num) ? num : 0;
  });
  return {
    id: String(item.id || makeId('hist')).trim(),
    url: rawUrl,
    url_host: String(item.url_host || extractHostFromUrl(rawUrl)),
    title: String(item.title || rawUrl).slice(0, 240),
    committed_at: Number(item.committed_at || nowTs()),
    source_sr_id: String(item.source_sr_id || '').trim(),
    source_tab_id: String(item.source_tab_id || '').trim(),
    content_excerpt: String(item.content_excerpt || '').slice(0, 600),
    semantic_tokens: Array.isArray(item.semantic_tokens)
      ? item.semantic_tokens.map((t) => String(t || '').trim()).filter(Boolean).slice(0, 24)
      : [],
    embedding: paddedEmbedding,
    cluster_id: Number.isFinite(Number(item.cluster_id)) ? Math.max(0, Math.round(Number(item.cluster_id))) : 0,
  };
}

function enforceHistoryRetention(entries = []) {
  const settings = readSettings();
  const maxEntries = Number.isFinite(Number(settings.history_max_entries))
    ? Math.max(500, Math.min(10000, Math.round(Number(settings.history_max_entries))))
    : HISTORY_DEFAULT_MAX_ENTRIES;
  const list = Array.isArray(entries) ? entries : [];
  if (list.length <= maxEntries) return list;
  const sorted = [...list].sort((a, b) => Number((b && b.committed_at) || 0) - Number((a && a.committed_at) || 0));
  return sorted.slice(0, maxEntries);
}

function computeHistoryClusterIds(entries = []) {
  const list = Array.isArray(entries) ? entries : [];
  const k = Math.max(1, Math.min(10, Math.round(Math.sqrt(Math.max(1, list.length)) / 2)));
  if (list.length === 0) return [];
  const centroids = [];
  for (let i = 0; i < k; i += 1) {
    const source = list[Math.floor((i * list.length) / k)] || list[0];
    centroids.push(Array.isArray(source.embedding) ? [...source.embedding] : new Array(HISTORY_EMBED_DIM).fill(0));
  }

  const assign = new Array(list.length).fill(0);
  for (let iter = 0; iter < 5; iter += 1) {
    for (let i = 0; i < list.length; i += 1) {
      const row = list[i];
      let bestIdx = 0;
      let bestScore = -1;
      for (let c = 0; c < centroids.length; c += 1) {
        const score = cosineSimilarityArray(row.embedding, centroids[c]);
        if (score > bestScore) {
          bestScore = score;
          bestIdx = c;
        }
      }
      assign[i] = bestIdx;
    }
    const nextCentroids = centroids.map(() => new Array(HISTORY_EMBED_DIM).fill(0));
    const counts = centroids.map(() => 0);
    for (let i = 0; i < list.length; i += 1) {
      const row = list[i];
      const idx = assign[i];
      counts[idx] += 1;
      for (let d = 0; d < HISTORY_EMBED_DIM; d += 1) {
        nextCentroids[idx][d] += Number((row.embedding && row.embedding[d]) || 0);
      }
    }
    for (let c = 0; c < nextCentroids.length; c += 1) {
      if (!counts[c]) continue;
      let norm = 0;
      for (let d = 0; d < HISTORY_EMBED_DIM; d += 1) {
        nextCentroids[c][d] /= counts[c];
        norm += nextCentroids[c][d] * nextCentroids[c][d];
      }
      norm = Math.sqrt(norm);
      if (norm > 0) {
        for (let d = 0; d < HISTORY_EMBED_DIM; d += 1) {
          nextCentroids[c][d] = Number((nextCentroids[c][d] / norm).toFixed(6));
        }
      }
    }
    for (let c = 0; c < centroids.length; c += 1) centroids[c] = nextCentroids[c];
  }
  return assign;
}

function upsertHistoryEntry(payload = {}) {
  const settings = readSettings();
  if (!settings.history_enabled) return { ok: true, skipped: true, reason: 'history_disabled' };

  const nextEntry = sanitizeHistoryEntry(payload);
  if (!nextEntry) return { ok: false, message: 'Invalid history entry.' };

  const state = readPrivateHistoryState();
  const list = Array.isArray(state.entries) ? state.entries.map((item) => sanitizeHistoryEntry(item)).filter(Boolean) : [];
  const existingIdx = list.findIndex((item) => (
    String(item.url || '') === nextEntry.url
    && Math.abs(Number(item.committed_at || 0) - Number(nextEntry.committed_at || 0)) <= HISTORY_RECENT_DUP_WINDOW_MS
  ));
  if (existingIdx >= 0) {
    list[existingIdx] = {
      ...list[existingIdx],
      ...nextEntry,
      id: list[existingIdx].id || nextEntry.id,
    };
  } else {
    list.unshift(nextEntry);
  }
  const retained = enforceHistoryRetention(list);
  const clusterIds = computeHistoryClusterIds(retained);
  const withClusters = retained.map((entry, idx) => ({ ...entry, cluster_id: Number(clusterIds[idx] || 0) }));
  const saved = writePrivateHistoryState({ version: 1, entries: withClusters });
  return { ok: true, entry: nextEntry, total: saved.entries.length };
}

function buildHistoryEntryFromTab(tab = {}, context = {}) {
  const url = normalizeUrl(String((tab && tab.url) || '').trim());
  if (!/^https?:\/\//i.test(url)) return null;
  const title = String((tab && tab.title) || url).trim();
  const excerpt = String((context && context.content_excerpt) || '').trim().slice(0, 600);
  const sourceText = [
    title,
    url,
    String((context && context.page_content) || '').slice(0, 4000),
    excerpt,
  ].join(' ').trim();
  return {
    id: makeId('hist'),
    url,
    url_host: extractHostFromUrl(url),
    title: title || url,
    committed_at: Number((context && context.committed_at) || nowTs()),
    source_sr_id: String((context && context.source_sr_id) || '').trim(),
    source_tab_id: String((context && context.source_tab_id) || '').trim(),
    content_excerpt: excerpt,
    semantic_tokens: tokenizeHistoryText(sourceText),
    embedding: buildHistoryEmbedding(sourceText),
  };
}

async function captureCommittedHistoryFromTab(tab = {}, context = {}) {
  const baseTab = {
    url: String((tab && tab.url) || '').trim(),
    title: String((tab && tab.title) || '').trim(),
  };
  if (!/^https?:\/\//i.test(normalizeUrl(baseTab.url))) return { ok: true, skipped: true, reason: 'not_http' };

  let content = '';
  let excerpt = '';
  try {
    const page = await getPageContentFromBrowser();
    if (page && page.success && page.data) {
      const payload = page.data || {};
      const pageUrl = normalizeUrl(String(payload.url || '').trim());
      if (pageUrl && pageUrl === normalizeUrl(baseTab.url)) {
        content = String(payload.content || '').trim().slice(0, 4000);
        excerpt = String(payload.content || '').trim().slice(0, 320);
        if (!baseTab.title) baseTab.title = String(payload.title || '').trim();
      }
    }
  } catch (_) {
    // fallback to tab metadata only
  }

  const entry = buildHistoryEntryFromTab(baseTab, {
    ...context,
    page_content: content,
    content_excerpt: excerpt,
  });
  if (!entry) return { ok: false, message: 'Unable to build history entry.' };
  return upsertHistoryEntry(entry);
}

function captureCommittedHistoryFromTabSync(tab = {}, context = {}) {
  const entry = buildHistoryEntryFromTab(tab, context);
  if (!entry) return { ok: true, skipped: true, reason: 'invalid_tab' };
  return upsertHistoryEntry(entry);
}

function queryHistoryEntries(payload = {}) {
  seedHistoryFromExistingReferencesIfEmpty();
  const query = String((payload && payload.query) || '').trim().toLowerCase();
  const limit = Math.max(1, Math.min(500, Number((payload && payload.limit) || 80)));
  const offset = Math.max(0, Number((payload && payload.offset) || 0));
  const state = readPrivateHistoryState();
  let list = Array.isArray(state.entries) ? state.entries.map((item) => sanitizeHistoryEntry(item)).filter(Boolean) : [];
  list.sort((a, b) => Number((b && b.committed_at) || 0) - Number((a && a.committed_at) || 0));
  if (query) {
    list = list.filter((item) => buildHistorySearchBlob(item).includes(query));
  }
  const total = list.length;
  const entries = list.slice(offset, offset + limit);
  return { ok: true, entries, total };
}

function getHistoryEntryById(historyId) {
  seedHistoryFromExistingReferencesIfEmpty();
  const target = String(historyId || '').trim();
  if (!target) return { ok: false, message: 'history_id is required.' };
  const state = readPrivateHistoryState();
  const found = (Array.isArray(state.entries) ? state.entries : [])
    .map((item) => sanitizeHistoryEntry(item))
    .find((item) => item && String(item.id || '') === target);
  if (!found) return { ok: false, message: 'History entry not found.' };
  return { ok: true, entry: found };
}

function deleteHistoryEntryById(historyId) {
  const target = String(historyId || '').trim();
  if (!target) return { ok: false, message: 'history_id is required.' };
  const state = readPrivateHistoryState();
  const entries = Array.isArray(state.entries) ? state.entries.map((item) => sanitizeHistoryEntry(item)).filter(Boolean) : [];
  const next = entries.filter((item) => String(item.id || '') !== target);
  if (next.length === entries.length) return { ok: true, deleted: false };
  writePrivateHistoryState({ version: 1, entries: next });
  return { ok: true, deleted: true };
}

function clearHistoryEntries(phrase = '') {
  if (String(phrase || '').trim().toUpperCase() !== 'DELETE') {
    return { ok: false, message: 'Type DELETE to confirm.' };
  }
  const state = readPrivateHistoryState();
  const clearedCount = Array.isArray(state.entries) ? state.entries.length : 0;
  writePrivateHistoryState(createDefaultPrivateHistoryState());
  return { ok: true, cleared_count: clearedCount };
}

function projectHistoryPoint(embedding = []) {
  const vec = Array.isArray(embedding) ? embedding : [];
  let x = 0;
  let y = 0;
  for (let i = 0; i < vec.length; i += 1) {
    const value = Number(vec[i] || 0);
    x += value * Math.sin((i + 1) * 0.173);
    y += value * Math.cos((i + 1) * 0.117);
  }
  return { x: Number(x.toFixed(6)), y: Number(y.toFixed(6)) };
}

function buildHistorySemanticMap(payload = {}) {
  seedHistoryFromExistingReferencesIfEmpty();
  const query = String((payload && payload.query) || '').trim().toLowerCase();
  const maxPoints = Math.max(100, Math.min(4000, Number((payload && payload.max_points) || 2000)));
  const state = readPrivateHistoryState();
  let list = Array.isArray(state.entries) ? state.entries.map((item) => sanitizeHistoryEntry(item)).filter(Boolean) : [];
  list.sort((a, b) => Number((b && b.committed_at) || 0) - Number((a && a.committed_at) || 0));
  if (query) {
    list = list.filter((item) => buildHistorySearchBlob(item).includes(query));
  }
  const total = list.length;
  const sampled = total > maxPoints ? list.filter((_, idx) => (idx % Math.ceil(total / maxPoints)) === 0).slice(0, maxPoints) : list;
  const clusterIds = computeHistoryClusterIds(sampled);
  const points = sampled.map((entry, idx) => {
    const projection = projectHistoryPoint(entry.embedding);
    const clusterId = Number(clusterIds[idx] || entry.cluster_id || 0);
    return {
      id: String(entry.id || ''),
      x: projection.x,
      y: projection.y,
      cluster_id: clusterId,
      title: String(entry.title || ''),
      url: String(entry.url || ''),
      url_host: String(entry.url_host || ''),
      committed_at: Number(entry.committed_at || 0),
      semantic_tokens: Array.isArray(entry.semantic_tokens) ? entry.semantic_tokens.slice(0, 8) : [],
    };
  });
  const xs = points.map((point) => Number(point.x || 0));
  const ys = points.map((point) => Number(point.y || 0));
  const bounds = {
    min_x: xs.length ? Math.min(...xs) : -1,
    max_x: xs.length ? Math.max(...xs) : 1,
    min_y: ys.length ? Math.min(...ys) : -1,
    max_y: ys.length ? Math.max(...ys) : 1,
  };
  const clusters = {};
  points.forEach((point) => {
    const key = String(point.cluster_id || 0);
    if (!clusters[key]) clusters[key] = { cluster_id: point.cluster_id, count: 0 };
    clusters[key].count += 1;
  });
  return {
    ok: true,
    points,
    clusters: Object.values(clusters).sort((a, b) => Number(a.cluster_id || 0) - Number(b.cluster_id || 0)),
    bounds,
    total,
  };
}

function seedHistoryFromExistingReferencesIfEmpty() {
  const state = readPrivateHistoryState();
  const existing = Array.isArray(state.entries) ? state.entries.map((item) => sanitizeHistoryEntry(item)).filter(Boolean) : [];
  if (existing.length > 0) return;

  const refs = getReferences();
  const seeded = [];
  refs.forEach((ref) => {
    const srId = String((ref && ref.id) || '').trim();
    const tabs = Array.isArray(ref && ref.tabs) ? ref.tabs : [];
    tabs.forEach((tab) => {
      const kind = String((tab && tab.tab_kind) || 'web').trim().toLowerCase();
      if (kind !== 'web') return;
      const entry = buildHistoryEntryFromTab(tab, {
        committed_at: Number((tab && tab.snapshot_at) || (tab && tab.last_active) || (ref && ref.updated_at) || nowTs()),
        source_sr_id: srId,
        source_tab_id: String((tab && tab.id) || ''),
        content_excerpt: String((tab && tab.excerpt) || ''),
      });
      if (entry) seeded.push(entry);
    });
  });
  if (!seeded.length) return;
  const dedupe = new Set();
  const unique = seeded.filter((entry) => {
    const key = String((entry && entry.url) || '').trim();
    if (!key || dedupe.has(key)) return false;
    dedupe.add(key);
    return true;
  });
  const retained = enforceHistoryRetention(unique);
  const clusterIds = computeHistoryClusterIds(retained);
  const withClusters = retained.map((entry, idx) => ({ ...entry, cluster_id: Number(clusterIds[idx] || 0) }));
  writePrivateHistoryState({ version: 1, entries: withClusters });
}

function sanitizePublicReference(ref) {
  const author = getLocalHyperwebAuthor();
  const tabs = Array.isArray(ref && ref.tabs) ? ref.tabs : [];
  const artifacts = Array.isArray(ref && ref.artifacts) ? ref.artifacts : [];
  const cachedSummary = getPublicReferenceSummaryCache(ref);
  const summaryText = cachedSummary
    ? cachedSummary.summary
    : buildPublicReferenceSummary(ref);
  const stateHash = cachedSummary && cachedSummary.state_hash
    ? cachedSummary.state_hash
    : buildPublicReferenceStateHash(ref);
  return {
    id: String((ref && ref.id) || ''),
    title: String((ref && ref.title) || 'Untitled'),
    intent: String((ref && ref.intent) || ''),
    tags: Array.isArray(ref && ref.tags) ? ref.tags.slice(0, 30) : [],
    relation_type: String((ref && ref.relation_type) || 'root'),
    updated_at: Number((ref && ref.updated_at) || nowTs()),
    tab_count: tabs.length,
    tabs: tabs.slice(0, 30).map((tab) => ({
      id: String((tab && tab.id) || ''),
      tab_kind: String((tab && tab.tab_kind) || 'web'),
      title: String((tab && tab.title) || ''),
      url: String((tab && tab.url) || ''),
      renderer: String((tab && tab.renderer) || ''),
      viz_request: (tab && typeof tab.viz_request === 'object') ? tab.viz_request : {},
      viz_source: String((tab && tab.viz_source) || ''),
      viz_png_path: String((tab && tab.viz_png_path) || ''),
      viz_png_base64: String((tab && tab.viz_png_base64) || ''),
      files_view_state: (tab && typeof tab.files_view_state === 'object') ? tab.files_view_state : {},
    })),
    artifacts: artifacts.slice(0, 40).map((artifact) => ({
      id: String((artifact && artifact.id) || ''),
      type: String((artifact && artifact.type) || 'markdown'),
      title: String((artifact && artifact.title) || ''),
      content: String((artifact && artifact.content) || '').slice(0, 8000),
      updated_at: Number((artifact && artifact.updated_at) || nowTs()),
    })),
    program: String((ref && ref.program) || ''),
    skills: Array.isArray(ref && ref.skills)
      ? ref.skills.map((item) => sanitizeSkillDescriptor(item)).filter(Boolean)
      : [],
    summary_text: summaryText,
    public_summary_state_hash: stateHash,
    source_type: 'local_public',
    source_peer_id: String(author.peer_id || ''),
    source_peer_name: String(author.peer_name || ''),
  };
}

function syncPublicFeedWithReferences(refs) {
  void refs;
  syncPublicFeedWithSnapshots();
}

function relevanceScoreForQuery(reference, query) {
  const q = String(query || '').trim().toLowerCase();
  if (!q) return 0;
  const title = String((reference && reference.title) || '').toLowerCase();
  const intent = String((reference && reference.intent) || '').toLowerCase();
  const tags = Array.isArray(reference && reference.tags) ? reference.tags.join(' ').toLowerCase() : '';
  const artifacts = Array.isArray(reference && reference.artifacts)
    ? reference.artifacts.map((artifact) => `${artifact.title || ''} ${artifact.content || ''}`).join(' ').toLowerCase()
    : '';
  const blob = `${title} ${intent} ${tags} ${artifacts}`;
  if (!blob) return 0;
  if (blob.includes(q)) return 1;
  const terms = q.split(/\s+/).filter(Boolean);
  if (!terms.length) return 0;
  let score = 0;
  terms.forEach((term) => {
    if (blob.includes(term)) score += 0.2;
  });
  return Math.min(0.95, score);
}

function isPinnableRootReference(ref, idMap = {}) {
  if (!ref || ref.is_public_candidate) return false;
  const parentId = String((ref && ref.parent_id) || '').trim();
  if (!parentId) return true;
  return !idMap[parentId];
}

function getPublicReferencesForHyperweb() {
  const state = pruneHyperwebPublicSnapshotsState(readHyperwebPublicSnapshotsState());
  writeHyperwebPublicSnapshotsState(state);
  return (Array.isArray(state.snapshots) ? state.snapshots : [])
    .filter((snapshot) => String((snapshot && snapshot.status) || 'visible') === 'visible')
    .map((snapshot) => {
      const payload = (snapshot && snapshot.reference_payload && typeof snapshot.reference_payload === 'object')
        ? snapshot.reference_payload
        : {};
      return {
        ...payload,
        id: String((payload && payload.id) || (snapshot && snapshot.reference_id) || ''),
        reference_id: String((snapshot && snapshot.reference_id) || ''),
        snapshot_id: String((snapshot && snapshot.snapshot_id) || ''),
        status: String((snapshot && snapshot.status) || 'visible'),
        source_type: 'public_snapshot',
        source_peer_id: String((snapshot && snapshot.author_fingerprint) || String(payload.source_peer_id || '')).toUpperCase(),
        source_peer_name: String((snapshot && snapshot.author_alias) || payload.source_peer_name || ''),
        peer_id: String((snapshot && snapshot.author_fingerprint) || '').toUpperCase(),
        peer_name: String((snapshot && snapshot.author_alias) || ''),
        published_at: Number((snapshot && snapshot.published_at) || 0),
        updated_at: Number((snapshot && snapshot.updated_at) || 0),
        hyperweb_payload_version: Number((payload && payload.hyperweb_payload_version) || 1),
      };
    });
}

function getHyperwebSocialPath() {
  return path.join(app.getPath('userData'), HYPERWEB_SOCIAL_FILENAME);
}

function localFingerprintFromPubKey(pubKeyPem = '') {
  const normalized = String(pubKeyPem || '').trim();
  if (!normalized) return '';
  return crypto.createHash('sha256').update(normalized).digest('hex').slice(0, 12).toUpperCase();
}

function defaultAliasFromFingerprint(fingerprint = '') {
  const code = String(fingerprint || '').trim().toUpperCase().slice(0, 6);
  return code ? `node-${code}` : 'node-ANON';
}

function createDefaultHyperwebSocialState() {
  return {
    identity: {
      pubkey: '',
      fingerprint: '',
      display_alias: '',
      created_at: 0,
    },
    peer_relations: [],
    known_peers: {},
    hyperweb_social_log: [],
    posts_by_id: {},
    replies_by_post_id: {},
    votes_by_target: {},
    tombstones: {},
    reports: [],
    last_compaction_at: 0,
  };
}

function writeHyperwebSocialState(state) {
  const payload = (state && typeof state === 'object') ? state : createDefaultHyperwebSocialState();
  fs.writeFileSync(getHyperwebSocialPath(), JSON.stringify(payload, null, 2), 'utf8');
}

function readHyperwebSocialState() {
  const filePath = getHyperwebSocialPath();
  if (!fs.existsSync(filePath)) {
    const initial = createDefaultHyperwebSocialState();
    writeHyperwebSocialState(initial);
    return initial;
  }
  try {
    const raw = fs.readFileSync(filePath, 'utf8');
    const parsed = JSON.parse(raw);
    if (!parsed || typeof parsed !== 'object') return createDefaultHyperwebSocialState();
    return {
      ...createDefaultHyperwebSocialState(),
      ...parsed,
    };
  } catch (_) {
    return createDefaultHyperwebSocialState();
  }
}

function getHyperwebIdentityPrivateKeyRaw() {
  const got = keychain.getSecret(HYPERWEB_IDENTITY_PRIVATE_KEY_ACCOUNT, { service: HYPERWEB_IDENTITY_SERVICE });
  if (got && got.ok && got.secret) return String(got.secret || '');
  return '';
}

function decodeHyperwebPrivateKey(rawValue = '') {
  const raw = String(rawValue || '').trim();
  if (!raw) return null;
  try {
    if (raw.startsWith(HYPERWEB_IDENTITY_DER_PREFIX)) {
      const der = Buffer.from(raw.slice(HYPERWEB_IDENTITY_DER_PREFIX.length), 'base64');
      return crypto.createPrivateKey({ key: der, format: 'der', type: 'pkcs8' });
    }
    if (raw.includes('BEGIN PRIVATE KEY')) {
      return crypto.createPrivateKey({ key: raw, format: 'pem' });
    }
    const der = Buffer.from(raw, 'base64');
    return crypto.createPrivateKey({ key: der, format: 'der', type: 'pkcs8' });
  } catch (_) {
    return null;
  }
}

function encodeHyperwebPrivateKey(keyObj) {
  if (!keyObj) return '';
  try {
    const der = keyObj.export({ format: 'der', type: 'pkcs8' });
    return `${HYPERWEB_IDENTITY_DER_PREFIX}${Buffer.from(der).toString('base64')}`;
  } catch (_) {
    return '';
  }
}

function validateHyperwebKeyPair(privateKeyObj, publicKeyPem) {
  if (!privateKeyObj || !publicKeyPem) return false;
  try {
    const message = Buffer.from('subgrapher-hyperweb-self-test', 'utf8');
    const signature = crypto.sign(null, message, privateKeyObj);
    return crypto.verify(null, message, publicKeyPem, signature);
  } catch (_) {
    return false;
  }
}

function generateAndPersistHyperwebIdentity(state) {
  const generated = crypto.generateKeyPairSync('ed25519');
  const privateKeyObj = generated.privateKey;
  const pubKeyPem = generated.publicKey.export({ type: 'spki', format: 'pem' });
  const encodedPrivate = encodeHyperwebPrivateKey(privateKeyObj);
  keychain.setSecret(HYPERWEB_IDENTITY_PRIVATE_KEY_ACCOUNT, encodedPrivate, { service: HYPERWEB_IDENTITY_SERVICE });
  const fingerprint = localFingerprintFromPubKey(pubKeyPem);
  const nextIdentity = {
    pubkey: pubKeyPem,
    fingerprint,
    display_alias: defaultAliasFromFingerprint(fingerprint),
    created_at: nowTs(),
  };
  state.identity = nextIdentity;
  writeHyperwebSocialState(state);
  return {
    identity: nextIdentity,
    private_key_obj: privateKeyObj,
  };
}

function ensureHyperwebIdentity() {
  const state = readHyperwebSocialState();
  const identity = (state.identity && typeof state.identity === 'object')
    ? state.identity
    : createDefaultHyperwebSocialState().identity;
  const privateKeyRaw = getHyperwebIdentityPrivateKeyRaw();
  let privateKeyObj = decodeHyperwebPrivateKey(privateKeyRaw);
  let nextIdentity = { ...identity };
  let changed = false;

  const hasPubKey = !!String(identity.pubkey || '').trim();
  const hasFp = !!String(identity.fingerprint || '').trim();
  const pairValid = hasPubKey && hasFp && validateHyperwebKeyPair(privateKeyObj, identity.pubkey);
  if (!privateKeyObj || !hasPubKey || !hasFp || !pairValid) {
    const regenerated = generateAndPersistHyperwebIdentity(state);
    hyperwebSocialState = state;
    return regenerated;
  } else {
    const expectedFp = localFingerprintFromPubKey(identity.pubkey);
    const display = String(identity.display_alias || '').trim() || defaultAliasFromFingerprint(expectedFp);
    if (expectedFp !== String(identity.fingerprint || '').trim().toUpperCase() || display !== String(identity.display_alias || '')) {
      nextIdentity = {
        pubkey: String(identity.pubkey || ''),
        fingerprint: expectedFp,
        display_alias: display,
        created_at: Number(identity.created_at || nowTs()),
      };
      changed = true;
    }
    const encoded = encodeHyperwebPrivateKey(privateKeyObj);
    if (encoded && !privateKeyRaw.startsWith(HYPERWEB_IDENTITY_DER_PREFIX)) {
      keychain.setSecret(HYPERWEB_IDENTITY_PRIVATE_KEY_ACCOUNT, encoded, { service: HYPERWEB_IDENTITY_SERVICE });
    }
  }

  if (changed) {
    state.identity = nextIdentity;
    writeHyperwebSocialState(state);
  }
  hyperwebSocialState = changed ? state : (hyperwebSocialState || state);
  return {
    identity: nextIdentity,
    private_key_obj: privateKeyObj,
  };
}

function getLocalHyperwebAuthor() {
  const ensured = ensureHyperwebIdentity();
  const identity = ensured.identity || {};
  return {
    peer_id: String(identity.fingerprint || '').trim(),
    peer_name: String(identity.display_alias || '').trim() || defaultAliasFromFingerprint(identity.fingerprint),
    pubkey: String(identity.pubkey || ''),
  };
}

function signHyperwebPayload(payload, privateKeyPem = '') {
  const raw = JSON.stringify(payload || {});
  const signWith = (keyInput) => {
    const keyObj = typeof keyInput === 'string'
      ? decodeHyperwebPrivateKey(keyInput)
      : (keyInput || null);
    if (!keyObj) throw new Error('Hyperweb signing key is unavailable.');
    return crypto.sign(null, Buffer.from(raw, 'utf8'), keyObj).toString('base64');
  };
  try {
    return signWith(privateKeyPem || getHyperwebIdentityPrivateKeyRaw());
  } catch (_) {
    const regenerated = ensureHyperwebIdentity();
    return signWith(regenerated && regenerated.private_key_obj ? regenerated.private_key_obj : null);
  }
}

function verifyHyperwebPayload(payload, signature, pubkeyPem) {
  if (!payload || !signature || !pubkeyPem) return false;
  try {
    return crypto.verify(
      null,
      Buffer.from(JSON.stringify(payload), 'utf8'),
      pubkeyPem,
      Buffer.from(String(signature || ''), 'base64')
    );
  } catch (_) {
    return false;
  }
}

function ensureHyperwebSocialState() {
  if (!hyperwebSocialState || typeof hyperwebSocialState !== 'object') {
    hyperwebSocialState = readHyperwebSocialState();
  }
  const ensuredIdentity = ensureHyperwebIdentity();
  hyperwebSocialState.identity = ensuredIdentity.identity;
  if (!Array.isArray(hyperwebSocialState.peer_relations)) hyperwebSocialState.peer_relations = [];
  if (!hyperwebSocialState.known_peers || typeof hyperwebSocialState.known_peers !== 'object') hyperwebSocialState.known_peers = {};
  if (!Array.isArray(hyperwebSocialState.hyperweb_social_log)) hyperwebSocialState.hyperweb_social_log = [];
  if (!hyperwebSocialState.posts_by_id || typeof hyperwebSocialState.posts_by_id !== 'object') hyperwebSocialState.posts_by_id = {};
  if (!hyperwebSocialState.replies_by_post_id || typeof hyperwebSocialState.replies_by_post_id !== 'object') hyperwebSocialState.replies_by_post_id = {};
  if (!hyperwebSocialState.votes_by_target || typeof hyperwebSocialState.votes_by_target !== 'object') hyperwebSocialState.votes_by_target = {};
  if (!hyperwebSocialState.tombstones || typeof hyperwebSocialState.tombstones !== 'object') hyperwebSocialState.tombstones = {};
  if (!Array.isArray(hyperwebSocialState.reports)) hyperwebSocialState.reports = [];
  return hyperwebSocialState;
}

function computeWilsonLowerBound(up, down) {
  const n = Number(up || 0) + Number(down || 0);
  if (n <= 0) return 0;
  const z = 1.96;
  const phat = Number(up || 0) / n;
  const a = phat + ((z * z) / (2 * n));
  const b = z * Math.sqrt((phat * (1 - phat) + ((z * z) / (4 * n))) / n);
  const c = 1 + ((z * z) / n);
  return (a - b) / c;
}

function computeDecayedScore(netScore, createdAt) {
  const ageHours = Math.max(0, (nowTs() - Number(createdAt || nowTs())) / (1000 * 60 * 60));
  const decay = Math.exp(-ageHours / 96);
  return Number(netScore || 0) * decay;
}

function rebuildHyperwebSocialMaterialized(state = null) {
  const next = state || ensureHyperwebSocialState();
  const posts = {};
  const repliesByPost = {};
  const votesByTarget = {};
  const tombstones = {};

  const sorted = (Array.isArray(next.hyperweb_social_log) ? next.hyperweb_social_log : [])
    .slice()
    .sort((a, b) => Number((a && a.ts) || 0) - Number((b && b.ts) || 0));

  sorted.forEach((event) => {
    if (!event || typeof event !== 'object') return;
    const type = String(event.type || '').trim().toLowerCase();
    const payload = (event.payload && typeof event.payload === 'object') ? event.payload : {};
    if (type === 'social_post') {
      const id = String(payload.post_id || '').trim();
      if (!id) return;
      posts[id] = {
        post_id: id,
        body: String(payload.body || ''),
        author_fingerprint: String(payload.author_fingerprint || ''),
        author_alias: String(payload.author_alias || ''),
        created_at: Number(payload.created_at || event.ts || nowTs()),
        search_blob: `${String(payload.body || '')} ${String(payload.author_alias || '')}`.replace(/\s+/g, ' ').trim(),
      };
      return;
    }
    if (type === 'social_reply') {
      const postId = String(payload.post_id || '').trim();
      const replyId = String(payload.reply_id || '').trim();
      if (!postId || !replyId) return;
      if (!Array.isArray(repliesByPost[postId])) repliesByPost[postId] = [];
      repliesByPost[postId].push({
        reply_id: replyId,
        post_id: postId,
        body: String(payload.body || ''),
        author_fingerprint: String(payload.author_fingerprint || ''),
        author_alias: String(payload.author_alias || ''),
        created_at: Number(payload.created_at || event.ts || nowTs()),
        search_blob: `${String(payload.body || '')} ${String(payload.author_alias || '')}`.replace(/\s+/g, ' ').trim(),
      });
      return;
    }
    if (type === 'social_vote') {
      const targetId = String(payload.target_id || '').trim();
      const actor = String(payload.actor_fingerprint || '').trim();
      const value = Number(payload.value || 0);
      if (!targetId || !actor || (value !== 1 && value !== -1)) return;
      if (!votesByTarget[targetId]) votesByTarget[targetId] = {};
      votesByTarget[targetId][actor] = value;
      return;
    }
    if (type === 'social_tombstone') {
      const targetId = String(payload.target_id || '').trim();
      if (!targetId) return;
      tombstones[targetId] = {
        target_id: targetId,
        ts: Number(event.ts || nowTs()),
        reason: String(payload.reason || 'moderation'),
      };
    }
  });

  next.posts_by_id = posts;
  next.replies_by_post_id = repliesByPost;
  next.votes_by_target = votesByTarget;
  next.tombstones = tombstones;
  next.last_compaction_at = nowTs();
  return next;
}

function appendHyperwebSocialEvent(event, options = {}) {
  const state = ensureHyperwebSocialState();
  const item = (event && typeof event === 'object') ? event : null;
  if (!item) return { ok: false, message: 'event is required.' };
  const eventId = String(item.event_id || '').trim();
  if (!eventId) return { ok: false, message: 'event_id is required.' };
  const dedupe = new Set(state.hyperweb_social_log.map((x) => String((x && x.event_id) || '').trim()));
  if (dedupe.has(eventId)) return { ok: true, deduped: true, event: item };
  state.hyperweb_social_log.push(item);
  if (state.hyperweb_social_log.length > 6000) {
    state.hyperweb_social_log = state.hyperweb_social_log.slice(-6000);
  }
  rebuildHyperwebSocialMaterialized(state);
  maybeApplyHyperwebModeration(state);
  writeHyperwebSocialState(state);
  if (!options.skipBroadcast) {
    hyperwebManager.broadcastProtocol({
      type: `hyperweb:${String(item.type || '').trim().toLowerCase()}`,
      event: item,
      ts: nowTs(),
    });
  }
  return { ok: true, event: item };
}

function getVoteStatsForTarget(state, targetId) {
  const votes = (state && state.votes_by_target && state.votes_by_target[targetId]) || {};
  const all = Object.values(votes);
  const up = all.filter((v) => Number(v) > 0).length;
  const down = all.filter((v) => Number(v) < 0).length;
  return {
    up,
    down,
    total: up + down,
    net: up - down,
  };
}

function maybeApplyHyperwebModeration(state = null) {
  const next = state || ensureHyperwebSocialState();
  const tombstoned = new Set(Object.keys(next.tombstones || {}));
  const shouldHideTarget = (targetId) => {
    const id = String(targetId || '').trim();
    if (!id || tombstoned.has(id)) return false;
    const stats = getVoteStatsForTarget(next, id);
    return stats.down >= HYPERWEB_MODERATION_HIDE_MIN_DOWNVOTERS && stats.net <= HYPERWEB_MODERATION_HIDE_SCORE;
  };
  const appendTombstone = (targetId, reason = 'community_threshold') => {
    const id = String(targetId || '').trim();
    if (!id || tombstoned.has(id)) return;
    const tomb = {
      event_id: makeId('hwevt'),
      type: 'social_tombstone',
      ts: nowTs(),
      signer_pubkey: String((next.identity && next.identity.pubkey) || ''),
      signer_fingerprint: String((next.identity && next.identity.fingerprint) || ''),
      payload: {
        target_id: id,
        reason,
      },
      signature: '',
    };
    tomb.signature = signHyperwebPayload({
      event_id: tomb.event_id,
      type: tomb.type,
      ts: tomb.ts,
      signer_pubkey: tomb.signer_pubkey,
      signer_fingerprint: tomb.signer_fingerprint,
      payload: tomb.payload,
    });
    next.hyperweb_social_log.push(tomb);
    tombstoned.add(id);
  };

  Object.values(next.posts_by_id || {}).forEach((post) => {
    const postId = String((post && post.post_id) || '').trim();
    if (!postId || !shouldHideTarget(postId)) return;
    appendTombstone(postId);
  });

  Object.values(next.replies_by_post_id || {}).forEach((replies) => {
    (Array.isArray(replies) ? replies : []).forEach((reply) => {
      const replyId = String((reply && reply.reply_id) || '').trim();
      if (!replyId || !shouldHideTarget(replyId)) return;
      appendTombstone(replyId);
    });
  });

  rebuildHyperwebSocialMaterialized(next);

  const snapshotState = pruneHyperwebPublicSnapshotsState(readHyperwebPublicSnapshotsState());
  let snapshotsChanged = false;
  (Array.isArray(snapshotState.snapshots) ? snapshotState.snapshots : []).forEach((snapshot) => {
    if (!snapshot || typeof snapshot !== 'object') return;
    const id = String(snapshot.snapshot_id || '').trim();
    if (!id) return;
    if (String(snapshot.status || 'visible') === 'hidden') return;
    const stats = getVoteStatsForTarget(next, id);
    if (stats.down < HYPERWEB_MODERATION_HIDE_MIN_DOWNVOTERS || stats.net > HYPERWEB_MODERATION_HIDE_SCORE) return;
    snapshot.status = 'hidden';
    snapshot.hidden_reason = 'community_threshold';
    snapshot.hidden_at = nowTs();
    snapshot.updated_at = nowTs();
    snapshotsChanged = true;
  });
  if (snapshotsChanged) {
    writeHyperwebPublicSnapshotsState(snapshotState);
    syncPublicFeedWithSnapshots();
  }
}

function buildHyperwebFeed(options = {}) {
  const state = ensureHyperwebSocialState();
  const authorFilter = String(options.author_fingerprint || '').trim().toUpperCase();
  const posts = Object.values(state.posts_by_id || {})
    .filter((post) => {
      if (!post) return false;
      if (!authorFilter) return true;
      return String(post.author_fingerprint || '').toUpperCase() === authorFilter;
    })
    .map((post) => {
      const stats = getVoteStatsForTarget(state, post.post_id);
      const confidence = computeWilsonLowerBound(stats.up, stats.down);
      const decayed = computeDecayedScore(stats.net, post.created_at);
      const postTombstone = state.tombstones && state.tombstones[post.post_id]
        ? state.tombstones[post.post_id]
        : null;
      const replies = Array.isArray(state.replies_by_post_id[post.post_id]) ? state.replies_by_post_id[post.post_id] : [];
      const visibleReplies = replies
        .filter((item) => (!authorFilter ? true : String(item.author_fingerprint || '').toUpperCase() === authorFilter))
        .map((reply) => {
          const vote = getVoteStatsForTarget(state, reply.reply_id);
          const replyTombstone = state.tombstones && state.tombstones[reply.reply_id]
            ? state.tombstones[reply.reply_id]
            : null;
          return {
            ...reply,
            votes: vote,
            status: replyTombstone ? 'hidden' : 'visible',
            removed_by_threshold: !!replyTombstone,
            tombstone_reason: replyTombstone ? String(replyTombstone.reason || '') : '',
          };
        })
        .sort((a, b) => Number(a.created_at || 0) - Number(b.created_at || 0));
      const rank = postTombstone
        ? -9999
        : Number((confidence + decayed).toFixed(4));
      return {
        ...post,
        votes: stats,
        confidence_score: Number(confidence.toFixed(4)),
        rank_score: rank,
        status: postTombstone ? 'hidden' : 'visible',
        removed_by_threshold: !!postTombstone,
        tombstone_reason: postTombstone ? String(postTombstone.reason || '') : '',
        replies: visibleReplies,
      };
    })
    .sort((a, b) => Number(b.rank_score || 0) - Number(a.rank_score || 0));
  return {
    ok: true,
    identity: state.identity,
    author_filter: authorFilter,
    posts,
    peer_count: Number((hyperwebManager.getStatus() && hyperwebManager.getStatus().peer_count) || 0),
  };
}

function queryHyperwebProfile(authorFingerprint = '') {
  const author = String(authorFingerprint || '').trim().toUpperCase();
  const feed = buildHyperwebFeed({ author_fingerprint: author });
  const refs = [];
  const allRefs = getPublicReferencesForHyperweb();
  allRefs.forEach((ref) => {
    const sourcePeerId = String((ref && ref.source_peer_id) || '').trim().toUpperCase();
    if (author && sourcePeerId !== author) return;
    refs.push(ref);
  });
  return {
    ok: true,
    author_fingerprint: author,
    identity: feed.identity,
    posts: feed.posts,
    references: refs,
  };
}

function buildPublicReferenceSearchBlob(reference) {
  const ref = reference || {};
  const tags = Array.isArray(ref.tags) ? ref.tags.join(' ') : '';
  const artifacts = Array.isArray(ref.artifacts)
    ? ref.artifacts.map((item) => `${item.title || ''} ${item.content || ''}`).join(' ')
    : '';
  const tabs = Array.isArray(ref.tabs)
    ? ref.tabs.map((tab) => `${tab.title || ''} ${tab.url || ''}`).join(' ')
    : '';
  return `${ref.title || ''} ${ref.intent || ''} ${tags} ${artifacts} ${tabs}`.replace(/\s+/g, ' ').trim();
}

function normalizePublicSummaryText(value, maxChars = PUBLIC_REFERENCE_SUMMARY_MAX_CHARS) {
  const text = String(value || '').replace(/\s+/g, ' ').trim();
  if (!text) return '';
  if (text.length <= maxChars) return text;
  return `${text.slice(0, Math.max(1, maxChars - 3))}...`;
}

function buildPublicReferenceSummary(reference) {
  const ref = reference || {};
  const tags = Array.isArray(ref.tags) ? ref.tags.slice(0, 5).join(', ') : '';
  const artifactText = Array.isArray(ref.artifacts)
    ? ref.artifacts.map((item) => String(item && item.content || '')).join(' ')
    : '';
  const tabText = Array.isArray(ref.tabs)
    ? ref.tabs.map((item) => String(item && item.title || '')).join(' ')
    : '';
  const combined = [ref.intent, tags, artifactText, tabText].map((x) => String(x || '').trim()).filter(Boolean).join('. ');
  const summary = normalizePublicSummaryText(combined);
  if (!summary) return 'No summary available.';
  return summary;
}

function buildPublicReferenceStateHash(reference) {
  const ref = reference || {};
  const payload = {
    title: String(ref.title || ''),
    intent: String(ref.intent || ''),
    tags: Array.isArray(ref.tags) ? ref.tags.map((item) => String(item || '')).slice(0, 60) : [],
    tabs: Array.isArray(ref.tabs)
      ? ref.tabs.slice(0, 50).map((tab) => ({
        kind: String((tab && tab.tab_kind) || 'web'),
        title: String((tab && tab.title) || ''),
        url: String((tab && tab.url) || ''),
      }))
      : [],
    artifacts: Array.isArray(ref.artifacts)
      ? ref.artifacts.slice(0, 60).map((artifact) => ({
        title: String((artifact && artifact.title) || ''),
        type: String((artifact && artifact.type) || ''),
        content: String((artifact && artifact.content) || '').slice(0, 12000),
      }))
      : [],
    context_files: Array.isArray(ref.context_files)
      ? ref.context_files.slice(0, 80).map((file) => ({
        name: String((file && file.original_name) || (file && file.relative_path) || ''),
        summary: String((file && file.summary) || ''),
      }))
      : [],
    graph: {
      nodes: Array.isArray(ref.reference_graph && ref.reference_graph.nodes) ? ref.reference_graph.nodes.length : 0,
      edges: Array.isArray(ref.reference_graph && ref.reference_graph.edges) ? ref.reference_graph.edges.length : 0,
    },
    program: String(ref.program || '').slice(0, 4000),
    skills: Array.isArray(ref.skills)
      ? ref.skills.slice(0, 50).map((skill) => String((skill && (skill.name || skill.id)) || ''))
      : [],
  };
  return crypto.createHash('sha256').update(JSON.stringify(payload)).digest('hex');
}

function getPublicReferenceSummaryCache(reference) {
  const cache = reference && reference.public_summary_cache && typeof reference.public_summary_cache === 'object'
    ? reference.public_summary_cache
    : null;
  if (!cache) return null;
  const summary = normalizePublicSummaryText(cache.summary || '');
  if (!summary) return null;
  return {
    summary,
    state_hash: String(cache.state_hash || ''),
    source: String(cache.source || ''),
    provider: String(cache.provider || ''),
    model: String(cache.model || ''),
    updated_at: Number(cache.updated_at || 0),
  };
}

function getPublicSummaryModelConfig() {
  const settings = readSettings();
  const provider = String((settings && settings.lumino_last_provider) || 'openai').trim().toLowerCase();
  const configuredProvider = PROVIDERS.includes(provider) ? provider : 'openai';
  const configuredModel = String((settings && settings.lumino_last_model) || '').trim();
  const model = configuredModel || String(PROVIDER_SUMMARY_MODEL_FALLBACK[configuredProvider] || '');
  if (!model) {
    return {
      available: false,
      provider: configuredProvider,
      model: '',
      apiKey: '',
      reason: 'model_unset',
    };
  }
  const credRes = resolveProviderRuntimeCredentials(configuredProvider, settings);
  if (!credRes || !credRes.ok) {
    return {
      available: false,
      provider: configuredProvider,
      model,
      apiKey: '',
      base_url: '',
      reason: 'key_unset',
    };
  }
  return {
    available: true,
    provider: configuredProvider,
    model,
    apiKey: String(credRes.apiKey || ''),
    base_url: String(credRes.base_url || ''),
    reason: '',
  };
}

function buildPublicReferenceSummaryPrompt(reference, query = '') {
  const ref = reference || {};
  const tabs = Array.isArray(ref.tabs) ? ref.tabs : [];
  const artifacts = Array.isArray(ref.artifacts) ? ref.artifacts : [];
  const contextFiles = Array.isArray(ref.context_files) ? ref.context_files : [];
  const tags = Array.isArray(ref.tags) ? ref.tags : [];
  const graphNodeCount = Array.isArray(ref.reference_graph && ref.reference_graph.nodes) ? ref.reference_graph.nodes.length : 0;
  const graphEdgeCount = Array.isArray(ref.reference_graph && ref.reference_graph.edges) ? ref.reference_graph.edges.length : 0;

  const tabLines = tabs.slice(0, 12).map((tab) => `- ${String((tab && tab.title) || (tab && tab.url) || 'Untitled')} (${String((tab && tab.url) || '').slice(0, 180)})`);
  const artifactLines = artifacts.slice(0, 8).map((artifact) => {
    const title = String((artifact && artifact.title) || 'Artifact');
    const text = String((artifact && artifact.content) || '').replace(/\s+/g, ' ').trim().slice(0, 900);
    return `- ${title}: ${text}`;
  });
  const contextLines = contextFiles.slice(0, 10).map((file) => {
    const name = String((file && file.original_name) || (file && file.relative_path) || 'context');
    const text = String((file && file.summary) || '').replace(/\s+/g, ' ').trim().slice(0, 260);
    return `- ${name}: ${text}`;
  });

  return [
    `Title: ${String(ref.title || 'Untitled')}`,
    `Intent: ${String(ref.intent || '').slice(0, 1000)}`,
    `Tags: ${tags.slice(0, 20).join(', ')}`,
    `Tabs (${tabs.length}):`,
    tabLines.join('\n') || '- (none)',
    `Artifacts (${artifacts.length}):`,
    artifactLines.join('\n') || '- (none)',
    `Context files (${contextFiles.length}):`,
    contextLines.join('\n') || '- (none)',
    `Reference graph: ${graphNodeCount} nodes, ${graphEdgeCount} edges`,
    query ? `Search query context: ${String(query || '').slice(0, 400)}` : '',
  ].filter(Boolean).join('\n');
}

async function generateModelPublicReferenceSummary(reference, query = '', modelConfig = null) {
  const config = modelConfig && typeof modelConfig === 'object' ? modelConfig : getPublicSummaryModelConfig();
  if (!config.available) return '';
  const systemPrompt = [
    'You summarize a public research reference for discovery results.',
    `Return a concise, factual summary in plain text (max ${PUBLIC_REFERENCE_SUMMARY_MAX_CHARS} characters).`,
    'Cover the main thesis, key evidence, and notable outputs.',
    'Do not output markdown headings or bullet lists.',
  ].join('\n');
  const userPrompt = buildPublicReferenceSummaryPrompt(reference, query);
  try {
    const res = await chatWithProvider({
      provider: config.provider,
      model: config.model,
      apiKey: config.apiKey,
      baseUrl: config.base_url,
      systemPrompt,
      userPrompt,
      timeoutMs: PUBLIC_REFERENCE_SUMMARY_TIMEOUT_MS,
    }, {
      timeoutMs: PUBLIC_REFERENCE_SUMMARY_TIMEOUT_MS,
    });
    if (!res || !res.ok) return '';
    return normalizePublicSummaryText(res.text || '');
  } catch (_) {
    return '';
  }
}

async function ensurePublicReferenceSummary(reference, query = '', options = {}) {
  if (!reference || typeof reference !== 'object') {
    return { summary: 'No summary available.', changed: false, state_hash: '' };
  }
  const stateHash = buildPublicReferenceStateHash(reference);
  const cache = getPublicReferenceSummaryCache(reference);
  const heuristic = buildPublicReferenceSummary(reference);
  const modelConfig = (options && options.modelConfig && typeof options.modelConfig === 'object')
    ? options.modelConfig
    : getPublicSummaryModelConfig();

  if (cache && cache.state_hash === stateHash) {
    if (cache.source === 'model' || !modelConfig.available) {
      return { summary: cache.summary, changed: false, state_hash: stateHash };
    }
  }

  const modelSummary = await generateModelPublicReferenceSummary(reference, query, modelConfig);
  const finalSummary = normalizePublicSummaryText(modelSummary || heuristic) || 'No summary available.';
  const source = modelSummary ? 'model' : 'heuristic';
  const changed = !cache
    || cache.state_hash !== stateHash
    || normalizePublicSummaryText(cache.summary || '') !== finalSummary
    || String(cache.source || '') !== source;

  if (changed) {
    reference.public_summary_cache = {
      state_hash: stateHash,
      summary: finalSummary,
      source,
      provider: modelSummary ? String(modelConfig.provider || '') : '',
      model: modelSummary ? String(modelConfig.model || '') : '',
      updated_at: nowTs(),
    };
  }
  return { summary: finalSummary, changed, state_hash: stateHash };
}

function scoreHybridTextMatch(text, query) {
  const blob = String(text || '').toLowerCase();
  const q = String(query || '').trim().toLowerCase();
  if (!q) return { score: 0, keyword: 0, semantic: 0 };
  const semantic = blob.includes(q) ? 1 : 0;
  const terms = q.split(/\s+/).filter(Boolean);
  const keyword = terms.length
    ? terms.filter((term) => blob.includes(term)).length / terms.length
    : 0;
  return {
    score: (semantic * 0.55) + (keyword * 0.45),
    keyword,
    semantic,
  };
}

async function runHyperwebReferenceSearch(query, options = {}) {
  const q = String(query || '').trim();
  const limit = Math.max(1, Math.min(HYPERWEB_SEARCH_MAX_RESULTS, Number(options.limit || 40) || 40));
  const authorFilter = String(options.author_fingerprint || '').trim().toUpperCase();
  const social = ensureHyperwebSocialState();
  const myMemberId = currentHyperwebMemberId();
  const snapshotState = pruneHyperwebPublicSnapshotsState(readHyperwebPublicSnapshotsState());
  let changed = false;
  (Array.isArray(snapshotState.snapshots) ? snapshotState.snapshots : []).forEach((snapshot) => {
    if (!snapshot || typeof snapshot !== 'object') return;
    if (String(snapshot.status || '').trim().toLowerCase() !== 'pending') return;
    snapshot.status = 'visible';
    snapshot.updated_at = nowTs();
    changed = true;
  });
  if (changed) {
    writeHyperwebPublicSnapshotsState(snapshotState);
    syncPublicFeedWithSnapshots();
  }
  writeHyperwebPublicSnapshotsState(snapshotState);
  const localResults = (Array.isArray(snapshotState.snapshots) ? snapshotState.snapshots : [])
    .filter((snapshot) => {
      const status = String((snapshot && snapshot.status) || 'visible').trim().toLowerCase();
      const authorFp = String((snapshot && snapshot.author_fingerprint) || '').trim().toUpperCase();
      if (status === 'hidden' && authorFp !== myMemberId) return false;
      if (!authorFilter) return true;
      return authorFp === authorFilter;
    })
    .map((snapshot) => {
      const payload = (snapshot && snapshot.reference_payload && typeof snapshot.reference_payload === 'object')
        ? snapshot.reference_payload
        : {};
      const snapshotId = String((snapshot && snapshot.snapshot_id) || '').trim();
      const blob = buildPublicReferenceSearchBlob(payload);
      const hybrid = q ? scoreHybridTextMatch(blob, q) : { score: 0.4, keyword: 0, semantic: 0 };
      const votes = getVoteStatsForTarget(social, snapshotId);
      const score = Number(hybrid.score || 0) + (Number(votes.net || 0) * 0.02);
      return {
        reference_key: `snapshot:${snapshotId}`,
        snapshot_id: snapshotId,
        reference_id: String((snapshot && snapshot.reference_id) || ''),
        title: String((payload && payload.title) || (snapshot && snapshot.reference_title) || 'Untitled'),
        intent: String((payload && payload.intent) || ''),
        peer_id: String((snapshot && snapshot.author_fingerprint) || '').toUpperCase(),
        peer_name: String((snapshot && snapshot.author_alias) || 'peer'),
        tags: Array.isArray(payload && payload.tags) ? payload.tags : [],
        status: String((snapshot && snapshot.status) || 'visible'),
        published_at: Number((snapshot && snapshot.published_at) || 0),
        updated_at: Number((snapshot && snapshot.updated_at) || 0),
        votes,
        score: Number(score || 0),
        score_breakdown: hybrid,
        summary_text: normalizePublicSummaryText(payload.summary_text || snapshot.summary_text || buildPublicReferenceSummary(payload))
          || 'No summary available.',
        content_excerpt: normalizePublicSummaryText(blob, 520),
        search_blob: blob,
        import_payload: {
          source_type: 'public_snapshot',
          snapshot_id: snapshotId,
          reference_id: String((snapshot && snapshot.reference_id) || ''),
          peer_id: String((snapshot && snapshot.author_fingerprint) || '').toUpperCase(),
          peer_name: String((snapshot && snapshot.author_alias) || 'peer'),
          reference_payload: payload,
          snapshot,
        },
      };
    })
    .filter((item) => (!q ? true : item.score > 0));

  let remoteResults = [];
  const status = hyperwebManager.getStatus();
  if (q && status && status.connected && Number(status.peer_count || 0) > 0) {
    const remote = await hyperwebManager.query(q, { limit, timeout_ms: 1400 });
    if (remote && remote.ok && Array.isArray(remote.suggestions)) {
      remoteResults = remote.suggestions.map((item) => {
        const payload = item.reference_payload && typeof item.reference_payload === 'object'
          ? item.reference_payload
          : item;
        const blob = buildPublicReferenceSearchBlob(payload);
        return {
          reference_key: `remote:${String(item.peer_id || '')}:${String(item.reference_id || '')}:${String(item.suggestion_id || '')}`,
          snapshot_id: String(item.snapshot_id || ''),
          reference_id: String(item.reference_id || ''),
          title: String(item.title || 'Untitled'),
          intent: String(item.intent || ''),
          peer_id: String(item.peer_id || '').trim().toUpperCase(),
          peer_name: String(item.peer_name || item.peer_id || 'peer'),
          tags: Array.isArray(item.tags) ? item.tags : [],
          status: String(item.status || 'visible'),
          published_at: Number(item.published_at || item.updated_at || 0),
          updated_at: Number(item.updated_at || 0),
          votes: {
            up: Number(item.upvotes || 0),
            down: Number(item.downvotes || 0),
            total: Number((item.upvotes || 0) + (item.downvotes || 0)),
            net: Number((item.upvotes || 0) - (item.downvotes || 0)),
          },
          score: Number(item.score || 0),
          score_breakdown: scoreHybridTextMatch(blob, q),
          summary_text: normalizePublicSummaryText(item.summary_text || payload.summary_text || buildPublicReferenceSummary(payload))
            || 'No summary available.',
          content_excerpt: normalizePublicSummaryText(blob, 520),
          search_blob: blob,
          import_payload: item,
        };
      }).filter((item) => (!authorFilter ? true : String(item.peer_id || '').toUpperCase() === authorFilter));
    }
  }

  const dedupe = new Set();
  const merged = [];
  [...localResults, ...remoteResults].forEach((item) => {
    const sig = `${String(item.peer_id || '')}:${String(item.reference_id || '')}:${String(item.title || '')}`;
    if (dedupe.has(sig)) return;
    dedupe.add(sig);
    merged.push(item);
  });

  merged.sort((a, b) => {
    const diff = Number((b && b.score) || 0) - Number((a && a.score) || 0);
    if (Math.abs(diff) > 0.0001) return diff;
    return Number((b && b.updated_at) || 0) - Number((a && a.updated_at) || 0);
  });

  return {
    ok: true,
    query: q,
    local_count: localResults.length,
    remote_count: remoteResults.length,
    results: merged.slice(0, limit),
  };
}

function runHyperwebPostSearch(query, options = {}) {
  const state = ensureHyperwebSocialState();
  const q = String(query || '').trim();
  const limit = Math.max(1, Math.min(HYPERWEB_SEARCH_MAX_RESULTS, Number(options.limit || 40) || 40));
  const authorFilter = String(options.author_fingerprint || '').trim().toUpperCase();
  if (!q) return { ok: true, query: '', results: [] };

  const hits = [];
  Object.values(state.posts_by_id || {}).forEach((post) => {
    if (!post) return;
    if (state.tombstones && state.tombstones[post.post_id]) return;
    if (authorFilter && String(post.author_fingerprint || '').toUpperCase() !== authorFilter) return;
    const searchBlob = String(post.search_blob || `${post.body || ''}`).trim();
    const score = scoreHybridTextMatch(searchBlob, q);
    if (score.score <= 0) return;
    hits.push({
      target_type: 'post',
      target_id: String(post.post_id || ''),
      post_id: String(post.post_id || ''),
      author_fingerprint: String(post.author_fingerprint || ''),
      author_alias: String(post.author_alias || ''),
      snippet: String(post.body || '').slice(0, 260),
      score: score.score,
      score_breakdown: score,
      created_at: Number(post.created_at || 0),
    });
  });
  Object.entries(state.replies_by_post_id || {}).forEach(([postId, replies]) => {
    (Array.isArray(replies) ? replies : []).forEach((reply) => {
      if (!reply) return;
      if (state.tombstones && state.tombstones[reply.reply_id]) return;
      if (authorFilter && String(reply.author_fingerprint || '').toUpperCase() !== authorFilter) return;
      const searchBlob = String(reply.search_blob || `${reply.body || ''}`).trim();
      const score = scoreHybridTextMatch(searchBlob, q);
      if (score.score <= 0) return;
      hits.push({
        target_type: 'reply',
        target_id: String(reply.reply_id || ''),
        post_id: String(postId || ''),
        author_fingerprint: String(reply.author_fingerprint || ''),
        author_alias: String(reply.author_alias || ''),
        snippet: String(reply.body || '').slice(0, 260),
        score: score.score,
        score_breakdown: score,
        created_at: Number(reply.created_at || 0),
      });
    });
  });
  hits.sort((a, b) => Number((b && b.score) || 0) - Number((a && a.score) || 0));
  return {
    ok: true,
    query: q,
    results: hits.slice(0, limit),
  };
}

function listHyperwebMembers() {
  const state = ensureHyperwebSocialState();
  const settings = readSettings();
  const localIdentity = state.identity || {};
  const localId = String(localIdentity.fingerprint || '').trim().toUpperCase();
  const localName = String(settings.trustcommons_display_name || localIdentity.display_alias || 'You').trim() || 'You';
  const membersById = new Map();

  if (localId) {
    membersById.set(localId, {
      member_id: localId,
      display_name: localName,
      is_self: true,
      source: 'local_identity',
      search_blob: `${localName} ${localId}`.toLowerCase(),
    });
  }

  Object.entries(state.known_peers || {}).forEach(([fingerprint, peer]) => {
    const id = String(fingerprint || '').trim().toUpperCase();
    if (!id) return;
    membersById.set(id, {
      member_id: id,
      display_name: String((peer && peer.alias) || `member-${id.slice(0, 6)}`),
      is_self: id === localId,
      source: 'hyperweb',
      search_blob: String((peer && peer.alias) || id).toLowerCase(),
    });
  });

  const ttcUserFileCandidates = [
    path.join(__dirname, '..', 'ttc_webapp', 'database', 'core', 'users.json'),
    path.join(process.cwd(), 'ttc_webapp', 'database', 'core', 'users.json'),
    path.join(process.cwd(), '..', 'ttc_webapp', 'database', 'core', 'users.json'),
    path.join(process.cwd(), 'database', 'core', 'users.json'),
  ];
  for (const filePath of ttcUserFileCandidates) {
    try {
      if (!filePath || !fs.existsSync(filePath)) continue;
      const parsed = JSON.parse(fs.readFileSync(filePath, 'utf8'));
      if (!parsed || typeof parsed !== 'object') continue;

      const nicknameMap = new Map();
      Object.entries(parsed).forEach(([ownerName, row]) => {
        void ownerName;
        const nicknames = (row && row.member_prefs && row.member_prefs.nicknames && typeof row.member_prefs.nicknames === 'object')
          ? row.member_prefs.nicknames
          : {};
        Object.entries(nicknames).forEach(([target, nickname]) => {
          const targetKey = String(target || '').trim();
          const nick = String(nickname || '').trim();
          if (!targetKey || !nick) return;
          const bucket = nicknameMap.get(targetKey) || new Set();
          bucket.add(nick);
          nicknameMap.set(targetKey, bucket);
        });
      });

      Object.keys(parsed).forEach((username) => {
        const clean = String(username || '').trim();
        if (!clean) return;
        const id = `ttc_user:${clean}`;
        const nicknames = Array.from(nicknameMap.get(clean) || []).slice(0, 4);
        const label = nicknames.length ? `${nicknames[0]} (${clean})` : clean;
        membersById.set(id, {
          member_id: id,
          display_name: label,
          ttc_username: clean,
          nicknames,
          is_self: false,
          source: 'ttc_directory',
          search_blob: `${clean} ${nicknames.join(' ')}`.toLowerCase(),
        });
      });
      break;
    } catch (_) {
      // Continue to next candidate path.
    }
  }

  return Array.from(membersById.values()).sort((a, b) => {
    if (a.is_self && !b.is_self) return -1;
    if (!a.is_self && b.is_self) return 1;
    return String(a.display_name || '').localeCompare(String(b.display_name || ''));
  });
}

function resolveTtcUsersFilePath() {
  const candidates = [
    path.join(__dirname, '..', 'ttc_webapp', 'database', 'core', 'users.json'),
    path.join(process.cwd(), 'ttc_webapp', 'database', 'core', 'users.json'),
    path.join(process.cwd(), '..', 'ttc_webapp', 'database', 'core', 'users.json'),
    path.join(process.cwd(), 'database', 'core', 'users.json'),
  ];
  for (const filePath of candidates) {
    if (filePath && fs.existsSync(filePath)) return filePath;
  }
  return '';
}

function resolveTtcPrivateMessagesFilePath(usersFilePath = '') {
  const inferred = String(usersFilePath || '').trim();
  if (inferred && inferred.endsWith(path.join('database', 'core', 'users.json'))) {
    const root = inferred.slice(0, -('core/users.json'.length));
    const fromUsers = path.join(root, 'social', 'private_messages.json');
    if (fs.existsSync(fromUsers)) return fromUsers;
  }
  const candidates = [
    path.join(__dirname, '..', 'ttc_webapp', 'database', 'social', 'private_messages.json'),
    path.join(process.cwd(), 'ttc_webapp', 'database', 'social', 'private_messages.json'),
    path.join(process.cwd(), '..', 'ttc_webapp', 'database', 'social', 'private_messages.json'),
    path.join(process.cwd(), 'database', 'social', 'private_messages.json'),
  ];
  for (const filePath of candidates) {
    if (filePath && fs.existsSync(filePath)) return filePath;
  }
  return '';
}

function findTtcUsernameExactOrCaseInsensitive(usersData = {}, hint = '') {
  const target = String(hint || '').trim();
  if (!target) return '';
  if (Object.prototype.hasOwnProperty.call(usersData, target)) return target;
  const lower = target.toLowerCase();
  return Object.keys(usersData).find((name) => String(name || '').toLowerCase() === lower) || '';
}

function resolveTtcDmSenderUsername(usersData = {}) {
  const settings = readSettings();
  const hints = [
    String((settings && settings.trustcommons_identity_id) || '').trim(),
    String((settings && settings.trustcommons_display_name) || '').trim(),
  ].filter(Boolean);
  for (const hint of hints) {
    const match = findTtcUsernameExactOrCaseInsensitive(usersData, hint);
    if (match) return match;
  }
  return '';
}

function encryptTtcPrivateMessage(plaintext = '', shiftKey = 1) {
  const shift = Number.isFinite(Number(shiftKey)) && Number(shiftKey) > 0 ? Number(shiftKey) : 1;
  return String(plaintext || '').split('').map((ch) => String.fromCharCode(ch.charCodeAt(0) + shift)).join('');
}

function writeJsonAtomic(filePath, payloadObj) {
  const dir = path.dirname(filePath);
  const temp = path.join(dir, `${path.basename(filePath)}.tmp-${process.pid}-${Date.now()}`);
  fs.writeFileSync(temp, JSON.stringify(payloadObj, null, 2), 'utf8');
  fs.renameSync(temp, filePath);
}

function appendTtcInviteDm(recipientUsername = '', messageText = '') {
  const recipientHint = String(recipientUsername || '').trim();
  const text = String(messageText || '').trim();
  if (!recipientHint || !text) return { ok: false, message: 'recipient and message are required.' };

  const usersFilePath = resolveTtcUsersFilePath();
  if (!usersFilePath) return { ok: false, message: 'TTC users directory not found.' };
  const messagesFilePath = resolveTtcPrivateMessagesFilePath(usersFilePath);
  if (!messagesFilePath) return { ok: false, message: 'TTC private messages store not found.' };

  try {
    const usersData = JSON.parse(fs.readFileSync(usersFilePath, 'utf8'));
    if (!usersData || typeof usersData !== 'object') {
      return { ok: false, message: 'TTC users data is invalid.' };
    }
    const sender = resolveTtcDmSenderUsername(usersData);
    if (!sender) {
      return { ok: false, message: 'TTC sender identity is not linked in Subgrapher settings.' };
    }
    const recipient = findTtcUsernameExactOrCaseInsensitive(usersData, recipientHint);
    if (!recipient) return { ok: false, message: `TTC recipient not found: ${recipientHint}` };

    const recipientRow = usersData[recipient] || {};
    const recipientKey = Number(recipientRow.key || 1);
    const encrypted = encryptTtcPrivateMessage(text, recipientKey);
    const messageId = `msg-${crypto.randomBytes(8).toString('hex')}`;
    const timestamp = Date.now() / 1000;

    let store = { history: [] };
    if (fs.existsSync(messagesFilePath)) {
      const parsed = JSON.parse(fs.readFileSync(messagesFilePath, 'utf8'));
      if (parsed && typeof parsed === 'object') {
        store = {
          ...parsed,
          history: Array.isArray(parsed.history) ? parsed.history : [],
        };
      }
    }
    store.history.push({
      message_id: messageId,
      from: sender,
      to: recipient,
      encrypted_message: encrypted,
      timestamp,
      parent_id: null,
      root_id: messageId,
      depth: 0,
      ancestry: [messageId],
    });
    writeJsonAtomic(messagesFilePath, store);
    return {
      ok: true,
      sender,
      recipient,
      message_id: messageId,
      file: messagesFilePath,
    };
  } catch (err) {
    return { ok: false, message: String((err && err.message) || 'Unable to append TTC invite DM.') };
  }
}

function publishSnapshotFromReference(srId) {
  const auth = requireTtcHyperwebAuth();
  if (!auth.ok) return auth;

  const refId = String(srId || '').trim();
  if (!refId) return { ok: false, message: 'srId is required.' };

  const refs = getReferences();
  const idx = findReferenceIndex(refs, refId);
  if (idx < 0) return { ok: false, message: 'Reference not found.' };
  const ref = refs[idx];
  if (!ref || ref.is_public_candidate) return { ok: false, message: 'Reference is not publishable.' };

  const author = getLocalHyperwebAuthor();
  const authorFp = String(author.peer_id || '').trim().toUpperCase();
  const snapshotsState = pruneHyperwebPublicSnapshotsState(readHyperwebPublicSnapshotsState());
  const now = nowTs();
  const dayStart = now - (24 * 60 * 60 * 1000);
  const monthStart = now - (30 * 24 * 60 * 60 * 1000);
  const dayCount = (Array.isArray(snapshotsState.publish_log) ? snapshotsState.publish_log : [])
    .filter((item) => String((item && item.author_fingerprint) || '').trim().toUpperCase() === authorFp)
    .filter((item) => Number((item && item.ts) || 0) >= dayStart)
    .length;
  const monthCount = (Array.isArray(snapshotsState.publish_log) ? snapshotsState.publish_log : [])
    .filter((item) => String((item && item.author_fingerprint) || '').trim().toUpperCase() === authorFp)
    .filter((item) => Number((item && item.ts) || 0) >= monthStart)
    .length;
  if (dayCount >= HYPERWEB_SNAPSHOT_DAILY_LIMIT) {
    return { ok: false, message: `Daily publish limit reached (${HYPERWEB_SNAPSHOT_DAILY_LIMIT}/day).` };
  }
  if (monthCount >= HYPERWEB_SNAPSHOT_MONTHLY_LIMIT) {
    return { ok: false, message: `Monthly publish limit reached (${HYPERWEB_SNAPSHOT_MONTHLY_LIMIT}/month).` };
  }

  const payload = sanitizePublicReference(ref);
  const snapshotId = makeId('hwsnap');
  const visible = true;
  const snapshot = {
    snapshot_id: snapshotId,
    reference_id: String(ref.id || ''),
    reference_title: String(ref.title || ''),
    author_fingerprint: authorFp,
    author_alias: String(author.peer_name || ''),
    published_at: now,
    updated_at: now,
    status: visible ? 'visible' : 'pending',
    trust_tier: visible ? 'verified' : 'pending',
    reference_payload: payload,
    summary_text: String(payload.summary_text || ''),
  };

  snapshotsState.snapshots = Array.isArray(snapshotsState.snapshots) ? snapshotsState.snapshots : [];
  snapshotsState.publish_log = Array.isArray(snapshotsState.publish_log) ? snapshotsState.publish_log : [];
  snapshotsState.snapshots.unshift(snapshot);
  snapshotsState.publish_log.push({
    snapshot_id: snapshotId,
    reference_id: String(ref.id || ''),
    author_fingerprint: authorFp,
    ts: now,
  });
  pruneHyperwebPublicSnapshotsState(snapshotsState);
  writeHyperwebPublicSnapshotsState(snapshotsState);
  syncPublicFeedWithSnapshots();
  hyperwebManager.refreshLocalPublicIndex().catch(() => {});

  return {
    ok: true,
    snapshot,
    references: refs,
    rate_limit: {
      day_used: dayCount + 1,
      day_limit: HYPERWEB_SNAPSHOT_DAILY_LIMIT,
      month_used: monthCount + 1,
      month_limit: HYPERWEB_SNAPSHOT_MONTHLY_LIMIT,
    },
  };
}

function deletePublishedSnapshot(snapshotId) {
  const targetSnapshotId = String(snapshotId || '').trim();
  if (!targetSnapshotId) return { ok: false, message: 'snapshot_id is required.' };

  const memberId = currentHyperwebMemberId();
  if (!memberId) return { ok: false, message: 'Hyperweb identity unavailable.' };

  const snapshotsState = pruneHyperwebPublicSnapshotsState(readHyperwebPublicSnapshotsState());
  const snapshots = Array.isArray(snapshotsState.snapshots) ? snapshotsState.snapshots : [];
  const idx = snapshots.findIndex((item) => String((item && item.snapshot_id) || '').trim() === targetSnapshotId);
  if (idx < 0) return { ok: false, message: 'Snapshot not found.' };

  const snapshot = snapshots[idx];
  const authorId = String((snapshot && snapshot.author_fingerprint) || '').trim().toUpperCase();
  if (!authorId || authorId !== memberId) {
    return { ok: false, message: 'Only the snapshot author can delete it.' };
  }

  const [removed] = snapshots.splice(idx, 1);
  snapshotsState.snapshots = snapshots;
  writeHyperwebPublicSnapshotsState(snapshotsState);
  syncPublicFeedWithSnapshots();
  hyperwebManager.refreshLocalPublicIndex().catch(() => {});

  return {
    ok: true,
    deleted: true,
    snapshot_id: targetSnapshotId,
    reference_id: String((removed && removed.reference_id) || ''),
  };
}

function importPublicReferenceAsPrivateCopy(item = {}) {
  const refs = getReferences();
  const sourcePayload = (item && item.reference_payload && typeof item.reference_payload === 'object')
    ? item.reference_payload
    : item;
  const candidate = createCandidateReferenceFromPublic({
    ...item,
    source_type: 'public_snapshot',
    reference_payload: sourcePayload,
  }, '');
  const imported = {
    ...createForkReference(candidate, {
      title: String(candidate.title || 'Imported Reference').replace(/^\[Public\]\s*/i, '').slice(0, 120),
      source_metadata: {
        ...(candidate.source_metadata || {}),
        imported_at: nowTs(),
        imported_from_snapshot_id: String((item && item.snapshot_id) || ''),
      },
    }),
    visibility: 'private',
    is_public_candidate: false,
    source_type: 'hyperweb_import',
    source_peer_id: String((item && item.peer_id) || (sourcePayload && sourcePayload.source_peer_id) || '').toUpperCase(),
    source_peer_name: String((item && item.peer_name) || (sourcePayload && sourcePayload.source_peer_name) || ''),
    source_candidate_key: '',
    is_temp_candidate: false,
    temp_imported_at: 0,
    parent_id: null,
    relation_type: 'root',
    lineage: [],
    updated_at: nowTs(),
  };
  refs.unshift(imported);
  setReferences(refs);
  return {
    ok: true,
    imported,
    references: refs,
  };
}

function appendHyperwebReport(targetId, targetKind = 'post', reason = '') {
  const state = ensureHyperwebSocialState();
  const author = getLocalHyperwebAuthor();
  state.reports = Array.isArray(state.reports) ? state.reports : [];
  state.reports.push({
    report_id: makeId('hwreport'),
    target_id: String(targetId || '').trim(),
    target_kind: String(targetKind || 'post').trim().toLowerCase(),
    reason: String(reason || '').trim().slice(0, 1200),
    reporter_fingerprint: String(author.peer_id || '').trim().toUpperCase(),
    reporter_alias: String(author.peer_name || ''),
    created_at: nowTs(),
  });
  if (state.reports.length > 5000) state.reports = state.reports.slice(-5000);
  writeHyperwebSocialState(state);
  return { ok: true };
}

function currentHyperwebMemberId() {
  const state = ensureHyperwebSocialState();
  return String((state.identity && state.identity.fingerprint) || '').trim().toUpperCase();
}

function findPrivateShareById(state, shareId) {
  const id = String(shareId || '').trim();
  const shares = Array.isArray(state && state.shares) ? state.shares : [];
  const idx = shares.findIndex((item) => String((item && item.share_id) || '') === id);
  if (idx < 0) return { share: null, idx: -1 };
  return { share: shares[idx], idx };
}

function refreshRoomParticipantsFromShare(room, share) {
  const ownerId = String((share && share.owner_id) || '').trim().toUpperCase();
  const ownerAlias = String((share && share.owner_alias) || 'owner');
  const recipients = Array.isArray(share && share.recipients) ? share.recipients : [];
  const participants = [{
    member_id: ownerId,
    display_name: ownerAlias,
    status: 'write_accepted',
  }];
  recipients.forEach((item) => {
    const id = String((item && item.member_id) || '').trim().toUpperCase();
    if (!id || id === ownerId) return;
    participants.push({
      member_id: id,
      display_name: String((item && item.display_name) || id),
      status: String((item && item.write_status) || String((item && item.status) || 'write_pending')),
    });
  });
  room.participants = participants;
}

function createPrivateShare(srId, recipientIds = []) {
  const auth = requireTtcHyperwebAuth();
  if (!auth.ok) return auth;
  const refId = String(srId || '').trim();
  if (!refId) return { ok: false, message: 'sr_id is required.' };
  const refs = getReferences();
  const idx = findReferenceIndex(refs, refId);
  if (idx < 0) return { ok: false, message: 'Reference not found.' };

  const memberDirectory = listHyperwebMembers();
  const memberById = new Map(memberDirectory.map((member) => [String(member.member_id || '').trim().toUpperCase(), member]));
  const owner = getLocalHyperwebAuthor();
  const ownerId = String(owner.peer_id || '').trim().toUpperCase();
  const selectedRecipientsByNormId = new Map();
  (Array.isArray(recipientIds) ? recipientIds : []).forEach((item) => {
    const raw = String(item || '').trim();
    if (!raw) return;
    const norm = raw.toUpperCase();
    if (!norm || norm === ownerId || selectedRecipientsByNormId.has(norm)) return;
    const meta = memberById.get(norm);
    const canonical = String((meta && meta.member_id) || raw).trim();
    if (!canonical) return;
    selectedRecipientsByNormId.set(norm, canonical);
  });
  const cleanRecipients = Array.from(selectedRecipientsByNormId.values());
  if (!cleanRecipients.length) return { ok: false, message: 'Select at least one recipient.' };

  const shareState = readHyperwebPrivateSharesState();
  shareState.shares = Array.isArray(shareState.shares) ? shareState.shares : [];
  shareState.rooms = Array.isArray(shareState.rooms) ? shareState.rooms : [];

  const shareId = makeId('hwshare');
  const roomId = makeId('hwroom');
  const recipients = cleanRecipients.map((memberId) => {
    const normalizedId = String(memberId || '').trim().toUpperCase();
    const meta = memberById.get(normalizedId) || { display_name: memberId };
    return {
      member_id: memberId,
      display_name: String(meta.display_name || memberId),
      ttc_username: String((meta && meta.ttc_username) || ''),
      read_access: true,
      write_status: 'write_pending',
      status: 'write_pending',
      updated_at: nowTs(),
    };
  });
  const share = {
    share_id: shareId,
    room_id: roomId,
    reference_id: refId,
    reference_title: String((refs[idx] && refs[idx].title) || 'Shared reference'),
    owner_id: ownerId,
    owner_alias: String(owner.peer_name || ownerId),
    recipients,
    created_at: nowTs(),
    updated_at: nowTs(),
  };
  const invite = createHyperwebInvite();
  if (invite && invite.ok && invite.invite_url) {
    share.invite_url = String(invite.invite_url || '');
    share.invite_message = [
      `Hi, ${share.owner_alias} shared a Subgrapher reference with you.`,
      'Subgrapher is TTC\'s browsing workspace for Hyperweb snapshots and private collaboration.',
      'Install or open it from: https://thetrustcommons.com/apps',
      `Invite link: ${share.invite_url}`,
    ].join('\n');
  }

  const dmRecipients = recipients
    .map((item) => String((item && item.ttc_username) || '').trim())
    .filter(Boolean);
  const inviteDm = {
    attempted: 0,
    delivered: 0,
    failed: 0,
    results: [],
  };
  if (dmRecipients.length > 0 && share.invite_message) {
    dmRecipients.forEach((username) => {
      inviteDm.attempted += 1;
      const result = appendTtcInviteDm(username, share.invite_message);
      if (result && result.ok) inviteDm.delivered += 1;
      else inviteDm.failed += 1;
      inviteDm.results.push({
        username,
        ok: !!(result && result.ok),
        message: String((result && result.message) || ''),
      });
    });
  }
  share.invite_dm = inviteDm;

  const room = {
    room_id: roomId,
    share_id: shareId,
    reference_id: refId,
    reference_title: share.reference_title,
    owner_id: ownerId,
    owner_alias: share.owner_alias,
    participants: [],
    content: '',
    updated_at: nowTs(),
  };
  refreshRoomParticipantsFromShare(room, share);
  shareState.shares.unshift(share);
  shareState.rooms.unshift(room);
  writeHyperwebPrivateSharesState(shareState);
  return { ok: true, share, room };
}

function listPrivateSharesForCurrentUser() {
  const memberId = currentHyperwebMemberId();
  const state = readHyperwebPrivateSharesState();
  const shares = Array.isArray(state.shares) ? state.shares : [];
  const incoming = [];
  const outgoing = [];
  shares.forEach((share) => {
    if (!share || typeof share !== 'object') return;
    const ownerId = String((share.owner_id) || '').trim().toUpperCase();
    const recipients = Array.isArray(share.recipients) ? share.recipients : [];
    if (ownerId === memberId) {
      outgoing.push({
        ...share,
        recipients,
      });
      return;
    }
    const mine = recipients.find((item) => String((item && item.member_id) || '').trim().toUpperCase() === memberId);
    if (!mine) return;
    incoming.push({
      share_id: String(share.share_id || ''),
      room_id: String(share.room_id || ''),
      reference_id: String(share.reference_id || ''),
      reference_title: String(share.reference_title || ''),
      owner_id: ownerId,
      owner_alias: String(share.owner_alias || ''),
      read_access: !!(mine && mine.read_access !== false),
      write_status: String((mine && mine.write_status) || String((mine && mine.status) || 'write_pending')),
      created_at: Number(share.created_at || 0),
      updated_at: Number((mine && mine.updated_at) || share.updated_at || 0),
    });
  });
  incoming.sort((a, b) => Number((b && b.created_at) || 0) - Number((a && a.created_at) || 0));
  outgoing.sort((a, b) => Number((b && b.created_at) || 0) - Number((a && a.created_at) || 0));
  return { ok: true, incoming, outgoing };
}

function updateRecipientShareStatus(shareId, nextStatus) {
  const memberId = currentHyperwebMemberId();
  const state = readHyperwebPrivateSharesState();
  const { share, idx } = findPrivateShareById(state, shareId);
  if (!share || idx < 0) return { ok: false, message: 'Share not found.' };
  const recipients = Array.isArray(share.recipients) ? share.recipients : [];
  const target = recipients.find((item) => String((item && item.member_id) || '').trim().toUpperCase() === memberId);
  if (!target) return { ok: false, message: 'You are not a recipient of this share.' };
  target.read_access = true;
  target.write_status = String(nextStatus || 'write_pending');
  target.status = target.write_status;
  target.updated_at = nowTs();
  share.updated_at = nowTs();

  const room = (Array.isArray(state.rooms) ? state.rooms : []).find((item) => String((item && item.share_id) || '') === String(share.share_id || ''));
  if (room) {
    refreshRoomParticipantsFromShare(room, share);
    room.updated_at = nowTs();
  }
  state.shares[idx] = share;
  writeHyperwebPrivateSharesState(state);
  return { ok: true, share };
}

function revokeShareAccess(shareId) {
  const memberId = currentHyperwebMemberId();
  const state = readHyperwebPrivateSharesState();
  const { share, idx } = findPrivateShareById(state, shareId);
  if (!share || idx < 0) return { ok: false, message: 'Share not found.' };
  const ownerId = String((share.owner_id) || '').trim().toUpperCase();
  if (!memberId || memberId !== ownerId) return { ok: false, message: 'Only owner can revoke access.' };
  share.recipients = (Array.isArray(share.recipients) ? share.recipients : []).map((item) => ({
    ...item,
    read_access: false,
    write_status: 'revoked',
    status: 'revoked',
    updated_at: nowTs(),
  }));
  share.updated_at = nowTs();
  const room = (Array.isArray(state.rooms) ? state.rooms : []).find((item) => String((item && item.share_id) || '') === String(share.share_id || ''));
  if (room) {
    refreshRoomParticipantsFromShare(room, share);
    room.updated_at = nowTs();
  }
  state.shares[idx] = share;
  writeHyperwebPrivateSharesState(state);
  return { ok: true, share };
}

function deletePrivateShare(shareId) {
  const memberId = currentHyperwebMemberId();
  const state = readHyperwebPrivateSharesState();
  const { share, idx } = findPrivateShareById(state, shareId);
  if (!share || idx < 0) return { ok: false, message: 'Share not found.' };
  const ownerId = String((share.owner_id) || '').trim().toUpperCase();
  if (!memberId || memberId !== ownerId) return { ok: false, message: 'Only owner can delete this share.' };

  const targetShareId = String(share.share_id || '');
  const targetRoomId = String(share.room_id || '');
  const rooms = Array.isArray(state.rooms) ? state.rooms : [];
  const nextRooms = rooms.filter((item) => {
    const roomShareId = String((item && item.share_id) || '');
    const roomId = String((item && item.room_id) || '');
    if (targetRoomId && roomId === targetRoomId) return false;
    if (roomShareId === targetShareId) return false;
    return true;
  });

  state.shares = (Array.isArray(state.shares) ? state.shares : [])
    .filter((_, itemIdx) => itemIdx !== idx);
  state.rooms = nextRooms;
  writeHyperwebPrivateSharesState(state);
  return {
    ok: true,
    deleted: true,
    share_id: targetShareId,
    rooms_deleted: Math.max(0, rooms.length - nextRooms.length),
  };
}

function listSharedRoomsForCurrentUser() {
  const memberId = currentHyperwebMemberId();
  const state = readHyperwebPrivateSharesState();
  const sharesById = new Map((Array.isArray(state.shares) ? state.shares : []).map((item) => [String((item && item.share_id) || ''), item]));
  const rooms = (Array.isArray(state.rooms) ? state.rooms : [])
    .filter((room) => {
      const share = sharesById.get(String((room && room.share_id) || ''));
      if (!share) return false;
      const ownerId = String((share.owner_id) || '').trim().toUpperCase();
      if (ownerId === memberId) return true;
      const recipient = (Array.isArray(share.recipients) ? share.recipients : [])
        .find((item) => String((item && item.member_id) || '').trim().toUpperCase() === memberId);
      return !!recipient && recipient.read_access !== false;
    })
    .map((room) => ({
      room_id: String((room && room.room_id) || ''),
      share_id: String((room && room.share_id) || ''),
      reference_id: String((room && room.reference_id) || ''),
      reference_title: String((room && room.reference_title) || ''),
      owner_id: String((room && room.owner_id) || ''),
      owner_alias: String((room && room.owner_alias) || ''),
      updated_at: Number((room && room.updated_at) || 0),
    }))
    .sort((a, b) => Number((b && b.updated_at) || 0) - Number((a && a.updated_at) || 0));
  return { ok: true, rooms };
}

function openSharedRoomForCurrentUser(roomId) {
  const memberId = currentHyperwebMemberId();
  const state = readHyperwebPrivateSharesState();
  const room = (Array.isArray(state.rooms) ? state.rooms : []).find((item) => String((item && item.room_id) || '') === String(roomId || '').trim());
  if (!room) return { ok: false, message: 'Room not found.' };
  const share = (Array.isArray(state.shares) ? state.shares : []).find((item) => String((item && item.share_id) || '') === String((room && room.share_id) || ''));
  if (!share) return { ok: false, message: 'Share not found.' };

  const ownerId = String((share.owner_id) || '').trim().toUpperCase();
  const recipients = Array.isArray(share.recipients) ? share.recipients : [];
  const recipient = recipients.find((item) => String((item && item.member_id) || '').trim().toUpperCase() === memberId);
  const canRead = ownerId === memberId || (recipient && recipient.read_access !== false);
  if (!canRead) return { ok: false, message: 'No access to this room.' };
  const canWrite = ownerId === memberId || (recipient && String((recipient && (recipient.write_status || recipient.status)) || '') === 'write_accepted');
  refreshRoomParticipantsFromShare(room, share);
  return {
    ok: true,
    room: {
      room_id: String(room.room_id || ''),
      share_id: String(room.share_id || ''),
      reference_id: String(room.reference_id || ''),
      reference_title: String(room.reference_title || ''),
      owner_id: ownerId,
      owner_alias: String(room.owner_alias || ''),
      participants: Array.isArray(room.participants) ? room.participants : [],
      content: String(room.content || ''),
      can_write: !!canWrite,
      updated_at: Number(room.updated_at || 0),
    },
  };
}

function applySharedRoomUpdate(roomId, update = {}) {
  const state = readHyperwebPrivateSharesState();
  const openRes = openSharedRoomForCurrentUser(roomId);
  if (!openRes || !openRes.ok || !openRes.room) return openRes;
  if (!openRes.room.can_write) return { ok: false, message: 'Write access is pending or revoked.' };
  const roomIdx = (Array.isArray(state.rooms) ? state.rooms : []).findIndex((item) => String((item && item.room_id) || '') === String(roomId || '').trim());
  if (roomIdx < 0) return { ok: false, message: 'Room not found.' };
  const room = state.rooms[roomIdx];
  room.content = String((update && update.content) || room.content || '');
  room.updated_at = nowTs();
  state.rooms[roomIdx] = room;
  writeHyperwebPrivateSharesState(state);
  return openSharedRoomForCurrentUser(roomId);
}

function normalizeInvitePayload(input = {}) {
  const obj = (input && typeof input === 'object') ? input : {};
  const author = getLocalHyperwebAuthor();
  return {
    invite_id: String(obj.invite_id || makeId('hwinvite')),
    inviter_fingerprint: String(obj.inviter_fingerprint || author.peer_id).toUpperCase(),
    inviter_alias: String(obj.inviter_alias || author.peer_name),
    inviter_pubkey: String(obj.inviter_pubkey || author.pubkey),
    one_time_token: String(obj.one_time_token || crypto.randomBytes(16).toString('hex')),
    expires_at: Number(obj.expires_at || (nowTs() + HYPERWEB_INVITE_EXPIRY_MS)),
    addresses: Array.isArray(obj.addresses) ? obj.addresses.map((a) => String(a || '').trim()).filter(Boolean).slice(0, 20) : [],
  };
}

function createHyperwebInvite() {
  const identity = ensureHyperwebIdentity();
  const payload = normalizeInvitePayload({
    inviter_pubkey: identity.identity.pubkey,
    inviter_fingerprint: identity.identity.fingerprint,
    inviter_alias: identity.identity.display_alias,
  });
  const signature = signHyperwebPayload(payload, identity.private_key_obj);
  const encoded = Buffer.from(JSON.stringify({
    payload,
    signature,
  }), 'utf8').toString('base64url');
  const inviteUrl = `${HYPERWEB_INVITE_PROTO}://${HYPERWEB_INVITE_ROUTE}?token=${encodeURIComponent(encoded)}`;
  return {
    ok: true,
    invite: payload,
    signature,
    token: encoded,
    invite_url: inviteUrl,
  };
}

function acceptHyperwebInviteToken(rawToken = '') {
  const token = String(rawToken || '').trim();
  if (!token) return { ok: false, message: 'invite token is required.' };
  let parsed = null;
  try {
    parsed = JSON.parse(Buffer.from(token, 'base64url').toString('utf8'));
  } catch (_) {
    return { ok: false, message: 'Invalid invite token format.' };
  }
  const payload = normalizeInvitePayload((parsed && parsed.payload) || {});
  const signature = String((parsed && parsed.signature) || '').trim();
  if (!signature || !payload.inviter_pubkey) {
    return { ok: false, message: 'Invite token is incomplete.' };
  }
  const verified = verifyHyperwebPayload(payload, signature, payload.inviter_pubkey);
  if (!verified) return { ok: false, message: 'Invite signature verification failed.' };
  if (Number(payload.expires_at || 0) < nowTs()) {
    return { ok: false, message: 'Invite token has expired.' };
  }

  const state = ensureHyperwebSocialState();
  const relId = `${String(payload.inviter_fingerprint || '').toUpperCase()}::${String((state.identity && state.identity.fingerprint) || '').toUpperCase()}`;
  const relationExists = state.peer_relations.some((item) => String((item && item.id) || '') === relId);
  if (!relationExists) {
    state.peer_relations.push({
      id: relId,
      inviter_fingerprint: String(payload.inviter_fingerprint || '').toUpperCase(),
      invitee_fingerprint: String((state.identity && state.identity.fingerprint) || '').toUpperCase(),
      inviter_alias: String(payload.inviter_alias || ''),
      created_at: nowTs(),
      invite_id: String(payload.invite_id || ''),
      one_time_token: String(payload.one_time_token || ''),
    });
  }
  state.known_peers[String(payload.inviter_fingerprint || '').toUpperCase()] = {
    fingerprint: String(payload.inviter_fingerprint || '').toUpperCase(),
    alias: String(payload.inviter_alias || ''),
    pubkey: String(payload.inviter_pubkey || ''),
    addresses: Array.isArray(payload.addresses) ? payload.addresses : [],
    updated_at: nowTs(),
  };
  writeHyperwebSocialState(state);
  hyperwebSocialState = state;
  return { ok: true, invite: payload, relation_id: relId };
}

function makeSignedSocialEvent(type, payload) {
  const identity = ensureHyperwebIdentity();
  const signer = identity.identity || {};
  const event = {
    event_id: makeId('hwevt'),
    type: String(type || '').trim().toLowerCase(),
    ts: nowTs(),
    signer_pubkey: String(signer.pubkey || ''),
    signer_fingerprint: String(signer.fingerprint || '').toUpperCase(),
    payload: (payload && typeof payload === 'object') ? payload : {},
  };
  event.signature = signHyperwebPayload({
    event_id: event.event_id,
    type: event.type,
    ts: event.ts,
    signer_pubkey: event.signer_pubkey,
    signer_fingerprint: event.signer_fingerprint,
    payload: event.payload,
  }, identity.private_key_obj);
  return event;
}

let cachedTrustCommonsSyncSecret = '';
let cachedTrustCommonsSyncAccount = '';
let ephemeralTrustCommonsSyncSecret = '';

function getTrustCommonsSyncAccount(settings = null) {
  const cfg = settings || readSettings();
  const identityId = String((cfg && cfg.trustcommons_identity_id) || '').trim();
  return identityId
    ? `${TRUSTCOMMONS_SYNC_SECRET_ACCOUNT}:${identityId}`
    : TRUSTCOMMONS_SYNC_SECRET_ACCOUNT;
}

function getTrustCommonsSyncSecret() {
  const settings = readSettings();
  const account = getTrustCommonsSyncAccount(settings);
  if (cachedTrustCommonsSyncSecret && cachedTrustCommonsSyncAccount === account) {
    return cachedTrustCommonsSyncSecret;
  }

  const service = TRUSTCOMMONS_SYNC_SECRET_SERVICE;
  const got = keychain.getSecret(account, { service });
  if (got && got.ok && got.secret) {
    cachedTrustCommonsSyncSecret = String(got.secret || '');
    cachedTrustCommonsSyncAccount = account;
    return cachedTrustCommonsSyncSecret;
  }

  const generated = crypto.randomBytes(32).toString('hex');
  const saved = keychain.setSecret(account, generated, { service });
  if (saved && saved.ok) {
    cachedTrustCommonsSyncSecret = generated;
    cachedTrustCommonsSyncAccount = account;
    return cachedTrustCommonsSyncSecret;
  }

  if (!ephemeralTrustCommonsSyncSecret) {
    ephemeralTrustCommonsSyncSecret = generated;
  }
  return ephemeralTrustCommonsSyncSecret;
}

function getSyncEligibleReferences() {
  const refs = getReferences();
  return refs
    .filter((ref) => ref && !ref.is_public_candidate)
    .filter((ref) => !ref.is_temp_candidate)
    .map((ref) => JSON.parse(JSON.stringify(ref)));
}

function normalizeSyncedReferenceTabs(rawTabs = []) {
  const tabs = Array.isArray(rawTabs) ? rawTabs : [];
  if (tabs.length === 0) {
    return [createWebTab({ url: getDefaultSearchHomeUrl(), title: getDefaultSearchHomeTitle() })];
  }
  const normalized = tabs.slice(0, MAX_BROWSER_TABS_PER_REFERENCE).map((tab) => {
    const kind = String((tab && tab.tab_kind) || 'web').trim().toLowerCase();
    if (kind === 'files') {
      return createFilesTab({
        id: String((tab && tab.id) || makeId('tab')),
        title: String((tab && tab.title) || 'Files'),
        files_view_state: (tab && typeof tab.files_view_state === 'object') ? tab.files_view_state : {},
        updated_at: Number((tab && tab.updated_at) || nowTs()),
      });
    }
    if (kind === 'viz') {
      return null;
    }
    if (kind === 'skills') {
      return createSkillsTab({
        id: String((tab && tab.id) || makeId('tab')),
        title: String((tab && tab.title) || 'Skills'),
      });
    }
    const next = createWebTab({
      url: String((tab && tab.url) || ''),
      title: String((tab && tab.title) || ''),
    });
    next.id = String((tab && tab.id) || next.id);
    next.tab_kind = 'web';
    next.updated_at = Number((tab && tab.updated_at) || nowTs());
    return next;
  });
  const filtered = normalized.filter(Boolean);
  return filtered.length > 0
    ? filtered
    : [createWebTab({ url: getDefaultSearchHomeUrl(), title: getDefaultSearchHomeTitle() })];
}

function normalizeSyncedReferenceArtifacts(rawArtifacts = []) {
  const artifacts = Array.isArray(rawArtifacts) ? rawArtifacts : [];
  if (artifacts.length === 0) {
    return [createArtifact({
      title: 'Research Draft',
      type: 'markdown',
      content: '',
    })];
  }
  return artifacts.slice(0, 80).map((artifact) => createArtifact({
    id: String((artifact && artifact.id) || makeId('artifact')),
    type: normalizeArtifactType((artifact && artifact.type) || 'markdown'),
    title: String((artifact && artifact.title) || 'Artifact').slice(0, 180),
    content: String((artifact && artifact.content) || ''),
    created_at: Number((artifact && artifact.created_at) || nowTs()),
    updated_at: Number((artifact && artifact.updated_at) || nowTs()),
  }));
}

function sanitizeReferenceColorTag(value) {
  const raw = String(value || '').trim().toLowerCase();
  if (raw === 'c1' || raw === 'c2' || raw === 'c3' || raw === 'c4' || raw === 'c5') return raw;
  return '';
}

function normalizeIncomingSyncedReference(raw, context = {}) {
  if (!raw || typeof raw !== 'object') return null;
  const refId = String(raw.id || '').trim();
  if (!refId) return null;

  const tabs = normalizeSyncedReferenceTabs(raw.tabs);
  const artifacts = normalizeSyncedReferenceArtifacts(raw.artifacts);
  const legacyVizArtifacts = (Array.isArray(raw.tabs) ? raw.tabs : [])
    .filter((tab) => String((tab && tab.tab_kind) || '').trim().toLowerCase() === 'viz')
    .map((tab) => buildLegacyVizArtifact(refId, tab));
  const mergedArtifacts = artifacts.concat(legacyVizArtifacts).slice(0, 80);
  const activeTabIdRaw = String(raw.active_tab_id || '').trim();
  const activeTabId = tabs.find((tab) => String(tab.id || '') === activeTabIdRaw)
    ? activeTabIdRaw
    : String((tabs[0] && tabs[0].id) || '').trim();

  const synced = {
    id: refId,
    title: String(raw.title || 'Untitled').slice(0, 120),
    title_user_edited: !!raw.title_user_edited,
    intent: String(raw.intent || ''),
    tags: Array.isArray(raw.tags) ? raw.tags.slice(0, 40).map((item) => String(item || '').trim()).filter(Boolean) : [],
    parent_id: raw.parent_id ? String(raw.parent_id) : null,
    children: Array.isArray(raw.children) ? raw.children.map((item) => String(item || '').trim()).filter(Boolean) : [],
    lineage: Array.isArray(raw.lineage) ? raw.lineage.map((item) => String(item || '').trim()).filter(Boolean) : [],
    relation_type: String(raw.relation_type || (raw.parent_id ? 'child' : 'root')),
    color_tag: sanitizeReferenceColorTag(raw.color_tag),
    visibility: String(raw.visibility || 'private').trim().toLowerCase() === 'public' ? 'public' : 'private',
    is_public_candidate: false,
    source_type: 'trustcommons_sync',
    source_peer_id: String((context && context.source_peer_id) || '').trim(),
    source_peer_name: String((context && context.source_app) || 'trustcommons').trim(),
    source_candidate_key: '',
    source_metadata: {
      sync_source_app: String((context && context.source_app) || 'trustcommons').trim(),
      sync_source_peer_id: String((context && context.source_peer_id) || '').trim(),
      sync_ingested_at: nowTs(),
    },
    is_temp_candidate: false,
    temp_imported_at: 0,
    hyperweb_payload_version: 1,
    tabs,
    active_tab_id: activeTabId || null,
    artifacts: mergedArtifacts,
    context_files: Array.isArray(raw.context_files)
      ? raw.context_files.slice(0, 100).map((file) => ({
        id: String((file && file.id) || makeId('ctx')),
        source_type: 'trustcommons_sync',
        original_name: String((file && file.original_name) || 'context.txt'),
        relative_path: String((file && file.relative_path) || ''),
        stored_path: '',
        mime_type: String((file && file.mime_type) || 'text/plain'),
        size_bytes: Number((file && file.size_bytes) || 0),
        content_hash: String((file && file.content_hash) || ''),
        summary: String((file && file.summary) || '').slice(0, 1200),
        read_only: true,
        created_at: nowTs(),
        updated_at: nowTs(),
      }))
      : [],
    folder_mounts: [],
    youtube_transcripts: sanitizeYouTubeTranscriptMap(raw.youtube_transcripts),
    reference_graph: (raw && typeof raw.reference_graph === 'object' && raw.reference_graph)
      ? raw.reference_graph
      : { nodes: [], edges: [] },
    agent_weights: (raw && typeof raw.agent_weights === 'object' && raw.agent_weights)
      ? raw.agent_weights
      : {},
    decision_trace: Array.isArray(raw.decision_trace) ? raw.decision_trace.slice(-DECISION_TRACE_MAX_STEPS) : [],
    program: String(raw.program || ''),
    skills: Array.isArray(raw.skills)
      ? raw.skills.map((item) => sanitizeSkillDescriptor(item)).filter(Boolean)
      : [],
    highlights: Array.isArray(raw.highlights)
      ? raw.highlights.map((item) => sanitizeHighlightEntry(item)).filter(Boolean).slice(-MAX_HIGHLIGHTS)
      : [],
    chat_thread: (raw && raw.chat_thread && typeof raw.chat_thread === 'object')
      ? {
        messages: Array.isArray(raw.chat_thread.messages) ? raw.chat_thread.messages.slice(-MAX_CHAT_MESSAGES) : [],
        last_message_at: Number((raw.chat_thread && raw.chat_thread.last_message_at) || null),
      }
      : { messages: [], last_message_at: null },
    pinned_root: !!raw.pinned_root,
    created_at: Number(raw.created_at || nowTs()),
    updated_at: Number(raw.updated_at || nowTs()),
    last_used_at: Number(raw.last_used_at || raw.updated_at || nowTs()),
  };

  return synced;
}

function mergeSyncedReferences(incomingRefs, context = {}) {
  const incoming = Array.isArray(incomingRefs) ? incomingRefs : [];
  if (incoming.length === 0) {
    return { ok: true, created_count: 0, updated_count: 0, skipped_count: 0, references: getReferences() };
  }

  const refs = getReferences();
  const indexById = new Map();
  refs.forEach((ref, idx) => {
    const id = String((ref && ref.id) || '').trim();
    if (id) indexById.set(id, idx);
  });

  let createdCount = 0;
  let updatedCount = 0;
  let skippedCount = 0;

  incoming.forEach((raw) => {
    const normalized = normalizeIncomingSyncedReference(raw, context);
    if (!normalized) {
      skippedCount += 1;
      return;
    }

    const id = String(normalized.id || '').trim();
    if (!id) {
      skippedCount += 1;
      return;
    }

    const idx = indexById.get(id);
    if (typeof idx === 'number' && idx >= 0) {
      const existing = refs[idx];
      const existingUpdated = Number((existing && existing.updated_at) || 0);
      const incomingUpdated = Number((normalized && normalized.updated_at) || 0);
      if (incomingUpdated <= existingUpdated) {
        skippedCount += 1;
        return;
      }
      const merged = {
        ...existing,
        ...normalized,
        chat_thread: (existing && existing.chat_thread && Array.isArray(existing.chat_thread.messages) && existing.chat_thread.messages.length > 0)
          ? existing.chat_thread
          : normalized.chat_thread,
      };
      refs[idx] = merged;
      updatedCount += 1;
      return;
    }

    refs.unshift(normalized);
    for (const [key, value] of indexById.entries()) {
      indexById.set(key, value + 1);
    }
    indexById.set(id, 0);
    createdCount += 1;
  });

  if (createdCount > 0 || updatedCount > 0) {
    setReferences(refs);
  }

  return {
    ok: true,
    created_count: createdCount,
    updated_count: updatedCount,
    skipped_count: skippedCount,
    references: refs,
  };
}

function getTrustCommonsAppCandidates() {
  const home = os.homedir();
  return [
    '/Applications/Trust Commons.app',
    '/Applications/TrustCommons.app',
    path.join(home, 'Applications', 'Trust Commons.app'),
    path.join(home, 'Applications', 'TrustCommons.app'),
  ];
}

async function launchTrustCommonsApp() {
  const settings = readSettings();
  const bundleId = String(settings.trustcommons_app_bundle_id || TRUSTCOMMONS_BUNDLE_ID).trim();
  const downloadUrl = String(settings.trustcommons_download_url || TRUSTCOMMONS_DOWNLOAD_URL).trim();
  const syncStatus = trustCommonsSyncBridge.getStatus();
  const localBridgeUrl = String((syncStatus && syncStatus.local_url) || '').trim();
  const peerUrl = String(settings.trustcommons_peer_sync_url || '').trim();
  const syncAccount = getTrustCommonsSyncAccount(settings);
  const deepLink = `trustcommons://subgrapher/connect?source=subgrapher${localBridgeUrl ? `&bridge_url=${encodeURIComponent(localBridgeUrl)}` : ''}${peerUrl ? `&peer_url=${encodeURIComponent(peerUrl)}` : ''}${syncAccount ? `&sync_account=${encodeURIComponent(syncAccount)}` : ''}&sync_service=${encodeURIComponent(TRUSTCOMMONS_SYNC_SECRET_SERVICE)}`;

  if (process.platform === 'darwin' && bundleId) {
    try {
      execFileSync('open', ['-b', bundleId], { stdio: 'ignore' });
      try {
        await shell.openExternal(deepLink);
        return { ok: true, opened: true, method: 'bundle_id+deeplink', download_opened: false };
      } catch (_) {
        return { ok: true, opened: true, method: 'bundle_id', download_opened: false };
      }
    } catch (_) {
      // fall through
    }
  }

  for (const appPath of getTrustCommonsAppCandidates()) {
    try {
      if (!fs.existsSync(appPath)) continue;
      const err = await shell.openPath(appPath);
      if (!err) {
        try {
          await shell.openExternal(deepLink);
          return { ok: true, opened: true, method: 'app_path+deeplink', app_path: appPath, download_opened: false };
        } catch (_) {
          return { ok: true, opened: true, method: 'app_path', app_path: appPath, download_opened: false };
        }
      }
    } catch (_) {
      // try next path
    }
  }

  if (downloadUrl) {
    try {
      await shell.openExternal(downloadUrl);
      return { ok: true, opened: false, method: 'download_fallback', download_opened: true, download_url: downloadUrl };
    } catch (err) {
      return {
        ok: false,
        opened: false,
        method: 'download_fallback_failed',
        download_opened: false,
        download_url: downloadUrl,
        message: String((err && err.message) || 'Unable to open Trust Commons download URL.'),
      };
    }
  }

  return { ok: false, opened: false, method: 'none', download_opened: false, message: 'Trust Commons app not found and no download URL is configured.' };
}

async function ensureTrustCommonsSyncBridge() {
  const settings = readSettings();
  trustCommonsSyncBridge.setEnabled(!!settings.trustcommons_sync_enabled);
  trustCommonsSyncBridge.setPeerBridgeUrl(String(settings.trustcommons_peer_sync_url || '').trim());
  trustCommonsSyncBridge.setSyncIntervalMs(Number(settings.trustcommons_sync_interval_sec || TRUSTCOMMONS_SYNC_DEFAULT_INTERVAL_SEC) * 1000);

  if (!settings.trustcommons_sync_enabled) {
    await trustCommonsSyncBridge.stop();
    return { ok: true, disabled: true, status: trustCommonsSyncBridge.getStatus() };
  }

  const secret = getTrustCommonsSyncSecret();
  if (!secret) {
    return { ok: false, message: 'Unable to initialize local sync secret.', status: trustCommonsSyncBridge.getStatus() };
  }

  const port = Number(settings.trustcommons_sync_port || TRUSTCOMMONS_SYNC_DEFAULT_PORT);
  const started = await trustCommonsSyncBridge.start(port);
  if (!started || !started.ok) return started;
  return { ok: true, status: trustCommonsSyncBridge.getStatus() };
}

function applyTrustCommonsSettingsPatch(patch = {}) {
  const next = writeSettings(patch || {});
  const load = trustCommonsIdentity.loadTrustCommonsIdentity(keychain, next);
  trustCommonsRuntime = {
    ...trustCommonsRuntime,
    bootstrapComplete: !!next.trustcommons_bootstrap_complete,
    identity: load && load.ok ? load.identity : null,
    connected: !!trustCommonsRuntime.connected,
    lastError: load && !load.ok ? String(load.message || '') : '',
  };
  return next;
}

function ensureTrustCommonsBootstrap() {
  const settings = readSettings();
  const bootstrap = trustCommonsIdentity.bootstrapTrustCommonsIdentity(keychain, settings, { appLabel: 'subgrapher' });
  if (!bootstrap || !bootstrap.ok) {
    trustCommonsRuntime = {
      ...trustCommonsRuntime,
      bootstrapComplete: false,
      identity: null,
      connected: false,
      lastError: String((bootstrap && bootstrap.message) || 'Trust Commons bootstrap failed.'),
    };
    return { ok: false, message: trustCommonsRuntime.lastError, settings };
  }

  const merged = applyTrustCommonsSettingsPatch(bootstrap.settings_patch || {});
  trustCommonsRuntime.bootstrapComplete = !!merged.trustcommons_bootstrap_complete;
  trustCommonsRuntime.lastError = '';
  if (trustCommonsRuntime.identity) {
    hyperwebManager.setIdentity(trustCommonsRuntime.identity);
  }
  return {
    ok: true,
    created: !!bootstrap.created,
    identity: trustCommonsRuntime.identity,
    settings: merged,
  };
}

function getTrustCommonsStatus() {
  const settings = readSettings();
  const managerStatus = hyperwebManager.getStatus();
  const syncStatus = trustCommonsSyncBridge.getStatus();
  return {
    ok: true,
    bootstrap_complete: !!settings.trustcommons_bootstrap_complete,
    identity_id: String(settings.trustcommons_identity_id || ''),
    identity_name: String(settings.trustcommons_display_name || ''),
    connected: !!managerStatus.connected,
    launched: !!trustCommonsRuntime.launched,
    launch_method: String(trustCommonsRuntime.launchMethod || ''),
    download_opened: !!trustCommonsRuntime.downloadOpened,
    relay_url: String(settings.hyperweb_relay_url || DEFAULT_HYPERWEB_RELAY_URL),
    hyperweb_enabled: !!settings.hyperweb_enabled,
    sync: syncStatus,
    last_error: trustCommonsRuntime.lastError || managerStatus.last_error || '',
  };
}

function getHyperwebIdentityDiagnostics() {
  const state = ensureHyperwebSocialState();
  const identity = (state && state.identity && typeof state.identity === 'object') ? state.identity : {};
  const privateRaw = getHyperwebIdentityPrivateKeyRaw();
  const keyObj = decodeHyperwebPrivateKey(privateRaw);
  const keyValid = !!(keyObj && validateHyperwebKeyPair(keyObj, String(identity.pubkey || '')));
  return {
    fingerprint: String(identity.fingerprint || ''),
    alias: String(identity.display_alias || ''),
    created_at: Number(identity.created_at || 0),
    key_status: keyValid ? 'valid' : 'invalid_or_missing',
  };
}

function pickEditableSettingsPatch(input = {}) {
  const src = (input && typeof input === 'object') ? input : {};
  const patch = {};
  for (const key of Object.keys(src)) {
    if (!SETTINGS_EDITABLE_KEYS.has(key)) continue;
    patch[key] = src[key];
  }
  return patch;
}

async function applySettingsRuntimeEffects(previousSettings, nextSettings) {
  const prev = previousSettings || readSettings();
  const next = nextSettings || readSettings();
  const applied = {
    hyperweb: { ok: true, changed: false },
    trustcommons_sync: { ok: true, changed: false },
    history: { ok: true, changed: false },
    telegram: { ok: true, changed: false },
    orchestrator_scheduler: { ok: true, changed: false },
  };

  const hyperwebChanged = (
    prev.hyperweb_enabled !== next.hyperweb_enabled
    || String(prev.hyperweb_relay_url || '') !== String(next.hyperweb_relay_url || '')
  );
  if (hyperwebChanged) {
    applied.hyperweb.changed = true;
    hyperwebManager.setRelayUrl(String(next.hyperweb_relay_url || DEFAULT_HYPERWEB_RELAY_URL));
    hyperwebManager.setEnabled(!!next.hyperweb_enabled);
    if (!next.hyperweb_enabled) {
      hyperwebManager.disconnect();
    } else {
      const connectRes = await connectTrustCommonsAndHyperweb({ launchApp: false });
      applied.hyperweb.ok = !!(connectRes && connectRes.ok);
      applied.hyperweb.message = String((connectRes && connectRes.message) || '');
    }
  }

  const syncChanged = (
    prev.trustcommons_sync_enabled !== next.trustcommons_sync_enabled
    || Number(prev.trustcommons_sync_port || 0) !== Number(next.trustcommons_sync_port || 0)
    || String(prev.trustcommons_peer_sync_url || '') !== String(next.trustcommons_peer_sync_url || '')
    || Number(prev.trustcommons_sync_interval_sec || 0) !== Number(next.trustcommons_sync_interval_sec || 0)
  );
  if (syncChanged) {
    applied.trustcommons_sync.changed = true;
    const syncRes = await ensureTrustCommonsSyncBridge();
    applied.trustcommons_sync.ok = !!(syncRes && syncRes.ok);
    applied.trustcommons_sync.message = String((syncRes && syncRes.message) || '');
    applied.trustcommons_sync.status = trustCommonsSyncBridge.getStatus();
  }
  const historyChanged = (
    prev.history_enabled !== next.history_enabled
    || Number(prev.history_max_entries || 0) !== Number(next.history_max_entries || 0)
  );
  if (historyChanged) {
    applied.history.changed = true;
    const state = readPrivateHistoryState();
    const entries = Array.isArray(state.entries) ? state.entries.map((item) => sanitizeHistoryEntry(item)).filter(Boolean) : [];
    const retained = enforceHistoryRetention(entries);
    writePrivateHistoryState({ version: 1, entries: retained });
  }

  const telegramChanged = (
    prev.telegram_enabled !== next.telegram_enabled
    || Number(prev.telegram_poll_interval_sec || 0) !== Number(next.telegram_poll_interval_sec || 0)
    || String(prev.telegram_bot_token_ref || '') !== String(next.telegram_bot_token_ref || '')
  );
  if (telegramChanged || (next.telegram_enabled && (!telegramService || !telegramService.status().running))) {
    applied.telegram.changed = true;
    const telegramRes = await ensureTelegramRuntime(next);
    applied.telegram.ok = !!(telegramRes && telegramRes.ok);
    applied.telegram.status = telegramRes || {};
    applied.telegram.message = String((telegramRes && telegramRes.message) || '');
  }

  if (!orchestratorScheduler || !orchestratorScheduler.status().running) {
    applied.orchestrator_scheduler.changed = true;
    const schedulerStatus = ensureOrchestratorSchedulerRuntime();
    applied.orchestrator_scheduler.ok = !!(schedulerStatus && schedulerStatus.ok);
    applied.orchestrator_scheduler.status = schedulerStatus || {};
  }

  return applied;
}

function confirmTypedResetPhrase(phrase = 'RESET') {
  const expected = String(phrase || 'RESET').trim().toUpperCase();
  return expected;
}

async function connectTrustCommonsAndHyperweb(options = {}) {
  const launchApp = options && Object.prototype.hasOwnProperty.call(options, 'launchApp')
    ? !!options.launchApp
    : true;
  const bootstrap = ensureTrustCommonsBootstrap();
  if (!bootstrap || !bootstrap.ok) {
    return {
      ok: false,
      message: (bootstrap && bootstrap.message) || 'Unable to bootstrap Trust Commons identity.',
      status: getTrustCommonsStatus(),
    };
  }

  const syncInit = await ensureTrustCommonsSyncBridge();
  if (!syncInit || !syncInit.ok) {
    trustCommonsRuntime.lastError = String((syncInit && syncInit.message) || 'Local sync bridge unavailable.');
  }

  let launch = {
    ok: true,
    opened: false,
    method: 'skipped',
    download_opened: false,
  };
  if (launchApp) {
    launch = await launchTrustCommonsApp();
    trustCommonsRuntime.launched = !!(launch && launch.opened);
    trustCommonsRuntime.launchMethod = String((launch && launch.method) || '');
    trustCommonsRuntime.downloadOpened = !!(launch && launch.download_opened);
    if (!launch || !launch.ok) {
      trustCommonsRuntime.lastError = String((launch && launch.message) || 'Unable to launch Trust Commons.');
    }
  }

  const settings = readSettings();
  hyperwebManager.setRelayUrl(settings.hyperweb_relay_url || DEFAULT_HYPERWEB_RELAY_URL);
  hyperwebManager.setEnabled(!!settings.hyperweb_enabled);
  hyperwebManager.setIdentity(bootstrap.identity || null);
  const connectRes = settings.hyperweb_enabled
    ? await hyperwebManager.connect(bootstrap.identity || null)
    : { ok: true, status: hyperwebManager.getStatus(), message: 'Hyperweb disabled in settings.' };
  trustCommonsRuntime.connected = !!(connectRes && connectRes.status && connectRes.status.connected);
  if (connectRes && connectRes.ok && (!syncInit || syncInit.ok) && (!launchApp || (launch && launch.ok))) {
    trustCommonsRuntime.lastError = '';
  } else if (connectRes && !connectRes.ok) {
    trustCommonsRuntime.lastError = String((connectRes && connectRes.message) || 'Hyperweb connect failed.');
  }

  let syncResult = { ok: true, skipped: true, reason: 'peer_not_configured' };
  const activeSyncStatus = trustCommonsSyncBridge.getStatus();
  if (activeSyncStatus.running && activeSyncStatus.peer_url) {
    try {
      syncResult = await trustCommonsSyncBridge.syncOnce();
      if (!syncResult || !syncResult.ok) {
        trustCommonsRuntime.lastError = String((syncResult && syncResult.message) || 'Trust Commons local sync failed.');
      }
    } catch (err) {
      syncResult = { ok: false, message: String((err && err.message) || 'Trust Commons local sync failed.') };
      trustCommonsRuntime.lastError = syncResult.message;
    }
  }

  const messageParts = [];
  if (launchApp) {
    if (launch && launch.download_opened) {
      messageParts.push('Trust Commons app not found. Opened download page.');
    } else if (launch && launch.opened) {
      messageParts.push('Trust Commons app opened.');
    }
  }
  if (connectRes && connectRes.message) {
    messageParts.push(String(connectRes.message));
  }
  if (syncResult && syncResult.ok && !syncResult.skipped) {
    messageParts.push(`Local sync: pushed ${Number(syncResult.pushed || 0)} and pulled ${Number(syncResult.pulled || 0)} references.`);
  } else if (syncResult && !syncResult.ok) {
    messageParts.push(`Local sync issue: ${syncResult.message || 'sync failed.'}`);
  }

  const ok = launchApp
    ? !!(launch && launch.ok)
    : !!(connectRes && connectRes.ok);
  const degraded = !!(
    (connectRes && (!connectRes.ok || connectRes.degraded))
    || (syncInit && !syncInit.ok)
    || (syncResult && !syncResult.ok)
  );

  return {
    ok,
    degraded,
    message: messageParts.filter(Boolean).join(' ').trim(),
    status: getTrustCommonsStatus(),
    hyperweb: connectRes && connectRes.status ? connectRes.status : hyperwebManager.getStatus(),
    launch,
    sync_init: syncInit,
    sync: trustCommonsSyncBridge.getStatus(),
    sync_result: syncResult,
  };
}

function createWebTab(seed = {}) {
  const hasSeedUrl = !!String(seed.url || '').trim();
  const fallbackUrl = getDefaultSearchHomeUrl();
  const url = normalizeUrl(hasSeedUrl ? seed.url : fallbackUrl);
  const title = String(seed.title || (hasSeedUrl ? url : getDefaultSearchHomeTitle()) || 'Untitled').trim();
  return {
    id: makeId('tab'),
    tab_kind: 'web',
    url,
    title,
    favicon: null,
    pinned: false,
    excerpt: '',
    snapshot_at: nowTs(),
    last_active: nowTs(),
  };
}

function insertWebTabAdjacent(ref, webTab, insertAfterTabId = '') {
  if (!ref || typeof ref !== 'object' || !webTab || typeof webTab !== 'object') return;
  ref.tabs = Array.isArray(ref.tabs) ? ref.tabs : [];
  const tabs = ref.tabs;
  const isWeb = (tab) => String((tab && tab.tab_kind) || 'web').trim().toLowerCase() === 'web';
  const requestedAnchorId = String(insertAfterTabId || '').trim();
  const activeAnchorId = String((ref && ref.active_tab_id) || '').trim();
  const anchorCandidates = [requestedAnchorId, activeAnchorId].filter(Boolean);

  let anchorIndex = -1;
  for (const anchorId of anchorCandidates) {
    anchorIndex = tabs.findIndex((tab) => String((tab && tab.id) || '') === anchorId && isWeb(tab));
    if (anchorIndex >= 0) break;
  }

  if (anchorIndex >= 0) {
    tabs.splice(anchorIndex + 1, 0, webTab);
    return;
  }

  let lastWebIndex = -1;
  tabs.forEach((tab, index) => {
    if (isWeb(tab)) lastWebIndex = index;
  });
  if (lastWebIndex >= 0) {
    tabs.splice(lastWebIndex + 1, 0, webTab);
    return;
  }

  tabs.push(webTab);
}

function createFilesTab(seed = {}) {
  return {
    id: String(seed.id || makeId('tab')),
    tab_kind: 'files',
    url: 'about:blank',
    title: String(seed.title || 'Files').slice(0, 120),
    files_view_state: (seed.files_view_state && typeof seed.files_view_state === 'object')
      ? seed.files_view_state
      : {},
    snapshot_at: nowTs(),
    last_active: nowTs(),
    updated_at: Number(seed.updated_at || nowTs()),
  };
}

function createSkillsTab(seed = {}) {
  return {
    id: String(seed.id || makeId('tab')),
    tab_kind: 'skills',
    url: 'about:blank',
    title: String(seed.title || 'Skills').slice(0, 120),
    snapshot_at: nowTs(),
    last_active: nowTs(),
    updated_at: Number(seed.updated_at || nowTs()),
  };
}

function ensureSingleFilesTab(ref) {
  if (!ref || typeof ref !== 'object') {
    return { tab: null, created: false, deduped: false };
  }

  ref.tabs = Array.isArray(ref.tabs) ? ref.tabs : [];
  const firstFilesTab = ref.tabs.find((tab) => String((tab && tab.tab_kind) || '').trim().toLowerCase() === 'files') || null;
  if (!firstFilesTab) {
    const created = createFilesTab({});
    ref.tabs.push(created);
    return { tab: created, created: true, deduped: false };
  }

  let kept = false;
  let deduped = false;
  ref.tabs = ref.tabs.filter((tab) => {
    const kind = String((tab && tab.tab_kind) || '').trim().toLowerCase();
    if (kind !== 'files') return true;
    if (!kept && String((tab && tab.id) || '') === String(firstFilesTab.id || '')) {
      kept = true;
      return true;
    }
    deduped = true;
    return false;
  });
  firstFilesTab.title = String(firstFilesTab.title || 'Files').slice(0, 120);
  firstFilesTab.files_view_state = (firstFilesTab.files_view_state && typeof firstFilesTab.files_view_state === 'object')
    ? firstFilesTab.files_view_state
    : {};

  return { tab: firstFilesTab, created: false, deduped };
}

function ensureSingleSkillsTab(ref) {
  if (!ref || typeof ref !== 'object') {
    return { tab: null, created: false, deduped: false };
  }

  ref.tabs = Array.isArray(ref.tabs) ? ref.tabs : [];
  const firstSkillsTab = ref.tabs.find((tab) => String((tab && tab.tab_kind) || '').trim().toLowerCase() === 'skills') || null;
  if (!firstSkillsTab) {
    const created = createSkillsTab({});
    ref.tabs.push(created);
    return { tab: created, created: true, deduped: false };
  }

  let kept = false;
  let deduped = false;
  ref.tabs = ref.tabs.filter((tab) => {
    const kind = String((tab && tab.tab_kind) || '').trim().toLowerCase();
    if (kind !== 'skills') return true;
    if (!kept && String((tab && tab.id) || '') === String(firstSkillsTab.id || '')) {
      kept = true;
      return true;
    }
    deduped = true;
    return false;
  });
  firstSkillsTab.title = String(firstSkillsTab.title || 'Skills').slice(0, 120);
  return { tab: firstSkillsTab, created: false, deduped };
}

function normalizeArtifactType(value) {
  const type = String(value || '').trim().toLowerCase();
  return type === 'html' ? 'html' : 'markdown';
}

function buildLegacyVizArtifact(refId, tab) {
  const safeRefId = String(refId || '').trim();
  const safeTab = (tab && typeof tab === 'object') ? tab : {};
  const tabId = String((safeTab && safeTab.id) || '').trim();
  const title = String((safeTab && safeTab.title) || 'Visualization').trim() || 'Visualization';
  const renderer = String((safeTab && safeTab.renderer) || 'canvas').trim().toLowerCase();
  const source = String((safeTab && safeTab.viz_source) || '').trim().toLowerCase();
  const request = (safeTab.viz_request && typeof safeTab.viz_request === 'object') ? safeTab.viz_request : {};
  const pythonCode = String((request && request.python_code) || '').trim();
  const pngPath = String((safeTab && safeTab.viz_png_path) || '').trim();
  const pngBase64 = String((safeTab && safeTab.viz_png_base64) || '').trim();
  const runtimeState = String((safeTab && safeTab.viz_runtime_state) || '').trim();
  const runtimeMessage = String((safeTab && safeTab.viz_runtime_message) || '').trim();
  const executionId = String((safeTab && safeTab.viz_runtime_last_execution_id) || '').trim();

  const lines = [
    '# Legacy Visualization Migration',
    '',
    'This artifact was automatically converted from a deprecated visualization tab.',
    '',
    `- Reference ID: ${safeRefId || '(unknown)'}`,
    `- Original tab ID: ${tabId || '(unknown)'}`,
    `- Original title: ${title}`,
    `- Renderer: ${renderer || 'canvas'}`,
    `- Source: ${source || 'procedural'}`,
    `- Runtime state: ${runtimeState || 'unknown'}`,
    `- Runtime message: ${runtimeMessage || 'n/a'}`,
    `- Execution ID: ${executionId || 'n/a'}`,
  ];

  if (pngPath && fs.existsSync(pngPath)) {
    lines.push('', '## Last Snapshot', '', `![Legacy visualization snapshot](${pathToFileURL(pngPath).toString()})`);
  } else if (pngBase64) {
    lines.push('', '## Last Snapshot', '', `![Legacy visualization snapshot](data:image/png;base64,${pngBase64})`);
  }

  if (pythonCode) {
    lines.push('', '## Stored Python Code', '', '```python', pythonCode, '```');
  }

  return createArtifact({
    type: 'markdown',
    title: `${title} (Migrated)`,
    content: lines.join('\n'),
    created_at: nowTs(),
    updated_at: nowTs(),
  });
}

function migrateLegacyVizTabsInReference(ref) {
  if (!ref || typeof ref !== 'object') return false;
  ref.tabs = Array.isArray(ref.tabs) ? ref.tabs : [];
  const vizTabs = ref.tabs.filter((tab) => String((tab && tab.tab_kind) || '').trim().toLowerCase() === 'viz');
  if (vizTabs.length === 0) return false;

  ref.artifacts = Array.isArray(ref.artifacts) ? ref.artifacts : [];
  vizTabs.forEach((tab) => {
    ref.artifacts.push(buildLegacyVizArtifact(String(ref.id || ''), tab));
  });

  const removedIds = new Set(vizTabs.map((tab) => String((tab && tab.id) || '').trim()).filter(Boolean));
  ref.tabs = ref.tabs.filter((tab) => !removedIds.has(String((tab && tab.id) || '').trim()));

  const activeTabId = String(ref.active_tab_id || '').trim();
  if (activeTabId && removedIds.has(activeTabId)) {
    ref.active_tab_id = ref.tabs[0] ? String((ref.tabs[0] && ref.tabs[0].id) || '').trim() : null;
  }
  return true;
}

function createArtifact(seed = {}) {
  return {
    id: String(seed.id || makeId('artifact')),
    type: normalizeArtifactType(seed.type),
    title: String(seed.title || 'Research Draft').slice(0, 180),
    content: String(seed.content || ''),
    created_at: Number(seed.created_at || nowTs()),
    updated_at: Number(seed.updated_at || nowTs()),
  };
}

function normalizeReferenceAgentMeta(input = {}) {
  const src = (input && typeof input === 'object') ? input : {};
  const createdBy = String(src.created_by || '').trim();
  if (!createdBy) return null;
  const statusRaw = String(src.status || 'active').trim().toLowerCase();
  const status = ['pending', 'active', 'failed'].includes(statusRaw) ? statusRaw : 'active';
  return {
    created_by: createdBy.slice(0, 120),
    path: String(src.path || '').trim().slice(0, 40),
    source: String(src.source || '').trim().slice(0, 160),
    run_id: String(src.run_id || '').trim().slice(0, 180),
    job_id: String(src.job_id || '').trim().slice(0, 180),
    idempotency_key: String(src.idempotency_key || '').trim().slice(0, 180),
    status,
    created_at: Number(src.created_at || nowTs()),
    updated_at: Number(src.updated_at || nowTs()),
  };
}

function createReferenceBase(seed = {}) {
  const tab = createWebTab(seed.current_tab || {});
  const meta = normalizeReferenceAgentMeta(seed.agent_meta);
  const ref = {
    id: makeId('sr'),
    title: String(seed.title || deriveReferenceTitleFromTab(tab) || 'Untitled Reference').slice(0, 120),
    title_user_edited: !!seed.title_user_edited,
    intent: String(seed.intent || ''),
    tags: Array.isArray(seed.tags) ? seed.tags : [],
    parent_id: seed.parent_id ? String(seed.parent_id) : null,
    children: [],
    lineage: Array.isArray(seed.lineage) ? seed.lineage.map((id) => String(id || '')).filter(Boolean) : [],
    relation_type: String(seed.relation_type || (seed.parent_id ? 'child' : 'root')),
    color_tag: sanitizeReferenceColorTag(seed.color_tag),
    visibility: String(seed.visibility || 'private') === 'public' ? 'public' : 'private',
    is_public_candidate: !!seed.is_public_candidate,
    source_type: String(seed.source_type || 'local'),
    source_peer_id: String(seed.source_peer_id || ''),
    source_peer_name: String(seed.source_peer_name || ''),
    source_candidate_key: String(seed.source_candidate_key || ''),
    source_metadata: (seed.source_metadata && typeof seed.source_metadata === 'object') ? seed.source_metadata : {},
    is_temp_candidate: !!seed.is_temp_candidate,
    temp_imported_at: Number(seed.temp_imported_at || 0),
    hyperweb_payload_version: Number(seed.hyperweb_payload_version || 1),
    tabs: [tab],
    active_tab_id: tab.id,
    artifacts: [createArtifact({
      title: 'Research Draft',
      type: 'markdown',
      content: '# Notes\n\nStart collecting findings here.',
    })],
    context_files: [],
    folder_mounts: [],
    youtube_transcripts: {},
    reference_graph: { nodes: [], edges: [] },
    agent_weights: {},
    decision_trace: [],
    program: String(seed.program || ''),
    skills: Array.isArray(seed.skills)
      ? seed.skills.map((item) => sanitizeSkillDescriptor(item)).filter(Boolean)
      : [],
    highlights: [],
    chat_thread: { messages: [], last_message_at: null },
    memory: memoryDefaultState(),
    pinned_root: false,
    agent_meta: meta,
    created_at: nowTs(),
    updated_at: nowTs(),
    last_used_at: nowTs(),
  };
  return ref;
}

function createForkReference(parent, seed = {}) {
  const tabs = Array.isArray(parent.tabs)
    ? parent.tabs.map((tab) => ({ ...tab, id: makeId('tab'), snapshot_at: nowTs(), last_active: nowTs() }))
    : [createWebTab({})];
  const artifacts = Array.isArray(parent.artifacts)
    ? parent.artifacts.map((artifact) => ({ ...createArtifact(artifact), id: makeId('artifact'), created_at: nowTs(), updated_at: nowTs() }))
    : [createArtifact({})];

  const meta = normalizeReferenceAgentMeta(seed.agent_meta);
  const inheritsColor = !Object.prototype.hasOwnProperty.call(seed, 'color_tag');
  const resolvedColorTag = inheritsColor
    ? sanitizeReferenceColorTag(parent && parent.color_tag)
    : sanitizeReferenceColorTag(seed.color_tag);
  return {
    id: makeId('sr'),
    title: String(seed.title || `${parent.title || 'Reference'} Fork`).slice(0, 120),
    title_user_edited: !!seed.title_user_edited,
    intent: String(seed.intent || parent.intent || ''),
    tags: Array.isArray(parent.tags) ? [...parent.tags] : [],
    parent_id: parent.id,
    children: [],
    lineage: [parent.id].concat(Array.isArray(parent.lineage) ? parent.lineage : []).slice(0, 100),
    relation_type: 'fork',
    color_tag: resolvedColorTag,
    visibility: 'private',
    is_public_candidate: false,
    source_type: 'local',
    source_peer_id: '',
    source_peer_name: '',
    source_candidate_key: '',
    source_metadata: (seed.source_metadata && typeof seed.source_metadata === 'object') ? seed.source_metadata : {},
    is_temp_candidate: false,
    temp_imported_at: 0,
    hyperweb_payload_version: 1,
    tabs,
    active_tab_id: tabs[0] ? tabs[0].id : null,
    artifacts,
    context_files: Array.isArray(parent.context_files) ? [...parent.context_files] : [],
    folder_mounts: Array.isArray(parent.folder_mounts) ? [...parent.folder_mounts] : [],
    youtube_transcripts: (parent.youtube_transcripts && typeof parent.youtube_transcripts === 'object') ? { ...parent.youtube_transcripts } : {},
    reference_graph: (parent.reference_graph && typeof parent.reference_graph === 'object') ? { ...parent.reference_graph } : { nodes: [], edges: [] },
    agent_weights: (parent.agent_weights && typeof parent.agent_weights === 'object') ? { ...parent.agent_weights } : {},
    decision_trace: Array.isArray(parent.decision_trace) ? [...parent.decision_trace] : [],
    program: String(seed.program || parent.program || ''),
    skills: Array.isArray(seed.skills)
      ? seed.skills.map((item) => sanitizeSkillDescriptor(item)).filter(Boolean)
      : (
        Array.isArray(parent.skills)
          ? parent.skills.map((item) => sanitizeSkillDescriptor(item)).filter(Boolean)
          : []
      ),
    highlights: Array.isArray(parent.highlights) ? [...parent.highlights] : [],
    chat_thread: { messages: [], last_message_at: null },
    memory: memoryDefaultState(),
    pinned_root: false,
    agent_meta: meta,
    created_at: nowTs(),
    updated_at: nowTs(),
    last_used_at: nowTs(),
  };
}

function sanitizeHighlightEntry(item = {}) {
  const source = String(item.source || 'web').trim().toLowerCase() === 'artifact' ? 'artifact' : 'web';
  const text = String(item.text || '').trim().slice(0, 4000);
  if (!text) return null;

  if (source === 'artifact') {
    const artifactId = String(item.artifact_id || '').trim();
    const artifactStart = Number(item.artifact_start);
    const artifactEnd = Number(item.artifact_end);
    if (!artifactId || !Number.isFinite(artifactStart) || !Number.isFinite(artifactEnd) || artifactEnd <= artifactStart) {
      return null;
    }
    return {
      id: String(item.id || makeId('hl')).trim() || makeId('hl'),
      source: 'artifact',
      artifact_id: artifactId,
      artifact_start: Math.max(0, Math.round(artifactStart)),
      artifact_end: Math.max(0, Math.round(artifactEnd)),
      text,
      created_at: Number((item && item.created_at) || nowTs()),
      updated_at: Number((item && item.updated_at) || nowTs()),
    };
  }

  const rawUrl = String(item.url || '').trim();
  const urlNorm = normalizeUrlForMatch(item.url_norm || rawUrl);
  if (!urlNorm) return null;
  const webStart = Number(item.web_start);
  const webEnd = Number(item.web_end);
  return {
    id: String(item.id || makeId('hl')).trim() || makeId('hl'),
    source: 'web',
    url: rawUrl.slice(0, 4000),
    url_norm: urlNorm,
    text,
    context_before: String(item.context_before || '').slice(0, 600),
    context_after: String(item.context_after || '').slice(0, 600),
    web_start: Number.isFinite(webStart) ? Math.max(0, Math.round(webStart)) : null,
    web_end: Number.isFinite(webEnd) ? Math.max(0, Math.round(webEnd)) : null,
    created_at: Number((item && item.created_at) || nowTs()),
    updated_at: Number((item && item.updated_at) || nowTs()),
  };
}

function highlightSignatureWeb(item = {}) {
  const urlNorm = normalizeUrlForMatch(item.url_norm || item.url);
  const text = String(item.text || '');
  const before = String(item.context_before || '');
  const after = String(item.context_after || '');
  const start = Number.isFinite(Number(item.web_start)) ? Math.round(Number(item.web_start)) : '';
  const end = Number.isFinite(Number(item.web_end)) ? Math.round(Number(item.web_end)) : '';
  return `${urlNorm}|${text}|${before}|${after}|${start}|${end}`;
}

function highlightSignatureArtifact(item = {}) {
  const artifactId = String(item.artifact_id || '').trim();
  const text = String(item.text || '');
  const start = Number.isFinite(Number(item.artifact_start)) ? Math.round(Number(item.artifact_start)) : '';
  const end = Number.isFinite(Number(item.artifact_end)) ? Math.round(Number(item.artifact_end)) : '';
  return `${artifactId}|${text}|${start}|${end}`;
}

function clearHighlightsByTarget(ref, target = {}) {
  if (!ref || typeof ref !== 'object') return { removed_count: 0, target: null };
  const targetType = String((target && target.type) || '').trim().toLowerCase();
  const normalizedHighlights = Array.isArray(ref.highlights)
    ? ref.highlights.map((item) => sanitizeHighlightEntry(item)).filter(Boolean)
    : [];
  let nextHighlights = normalizedHighlights;
  let normalizedTarget = null;

  if (targetType === 'artifact') {
    const artifactId = String((target && target.artifact_id) || '').trim();
    if (!artifactId) return { removed_count: 0, target: null };
    normalizedTarget = { type: 'artifact', artifact_id: artifactId };
    nextHighlights = normalizedHighlights.filter((item) => !(
      item
      && item.source === 'artifact'
      && String(item.artifact_id || '').trim() === artifactId
    ));
  } else if (targetType === 'web') {
    const urlNorm = normalizeUrlForMatch((target && (target.url_norm || target.url)) || '');
    if (!urlNorm) return { removed_count: 0, target: null };
    normalizedTarget = { type: 'web', url_norm: urlNorm };
    nextHighlights = normalizedHighlights.filter((item) => !(
      item
      && item.source === 'web'
      && normalizeUrlForMatch(item.url_norm || item.url) === urlNorm
    ));
  } else {
    return { removed_count: 0, target: null };
  }

  const removedCount = Math.max(0, normalizedHighlights.length - nextHighlights.length);
  ref.highlights = nextHighlights.slice(-MAX_HIGHLIGHTS);
  if (removedCount > 0) ref.updated_at = nowTs();
  return {
    removed_count: removedCount,
    target: normalizedTarget,
  };
}

function addWebHighlight(ref, payload = {}) {
  if (!ref || typeof ref !== 'object') {
    return { ok: false, added: false, message: 'Reference not found.', highlight: null };
  }
  const highlight = sanitizeHighlightEntry({
    ...(payload && typeof payload === 'object' ? payload : {}),
    source: 'web',
    created_at: nowTs(),
    updated_at: nowTs(),
  });
  if (!highlight || highlight.source !== 'web') {
    return { ok: false, added: false, message: 'Invalid web highlight payload.', highlight: null };
  }
  const normalizedHighlights = Array.isArray(ref.highlights)
    ? ref.highlights.map((item) => sanitizeHighlightEntry(item)).filter(Boolean)
    : [];
  const signature = highlightSignatureWeb(highlight);
  const existing = normalizedHighlights.find((item) => highlightSignatureWeb(item) === signature) || null;
  if (existing) {
    ref.highlights = normalizedHighlights.slice(-MAX_HIGHLIGHTS);
    return { ok: true, added: false, message: 'Web highlight already exists.', highlight: existing };
  }
  normalizedHighlights.push(highlight);
  ref.highlights = normalizedHighlights.slice(-MAX_HIGHLIGHTS);
  ref.updated_at = nowTs();
  return { ok: true, added: true, message: 'Web highlight added.', highlight };
}

function addArtifactHighlight(ref, payload = {}) {
  if (!ref || typeof ref !== 'object') {
    return { ok: false, added: false, message: 'Reference not found.', highlight: null };
  }
  const highlight = sanitizeHighlightEntry({
    ...(payload && typeof payload === 'object' ? payload : {}),
    source: 'artifact',
    created_at: nowTs(),
    updated_at: nowTs(),
  });
  if (!highlight || highlight.source !== 'artifact') {
    return { ok: false, added: false, message: 'Invalid artifact highlight payload.', highlight: null };
  }
  const normalizedHighlights = Array.isArray(ref.highlights)
    ? ref.highlights.map((item) => sanitizeHighlightEntry(item)).filter(Boolean)
    : [];
  const signature = highlightSignatureArtifact(highlight);
  const existing = normalizedHighlights.find((item) => highlightSignatureArtifact(item) === signature) || null;
  if (existing) {
    ref.highlights = normalizedHighlights.slice(-MAX_HIGHLIGHTS);
    return { ok: true, added: false, message: 'Artifact highlight already exists.', highlight: existing };
  }
  normalizedHighlights.push(highlight);
  ref.highlights = normalizedHighlights.slice(-MAX_HIGHLIGHTS);
  ref.updated_at = nowTs();
  return { ok: true, added: true, message: 'Artifact highlight added.', highlight };
}

function toggleArtifactHighlight(ref, payload = {}) {
  if (!ref || typeof ref !== 'object') {
    return { ok: false, added: false, removed: false, message: 'Reference not found.', highlight: null };
  }
  const highlight = sanitizeHighlightEntry({
    ...(payload && typeof payload === 'object' ? payload : {}),
    source: 'artifact',
    created_at: nowTs(),
    updated_at: nowTs(),
  });
  if (!highlight || highlight.source !== 'artifact') {
    return { ok: false, added: false, removed: false, message: 'Invalid artifact highlight payload.', highlight: null };
  }
  const normalizedHighlights = Array.isArray(ref.highlights)
    ? ref.highlights.map((item) => sanitizeHighlightEntry(item)).filter(Boolean)
    : [];
  const signature = highlightSignatureArtifact(highlight);
  const existingIdx = normalizedHighlights.findIndex((item) => highlightSignatureArtifact(item) === signature);
  if (existingIdx >= 0) {
    const removed = normalizedHighlights[existingIdx] || null;
    normalizedHighlights.splice(existingIdx, 1);
    ref.highlights = normalizedHighlights.slice(-MAX_HIGHLIGHTS);
    ref.updated_at = nowTs();
    return {
      ok: true,
      added: false,
      removed: true,
      message: 'Artifact highlight removed.',
      highlight: removed,
    };
  }
  normalizedHighlights.push(highlight);
  ref.highlights = normalizedHighlights.slice(-MAX_HIGHLIGHTS);
  ref.updated_at = nowTs();
  return {
    ok: true,
    added: true,
    removed: false,
    message: 'Artifact highlight added.',
    highlight,
  };
}

function getMarkerReference() {
  const srId = String((markerContext && markerContext.srId) || '').trim();
  if (!srId) return null;
  const refs = getReferences();
  const idx = findReferenceIndex(refs, srId);
  if (idx < 0) return null;
  return { refs, idx, ref: refs[idx] };
}

function getWebHighlightsForUrl(ref, url) {
  const urlNorm = normalizeUrlForMatch(url);
  if (!ref || !urlNorm) return [];
  const highlights = Array.isArray(ref.highlights) ? ref.highlights : [];
  return highlights
    .map((item) => sanitizeHighlightEntry(item))
    .filter((item) => item && item.source === 'web' && normalizeUrlForMatch(item.url_norm || item.url) === urlNorm)
    .map((item) => ({
      id: String(item.id || ''),
      source: 'web',
      url: String(item.url || ''),
      url_norm: String(item.url_norm || ''),
      text: String(item.text || ''),
      context_before: String(item.context_before || ''),
      context_after: String(item.context_after || ''),
      web_start: Number.isFinite(Number(item.web_start)) ? Number(item.web_start) : null,
      web_end: Number.isFinite(Number(item.web_end)) ? Number(item.web_end) : null,
    }));
}

function extractYouTubeVideoIdFromUrl(rawUrl) {
  const target = String(rawUrl || '').trim();
  if (!target) return '';
  try {
    const parsed = new URL(target);
    const host = String(parsed.hostname || '').toLowerCase();
    const segments = String(parsed.pathname || '/').split('/').filter(Boolean);
    let candidate = '';
    if (host === 'youtu.be') {
      candidate = segments[0] || '';
    } else if (host.includes('youtube.com') || host.includes('youtube-nocookie.com')) {
      if (segments.length >= 2 && ['shorts', 'embed', 'live', 'v'].includes(segments[0])) {
        candidate = segments[1] || '';
      } else {
        candidate = parsed.searchParams.get('v') || '';
      }
    }
    candidate = String(candidate || '').trim();
    return /^[A-Za-z0-9_-]{6,20}$/.test(candidate) ? candidate : '';
  } catch (_) {
    return '';
  }
}

function summarizeYouTubeTranscriptText(text, maxChars = YOUTUBE_TRANSCRIPT_SUMMARY_MAX_CHARS) {
  const rawText = String(text || '').trim();
  if (!rawText) return '';
  const normalized = rawText.replace(/\s+/g, ' ').trim();
  if (normalized.length <= maxChars) return normalized;
  const lines = rawText
    .split(/\r?\n/)
    .map((line) => line.replace(/\s+/g, ' ').trim())
    .filter(Boolean)
    .filter((line, idx, list) => idx === 0 || line !== list[idx - 1]);
  if (!lines.length) return `${normalized.slice(0, maxChars).trim()}...`;
  const picked = [
    lines[0],
    lines[Math.floor(lines.length / 4)] || '',
    lines[Math.floor(lines.length / 2)] || '',
    lines[Math.floor((lines.length * 3) / 4)] || '',
    lines[lines.length - 1] || '',
  ].filter(Boolean);
  const summary = picked.join(' | ');
  if (summary.length <= maxChars) return summary;
  return `${summary.slice(0, maxChars).trim()}...`;
}

function sanitizeYouTubeTranscriptRecord(record, videoIdHint = '') {
  if (!record || typeof record !== 'object') return null;
  const videoId = String(record.video_id || videoIdHint || '').trim();
  if (!/^[A-Za-z0-9_-]{6,20}$/.test(videoId)) return null;
  const transcriptText = String(record.transcript_text || '').slice(0, YOUTUBE_TRANSCRIPT_MAX_CHARS).trim();
  const hasTranscript = !!transcriptText;
  const statusInput = (record.status && typeof record.status === 'object') ? record.status : {};
  const stateRaw = String(statusInput.state || (hasTranscript ? 'ready' : 'error')).trim().toLowerCase();
  const state = stateRaw === 'ready' ? 'ready' : 'error';
  const retryAfter = Number(statusInput.retry_after);
  const transcriptCharCount = Number(record.transcript_char_count);
  const normalizedTranscriptCharCount = Number.isFinite(transcriptCharCount)
    ? Math.max(0, Math.round(transcriptCharCount))
    : transcriptText.length;

  return {
    video_id: videoId,
    url: String(record.url || '').slice(0, 4000),
    title: String(record.title || '').slice(0, 300),
    language: String(record.language || '').slice(0, 24),
    transcript_text: transcriptText,
    summary: String(record.summary || summarizeYouTubeTranscriptText(transcriptText)).slice(0, YOUTUBE_TRANSCRIPT_SUMMARY_MAX_CHARS + 3),
    transcript_char_count: normalizedTranscriptCharCount,
    transcript_truncated: !!record.transcript_truncated || normalizedTranscriptCharCount > transcriptText.length,
    source: String(record.source || 'youtube_captions').slice(0, 80),
    updated_at: Number.isFinite(Number(record.updated_at)) ? Number(record.updated_at) : nowTs(),
    status: {
      state,
      error_code: state === 'error' ? String(statusInput.error_code || 'transcript_error').slice(0, 80) : '',
      message: state === 'error' ? String(statusInput.message || 'Transcript unavailable.').slice(0, 600) : '',
      retry_after: (state === 'error' && Number.isFinite(retryAfter)) ? Math.max(0, Math.round(retryAfter)) : null,
    },
  };
}

function sanitizeYouTubeTranscriptMap(input) {
  if (!input || typeof input !== 'object') return {};
  const normalized = Object.entries(input)
    .map(([videoId, record]) => sanitizeYouTubeTranscriptRecord(record, videoId))
    .filter(Boolean)
    .sort((a, b) => Number((b && b.updated_at) || 0) - Number((a && a.updated_at) || 0))
    .slice(0, YOUTUBE_TRANSCRIPTS_MAX_ITEMS);
  const out = {};
  normalized.forEach((item) => {
    out[item.video_id] = item;
  });
  return out;
}

function resolveYouTubeTranscriptStoredPath(srId, videoId) {
  const safeSrId = String(srId || '').replace(/[^A-Za-z0-9._-]/g, '_').slice(0, 180) || 'sr_unknown';
  const safeVideoId = String(videoId || '').replace(/[^A-Za-z0-9_-]/g, '').slice(0, 64);
  const outDir = path.join(app.getPath('userData'), 'semantic_references', safeSrId, 'context_files');
  return {
    outDir,
    outPath: path.join(outDir, `yt_${safeVideoId}.txt`),
  };
}

function writeYouTubeTranscriptToDisk(srId, videoId, transcriptText, previousPath = '') {
  const text = String(transcriptText || '').trim();
  if (!text) return null;
  const { outDir, outPath } = resolveYouTubeTranscriptStoredPath(srId, videoId);
  try {
    fs.mkdirSync(outDir, { recursive: true });
    fs.writeFileSync(outPath, text, 'utf8');
    const previous = String(previousPath || '').trim();
    if (previous && previous !== outPath && fs.existsSync(previous)) {
      try { fs.unlinkSync(previous); } catch (_) { }
    }
    return outPath;
  } catch (_) {
    return null;
  }
}

function buildSyntheticYouTubeContextFile(srId, videoId, record, fullTranscriptText, existing = null) {
  const transcriptText = String(fullTranscriptText || '').trim();
  if (!transcriptText) return null;
  const now = nowTs();
  const contentHash = crypto.createHash('sha256').update(transcriptText, 'utf8').digest('hex');
  const summary = String((record && record.summary) || summarizeYouTubeTranscriptText(transcriptText))
    .slice(0, YOUTUBE_TRANSCRIPT_SUMMARY_MAX_CHARS + 3);
  const storedPath = writeYouTubeTranscriptToDisk(srId, videoId, transcriptText, existing && existing.stored_path);
  const item = {
    id: existing && existing.id ? String(existing.id) : `ctx_yt_${videoId}`,
    source_type: 'youtube_transcript',
    video_id: String(videoId || ''),
    original_name: `yt_${videoId}.txt`,
    relative_path: `yt_${videoId}.txt`,
    stored_path: storedPath,
    mime_type: 'text/plain',
    size_bytes: Buffer.byteLength(transcriptText, 'utf8'),
    content_hash: contentHash,
    ingest_status: 'ready',
    summary,
    title: String((record && record.title) || '').slice(0, 300),
    url: String((record && record.url) || '').slice(0, 4000),
    language: String((record && record.language) || '').slice(0, 24),
    transcript_char_count: Number.isFinite(Number(record && record.transcript_char_count))
      ? Math.max(0, Math.round(Number(record.transcript_char_count)))
      : transcriptText.length,
    synthetic: true,
    created_at: Number.isFinite(Number(existing && existing.created_at)) ? Number(existing.created_at) : now,
    updated_at: now,
  };
  if (!storedPath) item.content = transcriptText;
  return item;
}

function pruneYouTubeTranscriptContextFiles(ref) {
  if (!ref || typeof ref !== 'object') return;
  ref.context_files = Array.isArray(ref.context_files) ? ref.context_files : [];
  const ytMap = (ref.youtube_transcripts && typeof ref.youtube_transcripts === 'object') ? ref.youtube_transcripts : {};
  const keepVideoIds = new Set(Object.keys(ytMap).filter((videoId) => /^[A-Za-z0-9_-]{6,20}$/.test(String(videoId || ''))));
  const seenVideoIds = new Set();
  const nextFiles = [];
  ref.context_files.forEach((file) => {
    if (!file || typeof file !== 'object') return;
    const sourceType = String(file.source_type || '').trim().toLowerCase();
    if (sourceType !== 'youtube_transcript') {
      nextFiles.push(file);
      return;
    }
    const videoId = String(file.video_id || '').trim();
    const keep = !!videoId && keepVideoIds.has(videoId) && !seenVideoIds.has(videoId);
    if (!keep) {
      const stalePath = String(file.stored_path || '').trim();
      if (stalePath && fs.existsSync(stalePath)) {
        try { fs.unlinkSync(stalePath); } catch (_) { }
      }
      return;
    }
    seenVideoIds.add(videoId);
    const safeFile = { ...file };
    if (safeFile.stored_path) delete safeFile.content;
    nextFiles.push(safeFile);
  });
  ref.context_files = nextFiles;
}

function parseYouTubeTrackList(xmlText) {
  const xml = String(xmlText || '');
  const tracks = [];
  const matches = xml.match(/<track\b[^>]*>/gi) || [];
  matches.forEach((rawTag) => {
    const attrs = parseXmlAttributes(rawTag);
    const lang = String(attrs.lang_code || '').trim();
    if (!lang) return;
    tracks.push({
      lang_code: lang,
      name: String(attrs.name || ''),
      kind: String(attrs.kind || ''),
      vss_id: String(attrs.vss_id || ''),
    });
  });
  return tracks;
}

function parseYouTubeTimedText(xmlText) {
  const xml = String(xmlText || '');
  const lines = [];
  const textRegex = /<text\b[^>]*>([\s\S]*?)<\/text>/gi;
  let match = textRegex.exec(xml);
  while (match) {
    const inner = String(match[1] || '');
    const stripped = decodeXmlEntities(inner.replace(/<[^>]+>/g, ' ')).replace(/\s+/g, ' ').trim();
    if (stripped) {
      if (lines.length === 0 || lines[lines.length - 1] !== stripped) {
        lines.push(stripped);
      }
    }
    match = textRegex.exec(xml);
  }
  return lines;
}

async function fetchYouTubeTranscriptViaTimedtext(url, videoIdInput) {
  const rawUrl = String(url || '').trim();
  const videoId = String(videoIdInput || extractYouTubeVideoIdFromUrl(rawUrl) || '').trim();
  if (!videoId) {
    return { ok: false, error_code: 'invalid_url', message: 'Could not parse a valid YouTube video id from url.' };
  }

  const listUrl = `https://www.youtube.com/api/timedtext?type=list&v=${encodeURIComponent(videoId)}`;
  const listRes = await fetchTextWithTimeout(listUrl, {}, 12000);
  if (!listRes.ok) {
    return { ok: false, video_id: videoId, error_code: `http_${listRes.status || 0}`, message: 'Unable to fetch YouTube caption tracks.' };
  }
  const tracks = parseYouTubeTrackList(listRes.text);
  if (!tracks.length) {
    return { ok: false, video_id: videoId, error_code: 'no_transcript', message: 'No captions were found for this video.' };
  }

  const preferred = tracks.find((track) => /^en(-|$)/i.test(track.lang_code) && String(track.kind || '').toLowerCase() !== 'asr')
    || tracks.find((track) => /^en(-|$)/i.test(track.lang_code))
    || tracks[0];

  const params = new URLSearchParams({
    v: videoId,
    lang: preferred.lang_code,
  });
  if (preferred.name) params.set('name', preferred.name);
  if (preferred.kind) params.set('kind', preferred.kind);
  const transcriptUrl = `https://www.youtube.com/api/timedtext?${params.toString()}`;
  const transcriptRes = await fetchTextWithTimeout(transcriptUrl, {}, 12000);
  if (!transcriptRes.ok) {
    return { ok: false, video_id: videoId, error_code: `http_${transcriptRes.status || 0}`, message: 'Unable to fetch YouTube transcript.' };
  }
  const lines = parseYouTubeTimedText(transcriptRes.text);
  const transcriptFullText = lines.join('\n').trim();
  if (!transcriptFullText) {
    return { ok: false, video_id: videoId, error_code: 'empty_transcript', message: 'Transcript returned no readable text.' };
  }
  const transcriptText = transcriptFullText.slice(0, YOUTUBE_TRANSCRIPT_MAX_CHARS).trim();
  return {
    ok: true,
    video_id: videoId,
    language: preferred.lang_code || '',
    transcript_text: transcriptText,
    transcript_full_text: transcriptFullText,
    transcript_char_count: transcriptFullText.length,
    transcript_truncated: transcriptFullText.length > transcriptText.length,
    summary: summarizeYouTubeTranscriptText(transcriptFullText),
    source: 'youtube_timedtext',
  };
}

function ensureReferences() {
  const existing = readReferencesRaw();
  if (existing.length > 0) return existing;
  const root = createReferenceBase({
    title: 'Subgrapher Root',
    intent: 'Initial workspace',
    relation_type: 'root',
    current_tab: { url: getDefaultSearchHomeUrl(), title: getDefaultSearchHomeTitle() },
  });
  writeReferencesRaw([root]);
  syncPublicFeedWithReferences([root]);
  return [root];
}

function getReferences() {
  const refs = ensureReferences();
  let changed = false;
  refs.forEach((ref) => {
    if (migrateLegacyVizTabsInReference(ref)) {
      changed = true;
    }
    const prevMemoryRaw = JSON.stringify((ref && ref.memory && typeof ref.memory === 'object') ? ref.memory : {});
    ensureReferenceMemory(ref);
    if (JSON.stringify(ref.memory) !== prevMemoryRaw) {
      changed = true;
    }
    if (typeof ref.title_user_edited !== 'boolean') {
      ref.title_user_edited = false;
      changed = true;
    }
    const nextColorTag = sanitizeReferenceColorTag(ref.color_tag);
    if (nextColorTag !== String(ref.color_tag || '')) {
      ref.color_tag = nextColorTag;
      changed = true;
    }
    if (typeof ref.program !== 'string') {
      ref.program = String(ref.program || '');
      changed = true;
    }
    const nextAgentMeta = normalizeReferenceAgentMeta(ref.agent_meta);
    const prevAgentMetaRaw = JSON.stringify((ref.agent_meta && typeof ref.agent_meta === 'object') ? ref.agent_meta : null);
    if (JSON.stringify(nextAgentMeta) !== prevAgentMetaRaw) {
      ref.agent_meta = nextAgentMeta;
      changed = true;
    }
    if (!Array.isArray(ref.skills)) {
      ref.skills = [];
      changed = true;
    } else {
      const normalizedSkills = ref.skills.map((item) => sanitizeSkillDescriptor(item)).filter(Boolean);
      if (normalizedSkills.length !== ref.skills.length) {
        changed = true;
      }
      ref.skills = normalizedSkills;
    }
    const rawYouTubeMap = (ref.youtube_transcripts && typeof ref.youtube_transcripts === 'object') ? ref.youtube_transcripts : {};
    const normalizedYouTubeMap = sanitizeYouTubeTranscriptMap(rawYouTubeMap);
    if (JSON.stringify(rawYouTubeMap) !== JSON.stringify(normalizedYouTubeMap)) {
      changed = true;
    }
    ref.youtube_transcripts = normalizedYouTubeMap;

    const rawHighlights = Array.isArray(ref.highlights) ? ref.highlights : [];
    const normalizedHighlights = rawHighlights
      .map((item) => sanitizeHighlightEntry(item))
      .filter(Boolean)
      .slice(-MAX_HIGHLIGHTS);
    if (rawHighlights.length !== normalizedHighlights.length) {
      changed = true;
    } else {
      for (let i = 0; i < rawHighlights.length; i += 1) {
        if (JSON.stringify(rawHighlights[i]) !== JSON.stringify(normalizedHighlights[i])) {
          changed = true;
          break;
        }
      }
    }
    ref.highlights = normalizedHighlights;

    const rawArtifacts = Array.isArray(ref.artifacts) ? ref.artifacts : [];
    const normalizedArtifacts = rawArtifacts
      .slice(0, 80)
      .map((artifact) => createArtifact(artifact));
    if (rawArtifacts.length !== normalizedArtifacts.length) {
      changed = true;
    } else {
      for (let i = 0; i < rawArtifacts.length; i += 1) {
        if (JSON.stringify(rawArtifacts[i]) !== JSON.stringify(normalizedArtifacts[i])) {
          changed = true;
          break;
        }
      }
    }
    ref.artifacts = normalizedArtifacts;

    const contextBefore = JSON.stringify(Array.isArray(ref.context_files) ? ref.context_files : []);
    pruneYouTubeTranscriptContextFiles(ref);
    const contextAfter = JSON.stringify(Array.isArray(ref.context_files) ? ref.context_files : []);
    if (contextBefore !== contextAfter) {
      changed = true;
    }
    const hasMounts = Array.isArray(ref && ref.folder_mounts) && ref.folder_mounts.length > 0;
    if (hasMounts) {
      const filesTabRes = ensureSingleFilesTab(ref);
      if (filesTabRes && (filesTabRes.created || filesTabRes.deduped)) {
        changed = true;
      }
    }
    const hasSkillsTab = Array.isArray(ref && ref.tabs)
      ? ref.tabs.some((tab) => String((tab && tab.tab_kind) || '').trim().toLowerCase() === 'skills')
      : false;
    if (hasSkillsTab) {
      const skillsTabRes = ensureSingleSkillsTab(ref);
      if (skillsTabRes && (skillsTabRes.created || skillsTabRes.deduped)) {
        changed = true;
      }
    }
  });
  if (changed) {
    writeReferencesRaw(refs);
    syncPublicFeedWithReferences(refs);
  }
  return refs;
}

function setReferences(refs, options = {}) {
  const list = Array.isArray(refs) ? refs : [];
  const skipMemoryCapture = !!(options && options.skipMemoryCapture);
  list.forEach((ref) => {
    if (!ref || typeof ref !== 'object') return;
    migrateLegacyVizTabsInReference(ref);
    ensureReferenceMemory(ref);
    ref.color_tag = sanitizeReferenceColorTag(ref.color_tag);
    ref.youtube_transcripts = sanitizeYouTubeTranscriptMap(ref.youtube_transcripts);
    ref.highlights = Array.isArray(ref.highlights)
      ? ref.highlights.map((item) => sanitizeHighlightEntry(item)).filter(Boolean).slice(-MAX_HIGHLIGHTS)
      : [];
    ref.artifacts = Array.isArray(ref.artifacts)
      ? ref.artifacts.slice(0, 80).map((artifact) => createArtifact(artifact))
      : [createArtifact({ type: 'markdown', title: 'Research Draft', content: '' })];
    ref.agent_meta = normalizeReferenceAgentMeta(ref.agent_meta);
    pruneYouTubeTranscriptContextFiles(ref);
    if (!skipMemoryCapture) {
      capturePeriodicMemoryCheckpoints(ref);
    }
  });
  writeReferencesRaw(list);
  syncPublicFeedWithReferences(list);
  hyperwebManager.refreshLocalPublicIndex().then(() => {
    const status = hyperwebManager.getStatus();
    if (status && status.connected) {
      hyperwebManager.announcePublicIndex().catch(() => {});
    }
  }).catch(() => {});
}

function runMemorySemanticEvaluation() {
  const refs = getReferences();
  const now = nowTs();
  let changed = false;
  refs.forEach((ref) => {
    if (!ref || typeof ref !== 'object') return;
    const memory = ensureReferenceMemory(ref);
    if (!memory.enabled) return;
    const recentlyUpdated = (now - Number(ref.updated_at || 0)) <= MEMORY_UPDATED_LOOKBACK_MS;
    if (!recentlyUpdated && Number(memory.last_semantic_eval_at || 0) > 0) return;
    const evalDue = (now - Number(memory.last_semantic_eval_at || 0)) >= MEMORY_SEMANTIC_INTERVAL_MS;
    if (!evalDue) return;
    const didChange = maybeCaptureSemanticMemoryCheckpoint(ref);
    if (didChange) changed = true;
  });
  if (changed) {
    setReferences(refs, { skipMemoryCapture: true });
  }
}

function findReferenceIndex(refs, srId) {
  const target = String(srId || '').trim();
  if (!target) return -1;
  return refs.findIndex((ref) => String((ref && ref.id) || '') === target);
}

function ensureReferenceGraphShape(ref) {
  if (!ref || typeof ref !== 'object') return;
  if (!ref.reference_graph || typeof ref.reference_graph !== 'object') {
    ref.reference_graph = { nodes: [], edges: [] };
  }
  if (!Array.isArray(ref.reference_graph.nodes)) ref.reference_graph.nodes = [];
  if (!Array.isArray(ref.reference_graph.edges)) ref.reference_graph.edges = [];
}

function appendDecisionTraceGraph(ref, step) {
  if (!ref || !step || typeof step !== 'object') return;
  ensureReferenceGraphShape(ref);
  const graph = ref.reference_graph;
  const trace = Array.isArray(ref.decision_trace) ? ref.decision_trace : [];
  const currentNodeId = String(step.id || makeId('trace'));
  const nodeMap = new Map(graph.nodes.map((node) => [String((node && node.id) || ''), node]));
  const edgeMap = new Map(graph.edges.map((edge) => [String((edge && edge.id) || ''), edge]));

  nodeMap.set(currentNodeId, {
    id: currentNodeId,
    ts: Number(step.ts || nowTs()),
    label: String(step.action || 'trace_step').slice(0, 120),
    outcome: String(step.outcome || '').slice(0, 220),
    sr_id: String(ref.id || ''),
  });

  const prev = trace.length > 0 ? trace[trace.length - 1] : null;
  const prevNodeId = String((prev && prev.id) || '').trim();
  if (prevNodeId && prevNodeId !== currentNodeId) {
    const edgeId = `edge_${prevNodeId}_${currentNodeId}`;
    edgeMap.set(edgeId, {
      id: edgeId,
      source: prevNodeId,
      target: currentNodeId,
      type: 'trace_next',
      ts: Number(step.ts || nowTs()),
    });
  }

  const nextSrIds = Array.isArray(step.next_sr_ids) ? step.next_sr_ids : [];
  nextSrIds.slice(0, 12).forEach((nextSrId) => {
    const targetSrId = String(nextSrId || '').trim();
    if (!targetSrId) return;
    const edgeId = `edge_ref_${String(ref.id || '')}_${targetSrId}_${currentNodeId}`;
    edgeMap.set(edgeId, {
      id: edgeId,
      source: String(ref.id || ''),
      target: targetSrId,
      type: 'ref_transition',
      via: currentNodeId,
      ts: Number(step.ts || nowTs()),
    });
  });

  graph.nodes = Array.from(nodeMap.values()).slice(-GRAPH_MAX_NODES);
  graph.edges = Array.from(edgeMap.values()).slice(-GRAPH_MAX_EDGES);
}

function getLuminoScopedReferences(activeSrId, refs) {
  const activeId = String(activeSrId || '').trim();
  const allRefs = Array.isArray(refs) ? refs : [];
  const idMap = new Map();
  allRefs.forEach((ref) => {
    const id = String((ref && ref.id) || '').trim();
    if (id) idMap.set(id, ref);
  });
  const active = idMap.get(activeId);
  if (!active) return [];

  const allowed = new Set([activeId]);
  const parentId = String((active.parent_id || '')).trim();
  if (parentId && idMap.has(parentId)) allowed.add(parentId);

  const children = Array.isArray(active.children) ? active.children : [];
  children.forEach((childId) => {
    const id = String(childId || '').trim();
    if (id && idMap.has(id)) allowed.add(id);
  });

  return allRefs.filter((ref) => allowed.has(String((ref && ref.id) || '').trim()));
}

function normalizeTopicForIdempotency(topic = '') {
  return String(topic || '')
    .toLowerCase()
    .replace(/https?:\/\/[^\s]+/g, ' ')
    .replace(/[^a-z0-9\s]/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function computeReferenceTopicKey(params = {}) {
  const normalizedTopic = normalizeTopicForIdempotency(params.topic || '');
  const source = String(params.source || 'orchestrator').trim().toLowerCase();
  const dayBucket = String(params.day_bucket || new Date().toISOString().slice(0, 10)).trim();
  const scope = String(params.user_scope || 'default').trim().toLowerCase();
  const raw = `${normalizedTopic}|${source}|${dayBucket}|${scope}`;
  return crypto.createHash('sha256').update(raw).digest('hex');
}

function deriveReferenceTitleFromTopic(topic = '') {
  const normalized = String(topic || '').replace(/\s+/g, ' ').trim();
  if (!normalized) return 'Untitled Reference';
  return normalized.slice(0, 120);
}

async function ensureReferenceForTopic(params = {}) {
  const topic = String(params.topic || '').trim();
  if (!topic) return { ok: false, message: 'topic is required.' };
  const source = String(params.source || 'orchestrator').trim().toLowerCase();
  const dayBucket = String(params.day_bucket || new Date().toISOString().slice(0, 10)).trim();
  const userScope = String(params.user_scope || 'default').trim().toLowerCase();
  const providedKey = String(params.idempotency_key || '').trim();
  const key = providedKey || computeReferenceTopicKey({
    topic,
    source,
    day_bucket: dayBucket,
    user_scope: userScope,
  });
  if (!key) return { ok: false, message: 'Failed to compute idempotency key.' };

  if (orchestratorReferenceLockMap.has(key)) {
    return orchestratorReferenceLockMap.get(key);
  }

  const task = (async () => {
    const refs = getReferences();
    const allRefs = Array.isArray(refs) ? refs : [];
    const exact = allRefs.find((ref) => {
      const meta = (ref && ref.agent_meta && typeof ref.agent_meta === 'object') ? ref.agent_meta : null;
      return !!(
        meta
        && String(meta.idempotency_key || '').trim()
        && String(meta.idempotency_key || '').trim() === key
      );
    });
    if (exact) {
      return { ok: true, sr_id: String(exact.id || '').trim(), reference: exact, created: false, idempotency_key: key };
    }

    const searchResults = Array.isArray(params.search_results) ? params.search_results : [];
    const top = searchResults
      .map((item) => ({
        sr_id: String((item && item.sr_id) || '').trim(),
        score: Number((item && item.score) || 0),
      }))
      .filter((item) => item.sr_id)
      .sort((a, b) => b.score - a.score);
    const semanticMatch = top.find((item) => item.score >= 0.68);
    if (semanticMatch) {
      const idx = findReferenceIndex(allRefs, semanticMatch.sr_id);
      if (idx >= 0) {
        const matched = allRefs[idx];
        const currentMeta = normalizeReferenceAgentMeta(matched.agent_meta) || {};
        matched.agent_meta = normalizeReferenceAgentMeta({
          ...currentMeta,
          created_by: currentMeta.created_by || 'lumino_b',
          path: currentMeta.path || 'path_b',
          source: currentMeta.source || source,
          idempotency_key: currentMeta.idempotency_key || key,
          status: 'active',
          updated_at: nowTs(),
        });
        matched.updated_at = nowTs();
        setReferences(allRefs);
        return { ok: true, sr_id: String(matched.id || '').trim(), reference: matched, created: false, idempotency_key: key, reused_by_semantic_match: true };
      }
    }

    const currentTab = {
      url: getDefaultSearchHomeUrl(),
      title: getDefaultSearchHomeTitle(),
    };
    const created = createReferenceBase({
      title: deriveReferenceTitleFromTopic(topic),
      intent: topic,
      relation_type: 'root',
      current_tab: currentTab,
      agent_meta: {
        created_by: 'lumino_b',
        path: 'path_b',
        source,
        run_id: String(params.run_id || '').trim(),
        job_id: String(params.job_id || '').trim(),
        idempotency_key: key,
        status: 'active',
        created_at: nowTs(),
        updated_at: nowTs(),
      },
    });
    allRefs.unshift(created);
    setReferences(allRefs);
    return { ok: true, sr_id: String(created.id || '').trim(), reference: created, created: true, idempotency_key: key };
  })()
    .finally(() => {
      orchestratorReferenceLockMap.delete(key);
    });

  orchestratorReferenceLockMap.set(key, task);
  return task;
}

async function applyPendingAgentUpdatesInMain(response, targetSrId = '', options = {}) {
  const payload = (response && typeof response === 'object') ? response : {};
  const scopeSrId = String(targetSrId || '').trim();
  const refs = getReferences();
  let changed = false;
  const touched = new Set();

  const touchRef = (srId) => {
    const id = String(srId || '').trim();
    if (!id) return;
    touched.add(id);
  };

  const pendingWeight = Array.isArray(payload.pending_weight_updates) ? payload.pending_weight_updates : [];
  pendingWeight.forEach((item) => {
    const srId = String((item && item.sr_id) || scopeSrId).trim();
    const weights = (item && typeof item.weights === 'object') ? item.weights : null;
    if (!srId || !weights) return;
    const idx = findReferenceIndex(refs, srId);
    if (idx < 0) return;
    refs[idx].agent_weights = (refs[idx].agent_weights && typeof refs[idx].agent_weights === 'object')
      ? { ...refs[idx].agent_weights, ...weights }
      : { ...weights };
    refs[idx].updated_at = nowTs();
    touchRef(srId);
    changed = true;
  });

  const pendingTrace = Array.isArray(payload.pending_decision_traces) ? payload.pending_decision_traces : [];
  pendingTrace.forEach((item) => {
    const srId = String((item && item.sr_id) || scopeSrId).trim();
    const step = (item && item.step && typeof item.step === 'object') ? item.step : null;
    if (!srId || !step) return;
    const idx = findReferenceIndex(refs, srId);
    if (idx < 0) return;
    refs[idx].decision_trace = Array.isArray(refs[idx].decision_trace) ? refs[idx].decision_trace : [];
    refs[idx].decision_trace.push(step);
    refs[idx].decision_trace = refs[idx].decision_trace.slice(-DECISION_TRACE_MAX_STEPS);
    appendDecisionTraceGraph(refs[idx], step);
    refs[idx].updated_at = nowTs();
    touchRef(srId);
    changed = true;
  });

  const pendingArtifacts = Array.isArray(payload.pending_artifacts) ? payload.pending_artifacts : [];
  pendingArtifacts.forEach((artifactInput) => {
    if (!artifactInput || typeof artifactInput !== 'object') return;
    const srId = String(artifactInput.reference_id || scopeSrId).trim();
    if (!srId) return;
    const idx = findReferenceIndex(refs, srId);
    if (idx < 0) return;
    refs[idx].artifacts = Array.isArray(refs[idx].artifacts) ? refs[idx].artifacts : [];
    const artifact = createArtifact(artifactInput);
    const existingIdx = refs[idx].artifacts.findIndex((item) => String((item && item.id) || '').trim() === String(artifact.id || '').trim());
    if (existingIdx >= 0) refs[idx].artifacts[existingIdx] = artifact;
    else refs[idx].artifacts.push(artifact);
    refs[idx].updated_at = nowTs();
    touchRef(srId);
    changed = true;
  });

  const pendingTabs = Array.isArray(payload.pending_workspace_tabs) ? payload.pending_workspace_tabs : [];
  pendingTabs.forEach((tabInput) => {
    if (!tabInput || typeof tabInput !== 'object') return;
    const type = String(tabInput.type || '').trim().toLowerCase();
    const srId = String(tabInput.reference_id || scopeSrId).trim();
    if (!srId) return;
    const idx = findReferenceIndex(refs, srId);
    if (idx < 0) return;
    if (type === 'web') {
      const url = String(tabInput.url || '').trim();
      if (!url) return;
      refs[idx].tabs = Array.isArray(refs[idx].tabs) ? refs[idx].tabs : [];
      const existing = refs[idx].tabs.find((item) => String((item && item.url) || '').trim() === url);
      if (existing) {
        refs[idx].active_tab_id = String(existing.id || '').trim() || refs[idx].active_tab_id;
      } else {
        const nextTab = createWebTab({
          url,
          title: String((tabInput && tabInput.title) || url),
        });
        refs[idx].tabs.push(nextTab);
        refs[idx].active_tab_id = String(nextTab.id || '').trim();
      }
      refs[idx].updated_at = nowTs();
      touchRef(srId);
      changed = true;
    }
  });

  const pendingDiffOps = Array.isArray(payload.pending_diff_ops) ? payload.pending_diff_ops : [];
  pendingDiffOps.forEach((op) => {
    const diffOp = (op && typeof op === 'object') ? op : null;
    if (!diffOp) return;
    const srId = String(diffOp.reference_id || scopeSrId).trim();
    const kind = String(diffOp.target_kind || '').trim().toLowerCase();
    if (!srId || !kind) return;
    const idx = findReferenceIndex(refs, srId);
    if (idx < 0) return;
    let result = { ok: false };
    if (kind === 'artifact') {
      result = applyArtifactDiff(refs[idx], diffOp);
    } else if (kind === 'context_file') {
      result = applyContextFileDiff(refs[idx], diffOp);
    }
    if (result && result.ok) {
      refs[idx].updated_at = nowTs();
      touchRef(srId);
      changed = true;
    }
  });

  if (changed) {
    if (options && options.lane === 'path_b') {
      const runId = String((options && options.run_id) || '').trim();
      const jobId = String((options && options.job_id) || '').trim();
      const source = String((options && options.source) || '').trim();
      const idempotencyKey = String((options && options.idempotency_key) || '').trim();
      touched.forEach((srId) => {
        const idx = findReferenceIndex(refs, srId);
        if (idx < 0) return;
        const currentMeta = normalizeReferenceAgentMeta(refs[idx].agent_meta) || {};
        refs[idx].agent_meta = normalizeReferenceAgentMeta({
          ...currentMeta,
          created_by: currentMeta.created_by || 'lumino_b',
          path: 'path_b',
          source: source || currentMeta.source || 'orchestrator',
          run_id: runId || currentMeta.run_id,
          job_id: jobId || currentMeta.job_id,
          idempotency_key: idempotencyKey || currentMeta.idempotency_key,
          status: 'active',
          updated_at: nowTs(),
          created_at: Number(currentMeta.created_at || nowTs()),
        });
      });
    }
    setReferences(refs);
  }

  return { ok: true, changed, touched_sr_ids: Array.from(touched) };
}

function trimForPrompt(value, maxLen = 800) {
  const text = String(value || '').replace(/\s+/g, ' ').trim();
  if (!text) return '';
  if (text.length <= maxLen) return text;
  return `${text.slice(0, Math.max(1, maxLen - 3))}...`;
}

// ── Rolling Memory Artifact Constants ───────────────────────────────────────
const MEMORY_ARTIFACT_MARKER = '<!-- subgrapher:memory -->';
const MEMORY_ARTIFACT_TITLE = 'Research Memory';
const SUBSTANTIVE_REPLY_MIN_CHARS = 350;
const DETAILED_DELIVERABLE_KEYWORDS = [
  'report', 'essay', 'article', 'writeup', 'write-up', 'detailed', 'in-depth',
  'comprehensive', 'artifact', 'long-form', 'draft',
];
const RESEARCH_INTENT_KEYWORDS = [
  'research',
  'search',
  'related paper',
  'related papers',
  'paper',
  'papers',
  'source',
  'sources',
  'literature review',
  'reference',
  'references',
  'find studies',
  'find paper',
];
const ARTIFACT_ITERATION_KEYWORDS = [
  'fix',
  'improve',
  'update',
  'refactor',
  'correct',
  'revise',
  'tweak',
  'optimize',
  'optimise',
  'polish',
  'debug',
  'repair',
];
const EXPLICIT_NEW_ARTIFACT_PATTERNS = [
  /\bnew\s+(artifact|version|draft|copy|one|file)\b/i,
  /\banother\s+(artifact|version|draft|copy|one|file)\b/i,
  /\bseparate\s+(artifact|version|draft|copy|file)\b/i,
  /\b(?:alternate|alternative)\s+(artifact|version|draft)\b/i,
  /\bfrom\s+scratch\b/i,
  /\bfresh\s+start\b/i,
  /\b(?:duplicate|clone|copy)\s+(?:this|that|it|artifact|version)\b/i,
];

function isDetailedDeliverableRequest(userMessage) {
  const lower = String(userMessage || '').toLowerCase();
  return DETAILED_DELIVERABLE_KEYWORDS.some((kw) => lower.includes(kw));
}

function isResearchIntentRequest(userMessage) {
  const lower = String(userMessage || '').toLowerCase();
  return RESEARCH_INTENT_KEYWORDS.some((kw) => lower.includes(kw));
}

function hasLocalEvidenceForResearch(ref) {
  const target = (ref && typeof ref === 'object') ? ref : {};
  const artifacts = Array.isArray(target.artifacts) ? target.artifacts : [];
  const contextFiles = Array.isArray(target.context_files) ? target.context_files : [];
  const graph = (target.reference_graph && typeof target.reference_graph === 'object') ? target.reference_graph : {};
  const nodeCount = Array.isArray(graph.nodes) ? graph.nodes.length : 0;
  const edgeCount = Array.isArray(graph.edges) ? graph.edges.length : 0;
  return artifacts.length > 0 || contextFiles.length > 0 || nodeCount > 0 || edgeCount > 0;
}

function isArtifactIterationRequest(userMessage) {
  const lower = String(userMessage || '').toLowerCase();
  return ARTIFACT_ITERATION_KEYWORDS.some((kw) => lower.includes(kw));
}

function isExplicitNewArtifactRequest(userMessage) {
  const text = String(userMessage || '').trim();
  if (!text) return false;
  return EXPLICIT_NEW_ARTIFACT_PATTERNS.some((pattern) => pattern.test(text));
}

function resolveActiveArtifactContext(payload, activeRef, srId) {
  const input = (payload && typeof payload === 'object') ? payload : {};
  const ref = (activeRef && typeof activeRef === 'object') ? activeRef : {};
  const artifacts = Array.isArray(ref.artifacts) ? ref.artifacts : [];
  if (artifacts.length === 0) return null;

  const surface = (input.active_surface && typeof input.active_surface === 'object')
    ? input.active_surface
    : {};
  const surfaceKind = String(surface.kind || '').trim().toLowerCase();
  const surfaceArtifactId = surfaceKind === 'artifact'
    ? String((surface.artifact_id || surface.artifactId) || '').trim()
    : '';
  const markerSrId = String((markerContext && markerContext.srId) || '').trim();
  const markerArtifactId = markerSrId === String(srId || '').trim()
    ? String((markerContext && markerContext.artifactId) || '').trim()
    : '';
  const candidates = [surfaceArtifactId, markerArtifactId].filter(Boolean);
  for (const candidateId of candidates) {
    const artifact = artifacts.find((item) => String((item && item.id) || '').trim() === candidateId);
    if (!artifact) continue;
    return {
      id: String((artifact && artifact.id) || '').trim(),
      type: normalizeArtifactType((artifact && artifact.type) || 'markdown'),
      title: String((artifact && artifact.title) || 'Artifact').trim() || 'Artifact',
    };
  }
  return null;
}

function resolveImplicitArtifactId(options = {}) {
  const opts = (options && typeof options === 'object') ? options : {};
  const providedId = String(opts.providedId || '').trim();
  if (providedId) return providedId;

  const activeArtifact = (opts.activeArtifactContext && typeof opts.activeArtifactContext === 'object')
    ? opts.activeArtifactContext
    : null;
  const activeArtifactId = String((activeArtifact && activeArtifact.id) || '').trim();
  if (!activeArtifactId) return '';

  const message = String(opts.message || '').trim();
  if (isExplicitNewArtifactRequest(message)) return '';
  if (!isArtifactIterationRequest(message)) return '';

  const requestedType = String(opts.requestedType || '').trim();
  if (requestedType) {
    const normalizedRequested = normalizeArtifactType(requestedType);
    const normalizedActive = normalizeArtifactType((activeArtifact && activeArtifact.type) || 'markdown');
    if (normalizedRequested !== normalizedActive) return '';
  }
  return activeArtifactId;
}

function isSubstantiveAssistantReply(text) {
  const str = String(text || '').trim();
  if (str.length >= SUBSTANTIVE_REPLY_MIN_CHARS) return true;
  const lines = str.split('\n');
  const structuredLines = lines.filter((l) => /^#{1,6}\s|^[-*+]\s|^\d+\.\s/.test(l.trim()));
  return structuredLines.length >= 3;
}

function buildChatThreadPrompt(ref) {
  const thread = (ref && ref.chat_thread && typeof ref.chat_thread === 'object') ? ref.chat_thread : {};
  const messages = Array.isArray(thread.messages) ? thread.messages : [];
  const recent = messages.slice(-24);
  if (recent.length === 0) return '';
  const lines = recent.map((m) => {
    const role = String((m && m.role) || 'assistant').toUpperCase();
    const text = trimForPrompt(String((m && m.text) || ''), 400);
    return `[${role}] ${text}`;
  });
  return `Recent conversation (last ${recent.length} messages):\n${lines.join('\n')}`;
}

function replaceSection(content, heading, newBody) {
  const lines = content.split('\n');
  const result = [];
  let inTarget = false;
  let found = false;

  for (const line of lines) {
    if (line.startsWith(`## ${heading}`)) {
      inTarget = true;
      found = true;
      result.push(line);
      result.push('');
      result.push(newBody.trim());
      result.push('');
      continue;
    }
    if (inTarget && line.startsWith('## ')) {
      inTarget = false;
    }
    if (!inTarget) {
      result.push(line);
    }
  }

  if (!found) {
    return `${content.trimEnd()}\n\n## ${heading}\n\n${newBody.trim()}\n`;
  }
  return result.join('\n');
}

function updateRollingMemorySections(artifact, assistantText, isDetailed) {
  let content = String((artifact && artifact.content) || '');
  const trimmedText = String(assistantText || '').trim();
  if (!trimmedText) return content;
  if (isDetailed) {
    content = replaceSection(content, 'Latest Detailed Response', trimmedText);
  } else {
    const excerpt = trimmedText.length > 600 ? `${trimmedText.slice(0, 597)}...` : trimmedText;
    content = replaceSection(content, 'Key Findings', excerpt);
  }
  return content;
}

function ensureRollingMemoryArtifact(ref, srId) {
  const artifacts = Array.isArray(ref.artifacts) ? ref.artifacts : [];
  const existing = artifacts.find((a) => {
    const content = String((a && a.content) || '');
    const title = String((a && a.title) || '');
    return content.includes(MEMORY_ARTIFACT_MARKER) || title === MEMORY_ARTIFACT_TITLE;
  });
  if (existing) return existing;
  const newArtifact = {
    id: makeId('artifact'),
    type: 'markdown',
    title: MEMORY_ARTIFACT_TITLE,
    content: [
      MEMORY_ARTIFACT_MARKER,
      '',
      '## Current Thesis',
      '',
      '_Not yet established._',
      '',
      '## Key Findings',
      '',
      '_None yet._',
      '',
      '## Evidence / Sources',
      '',
      '_None yet._',
      '',
      '## Open Questions',
      '',
      '_None yet._',
      '',
      '## Latest Detailed Response',
      '',
      '_No detailed response yet._',
      '',
    ].join('\n'),
    reference_id: srId,
    created_at: nowTs(),
    updated_at: nowTs(),
  };
  ref.artifacts = [...artifacts, newArtifact];
  return newArtifact;
}
// ────────────────────────────────────────────────────────────────────────────

function buildLuminoProviderPrompts(message, activeRef, scopedRefs = [], options = {}) {
  const ref = activeRef || {};
  const tabs = Array.isArray(ref.tabs) ? ref.tabs : [];
  const artifacts = Array.isArray(ref.artifacts) ? ref.artifacts : [];
  const contextFiles = Array.isArray(ref.context_files) ? ref.context_files : [];
  const scoped = Array.isArray(scopedRefs) ? scopedRefs : [];
  const toolingEnabled = !!(options && options.toolingEnabled);
  const activeArtifactContext = (options && options.activeArtifactContext && typeof options.activeArtifactContext === 'object')
    ? options.activeArtifactContext
    : null;

  const activeTab = tabs.find((tab) => String((tab && tab.id) || '') === String((ref && ref.active_tab_id) || '')) || tabs[0] || null;
  const tabSummary = tabs.slice(0, 8).map((tab) => {
    const kind = String((tab && tab.tab_kind) || 'web');
    const title = trimForPrompt((tab && tab.title) || (tab && tab.url) || 'Untitled', 90);
    const url = trimForPrompt((tab && tab.url) || '', 120);
    return `- [${kind}] ${title}${url ? ` (${url})` : ''}`;
  }).join('\n');
  const artifactSummary = artifacts.slice(0, 8).map((artifact) => {
    const id = trimForPrompt((artifact && artifact.id) || '', 56);
    const type = trimForPrompt(normalizeArtifactType((artifact && artifact.type) || 'markdown'), 18);
    const title = trimForPrompt((artifact && artifact.title) || 'Artifact', 90);
    const content = trimForPrompt((artifact && artifact.content) || '', 220);
    return `- [${id || 'no-id'}] (${type}) ${title}${content ? `: ${content}` : ''}`;
  }).join('\n');
  const activeArtifactLine = activeArtifactContext && activeArtifactContext.id
    ? `Active artifact: ${trimForPrompt(activeArtifactContext.title || 'Artifact', 120)} (id=${trimForPrompt(activeArtifactContext.id, 56)}, type=${trimForPrompt(activeArtifactContext.type || 'markdown', 18)})`
    : 'Active artifact: (none)';
  const contextSummary = contextFiles.slice(0, 12).map((file) => {
    const name = trimForPrompt((file && file.original_name) || (file && file.relative_path) || 'context.txt', 80);
    const note = trimForPrompt((file && file.summary) || '', 180);
    return `- ${name}${note ? `: ${note}` : ''}`;
  }).join('\n');
  const scopedSummary = scoped.slice(0, 20).map((item) => {
    const title = trimForPrompt((item && item.title) || 'Untitled', 80);
    const id = trimForPrompt((item && item.id) || '', 50);
    const relation = trimForPrompt((item && item.relation_type) || 'root', 32);
    return `- ${title} (${relation}${id ? `, ${id}` : ''})`;
  }).join('\n');
  const referenceProgram = String((ref && ref.program) || '').trim();
  const programBlock = referenceProgram
    ? [
      'Reference Program (persistent system prompt for this reference):',
      referenceProgram,
    ].join('\n')
    : '';

  const threadContext = buildChatThreadPrompt(ref);

  const systemPrompt = toolingEnabled
    ? [
      'You are Lumino, the Subgrapher workspace assistant.',
      'Be precise and concise.',
      programBlock,
      'Artifact persistence rules:',
      '- When the user asks for a detailed report, essay, article, writeup, or any long-form deliverable, use write_markdown_artifact to write the full content into a persistent deliverable artifact.',
      '- When the user asks for interactive browser visualizations/games, use write_html_artifact (or create_artifact with artifact_type="html").',
      '- When the user says "write it into an artifact", "save it as an artifact", or similar, use the appropriate artifact writer with the relevant content.',
      '- For iterative edit requests (fix/improve/update/refactor/correct), update the active artifact in place by passing artifact_id.',
      '- Create a new artifact only when the user explicitly asks for a new/another/separate version.',
      '- Complete research/tool work first, then write the requested deliverable artifact, then provide a concise final reply.',
      '- For short answers and follow-ups, reply directly in chat without opening any artifact.',
      'Research and citation policy:',
      '- For local research, call search_local_evidence before final synthesis.',
      '- For research outputs, use footnote citation format in content: [1], [2], ... and include a "## Sources" section.',
      '- In chat responses, format source URLs as markdown links.',
      '- In artifact content, keep source URLs as plain text (non-markdown links).',
      'Interactive artifact policy:',
      '- Prefer HTML artifacts for interactive visualizations and games.',
      '- Use write_html_artifact (or create_artifact with artifact_type="html") for dynamic browser-executed outputs.',
      '- Keep HTML artifacts full-document (`<!doctype html>...`) and responsive to container size.',
      '- /diff artifact queues an append patch that requires manual Apply. For direct corrections, prefer write_markdown_artifact/write_html_artifact with artifact_id.',
      'When helpful, suggest direct workspace mutation commands exactly in this syntax:',
      '/artifact <title>: <content>',
      '/viz <title>',
      '/diff artifact <artifact_id> <text>',
      'Never claim to have executed commands unless they were explicitly issued by the user.',
    ].join('\n')
    : [
      'You are Lumino, the Subgrapher workspace assistant.',
      'Be precise and concise.',
      programBlock,
      'Tools are currently unavailable in this chat mode.',
      'Do not claim to have written artifacts, opened tabs, searched the web, or executed tools.',
      'If a request depends on tools, state the limitation clearly and provide the best possible direct answer or concise next step.',
      'For short answers and follow-ups, reply directly in chat.',
      'Never claim to have executed commands unless they were explicitly issued by the user.',
    ].join('\n');

  const userPrompt = [
    `User request: ${String(message || '').trim()}`,
    '',
    `Active reference: ${trimForPrompt((ref && ref.title) || 'Untitled', 120)}`,
    `Intent: ${trimForPrompt((ref && ref.intent) || '', 220) || '(none)'}`,
    `Active tab: ${trimForPrompt((activeTab && activeTab.title) || (activeTab && activeTab.url) || 'none', 120)}`,
    activeArtifactLine,
    tabSummary ? `Tabs:\n${tabSummary}` : 'Tabs: (none)',
    artifactSummary ? `Artifacts:\n${artifactSummary}` : 'Artifacts: (none)',
    contextSummary ? `Context files:\n${contextSummary}` : 'Context files: (none)',
    scopedSummary ? `Visible reference scope:\n${scopedSummary}` : 'Visible reference scope: (none)',
    threadContext ? `\n${threadContext}` : '',
  ].filter(Boolean).join('\n');

  return { systemPrompt, userPrompt };
}

function sanitizeCrawlerSettings(input = {}) {
  const settings = readSettings();
  return {
    mode: String(input.mode || settings.crawler_mode || 'broad').trim().toLowerCase() === 'safe' ? 'safe' : 'broad',
    markdown_first: Object.prototype.hasOwnProperty.call(input, 'markdown_first')
      ? !!input.markdown_first
      : !!settings.crawler_markdown_first,
    robots_policy: String(input.robots_policy || settings.crawler_robots_default || 'respect').trim().toLowerCase() === 'ignore'
      ? 'ignore'
      : 'respect',
    depth: Number.isFinite(Number(input.depth))
      ? Math.max(1, Math.min(6, Math.round(Number(input.depth))))
      : Math.max(1, Math.min(6, Math.round(Number(settings.crawler_depth_default || 3)))),
    page_cap: Number.isFinite(Number(input.page_cap))
      ? Math.max(5, Math.min(300, Math.round(Number(input.page_cap))))
      : Math.max(5, Math.min(300, Math.round(Number(settings.crawler_page_cap_default || 80)))),
  };
}

function getLatestCrawlerJobForReference(srId = '') {
  const cleanSrId = String(srId || '').trim();
  const allJobs = Array.from(luminoCrawler.jobs.values());
  const filtered = cleanSrId
    ? allJobs.filter((job) => String((job && job.sr_id) || '').trim() === cleanSrId)
    : allJobs;
  if (filtered.length === 0) return null;
  return filtered.sort((a, b) => Number((b && b.updated_at) || 0) - Number((a && a.updated_at) || 0))[0] || null;
}

async function ingestCrawlerPagesIntoReference(srId, pages = [], job = {}) {
  const cleanSrId = String(srId || '').trim();
  if (!cleanSrId) return { ok: false, message: 'sr_id is required.' };
  const refs = getReferences();
  const idx = findReferenceIndex(refs, cleanSrId);
  if (idx < 0) return { ok: false, message: 'Reference not found.' };

  const ref = refs[idx];
  ref.context_files = Array.isArray(ref.context_files) ? ref.context_files : [];
  const outDir = path.join(app.getPath('userData'), 'semantic_references', cleanSrId, 'context_files');
  fs.mkdirSync(outDir, { recursive: true });
  const dedupe = new Set(ref.context_files.map((file) => `${file.content_hash || ''}:${file.relative_path || ''}`));

  const imported = [];
  const list = Array.isArray(pages) ? pages : [];
  list.slice(0, 300).forEach((page) => {
    const sourceType = String((page && page.source_type) || 'crawler_web').trim().toLowerCase() === 'crawler_local'
      ? 'crawler_local'
      : 'crawler_web';
    const sourceUrl = String((page && page.url) || '').trim();
    const relativePath = String((page && page.relative_path) || '').trim() || `${sourceType}_${imported.length + 1}.md`;
    const markdown = String((page && page.markdown) || '').trim();
    const text = String((page && page.text) || '').trim();
    const content = markdown || text;
    if (!content) return;
    const hash = crypto.createHash('sha256').update(content, 'utf8').digest('hex');
    const dedupeKey = `${hash}:${relativePath}`;
    if (dedupe.has(dedupeKey)) return;
    dedupe.add(dedupeKey);

    const fileId = makeId('ctx');
    const safeName = relativePath.replace(/[^a-zA-Z0-9._/-]/g, '_').replace(/\//g, '__');
    const outPath = path.join(outDir, `${fileId}_${safeName}`);
    fs.writeFileSync(outPath, content, 'utf8');
    const summary = content.split('\n').slice(0, 16).join(' ').trim();
    const item = {
      id: fileId,
      source_type: sourceType,
      original_name: String((page && page.title) || path.basename(relativePath) || 'crawl.md').slice(0, 200),
      relative_path: relativePath.slice(0, 300),
      stored_path: outPath,
      mime_type: 'text/markdown',
      size_bytes: Buffer.byteLength(content, 'utf8'),
      content_hash: hash,
      ingest_status: 'ready',
      summary: summary.slice(0, 600),
      read_only: false,
      crawl_job_id: String((job && job.id) || ''),
      crawl_source_url: sourceUrl,
      crawl_depth: Number((job && job.depth) || 0),
      crawl_fetched_at: nowTs(),
      created_at: nowTs(),
      updated_at: nowTs(),
    };
    ref.context_files.push(item);
    imported.push(item);
  });

  let filesTabRes = { tab: null, created: false };
  if (imported.length > 0) {
    filesTabRes = ensureSingleFilesTab(ref);
    ref.artifacts = Array.isArray(ref.artifacts) ? ref.artifacts : [];
    const summaryArtifact = createArtifact({
      title: `crawl-summary-${new Date(nowTs()).toISOString().slice(0, 10)}.md`,
      type: 'markdown',
      content: [
        `# Crawl Summary`,
        ``,
        `Imported files: ${imported.length}`,
        `Job: ${String((job && job.id) || '')}`,
        `Mode: ${String((job && job.mode) || 'broad')}`,
        ``,
        ...imported.slice(0, 40).map((item) => `- ${item.original_name} (${item.crawl_source_url || item.relative_path})`),
      ].join('\n'),
    });
    ref.artifacts.push(summaryArtifact);
  }

  ref.updated_at = nowTs();
  setReferences(refs);
  return {
    ok: true,
    imported_count: imported.length,
    files_tab: filesTabRes && filesTabRes.tab ? filesTabRes.tab : null,
    files_tab_created: !!(filesTabRes && filesTabRes.created),
  };
}

function getReferenceSkillDescriptors(ref) {
  return Array.isArray(ref && ref.skills)
    ? ref.skills.map((item) => sanitizeSkillDescriptor(item)).filter(Boolean)
    : [];
}

function ensureReferenceSkillConsistency(ref, localSkills, globalSkills) {
  const localMap = new Map((Array.isArray(localSkills) ? localSkills : []).map((skill) => [String(skill.id || ''), skill]));
  const globalMap = new Map((Array.isArray(globalSkills) ? globalSkills : []).map((skill) => [String(skill.id || ''), skill]));
  const descriptors = getReferenceSkillDescriptors(ref);
  ref.skills = descriptors.filter((descriptor) => {
    if (descriptor.scope === 'global') return globalMap.has(descriptor.id);
    return localMap.has(descriptor.id);
  }).map((descriptor) => {
    const source = descriptor.scope === 'global' ? globalMap.get(descriptor.id) : localMap.get(descriptor.id);
    return {
      id: descriptor.id,
      scope: descriptor.scope,
      name: String((source && source.name) || descriptor.name || 'skill').slice(0, 120),
    };
  });
}

function findSkillById(skillId, scope = '') {
  const targetId = String(skillId || '').trim();
  if (!targetId) return null;
  const normalizedScope = String(scope || '').trim().toLowerCase();
  if (!normalizedScope || normalizedScope === 'local') {
    const localSkill = readSkillStore('local').find((item) => String((item && item.id) || '') === targetId);
    if (localSkill) return localSkill;
  }
  if (!normalizedScope || normalizedScope === 'global') {
    const globalSkill = readSkillStore('global').find((item) => String((item && item.id) || '') === targetId);
    if (globalSkill) return globalSkill;
  }
  return null;
}

function upsertSkillForReference(srId, skillInput, scope = 'local') {
  const refId = String(srId || '').trim();
  if (!refId) return { ok: false, message: 'srId is required.' };
  const refs = getReferences();
  const idx = findReferenceIndex(refs, refId);
  if (idx < 0) return { ok: false, message: 'Reference not found.' };
  const normalizedScope = String(scope || '').trim().toLowerCase() === 'global' ? 'global' : 'local';

  const draft = sanitizeSkillObject({
    ...(skillInput && typeof skillInput === 'object' ? skillInput : {}),
    scope: normalizedScope,
    owner_reference_id: normalizedScope === 'local' ? refId : null,
  }, normalizedScope);
  if (!draft) {
    return { ok: false, message: 'Skill requires non-empty name and code.' };
  }

  const store = readSkillStore(normalizedScope);
  const existingIdx = store.findIndex((item) => String((item && item.id) || '') === draft.id);
  if (existingIdx >= 0) {
    const existing = store[existingIdx];
    store[existingIdx] = {
      ...existing,
      ...draft,
      created_at: Number(existing.created_at || nowTs()),
      updated_at: nowTs(),
      owner_reference_id: normalizedScope === 'local'
        ? String(draft.owner_reference_id || existing.owner_reference_id || refId)
        : null,
    };
  } else {
    store.push({
      ...draft,
      created_at: nowTs(),
      updated_at: nowTs(),
      owner_reference_id: normalizedScope === 'local' ? refId : null,
      scope: normalizedScope,
    });
  }
  const nextStore = writeSkillStore(normalizedScope, store);
  const saved = nextStore.find((item) => String((item && item.id) || '') === draft.id) || draft;

  refs[idx].skills = getReferenceSkillDescriptors(refs[idx]);
  attachSkillDescriptorToReference(refs[idx], saved);

  const localSkills = normalizedScope === 'local' ? nextStore : readSkillStore('local');
  const globalSkills = normalizedScope === 'global' ? nextStore : readSkillStore('global');
  ensureReferenceSkillConsistency(refs[idx], localSkills, globalSkills);
  refs[idx].updated_at = nowTs();
  setReferences(refs);

  return {
    ok: true,
    skill: saved,
    linked_skills: listSkillsForReference(refs[idx]).linked_skills,
    reference: refs[idx],
    references: refs,
  };
}

function deleteSkillForReference(srId, skillId, scope = 'local') {
  const refId = String(srId || '').trim();
  const targetId = String(skillId || '').trim();
  const normalizedScope = String(scope || '').trim().toLowerCase() === 'global' ? 'global' : 'local';
  if (!refId || !targetId) {
    return { ok: false, message: 'srId and skillId are required.' };
  }
  const refs = getReferences();
  const idx = findReferenceIndex(refs, refId);
  if (idx < 0) return { ok: false, message: 'Reference not found.' };

  const removed = removeSkillDescriptorFromReference(refs[idx], targetId, normalizedScope);
  if (!removed) {
    return { ok: false, message: 'Skill link not found in this reference.' };
  }
  refs[idx].updated_at = nowTs();

  if (!isSkillLinkedAnywhere(refs, targetId, normalizedScope)) {
    const store = readSkillStore(normalizedScope);
    const nextStore = store.filter((item) => String((item && item.id) || '') !== targetId);
    writeSkillStore(normalizedScope, nextStore);
  }

  setReferences(refs);
  return { ok: true, reference: refs[idx], references: refs };
}

async function runPythonForReference(srId, code, timeoutMs = PYTHON_EXEC_TIMEOUT_MS, bridgeOpts = {}, runtimeRole = 'tool') {
  const role = String(runtimeRole || '').trim().toLowerCase() === 'viz' ? 'viz' : 'tool';
  const { manager, runtime } = await getPythonSandboxManagerForRole(role);
  const refId = String(srId || '').trim() || 'global';
  const script = String(code || '');
  const extraEnv = {};

  const runOnce = () => manager.enqueue({
    srId: refId,
    code: script,
    timeoutMs,
    extraEnv,
    toolBridge: typeof (bridgeOpts && bridgeOpts.toolBridge) === 'function' ? bridgeOpts.toolBridge : null,
    bridgeToolNames: Array.isArray(bridgeOpts && bridgeOpts.bridgeToolNames) ? bridgeOpts.bridgeToolNames : [],
  });

  const runRes = await runOnce();

  if (runRes && typeof runRes === 'object') {
    runRes.python_code = script;
    runRes.python_runtime = runtime;
    runRes.runtime_role = role;
  }
  return runRes;
}

function dispatchProgrammaticTool(req, { srId, refs }) {
  const name = String((req && req.name) || '').trim();
  const args = (req && typeof req.args === 'object' && req.args !== null) ? req.args : {};
  const ref = Array.isArray(refs) ? refs.find((r) => String((r && r.id) || '') === String(srId || '')) : null;
  if (!ref) return { error: `Reference not found: ${srId}` };

  if (name === 'list_artifacts') {
    const artifacts = Array.isArray(ref.artifacts) ? ref.artifacts : [];
    return artifacts.map((a) => ({
      id: String((a && a.id) || ''),
      title: String((a && a.title) || ''),
      kind: String((a && a.type) || (a && a.kind) || 'markdown'),
    }));
  }

  if (name === 'read_artifact') {
    const artifactId = String(args.artifact_id || '').trim();
    const artifacts = Array.isArray(ref.artifacts) ? ref.artifacts : [];
    const art = artifacts.find((a) => String((a && a.id) || '') === artifactId);
    if (!art) return null;
    return {
      id: String(art.id || ''),
      title: String(art.title || ''),
      content: String(art.content || ''),
      kind: String(art.type || art.kind || 'markdown'),
    };
  }

  if (name === 'list_highlights') {
    const highlights = Array.isArray(ref.highlights) ? ref.highlights : [];
    return highlights
      .slice(-200)
      .map((h) => sanitizeHighlightEntry(h))
      .filter(Boolean)
      .map((item) => {
        if (item.source === 'artifact') {
          return {
            id: String(item.id || ''),
            source: 'artifact',
            text: String(item.text || ''),
            artifact_id: String(item.artifact_id || ''),
            artifact_start: Number.isFinite(Number(item.artifact_start)) ? Number(item.artifact_start) : null,
            artifact_end: Number.isFinite(Number(item.artifact_end)) ? Number(item.artifact_end) : null,
          };
        }
        return {
          id: String(item.id || ''),
          source: 'web',
          text: String(item.text || ''),
          url: String(item.url || ''),
          url_norm: String(item.url_norm || ''),
          context_before: String(item.context_before || ''),
          context_after: String(item.context_after || ''),
          web_start: Number.isFinite(Number(item.web_start)) ? Number(item.web_start) : null,
          web_end: Number.isFinite(Number(item.web_end)) ? Number(item.web_end) : null,
        };
      });
  }

  if (name === 'search_reference_graph') {
    const query = String(args.query || '').toLowerCase().trim();
    const graph = (ref.reference_graph && typeof ref.reference_graph === 'object') ? ref.reference_graph : {};
    const nodes = Array.isArray(graph.nodes) ? graph.nodes : [];
    const edges = Array.isArray(graph.edges) ? graph.edges : [];
    if (!query) return { nodes: nodes.slice(0, 50), edges: edges.slice(0, 100) };
    return {
      nodes: nodes.filter((n) => JSON.stringify(n).toLowerCase().includes(query)).slice(0, 50),
      edges: edges.filter((e) => JSON.stringify(e).toLowerCase().includes(query)).slice(0, 100),
    };
  }

  if (name === 'list_context_files') {
    const contextFiles = Array.isArray(ref.context_files) ? ref.context_files : [];
    return contextFiles.map((f) => ({
      id: String((f && f.id) || ''),
      name: String((f && f.original_name) || (f && f.relative_path) || ''),
      size_bytes: Number((f && f.size_bytes) || 0),
      summary: String((f && f.summary) || ''),
    }));
  }

  if (name === 'read_context_file') {
    const fileId = String(args.file_id || '').trim();
    const contextFiles = Array.isArray(ref.context_files) ? ref.context_files : [];
    const ctxFile = contextFiles.find((f) => String((f && f.id) || '') === fileId);
    if (!ctxFile) return null;
    const storedPath = String((ctxFile && ctxFile.stored_path) || '').trim();
    if (!storedPath || !fs.existsSync(storedPath)) return null;
    try {
      const content = fs.readFileSync(storedPath, 'utf8');
      return {
        name: String(ctxFile.original_name || ctxFile.relative_path || ''),
        content: content.slice(0, 200_000),
      };
    } catch (_) {
      return null;
    }
  }

  return { error: `Unknown programmatic tool: ${name}` };
}

async function runSkillForReference(srId, skillId, scope = 'local', args = null) {
  const refId = String(srId || '').trim();
  if (!refId) return { ok: false, message: 'srId is required.' };
  const refs = getReferences();
  const idx = findReferenceIndex(refs, refId);
  if (idx < 0) return { ok: false, message: 'Reference not found.' };
  const ref = refs[idx];

  const descriptor = getReferenceSkillDescriptors(ref).find((item) => (
    String(item.id || '') === String(skillId || '')
    && String(item.scope || 'local') === String(scope || 'local')
  ));
  if (!descriptor) {
    return { ok: false, message: 'Skill is not linked to this reference.' };
  }
  const skill = findSkillById(descriptor.id, descriptor.scope);
  if (!skill) return { ok: false, message: 'Skill not found in store.' };

  const argBlock = args && typeof args === 'object'
    ? `\nimport json\nskill_args = json.loads(${JSON.stringify(JSON.stringify(args))})\n`
    : '\n';
  const code = `${String(skill.code || '')}${argBlock}`;
  const exec = await runPythonForReference(refId, code, PYTHON_EXEC_TIMEOUT_MS, {}, 'tool');
  return {
    ...exec,
    skill,
  };
}

function buildPythonArtifactPendingOutput(srId, title, runResult) {
  const refId = String(srId || '').trim();
  const safeTitle = String(title || 'Python Output').trim().slice(0, 120) || 'Python Output';
  if (!refId) return null;

  const run = (runResult && typeof runResult === 'object') ? runResult : {};
  const stdout = String(run.stdout || '').trim();
  const stderr = String(run.stderr || '').trim();
  const executionId = String(run.execution_id || '').trim();
  const timedOut = !!run.timed_out;
  const runtime = (run.python_runtime && typeof run.python_runtime === 'object') ? run.python_runtime : {};
  const pngBase64 = String(run.png_base64 || '').trim();

  let imageMarkdown = '';
  if (pngBase64) {
    const pngId = executionId ? `python_${executionId}` : `python_${Date.now()}`;
    const persistedPath = persistVizPngFromBase64(pngId, pngBase64);
    if (persistedPath) {
      imageMarkdown = `![Python output image](${pathToFileURL(persistedPath).toString()})`;
    } else {
      imageMarkdown = `![Python output image](data:image/png;base64,${pngBase64})`;
    }
  }

  const lines = [
    '# Python Execution Output',
    '',
    `- Execution ID: ${executionId || 'n/a'}`,
    `- Timed out: ${timedOut ? 'yes' : 'no'}`,
    `- Runtime source: ${String(runtime.source || 'unknown')}`,
    `- Runtime binary: ${String(runtime.python_bin || 'unknown')}`,
    '',
  ];
  if (imageMarkdown) {
    lines.push('## Rendered Output', '', imageMarkdown, '');
  }
  if (stdout) {
    lines.push('## stdout', '', '```text', stdout, '```', '');
  }
  if (stderr) {
    lines.push('## stderr', '', '```text', stderr, '```', '');
  }
  if (!stdout && !stderr && !imageMarkdown) {
    lines.push('No stdout/stderr/image output was returned.');
  }

  const artifactId = makeId('artifact');
  return {
    artifact: {
      id: artifactId,
      reference_id: refId,
      type: 'markdown',
      title: safeTitle,
      content: lines.join('\n'),
      created_at: nowTs(),
      updated_at: nowTs(),
    },
    workspace_tab: {
      type: 'artifact',
      reference_id: refId,
      artifact_id: artifactId,
      title: safeTitle,
    },
  };
}

function summarizeUrlForStatus(rawUrl) {
  const target = String(rawUrl || '').trim();
  if (!target) return 'unknown page';
  try {
    const parsed = new URL(target);
    const pathname = String(parsed.pathname || '').trim();
    const shortPath = pathname && pathname !== '/' ? pathname.slice(0, 48) : '';
    return `${parsed.host}${shortPath}`;
  } catch (_) {
    return target.slice(0, 72);
  }
}

function trimStatusText(value, maxLen = 120) {
  const text = String(value || '').replace(/\s+/g, ' ').trim();
  if (!text) return '';
  if (text.length <= maxLen) return text;
  return `${text.slice(0, Math.max(1, maxLen - 3))}...`;
}

function formatToolStatusStart(toolName, args = {}) {
  const name = String(toolName || '').trim();
  const payload = (args && typeof args === 'object') ? args : {};
  if (name === 'list_context_files') return 'Listing context files...';
  if (name === 'read_context_file') return `Reading context file ${trimStatusText(payload.file_id || '', 36) || '(selected file)'}...`;
  if (name === 'search_reference_graph') return `Searching reference graph for "${trimStatusText(payload.query || '', 90)}"...`;
  if (name === 'read_artifact') return `Reading artifact ${trimStatusText(payload.artifact_id || '', 36) || '(selected artifact)'}...`;
  if (name === 'list_artifacts') return 'Listing artifacts...';
  if (name === 'list_highlights') return 'Reading highlights...';
  if (name === 'search_local_evidence') return `Ranking local evidence for "${trimStatusText(payload.query || '', 90)}"...`;
  if (name === 'open_web_tab') return `Opening web tab: ${summarizeUrlForStatus(payload.url)}`;
  if (name === 'add_web_highlight') return `Adding web highlight for ${summarizeUrlForStatus(payload.url)}`;
  if (name === 'add_artifact_highlight') {
    return `Adding artifact highlight for ${trimStatusText(payload.artifact_id || '', 40) || '(artifact)' }...`;
  }
  if (name === 'clear_highlights') {
    const targetType = String(payload.target_type || '').trim().toLowerCase();
    if (targetType === 'artifact') {
      return `Clearing highlights for artifact ${trimStatusText(payload.artifact_id || '', 40) || '(artifact)' }...`;
    }
    return `Clearing highlights for ${summarizeUrlForStatus(payload.url)}`;
  }
  if (name === 'web_search') {
    return `Searching the web for "${trimStatusText(payload.query, 90)}"...`;
  }
  if (name === 'fetch_webpage') {
    return `Reading webpage: ${summarizeUrlForStatus(payload.url)}`;
  }
  if (name === 'write_markdown_artifact' || name === 'write_html_artifact') {
    return `Writing artifact "${trimStatusText(payload.title || 'Research Draft', 80)}"...`;
  }
  if (name === 'create_artifact') {
    return `Creating artifact "${trimStatusText(payload.title || 'Agent Artifact', 80)}"...`;
  }
  if (name === 'run_python') {
    return 'Running Python sandbox...';
  }
  if (name === 'pip_install') {
    const packages = Array.isArray(payload.packages)
      ? payload.packages.map((item) => trimStatusText(item, 32)).filter(Boolean)
      : [];
    return packages.length > 0
      ? `Installing Python package(s): ${packages.join(', ')}`
      : 'Installing Python packages...';
  }
  if (name === 'run_skill') {
    return `Running skill "${trimStatusText(payload.name || 'unnamed', 80)}"...`;
  }
  return `Using tool: ${name || 'unknown'}`;
}

function formatPythonBridgeStatusStart(toolName, args = {}) {
  const name = String(toolName || '').trim();
  const payload = (args && typeof args === 'object') ? args : {};
  if (name === 'list_context_files') return 'Listing context files...';
  if (name === 'read_context_file') return `Reading context file ${trimStatusText(payload.file_id || '', 36) || '(selected file)'}...`;
  if (name === 'search_reference_graph') return `Searching reference graph for "${trimStatusText(payload.query || '', 90)}"...`;
  if (name === 'read_artifact') return `Reading artifact ${trimStatusText(payload.artifact_id || '', 36) || '(selected artifact)'}...`;
  if (name === 'list_artifacts') return 'Listing artifacts...';
  if (name === 'list_highlights') return 'Reading highlights...';
  return `Running Python bridge tool: ${name || 'unknown'}`;
}

function formatPythonBridgeStatusDone(toolName, result) {
  const name = String(toolName || '').trim();
  const payload = result;
  if (name === 'list_context_files' && Array.isArray(payload)) {
    return `Listed ${payload.length} context file(s).`;
  }
  if (name === 'list_artifacts' && Array.isArray(payload)) {
    return `Listed ${payload.length} artifact(s).`;
  }
  if (name === 'search_reference_graph' && payload && typeof payload === 'object') {
    const nodeCount = Array.isArray(payload.nodes) ? payload.nodes.length : 0;
    const edgeCount = Array.isArray(payload.edges) ? payload.edges.length : 0;
    return `Reference graph search returned ${nodeCount} node(s), ${edgeCount} edge(s).`;
  }
  if (name === 'read_context_file') return 'Context file read complete.';
  if (name === 'read_artifact') return 'Artifact read complete.';
  if (name === 'list_highlights' && Array.isArray(payload)) {
    return `Read ${payload.length} highlight(s).`;
  }
  return `${name || 'Python bridge tool'} completed.`;
}

function isMemoryArtifact(artifact) {
  const title = String((artifact && artifact.title) || '').trim();
  const content = String((artifact && artifact.content) || '');
  return title === MEMORY_ARTIFACT_TITLE || content.includes(MEMORY_ARTIFACT_MARKER);
}

function buildDetailedMemoryEntry(userMessage, assistantText, deliverableArtifact) {
  const lines = [
    `Request: ${trimForPrompt(String(userMessage || '').trim(), 220) || '(none)'}`,
  ];
  if (deliverableArtifact) {
    lines.push(`Deliverable Artifact: ${trimForPrompt(String(deliverableArtifact.title || 'Artifact'), 120)} (${String(deliverableArtifact.id || '').trim()})`);
  } else {
    lines.push('Deliverable Artifact: (not created)');
  }
  const summary = trimForPrompt(String(assistantText || '').trim(), 420);
  lines.push(`Summary: ${summary || '(no summary)'}`);
  return lines.join('\n');
}

async function executeLuminoChat(input, options = {}) {
  const payload = (input && typeof input === 'object') ? input : {};
  const srId = String(payload.sr_id || payload.srId || '').trim();
  const message = String(payload.message || '').trim();
  const requestId = String(payload.request_id || makeId('req')).trim();
  const provider = String(payload.provider || '').trim().toLowerCase();
  const model = String(payload.model || '').trim();
  const lane = String((options && options.lane) || payload.lane || 'path_a').trim().toLowerCase() || 'path_a';
  const agentModeRequested = true;
  const onDelta = typeof options.onDelta === 'function' ? options.onDelta : null;
  const onStatus = typeof options.onStatus === 'function' ? options.onStatus : null;
  const signal = options.signal;
  const timeoutMs = Number.isFinite(Number(options.timeoutMs))
    ? Math.max(8_000, Number(options.timeoutMs))
    : CHAT_REQUEST_TIMEOUT_MS;
  const emitStatus = (stateValue, sourceValue, textValue, extra = {}) => {
    if (!onStatus) return;
    const statusPayload = {
      state: String(stateValue || 'info'),
      source: String(sourceValue || 'agent'),
      text: String(textValue || '').trim(),
      ...((extra && typeof extra === 'object') ? extra : {}),
    };
    if (!statusPayload.text) return;
    onStatus(statusPayload);
  };

  const refs = getReferences();
  const idx = findReferenceIndex(refs, srId);
  if (idx < 0) {
    throw new Error('Active reference not found.');
  }

  const activeRef = refs[idx];
  const activeArtifactContext = resolveActiveArtifactContext(payload, activeRef, srId);
  const scopedRefs = getPathCScopedReferences(srId, refs);
  const pathcHarness = buildPathCHarnessPayload(activeRef, scopedRefs);
  const runtimePayload = {
    message,
    sr_id: srId,
    request_id: requestId,
    provider,
    model,
    sr_artifacts: Array.isArray(activeRef.artifacts) ? activeRef.artifacts : [],
    sr_context_files: Array.isArray(activeRef.context_files) ? activeRef.context_files : [],
    sr_all_refs: scopedRefs,
    pathc_harness: pathcHarness,
    active_surface: (payload.active_surface && typeof payload.active_surface === 'object')
      ? payload.active_surface
      : null,
    active_artifact: activeArtifactContext,
    sr_chat_thread: (activeRef.chat_thread && typeof activeRef.chat_thread === 'object')
      ? activeRef.chat_thread
      : { messages: [] },
  };

  const command = parseChatCommand(message);
  const baseResponse = handleChat(runtimePayload);
  if (!command || command.type !== 'plain') {
    if (command && command.type === 'crawl_start') {
      const crawlConfig = sanitizeCrawlerSettings(command);
      const startRes = await luminoCrawler.start({
        sr_id: srId,
        source_type: command.source_type || 'web',
        url: command.url || '',
        absolute_path: command.absolute_path || '',
        depth: crawlConfig.depth,
        page_cap: crawlConfig.page_cap,
        mode: crawlConfig.mode,
        markdown_first: crawlConfig.markdown_first,
        robots_policy: crawlConfig.robots_policy,
      });
      return {
        ...baseResponse,
        message: startRes && startRes.ok
          ? `Crawler started (${startRes.job.id}) for ${command.source_type === 'local' ? 'local source' : command.url}.`
          : String((startRes && startRes.message) || 'Unable to start crawler job.'),
        request_id: requestId,
        sr_id: srId,
      };
    }
    if (command && command.type === 'crawl_status') {
      const statusRes = luminoCrawler.getStatus(command.job_id || '');
      return {
        ...baseResponse,
        message: statusRes && statusRes.ok
          ? (statusRes.job
            ? `Crawler ${statusRes.job.id}: ${statusRes.job.state}, visited ${statusRes.job.visited_count}, results ${statusRes.job.result_count}.`
            : `Crawler jobs: ${Array.isArray(statusRes.jobs) ? statusRes.jobs.length : 0}.`)
          : String((statusRes && statusRes.message) || 'Crawler status unavailable.'),
        request_id: requestId,
        sr_id: srId,
      };
    }
    if (command && command.type === 'crawl_stop') {
      const latestJob = getLatestCrawlerJobForReference(srId);
      const stopTarget = command.job_id || (latestJob && latestJob.id) || '';
      const stopRes = luminoCrawler.stop(stopTarget);
      return {
        ...baseResponse,
        message: stopRes && stopRes.ok
          ? `Crawler stop requested for ${stopTarget || 'latest job'}.`
          : String((stopRes && stopRes.message) || 'Unable to stop crawler job.'),
        request_id: requestId,
        sr_id: srId,
      };
    }
    return {
      ...baseResponse,
      request_id: requestId,
      sr_id: srId,
    };
  }

  if (!provider) {
    throw new Error('Select a provider before sending to Lumino.');
  }
  if (!model) {
    throw new Error('Select a model before sending to Lumino.');
  }

  const providerCreds = resolveProviderRuntimeCredentials(provider, readSettings(), String(payload.provider_key_id || payload.key_id || ''));
  if (!providerCreds || !providerCreds.ok) {
    throw new Error(String((providerCreds && providerCreds.message) || `Provider credentials are not configured for ${provider}.`));
  }
  const providerApiKey = String(providerCreds.apiKey || '');
  const providerBaseUrl = String(providerCreds.base_url || '').trim();

  const providerSupportsAgentMode = AGENT_MODE_SUPPORTED_PROVIDERS.includes(provider);
  const canUseAgentMode = providerSupportsAgentMode;
  const prompts = buildLuminoProviderPrompts(message, activeRef, scopedRefs, {
    toolingEnabled: canUseAgentMode,
    activeArtifactContext,
  });
  const isPathBDelegation = !!payload.path_b_delegate;
  const isDetailed = isPathBDelegation ? false : isDetailedDeliverableRequest(message);
  const isResearchIntent = isPathBDelegation ? false : isResearchIntentRequest(message);
  const researchPolicy = {
    isDetailed,
    requiresWebResearch: isDetailed && isResearchIntent,
    requireDeliverableBeforeFinish: isDetailed,
    localEvidenceAvailable: hasLocalEvidenceForResearch(activeRef),
    requiresCitations: isResearchIntent,
    citationMode: 'hybrid',
    minCitationsShort: 1,
    minCitationsDetailed: 2,
    requireMarkerForQuotedClaims: true,
    allowRecoveryReprompt: true,
    maxRecoveryTurns: 1,
  };
  let agentModeNotice = '';
  if (agentModeRequested && !providerSupportsAgentMode) {
    agentModeNotice = `Agent mode supports ${AGENT_MODE_SUPPORTED_PROVIDERS.join(', ')} only; using standard chat.`;
  }

  let finalResult;

  if (canUseAgentMode) {
    const mergeArrays = (target, source, key) => {
      const incoming = Array.isArray(source && source[key]) ? source[key] : [];
      if (incoming.length === 0) return;
      if (!Array.isArray(target[key])) target[key] = [];
      incoming.forEach((item) => target[key].push(item));
    };
    const agentResult = await executeAgenticLoop({
      provider,
      model,
      apiKey: providerApiKey,
      baseUrl: providerBaseUrl,
      systemPrompt: prompts.systemPrompt,
      userPrompt: prompts.userPrompt,
      signal,
      maxTurns: AGENTIC_MAX_TURNS,
      onDelta,
      onStatus: (statusPayload) => {
        const status = (statusPayload && typeof statusPayload === 'object') ? statusPayload : {};
        const stateValue = String(status.state || 'info').trim().toLowerCase() || 'info';
        const sourceValue = String(status.source || 'agent').trim().toLowerCase() || 'agent';
        const toolName = String(status.tool_name || '').trim();
        const meta = (status.meta && typeof status.meta === 'object') ? status.meta : {};
        if (sourceValue === 'tool' && stateValue === 'start') {
          emitStatus('start', 'tool', formatToolStatusStart(toolName, meta.args || {}), {
            tool_name: toolName,
            meta,
          });
          return;
        }
        if (sourceValue === 'tool' && (stateValue === 'done' || stateValue === 'error')) {
          emitStatus(stateValue, 'tool', String(status.text || `${toolName || 'Tool'} completed.`), {
            tool_name: toolName,
            meta,
          });
          return;
        }
        emitStatus(stateValue, sourceValue, String(status.text || 'Agent update.'), { meta });
      },
      callProviderWithTools: async (providerPayload, providerOptions = {}) => callProviderWithTools({
        ...((providerPayload && typeof providerPayload === 'object') ? providerPayload : {}),
        baseUrl: providerBaseUrl,
      }, {
        signal,
        timeoutMs,
        ...((providerOptions && typeof providerOptions === 'object') ? providerOptions : {}),
      }),
      researchPolicy,
      executeTool: async (toolInput) => {
        const name = String((toolInput && toolInput.name) || '').trim();
        const args = (toolInput && toolInput.arguments && typeof toolInput.arguments === 'object')
          ? toolInput.arguments
          : {};
        if (!name) {
          return {
            ok: false,
            message: 'Tool name missing.',
            tool_output: { ok: false, message: 'Tool name missing.' },
          };
        }

        const toolAllowed = lane === 'path_b'
          ? isPathBToolAllowed(name)
          : isPathAToolAllowed(name);
        if (!toolAllowed) {
          return {
            ok: false,
            message: `Tool ${name} is not permitted in ${lane}.`,
            tool_output: {
              ok: false,
              denied: true,
              lane,
              tool_name: name,
              message: `Tool ${name} is not permitted in ${lane}.`,
            },
          };
        }

        if (name === 'run_python') {
          const code = String(args.code || '');
          const reason = String(args.reason || '').trim();
          const toolBridge = async (req) => {
            const bridgeReq = (req && typeof req === 'object') ? req : {};
            const bridgeName = String(bridgeReq.name || '').trim();
            const bridgeArgs = (bridgeReq.args && typeof bridgeReq.args === 'object') ? bridgeReq.args : {};
            emitStatus('start', 'python_bridge', formatPythonBridgeStatusStart(bridgeName, bridgeArgs), {
              tool_name: bridgeName,
            });
            try {
              const bridgeResult = dispatchProgrammaticTool(bridgeReq, { srId, refs: getReferences() });
              const hasError = !!(bridgeResult && typeof bridgeResult === 'object' && String(bridgeResult.error || '').trim());
              if (hasError) {
                emitStatus('error', 'python_bridge', `Python bridge ${bridgeName || 'tool'} failed: ${String(bridgeResult.error || '').trim()}`, {
                  tool_name: bridgeName,
                });
              } else {
                emitStatus('done', 'python_bridge', formatPythonBridgeStatusDone(bridgeName, bridgeResult), {
                  tool_name: bridgeName,
                });
              }
              return bridgeResult;
            } catch (bridgeErr) {
              emitStatus('error', 'python_bridge', `Python bridge ${bridgeName || 'tool'} failed: ${String((bridgeErr && bridgeErr.message) || 'unknown error')}`, {
                tool_name: bridgeName,
              });
              throw bridgeErr;
            }
          };
          const runRes = await runPythonForReference(srId, code, Math.min(timeoutMs, 45_000), {
            toolBridge,
            bridgeToolNames: PROGRAMMATIC_TOOL_NAMES,
          }, 'tool');
          const pending_workspace_tabs = [];
          const pending_artifacts = [];
          const pythonArtifact = buildPythonArtifactPendingOutput(srId, reason || 'Python Output', runRes);
          if (pythonArtifact && pythonArtifact.artifact) pending_artifacts.push(pythonArtifact.artifact);
          if (pythonArtifact && pythonArtifact.workspace_tab) pending_workspace_tabs.push(pythonArtifact.workspace_tab);
          return {
            ok: !!(runRes && runRes.ok),
            message: runRes && runRes.ok ? 'Python execution completed.' : String((runRes && runRes.stderr) || 'Python execution failed.'),
            tool_output: {
              ok: !!(runRes && runRes.ok),
              stdout: String((runRes && runRes.stdout) || ''),
              stderr: String((runRes && runRes.stderr) || ''),
              timed_out: !!(runRes && runRes.timed_out),
              execution_id: String((runRes && runRes.execution_id) || ''),
            },
            pending_artifacts,
            pending_workspace_tabs,
          };
        }

        if (name === 'pip_install') {
          if (app.isPackaged) {
            return {
              ok: false,
              message: PACKAGED_PYTHON_IMMUTABLE_MESSAGE,
              tool_output: {
                ok: false,
                installed: [],
                rejected: [],
                stdout: '',
                stderr: PACKAGED_PYTHON_IMMUTABLE_MESSAGE,
                timed_out: false,
                allowlist: [],
              },
            };
          }
          const packages = Array.isArray(args.packages)
            ? args.packages
            : String(args.packages || '').split(',').map((item) => String(item || '').trim()).filter(Boolean);
          const runtime = await getPythonRuntimeResolver().resolve('tool');
          const installRes = await installAllowedPackages({
            packages,
            pythonBin: String((runtime && runtime.python_bin) || 'python3').trim() || 'python3',
            cwd: app.getPath('userData'),
            timeoutMs: Math.min(timeoutMs, 120_000),
          });
          return {
            ok: !!(installRes && installRes.ok),
            message: installRes && installRes.ok
              ? `Installed package(s): ${(installRes.installed || []).join(', ')}`
              : String((installRes && installRes.stderr) || 'Package install failed.'),
            tool_output: installRes || { ok: false, message: 'Package install failed.' },
          };
        }

        if (name === 'save_skill') {
          const saveRes = upsertSkillForReference(srId, {
            name: String(args.name || ''),
            description: String(args.description || ''),
            code: String(args.code || ''),
          }, String(args.scope || 'local'));
          if (saveRes && saveRes.ok) {
            const refs = getReferences();
            const saveIdx = findReferenceIndex(refs, srId);
            if (saveIdx >= 0) {
              const tabRes = ensureSingleSkillsTab(refs[saveIdx]);
              if (tabRes && tabRes.created) {
                refs[saveIdx].updated_at = nowTs();
                setReferences(refs);
              }
            }
          }
          return {
            ok: !!(saveRes && saveRes.ok),
            message: saveRes && saveRes.ok
              ? `Saved skill "${String((saveRes.skill && saveRes.skill.name) || '')}".`
              : String((saveRes && saveRes.message) || 'Unable to save skill.'),
            tool_output: saveRes && saveRes.ok
              ? { ok: true, skill: saveRes.skill }
              : { ok: false, message: String((saveRes && saveRes.message) || 'Unable to save skill.') },
          };
        }

        if (name === 'run_skill') {
          const refs = getReferences();
          const refIdx = findReferenceIndex(refs, srId);
          if (refIdx < 0) {
            return { ok: false, message: 'Reference not found.', tool_output: { ok: false, message: 'Reference not found.' } };
          }
          const selected = findSkillByName(refs[refIdx], String(args.name || ''), String(args.scope || ''));
          if (!selected) {
            return { ok: false, message: 'Skill not found for this reference.', tool_output: { ok: false, message: 'Skill not found.' } };
          }
          const runRes = await runSkillForReference(srId, selected.id, selected.scope, args.args);
          const pending_workspace_tabs = [];
          const pending_artifacts = [];
          const pythonArtifact = buildPythonArtifactPendingOutput(srId, `${selected.name} Output`, runRes);
          if (pythonArtifact && pythonArtifact.artifact) pending_artifacts.push(pythonArtifact.artifact);
          if (pythonArtifact && pythonArtifact.workspace_tab) pending_workspace_tabs.push(pythonArtifact.workspace_tab);
          return {
            ok: !!(runRes && runRes.ok),
            message: runRes && runRes.ok
              ? `Executed skill "${selected.name}".`
              : String((runRes && runRes.stderr) || 'Skill execution failed.'),
            tool_output: {
              ok: !!(runRes && runRes.ok),
              skill_id: selected.id,
              skill_name: selected.name,
              stdout: String((runRes && runRes.stdout) || ''),
              stderr: String((runRes && runRes.stderr) || ''),
              timed_out: !!(runRes && runRes.timed_out),
              execution_id: String((runRes && runRes.execution_id) || ''),
            },
            pending_artifacts,
            pending_workspace_tabs,
          };
        }

        if (name === 'search_local_evidence') {
          const query = String(args.query || '').trim();
          if (!query) {
            return { ok: false, message: 'query is required.', tool_output: { ok: false, message: 'query is required.' } };
          }
          const topK = Math.max(1, Math.min(Number(Number.isFinite(Number(args.top_k)) ? args.top_k : 8), 24));
          const includeKinds = Array.isArray(args.include_kinds)
            ? args.include_kinds.map((item) => String(item || '').trim().toLowerCase()).filter(Boolean)
            : [];
          const searchRes = searchLocalEvidence(query, scopedRefs, {
            topK,
            includeKinds,
          });
          const results = Array.isArray(searchRes && searchRes.results) ? searchRes.results : [];
          const citations = Array.isArray(searchRes && searchRes.citations) ? searchRes.citations : [];
          return {
            ok: !!(searchRes && searchRes.ok),
            message: `Ranked ${results.length} local evidence result(s) for "${query}".`,
            tool_output: {
              ok: !!(searchRes && searchRes.ok),
              query,
              method: String((searchRes && searchRes.method) || ''),
              results,
              citations,
            },
          };
        }

        if (name === 'open_web_tab') {
          const rawUrl = String(args.url || '').trim();
          if (!rawUrl) {
            return { ok: false, message: 'url is required.', tool_output: { ok: false, message: 'url is required.' } };
          }
          let parsedUrl = '';
          try {
            const parsed = new URL(rawUrl);
            const protocol = String(parsed.protocol || '').toLowerCase();
            if (protocol !== 'http:' && protocol !== 'https:') {
              return {
                ok: false,
                message: 'Only http(s) URLs are supported for open_web_tab.',
                tool_output: { ok: false, message: 'Only http(s) URLs are supported for open_web_tab.' },
              };
            }
            parsedUrl = parsed.toString();
          } catch (_) {
            return { ok: false, message: 'Invalid URL.', tool_output: { ok: false, message: 'Invalid URL.' } };
          }
          const title = String(args.title || parsedUrl).trim().slice(0, 180) || parsedUrl;
          return {
            ok: true,
            message: `Opened web tab for ${summarizeUrlForStatus(parsedUrl)}.`,
            tool_output: {
              ok: true,
              url: parsedUrl,
              title,
            },
            pending_workspace_tabs: [{
              type: 'web',
              reference_id: srId,
              url: parsedUrl,
              title,
            }],
          };
        }

        if (name === 'add_web_highlight') {
          const rawUrl = String(args.url || '').trim();
          const text = String(args.text || '').trim();
          if (!rawUrl) {
            return { ok: false, message: 'url is required.', tool_output: { ok: false, message: 'url is required.' } };
          }
          if (!text) {
            return { ok: false, message: 'text is required.', tool_output: { ok: false, message: 'text is required.' } };
          }
          let parsedUrl = '';
          try {
            const parsed = new URL(rawUrl);
            const protocol = String(parsed.protocol || '').toLowerCase();
            if (protocol !== 'http:' && protocol !== 'https:') {
              return {
                ok: false,
                message: 'Only http(s) URLs are supported for add_web_highlight.',
                tool_output: { ok: false, message: 'Only http(s) URLs are supported for add_web_highlight.' },
              };
            }
            parsedUrl = parsed.toString();
          } catch (_) {
            return { ok: false, message: 'Invalid URL.', tool_output: { ok: false, message: 'Invalid URL.' } };
          }
          const refs = getReferences();
          const refIdx = findReferenceIndex(refs, srId);
          if (refIdx < 0) {
            return { ok: false, message: 'Reference not found.', tool_output: { ok: false, message: 'Reference not found.' } };
          }
          const addRes = addWebHighlight(refs[refIdx], {
            source: 'web',
            url: parsedUrl,
            url_norm: normalizeUrlForMatch(parsedUrl),
            text,
            context_before: String(args.context_before || ''),
            context_after: String(args.context_after || ''),
            web_start: Number.isFinite(Number(args.web_start)) ? Math.round(Number(args.web_start)) : null,
            web_end: Number.isFinite(Number(args.web_end)) ? Math.round(Number(args.web_end)) : null,
          });
          if (!addRes.ok) {
            return {
              ok: false,
              message: String(addRes.message || 'Unable to add web highlight.'),
              tool_output: { ok: false, message: String(addRes.message || 'Unable to add web highlight.') },
            };
          }
          setReferences(refs);
          syncMarkerStateToBrowserView();
          return {
            ok: true,
            message: addRes.added ? 'Web highlight added.' : 'Web highlight already exists.',
            tool_output: {
              ok: true,
              added: !!addRes.added,
              highlight: addRes.highlight || null,
            },
          };
        }

        if (name === 'add_artifact_highlight') {
          const artifactId = String(args.artifact_id || '').trim();
          const startRaw = Number(args.artifact_start);
          const endRaw = Number(args.artifact_end);
          if (!artifactId) {
            return { ok: false, message: 'artifact_id is required.', tool_output: { ok: false, message: 'artifact_id is required.' } };
          }
          if (!Number.isFinite(startRaw) || !Number.isFinite(endRaw)) {
            return {
              ok: false,
              message: 'artifact_start and artifact_end must be numbers.',
              tool_output: { ok: false, message: 'artifact_start and artifact_end must be numbers.' },
            };
          }
          const refs = getReferences();
          const refIdx = findReferenceIndex(refs, srId);
          if (refIdx < 0) {
            return { ok: false, message: 'Reference not found.', tool_output: { ok: false, message: 'Reference not found.' } };
          }
          const artifacts = Array.isArray(refs[refIdx].artifacts) ? refs[refIdx].artifacts : [];
          const artifact = artifacts.find((item) => String((item && item.id) || '').trim() === artifactId) || null;
          if (!artifact) {
            return { ok: false, message: 'Artifact not found.', tool_output: { ok: false, message: 'Artifact not found.' } };
          }
          const content = String((artifact && artifact.content) || '');
          const start = Math.max(0, Math.min(Math.round(startRaw), content.length));
          const end = Math.max(start, Math.min(Math.round(endRaw), content.length));
          if (end <= start) {
            return {
              ok: false,
              message: 'artifact_end must be greater than artifact_start.',
              tool_output: { ok: false, message: 'artifact_end must be greater than artifact_start.' },
            };
          }
          const providedText = String(args.text || '').trim();
          const derivedText = content.slice(start, end).trim();
          const text = providedText || derivedText;
          if (!text) {
            return {
              ok: false,
              message: 'Could not derive non-empty artifact highlight text from the selected range.',
              tool_output: { ok: false, message: 'Could not derive non-empty artifact highlight text from the selected range.' },
            };
          }
          const addRes = addArtifactHighlight(refs[refIdx], {
            source: 'artifact',
            artifact_id: artifactId,
            artifact_start: start,
            artifact_end: end,
            text,
          });
          if (!addRes.ok) {
            return {
              ok: false,
              message: String(addRes.message || 'Unable to add artifact highlight.'),
              tool_output: { ok: false, message: String(addRes.message || 'Unable to add artifact highlight.') },
            };
          }
          setReferences(refs);
          syncMarkerStateToBrowserView();
          return {
            ok: true,
            message: addRes.added ? 'Artifact highlight added.' : 'Artifact highlight already exists.',
            tool_output: {
              ok: true,
              added: !!addRes.added,
              highlight: addRes.highlight || null,
            },
          };
        }

        if (name === 'clear_highlights') {
          const targetType = String(args.target_type || '').trim().toLowerCase();
          if (targetType !== 'url' && targetType !== 'artifact') {
            return {
              ok: false,
              message: 'target_type must be "url" or "artifact".',
              tool_output: { ok: false, message: 'target_type must be "url" or "artifact".' },
            };
          }
          const refs = getReferences();
          const refIdx = findReferenceIndex(refs, srId);
          if (refIdx < 0) {
            return { ok: false, message: 'Reference not found.', tool_output: { ok: false, message: 'Reference not found.' } };
          }
          let clearTarget = null;
          if (targetType === 'url') {
            const rawUrl = String(args.url || '').trim();
            if (!rawUrl) {
              return { ok: false, message: 'url is required when target_type=url.', tool_output: { ok: false, message: 'url is required when target_type=url.' } };
            }
            let parsedUrl = '';
            try {
              const parsed = new URL(rawUrl);
              const protocol = String(parsed.protocol || '').toLowerCase();
              if (protocol !== 'http:' && protocol !== 'https:') {
                return {
                  ok: false,
                  message: 'Only http(s) URLs are supported for clear_highlights target_type=url.',
                  tool_output: { ok: false, message: 'Only http(s) URLs are supported for clear_highlights target_type=url.' },
                };
              }
              parsedUrl = parsed.toString();
            } catch (_) {
              return { ok: false, message: 'Invalid URL.', tool_output: { ok: false, message: 'Invalid URL.' } };
            }
            clearTarget = { type: 'web', url: parsedUrl };
          } else {
            const artifactId = String(args.artifact_id || '').trim();
            if (!artifactId) {
              return {
                ok: false,
                message: 'artifact_id is required when target_type=artifact.',
                tool_output: { ok: false, message: 'artifact_id is required when target_type=artifact.' },
              };
            }
            clearTarget = { type: 'artifact', artifact_id: artifactId };
          }
          const clearRes = clearHighlightsByTarget(refs[refIdx], clearTarget);
          if (!clearRes.target) {
            return {
              ok: false,
              message: 'Unable to resolve highlight clear target.',
              tool_output: { ok: false, message: 'Unable to resolve highlight clear target.' },
            };
          }
          setReferences(refs);
          syncMarkerStateToBrowserView();
          return {
            ok: true,
            message: clearRes.removed_count > 0
              ? `Cleared ${clearRes.removed_count} highlight(s).`
              : 'No highlights matched the requested target.',
            tool_output: {
              ok: true,
              removed_count: Number(clearRes.removed_count || 0),
              target: clearRes.target,
            },
          };
        }

        if (
          name === 'list_artifacts'
          || name === 'read_artifact'
          || name === 'list_highlights'
          || name === 'search_reference_graph'
          || name === 'list_context_files'
          || name === 'read_context_file'
        ) {
          try {
            const directResult = dispatchProgrammaticTool({ name, args }, { srId, refs: getReferences() });
            const hasError = !!(directResult && typeof directResult === 'object' && String(directResult.error || '').trim());
            if (hasError) {
              return {
                ok: false,
                message: String(directResult.error || `${name} failed.`),
                tool_output: { ok: false, message: String(directResult.error || `${name} failed.`) },
              };
            }
            const missingReadResult = (
              (name === 'read_artifact' || name === 'read_context_file')
              && !directResult
            );
            if (missingReadResult) {
              return {
                ok: false,
                message: `${name} target was not found.`,
                tool_output: { ok: false, message: `${name} target was not found.` },
              };
            }
            return {
              ok: true,
              message: formatPythonBridgeStatusDone(name, directResult),
              tool_output: {
                ok: true,
                result: directResult,
              },
            };
          } catch (directErr) {
            return {
              ok: false,
              message: String((directErr && directErr.message) || `${name} failed.`),
              tool_output: { ok: false, message: String((directErr && directErr.message) || `${name} failed.`) },
            };
          }
        }

        if (name === 'create_artifact') {
          const artifactTitle = String(args.title || 'Agent Artifact').trim().slice(0, 180) || 'Agent Artifact';
          const artifactContent = String(args.content || '');
          const explicitType = String(args.artifact_type || '').trim();
          const requestedType = explicitType ? normalizeArtifactType(explicitType) : '';
          const artifactId = String(resolveImplicitArtifactId({
            providedId: args.artifact_id,
            requestedType,
            activeArtifactContext,
            message,
          }) || makeId('artifact')).trim();
          const artifactType = requestedType
            || (
              activeArtifactContext
              && String(activeArtifactContext.id || '') === artifactId
              ? normalizeArtifactType(activeArtifactContext.type || 'markdown')
              : 'markdown'
            );
          const artifact = {
            id: artifactId,
            reference_id: srId,
            type: artifactType,
            title: artifactTitle,
            content: artifactContent,
            created_at: nowTs(),
            updated_at: nowTs(),
          };
          return {
            ok: true,
            message: `Artifact prepared: ${artifactTitle}`,
            tool_output: { ok: true, artifact_id: artifactId, title: artifactTitle, artifact_type: artifactType },
            pending_artifacts: [artifact],
            pending_workspace_tabs: [{
              type: 'artifact',
              reference_id: srId,
              artifact_id: artifactId,
              title: artifactTitle,
            }],
          };
        }

        if (name === 'web_search') {
          const query = String(args.query || '').trim();
          if (!query) {
            return { ok: false, message: 'query is required.', tool_output: { ok: false, message: 'query is required.' } };
          }
          const maxResults = Math.max(1, Math.min(Number(Number.isFinite(Number(args.max_results)) ? args.max_results : 5), 10));
          const ddgUrl = `https://api.duckduckgo.com/?q=${encodeURIComponent(query)}&format=json&no_redirect=1&no_html=1`;
          try {
            const fetchRes = await fetchJsonWithTimeout(ddgUrl, {}, 10_000);
            if (!fetchRes.ok) {
              return {
                ok: false,
                unavailable: true,
                message: `Web search is unavailable right now (status ${fetchRes.status || 'unknown'}).`,
                tool_output: {
                  ok: false,
                  query,
                  unavailable: true,
                  status: Number(fetchRes.status || 0) || null,
                  message: 'Web search request failed.',
                },
              };
            }
            const results = [];
            if (fetchRes.json) {
              const j = fetchRes.json;
              if (j.AbstractText && j.AbstractURL) {
                results.push({
                  title: String(j.Heading || query).slice(0, 120),
                  url: j.AbstractURL,
                  snippet: String(j.AbstractText).slice(0, 300),
                });
              }
              const topics = Array.isArray(j.RelatedTopics) ? j.RelatedTopics : [];
              topics.slice(0, maxResults).forEach((t) => {
                if (t && t.FirstURL && t.Text) {
                  results.push({
                    title: String(t.Text).slice(0, 120),
                    url: t.FirstURL,
                    snippet: String(t.Text).slice(0, 300),
                  });
                }
              });
            }
            return {
              ok: true,
              message: `Found ${results.length} result(s) for "${query}".`,
              tool_output: { ok: true, query, results },
            };
          } catch (searchErr) {
            return {
              ok: false,
              unavailable: true,
              message: `Web search is unavailable: ${String((searchErr && searchErr.message) || 'unknown error')}`,
              tool_output: {
                ok: false,
                query,
                unavailable: true,
                error: String((searchErr && searchErr.message) || 'unknown error'),
              },
            };
          }
        }

        if (name === 'fetch_webpage') {
          const url = String(args.url || '').trim();
          if (!url) {
            return { ok: false, message: 'url is required.', tool_output: { ok: false, message: 'url is required.' } };
          }
          const maxLength = Math.min(Number(Number.isFinite(Number(args.max_length)) ? args.max_length : 4_000), 8_000);
          try {
            const fetchRes = await fetchTextWithTimeout(url, {}, 12_000);
            if (!fetchRes.ok) {
              return {
                ok: false,
                message: `Failed to fetch ${url}: HTTP ${fetchRes.status || 'unknown'}`,
                tool_output: {
                  ok: false,
                  url,
                  status: Number(fetchRes.status || 0) || null,
                  message: `HTTP ${fetchRes.status || 'unknown'}`,
                },
              };
            }
            const html = String(fetchRes.text || '');
            const text = html.replace(/<[^>]+>/g, ' ').replace(/\s+/g, ' ').trim().slice(0, maxLength);
            return {
              ok: true,
              message: `Fetched ${url} (${text.length} chars).`,
              tool_output: { ok: true, url, content: text },
            };
          } catch (fetchErr) {
            return {
              ok: false,
              unavailable: true,
              message: `Failed to fetch ${url}: ${String((fetchErr && fetchErr.message) || 'unknown error')}`,
              tool_output: {
                ok: false,
                unavailable: true,
                url,
                error: String((fetchErr && fetchErr.message) || 'unknown error'),
              },
            };
          }
        }

        if (name === 'write_markdown_artifact' || name === 'write_html_artifact') {
          const artifactTitle = String(args.title || 'Research Draft').trim().slice(0, 180) || 'Research Draft';
          const artifactContent = String(args.content || '');
          const providedId = String(args.artifact_id || '').trim();
          const artifactType = name === 'write_html_artifact' ? 'html' : 'markdown';
          const resolvedId = resolveImplicitArtifactId({
            providedId,
            requestedType: artifactType,
            activeArtifactContext,
            message,
          }) || makeId('artifact');

          const artifact = {
            id: resolvedId,
            reference_id: srId,
            type: artifactType,
            title: artifactTitle,
            content: artifactContent,
            created_at: nowTs(),
            updated_at: nowTs(),
          };
          return {
            ok: true,
            message: `Artifact "${artifactTitle}" written.`,
            tool_output: { ok: true, artifact_id: resolvedId, title: artifactTitle, artifact_type: artifactType },
            pending_artifacts: [artifact],
            pending_workspace_tabs: [{
              type: 'artifact',
              reference_id: srId,
              artifact_id: resolvedId,
              title: artifactTitle,
            }],
          };
        }

        if (name === 'finish') {
          return {
            ok: true,
            finish: true,
            final_message: String(args.message || '').trim() || 'Done.',
            message: 'Agent requested finish.',
            tool_output: {
              ok: true,
              message: String(args.message || '').trim() || 'Done.',
            },
          };
        }

        return {
          ok: false,
          message: `Unsupported tool: ${name}`,
          tool_output: { ok: false, message: `Unsupported tool: ${name}` },
        };
      },
    });

    const merged = {
      ...baseResponse,
      message: String((agentResult && agentResult.message) || '').trim() || baseResponse.message,
    };
    [
      'pending_artifacts',
      'pending_workspace_tabs',
      'pending_diff_ops',
      'pending_hyperweb_queries',
      'pending_hyperweb_suggestions',
      'pending_weight_updates',
      'pending_decision_traces',
    ].forEach((key) => mergeArrays(merged, agentResult, key));

    finalResult = {
      ...merged,
      request_id: requestId,
      sr_id: srId,
      stopped_reason: String((agentResult && agentResult.stopped_reason) || '').trim(),
      research_policy_state: (agentResult && typeof agentResult.research_policy_state === 'object')
        ? agentResult.research_policy_state
        : null,
      policy_diagnostics: (agentResult && typeof agentResult.policy_diagnostics === 'object')
        ? agentResult.policy_diagnostics
        : null,
    };
  } else {
    emitStatus('info', 'provider', `Tool execution unavailable for ${provider}; generating direct response.`);
    const providerResult = await chatWithProvider({
      provider,
      model,
      apiKey: providerApiKey,
      baseUrl: providerBaseUrl,
      systemPrompt: prompts.systemPrompt,
      userPrompt: prompts.userPrompt,
    }, {
      signal,
      onDelta,
      timeoutMs,
    });

    const assistantText = String((providerResult && providerResult.text) || '').trim();
    finalResult = {
      ...baseResponse,
      message: [agentModeNotice, assistantText || baseResponse.message].filter(Boolean).join(' ').trim(),
      request_id: requestId,
      sr_id: srId,
    };
  }

  // Rolling memory update pipeline.
  const finalAssistantText = String(finalResult.message || '').trim();
  const isSubstantive = isSubstantiveAssistantReply(finalAssistantText);
  const researchState = (finalResult.research_policy_state && typeof finalResult.research_policy_state === 'object')
    ? finalResult.research_policy_state
    : {};
  const policyDiagnostics = (finalResult.policy_diagnostics && typeof finalResult.policy_diagnostics === 'object')
    ? finalResult.policy_diagnostics
    : {};
  const webRequirementSatisfied = !researchPolicy.requiresWebResearch || (
    Number(researchState.web_evidence_count || 0) > 0
    || !!researchState.web_unavailable
  );
  const citationRequirementSatisfied = !researchPolicy.requiresCitations || !!researchState.citation_gate_passed;
  if (!Array.isArray(finalResult.pending_artifacts)) finalResult.pending_artifacts = [];
  if (!Array.isArray(finalResult.pending_workspace_tabs)) finalResult.pending_workspace_tabs = [];
  let deliverableArtifact = finalResult.pending_artifacts.find((artifact) => !isMemoryArtifact(artifact)) || null;
  if (isDetailed && !deliverableArtifact) {
    if (String(finalResult.stopped_reason || '').trim() === 'required_phase_unmet_after_recovery') {
      const missingPhase = String(policyDiagnostics.missing_phase || '').trim().toLowerCase();
      if (missingPhase === 'citation' || missingPhase === 'citation_format' || missingPhase === 'marker') {
        finalResult.message = 'Citation requirements were not met. Gather local evidence with search_local_evidence and format output with footnote citations.';
      } else {
        finalResult.message = 'Required web research did not complete. Please retry or connect web access.';
      }
    } else if (isSubstantive && webRequirementSatisfied && citationRequirementSatisfied) {
      const fallbackArtifact = {
        id: makeId('artifact'),
        reference_id: srId,
        type: 'markdown',
        title: 'Detailed Response',
        content: finalAssistantText,
        created_at: nowTs(),
        updated_at: nowTs(),
      };
      finalResult.pending_artifacts.push(fallbackArtifact);
      finalResult.pending_workspace_tabs.push({
        type: 'artifact',
        reference_id: srId,
        artifact_id: fallbackArtifact.id,
        title: fallbackArtifact.title,
      });
      deliverableArtifact = fallbackArtifact;
    } else if (isSubstantive && !webRequirementSatisfied) {
      finalResult.message = 'I need at least one successful web research step before creating the deliverable artifact.';
    } else if (isSubstantive && !citationRequirementSatisfied) {
      finalResult.message = 'I need sufficient citations in footnote format before creating the deliverable artifact.';
    } else {
      finalResult.message = 'I could not create the requested deliverable artifact in this workspace. Please try again.';
    }
  }
  if (!isDetailed && isSubstantive && !citationRequirementSatisfied) {
    const missingPhase = String(policyDiagnostics.missing_phase || '').trim().toLowerCase();
    if (missingPhase === 'citation' || missingPhase === 'citation_format' || missingPhase === 'marker') {
      finalResult.message = 'Citation requirements were not met. Use search_local_evidence, add marker-backed evidence for quoted claims, and provide footnote citations with a Sources section.';
    }
  }
  const shouldUpdateMemory = isDetailed ? !!deliverableArtifact : isSubstantive;
  if (shouldUpdateMemory) {
    const refsForMemory = getReferences();
    const memIdx = findReferenceIndex(refsForMemory, srId);
    if (memIdx >= 0) {
      const memRef = refsForMemory[memIdx];
      const memArtifact = ensureRollingMemoryArtifact(memRef, srId);
      const memoryText = isDetailed
        ? buildDetailedMemoryEntry(message, finalAssistantText, deliverableArtifact)
        : finalAssistantText;
      memArtifact.content = updateRollingMemorySections(memArtifact, memoryText, isDetailed);
      memArtifact.updated_at = nowTs();
      memRef.updated_at = nowTs();
      setReferences(refsForMemory);

      finalResult.pending_artifacts.push({ ...memArtifact, reference_id: srId });
    }
  }
  if (isDetailed && deliverableArtifact && /research memory artifact/i.test(String(finalResult.message || ''))) {
    finalResult.message = String(finalResult.message || '').replace(/research memory artifact/ig, `"${deliverableArtifact.title}" artifact`);
  }

  return finalResult;
}

function sendBrowserEvent(channel, payload) {
  if (!mainWindow || mainWindow.isDestroyed()) return;
  mainWindow.webContents.send(channel, payload);
}

function publishBrowserAudibleState(audible) {
  const next = !!audible;
  browserViewAudible = next;
  sendBrowserEvent('browser:audible', { audible: next });
}

function isBrowserPlaybackAllowed() {
  return !!browserViewVisible;
}

async function stopMediaPlaybackForWebContents(webContents, options = {}) {
  if (!webContents) return;
  const muteAudio = options.muteAudio !== false;
  const muteElements = options.muteElements !== false;
  try {
    if (muteAudio && typeof webContents.setAudioMuted === 'function') {
      webContents.setAudioMuted(true);
    }
  } catch (_) {
    // noop
  }
  try {
    await webContents.executeJavaScript(`
      (function () {
        try {
          const shouldMute = ${muteElements ? 'true' : 'false'};
          const media = Array.from(document.querySelectorAll('video, audio'));
          media.forEach((el) => {
            try {
              el.pause();
              if (shouldMute) el.muted = true;
              el.srcObject = null;
            } catch (_) {}
          });
          if (document.pictureInPictureElement && document.exitPictureInPicture) {
            document.exitPictureInPicture().catch(() => {});
          }
        } catch (_) {}
      })();
    `, true);
  } catch (_) {
    // noop
  }
}

async function enforceBrowserPlaybackPolicy() {
  const view = getOperationalBrowserView();
  if (!view || !view.webContents || view.webContents.isDestroyed()) return;
  if (isBrowserPlaybackAllowed()) {
    try {
      if (typeof view.webContents.setAudioMuted === 'function') {
        view.webContents.setAudioMuted(false);
      }
    } catch (_) {
      // noop
    }
    return;
  }
  await stopMediaPlaybackForWebContents(view.webContents, { muteAudio: true, muteElements: true });
  publishBrowserAudibleState(false);
}

function isUsableBrowserView(view) {
  if (!view || !view.webContents) return false;
  try {
    return !view.webContents.isDestroyed();
  } catch (_) {
    return false;
  }
}

function getActiveRuntimeTabEntry() {
  const tabId = String(activeRuntimeBrowserTabId || '').trim();
  if (!tabId) return null;
  const tab = runtimeBrowserTabs.get(tabId);
  if (!tab || !isUsableBrowserView(tab.view)) return null;
  return tab;
}

function getOperationalBrowserView() {
  const activeRuntime = getActiveRuntimeTabEntry();
  if (activeRuntime && isUsableBrowserView(activeRuntime.view)) return activeRuntime.view;
  if (isUsableBrowserView(browserView)) return browserView;
  return null;
}

function clampBrowserViewZoom(rawFactor) {
  const next = Number(rawFactor);
  if (!Number.isFinite(next)) return BROWSER_VIEW_DEFAULT_ZOOM;
  return Math.max(BROWSER_VIEW_MIN_ZOOM, Math.min(BROWSER_VIEW_MAX_ZOOM, next));
}

function getBrowserViewZoomFactor() {
  const view = getOperationalBrowserView();
  if (!view || !view.webContents || view.webContents.isDestroyed()) return BROWSER_VIEW_DEFAULT_ZOOM;
  try {
    const current = Number(view.webContents.getZoomFactor());
    if (!Number.isFinite(current)) return BROWSER_VIEW_DEFAULT_ZOOM;
    return clampBrowserViewZoom(current);
  } catch (_) {
    return BROWSER_VIEW_DEFAULT_ZOOM;
  }
}

function setBrowserViewZoomFactor(factor) {
  const view = getOperationalBrowserView();
  if (!view || !view.webContents || view.webContents.isDestroyed()) {
    return { ok: false, zoom: BROWSER_VIEW_DEFAULT_ZOOM, message: 'Browser view is not available.' };
  }
  const next = clampBrowserViewZoom(factor);
  try {
    view.webContents.setZoomFactor(next);
    return { ok: true, zoom: next };
  } catch (err) {
    return { ok: false, zoom: getBrowserViewZoomFactor(), message: String((err && err.message) || 'Unable to set browser zoom.') };
  }
}

function getAllRuntimeBrowserTabs() {
  return Array.from(runtimeBrowserTabs.entries()).map(([id, tab]) => ({
    id,
    url: String((tab && tab.url) || ''),
    title: String((tab && tab.title) || ''),
    favicon: (tab && tab.favicon) || null,
    loading: !!(tab && tab.loading),
    audible: !!(tab && tab.audible),
    active: id === activeRuntimeBrowserTabId,
    last_active: Number((tab && tab.last_active) || 0),
  }));
}

function setupRuntimeTabEventHandlers(tabId, view) {
  if (!isUsableBrowserView(view)) return;
  const runtimeTabId = String(tabId || '').trim();
  if (!runtimeTabId) return;

  view.webContents.on('did-navigate', (_event, url) => {
    const tab = runtimeBrowserTabs.get(runtimeTabId);
    if (tab) tab.url = String(url || '');
    if (runtimeTabId === String(activeRuntimeBrowserTabId || '')) {
      sendBrowserEvent('browser:did-navigate', { url });
      syncMarkerStateToBrowserView();
    }
  });

  view.webContents.on('did-navigate-in-page', (_event, url) => {
    const tab = runtimeBrowserTabs.get(runtimeTabId);
    if (tab) tab.url = String(url || '');
    if (runtimeTabId === String(activeRuntimeBrowserTabId || '')) {
      sendBrowserEvent('browser:did-navigate', { url });
      syncMarkerStateToBrowserView();
    }
  });

  view.webContents.on('page-title-updated', (_event, title) => {
    const tab = runtimeBrowserTabs.get(runtimeTabId);
    if (tab) tab.title = String(title || '');
    if (runtimeTabId === String(activeRuntimeBrowserTabId || '')) {
      sendBrowserEvent('browser:title-updated', { title });
    }
  });

  view.webContents.on('did-start-loading', () => {
    const tab = runtimeBrowserTabs.get(runtimeTabId);
    if (tab) tab.loading = true;
    if (runtimeTabId === String(activeRuntimeBrowserTabId || '')) {
      sendBrowserEvent('browser:loading', { loading: true });
    }
  });

  view.webContents.on('did-stop-loading', () => {
    const tab = runtimeBrowserTabs.get(runtimeTabId);
    if (tab) tab.loading = false;
    if (runtimeTabId === String(activeRuntimeBrowserTabId || '')) {
      sendBrowserEvent('browser:loading', { loading: false });
      syncMarkerStateToBrowserView();
    }
  });

  view.webContents.on('media-started-playing', () => {
    const tab = runtimeBrowserTabs.get(runtimeTabId);
    if (!tab) return;
    if (!isBrowserPlaybackAllowed() || runtimeTabId !== String(activeRuntimeBrowserTabId || '')) {
      tab.audible = false;
      stopMediaPlaybackForWebContents(view.webContents, { muteAudio: true, muteElements: true }).catch(() => {});
      return;
    }
    tab.audible = true;
    publishBrowserAudibleState(true);
  });

  view.webContents.on('media-paused', () => {
    const tab = runtimeBrowserTabs.get(runtimeTabId);
    if (tab) tab.audible = false;
    if (runtimeTabId === String(activeRuntimeBrowserTabId || '')) {
      publishBrowserAudibleState(false);
    }
  });

  view.webContents.setWindowOpenHandler(({ url }) => {
    if (/^https?:\/\//i.test(url)) {
      view.webContents.loadURL(url);
    }
    return { action: 'deny' };
  });
}

function createRuntimeBrowserTab(url, makeActive = true) {
  if (runtimeBrowserTabs.size >= MAX_RUNTIME_BROWSER_TABS) {
    return { ok: false, error: `Maximum runtime browser tabs reached (${MAX_RUNTIME_BROWSER_TABS}).` };
  }
  const runtimeTabId = makeId('rtab');
  const targetUrl = normalizeUrl(url);
  const view = new BrowserView({
    webPreferences: {
      nodeIntegration: false,
      contextIsolation: true,
      sandbox: true,
      preload: BROWSER_VIEW_PRELOAD_PATH,
    },
  });
  const tabState = {
    view,
    url: targetUrl,
    title: targetUrl,
    favicon: null,
    loading: true,
    audible: false,
    last_active: nowTs(),
  };
  runtimeBrowserTabs.set(runtimeTabId, tabState);
  setupRuntimeTabEventHandlers(runtimeTabId, view);
  view.webContents.loadURL(targetUrl);
  if (makeActive) {
    return switchRuntimeBrowserTab(runtimeTabId);
  }
  return { ok: true, tabId: runtimeTabId, activeTabId: activeRuntimeBrowserTabId, tabs: getAllRuntimeBrowserTabs() };
}

function switchRuntimeBrowserTab(runtimeTabId) {
  const nextTabId = String(runtimeTabId || '').trim();
  if (!nextTabId) return { ok: false, error: 'Tab id is required.' };
  const tab = runtimeBrowserTabs.get(nextTabId);
  if (!tab || !isUsableBrowserView(tab.view)) return { ok: false, error: 'Runtime tab not found.' };

  const previousTabId = String(activeRuntimeBrowserTabId || '').trim();
  if (previousTabId && previousTabId !== nextTabId) {
    const previous = runtimeBrowserTabs.get(previousTabId);
    if (previous && previous.view && previous.view.webContents) {
      stopMediaPlaybackForWebContents(previous.view.webContents, { muteAudio: true, muteElements: true }).catch(() => {});
      previous.audible = false;
    }
  }

  activeRuntimeBrowserTabId = nextTabId;
  tab.last_active = nowTs();
  browserView = tab.view;

  if (browserViewVisible) {
    detachAllAttachedViewsExcept(tab.view);
    attachView(tab.view);
    if (browserViewBounds) {
      try {
        tab.view.setBounds(browserViewBounds);
        tab.view.setAutoResize({ width: false, height: false });
      } catch (_) {
        // noop
      }
    }
  }

  try {
    if (tab.view && tab.view.webContents && typeof tab.view.webContents.setAudioMuted === 'function') {
      tab.view.webContents.setAudioMuted(!isBrowserPlaybackAllowed());
    }
  } catch (_) {
    // noop
  }

  syncMarkerStateToBrowserView();
  publishBrowserAudibleState(!!tab.audible);
  return { ok: true, tabId: nextTabId, activeTabId: activeRuntimeBrowserTabId, tabs: getAllRuntimeBrowserTabs() };
}

function closeRuntimeBrowserTab(runtimeTabId) {
  const targetId = String(runtimeTabId || '').trim();
  if (!targetId) return { ok: false, error: 'Tab id is required.' };
  const tab = runtimeBrowserTabs.get(targetId);
  if (!tab) return { ok: false, error: 'Runtime tab not found.' };

  detachView(tab.view);
  try {
    if (tab.view && tab.view.webContents && !tab.view.webContents.isDestroyed()) {
      tab.view.webContents.destroy();
    }
  } catch (_) {
    // noop
  }
  runtimeBrowserTabs.delete(targetId);

  if (targetId === String(activeRuntimeBrowserTabId || '')) {
    activeRuntimeBrowserTabId = null;
    const nextEntry = runtimeBrowserTabs.entries().next();
    if (!nextEntry.done && nextEntry.value && nextEntry.value[0]) {
      return switchRuntimeBrowserTab(nextEntry.value[0]);
    }
    if (browserViewVisible) {
      const historyViewToKeep = historyPreviewVisible ? historyPreviewView : null;
      detachAllAttachedViewsExcept(historyViewToKeep);
    }
    publishBrowserAudibleState(false);
  }

  return { ok: true, activeTabId: activeRuntimeBrowserTabId, tabs: getAllRuntimeBrowserTabs() };
}

function navigateRuntimeBrowserTab(runtimeTabId, url) {
  const targetId = String(runtimeTabId || '').trim();
  if (!targetId) return { ok: false, error: 'Tab id is required.' };
  const tab = runtimeBrowserTabs.get(targetId);
  if (!tab || !isUsableBrowserView(tab.view)) return { ok: false, error: 'Runtime tab not found.' };
  const targetUrl = normalizeUrl(url);
  tab.url = targetUrl;
  tab.last_active = nowTs();
  tab.view.webContents.loadURL(targetUrl);
  return { ok: true, tabId: targetId, url: targetUrl };
}

function isViewAttached(view) {
  if (!mainWindow || mainWindow.isDestroyed() || !view) return false;
  try {
    const views = mainWindow.getBrowserViews();
    return Array.isArray(views) && views.includes(view);
  } catch (_) {
    return false;
  }
}

function attachView(view) {
  if (!mainWindow || mainWindow.isDestroyed() || !view) return false;
  if (isViewAttached(view)) return true;
  try {
    mainWindow.addBrowserView(view);
    return true;
  } catch (_) {
    return false;
  }
}

function detachView(view) {
  if (!mainWindow || mainWindow.isDestroyed() || !view) return;
  if (!isViewAttached(view)) return;
  try {
    mainWindow.removeBrowserView(view);
  } catch (_) {
    // noop
  }
}

function detachAllAttachedViewsExcept(exceptView = null) {
  if (!mainWindow || mainWindow.isDestroyed()) return;
  let attachedViews = [];
  try {
    attachedViews = mainWindow.getBrowserViews();
  } catch (_) {
    attachedViews = [];
  }
  (Array.isArray(attachedViews) ? attachedViews : []).forEach((view) => {
    if (!view || (exceptView && view === exceptView)) return;
    detachView(view);
  });
}

function createBrowserViewIfNeeded() {
  const activeRuntime = getActiveRuntimeTabEntry();
  if (activeRuntime && isUsableBrowserView(activeRuntime.view)) {
    browserView = activeRuntime.view;
    enforceBrowserPlaybackPolicy().catch(() => {});
    return browserView;
  }

  if (browserView && browserView.webContents && !browserView.webContents.isDestroyed()) {
    enforceBrowserPlaybackPolicy().catch(() => {});
    return browserView;
  }

  browserView = new BrowserView({
    webPreferences: {
      nodeIntegration: false,
      contextIsolation: true,
      sandbox: true,
      preload: BROWSER_VIEW_PRELOAD_PATH,
    },
  });

  publishBrowserAudibleState(false);
  try {
    if (typeof browserView.webContents.setAudioMuted === 'function') {
      browserView.webContents.setAudioMuted(true);
    }
  } catch (_) {
    // noop
  }

  browserView.webContents.on('did-navigate', (_event, url) => {
    sendBrowserEvent('browser:did-navigate', { url });
    syncMarkerStateToBrowserView();
  });

  browserView.webContents.on('did-navigate-in-page', (_event, url) => {
    sendBrowserEvent('browser:did-navigate', { url });
    syncMarkerStateToBrowserView();
  });

  browserView.webContents.on('page-title-updated', (_event, title) => {
    sendBrowserEvent('browser:title-updated', { title });
  });

  browserView.webContents.on('did-start-loading', () => {
    sendBrowserEvent('browser:loading', { loading: true });
  });

  browserView.webContents.on('did-stop-loading', () => {
    sendBrowserEvent('browser:loading', { loading: false });
    syncMarkerStateToBrowserView();
  });

  browserView.webContents.on('media-started-playing', () => {
    if (!isBrowserPlaybackAllowed()) {
      publishBrowserAudibleState(false);
      enforceBrowserPlaybackPolicy().catch(() => {});
      return;
    }
    publishBrowserAudibleState(true);
  });

  browserView.webContents.on('media-paused', () => {
    publishBrowserAudibleState(false);
  });

  browserView.webContents.setWindowOpenHandler(({ url }) => {
    if (/^https?:\/\//i.test(url)) {
      browserView.webContents.loadURL(url);
    }
    return { action: 'deny' };
  });

  browserView.webContents.loadURL('about:blank');
  return browserView;
}

function createHistoryPreviewViewIfNeeded() {
  if (historyPreviewView && historyPreviewView.webContents && !historyPreviewView.webContents.isDestroyed()) {
    return historyPreviewView;
  }
  historyPreviewView = new BrowserView({
    webPreferences: {
      nodeIntegration: false,
      contextIsolation: true,
      sandbox: true,
      preload: BROWSER_VIEW_PRELOAD_PATH,
    },
  });
  historyPreviewView.webContents.setWindowOpenHandler(({ url }) => {
    if (/^https?:\/\//i.test(url)) {
      historyPreviewView.webContents.loadURL(url);
    }
    return { action: 'deny' };
  });
  return historyPreviewView;
}

function normalizeBounds(bounds) {
  if (!bounds || typeof bounds !== 'object') return null;
  const x = Number(bounds.x);
  const y = Number(bounds.y);
  const width = Number(bounds.width);
  const height = Number(bounds.height);
  if (!Number.isFinite(x) || !Number.isFinite(y) || !Number.isFinite(width) || !Number.isFinite(height)) return null;
  if (width <= 0 || height <= 0) return null;
  return {
    x: Math.round(x),
    y: Math.round(y),
    width: Math.round(width),
    height: Math.round(height),
  };
}

function showBrowserView(bounds) {
  const normalized = normalizeBounds(bounds);
  if (normalized) browserViewBounds = normalized;
  const view = createBrowserViewIfNeeded();
  if (!mainWindow || mainWindow.isDestroyed()) return false;
  detachAllAttachedViewsExcept(view);
  attachView(view);
  browserViewVisible = true;
  if (browserViewBounds) {
    view.setBounds(browserViewBounds);
    view.setAutoResize({ width: false, height: false });
  }
  syncMarkerStateToBrowserView();
  enforceBrowserPlaybackPolicy().catch(() => {});
  return true;
}

function hideBrowserView() {
  const historyViewToKeep = historyPreviewVisible ? historyPreviewView : null;
  detachAllAttachedViewsExcept(historyViewToKeep);
  browserViewVisible = false;
  enforceBrowserPlaybackPolicy().catch(() => {});
}

function updateBrowserBounds(bounds) {
  const normalized = normalizeBounds(bounds);
  if (!normalized) return false;
  browserViewBounds = normalized;
  const view = getOperationalBrowserView();
  if (isViewAttached(view)) {
    try {
      view.setBounds(browserViewBounds);
    } catch (_) {
      // noop
    }
  }
  return true;
}

function showHistoryPreviewView(bounds) {
  const normalized = normalizeBounds(bounds);
  if (normalized) historyPreviewBounds = normalized;
  const view = createHistoryPreviewViewIfNeeded();
  if (!mainWindow || mainWindow.isDestroyed()) return false;
  detachAllAttachedViewsExcept(view);
  attachView(view);
  historyPreviewVisible = true;
  if (historyPreviewBounds) {
    view.setBounds(historyPreviewBounds);
    view.setAutoResize({ width: false, height: false });
  }
  return true;
}

function hideHistoryPreviewView() {
  detachView(historyPreviewView);
  historyPreviewVisible = false;
}

function updateHistoryPreviewBounds(bounds) {
  const normalized = normalizeBounds(bounds);
  if (!normalized) return false;
  historyPreviewBounds = normalized;
  if (isViewAttached(historyPreviewView)) {
    try {
      historyPreviewView.setBounds(historyPreviewBounds);
    } catch (_) {
      // noop
    }
  }
  return true;
}

function syncMarkerStateToBrowserView() {
  if (!browserView || !browserView.webContents || browserView.webContents.isDestroyed()) return;
  let currentUrl = '';
  try {
    currentUrl = String(browserView.webContents.getURL() || '').trim();
  } catch (_) {
    currentUrl = '';
  }
  const markerRef = getMarkerReference();
  const highlights = markerRef ? getWebHighlightsForUrl(markerRef.ref, currentUrl) : [];
  const payload = {
    enabled: markerModeEnabled,
    sr_id: markerContext.srId || null,
    artifact_id: markerContext.artifactId || null,
    highlights,
  };
  browserView.webContents.send('browser:marker-mode', payload);
  browserView.webContents.send('browser:marker-sync', payload);
}

async function getPageContentFromBrowser() {
  const view = getOperationalBrowserView();
  if (!view || !view.webContents || view.webContents.isDestroyed()) {
    return { success: false, message: 'Browser view not available.' };
  }
  try {
    const payload = await view.webContents.executeJavaScript(`
      (() => {
        const bodyText = (document.body && document.body.innerText) ? document.body.innerText : '';
        return {
          title: document.title || '',
          url: location.href || '',
          content: bodyText.slice(0, 20000),
        };
      })();
    `);
    return { success: true, data: payload };
  } catch (err) {
    return { success: false, message: err.message || 'Unable to extract page content.' };
  }
}

function appendChatMessage(ref, role, text) {
  if (!ref.chat_thread || typeof ref.chat_thread !== 'object') {
    ref.chat_thread = { messages: [], last_message_at: null };
  }
  if (!Array.isArray(ref.chat_thread.messages)) {
    ref.chat_thread.messages = [];
  }
  ref.chat_thread.messages.push({
    id: makeId('msg'),
    role: String(role || 'assistant'),
    text: String(text || ''),
    ts: nowTs(),
  });
  if (ref.chat_thread.messages.length > MAX_CHAT_MESSAGES) {
    ref.chat_thread.messages = ref.chat_thread.messages.slice(-MAX_CHAT_MESSAGES);
  }
  ref.chat_thread.last_message_at = nowTs();
}

function applyArtifactDiff(ref, diffOp) {
  const targetId = String((diffOp && diffOp.target_id) || '').trim();
  if (!targetId) return { ok: false, message: 'target_id is required for artifact diff.' };

  ref.artifacts = Array.isArray(ref.artifacts) ? ref.artifacts : [];
  const idx = ref.artifacts.findIndex((artifact) => String((artifact && artifact.id) || '') === targetId);
  if (idx < 0) return { ok: false, message: 'Artifact not found.' };

  const artifact = ref.artifacts[idx];
  const mode = String((diffOp && diffOp.mode) || 'append').trim().toLowerCase();
  const patch = String((diffOp && diffOp.patch) || '').trim();
  if (!patch) return { ok: false, message: 'patch is required.' };

  if (mode === 'replace') {
    artifact.content = patch;
  } else {
    artifact.content = `${String(artifact.content || '')}\n${patch}`.trim();
  }
  artifact.updated_at = nowTs();
  ref.updated_at = nowTs();
  return { ok: true, artifact };
}

function applyContextFileDiff(ref, diffOp) {
  const targetId = String((diffOp && diffOp.target_id) || '').trim();
  if (!targetId) return { ok: false, message: 'target_id is required for context file diff.' };

  ref.context_files = Array.isArray(ref.context_files) ? ref.context_files : [];
  const fileItem = ref.context_files.find((file) => String((file && file.id) || '') === targetId);
  if (!fileItem) return { ok: false, message: 'Context file not found.' };
  if (fileItem.read_only) {
    return { ok: false, message: 'Context file is read-only. Diff rejected.' };
  }

  const filePath = String(fileItem.stored_path || '').trim();
  if (!filePath || !fs.existsSync(filePath)) {
    return { ok: false, message: 'Backing file path does not exist.' };
  }

  const patch = String((diffOp && diffOp.patch) || '').trim();
  const mode = String((diffOp && diffOp.mode) || 'append').trim().toLowerCase();
  let existing = '';
  try {
    existing = fs.readFileSync(filePath, 'utf8');
  } catch (err) {
    return { ok: false, message: err.message || 'Unable to read context file.' };
  }

  const next = mode === 'replace' ? patch : `${existing}\n${patch}`.trim();
  try {
    fs.writeFileSync(filePath, next, 'utf8');
  } catch (err) {
    return { ok: false, message: err.message || 'Unable to write context file.' };
  }

  fileItem.summary = next.replace(/\s+/g, ' ').trim().slice(0, 320);
  fileItem.updated_at = nowTs();
  ref.updated_at = nowTs();

  return { ok: true, context_file: fileItem };
}

function createCandidateReferenceFromPublic(item, query = '') {
  const sourcePayload = (item && item.reference_payload && typeof item.reference_payload === 'object')
    ? item.reference_payload
    : item;
  const tabs = Array.isArray(sourcePayload && sourcePayload.tabs) ? sourcePayload.tabs : [];
  const artifacts = Array.isArray(sourcePayload && sourcePayload.artifacts) ? sourcePayload.artifacts : [];
  const sourceId = String((sourcePayload && sourcePayload.id) || (item && item.reference_id) || makeId('pubsrc'));
  const sourceNode = String(
    (item && (item.peer_name || item.source_node_name || item.source_node_url || item.source || 'hyperweb-peer'))
    || 'hyperweb-peer'
  );
  const sourcePeerId = String((item && item.peer_id) || (sourcePayload && sourcePayload.source_peer_id) || '').trim();
  const sourceKey = `${sourceNode}::${sourceId}`;

  const candidate = createReferenceBase({
    title: `[Public] ${String((sourcePayload && sourcePayload.title) || 'Untitled')}`.slice(0, 120),
    intent: String((sourcePayload && sourcePayload.intent) || ''),
    relation_type: 'root',
    current_tab: tabs[0] || { url: 'https://duckduckgo.com', title: 'Imported Public Reference' },
    visibility: 'private',
    is_public_candidate: true,
    source_type: String((item && item.source_type) || (sourcePayload && sourcePayload.source_type) || 'hyperweb_candidate'),
    source_peer_id: sourcePeerId,
    source_peer_name: sourceNode,
    source_candidate_key: sourceKey,
    is_temp_candidate: true,
    temp_imported_at: nowTs(),
    hyperweb_payload_version: Number((item && item.hyperweb_payload_version) || (sourcePayload && sourcePayload.hyperweb_payload_version) || 1),
    source_metadata: {
      source_id: sourceId,
      source_node: sourceNode,
      query,
    },
  });

  candidate.tags = Array.isArray(sourcePayload && sourcePayload.tags) ? sourcePayload.tags.slice(0, 30) : [];
  const sourceTabs = (tabs.length > 0 ? tabs : candidate.tabs).slice(0, MAX_BROWSER_TABS_PER_REFERENCE);
  candidate.tabs = sourceTabs
    .map((tab) => {
      const kind = String((tab && tab.tab_kind) || 'web').trim().toLowerCase();
      if (kind === 'viz') return null;
      if (kind === 'files') {
        return createFilesTab({
          title: String((tab && tab.title) || 'Files').slice(0, 120),
          files_view_state: (tab && typeof tab.files_view_state === 'object') ? tab.files_view_state : {},
          updated_at: Number((tab && tab.updated_at) || nowTs()),
        });
      }
      if (kind === 'skills') {
        return createSkillsTab({
          title: String((tab && tab.title) || 'Skills').slice(0, 120),
          updated_at: Number((tab && tab.updated_at) || nowTs()),
        });
      }
      return {
        ...createWebTab({ url: tab.url || 'https://duckduckgo.com', title: tab.title || tab.url || 'Web Tab' }),
        tab_kind: 'web',
        renderer: '',
        viz_request: {},
        url: normalizeUrl(tab.url || 'https://duckduckgo.com'),
        title: String((tab && tab.title) || tab.url || 'Web Tab').slice(0, 180),
      };
    })
    .filter(Boolean);
  if (candidate.tabs.length === 0) {
    candidate.tabs = [createWebTab({ url: getDefaultSearchHomeUrl(), title: getDefaultSearchHomeTitle() })];
  }
  candidate.active_tab_id = candidate.tabs[0] ? candidate.tabs[0].id : null;
  const legacyVizArtifacts = sourceTabs
    .filter((tab) => String((tab && tab.tab_kind) || '').trim().toLowerCase() === 'viz')
    .map((tab) => buildLegacyVizArtifact(candidate.id, tab));
  candidate.artifacts = (artifacts.length > 0 ? artifacts : candidate.artifacts)
    .slice(0, 40)
    .map((artifact) => createArtifact({
      title: artifact.title || 'Artifact',
      type: artifact.type || 'markdown',
      content: artifact.content || '',
    }))
    .concat(legacyVizArtifacts)
    .slice(0, 80);
  candidate.reference_graph = (sourcePayload && typeof sourcePayload.reference_graph === 'object' && sourcePayload.reference_graph)
    ? sourcePayload.reference_graph
    : { nodes: [], edges: [] };
  candidate.program = String((sourcePayload && sourcePayload.program) || '');
  candidate.skills = Array.isArray(sourcePayload && sourcePayload.skills)
    ? sourcePayload.skills.map((item) => sanitizeSkillDescriptor(item)).filter(Boolean)
    : [];
  candidate.context_files = Array.isArray(sourcePayload && sourcePayload.context_files)
    ? sourcePayload.context_files.map((file) => ({
      id: makeId('ctx'),
      source_type: 'hyperweb',
      original_name: String((file && file.original_name) || (file && file.relative_path) || 'context.txt'),
      relative_path: String((file && file.relative_path) || ''),
      stored_path: '',
      mime_type: String((file && file.mime_type) || 'text/plain'),
      size_bytes: Number((file && file.size_bytes) || 0),
      content_hash: String((file && file.content_hash) || ''),
      summary: String((file && file.summary) || ''),
      read_only: true,
      created_at: nowTs(),
      updated_at: nowTs(),
    }))
    : [];
  candidate.updated_at = nowTs();

  return candidate;
}

async function discoverPublicReferences(query, limit = 20) {
  const q = String(query || '').trim();
  if (!q) return [];
  const queryRes = await hyperwebManager.query(q, {
    limit: Math.max(1, Math.min(100, Number(limit) || 20)),
  });
  if (!queryRes || !queryRes.ok) return [];
  return Array.isArray(queryRes.suggestions) ? queryRes.suggestions : [];
}

async function importDiscoveredPublicReferences(query, limit = 20) {
  const refs = getReferences();
  const discovered = await discoverPublicReferences(query, limit);
  if (discovered.length === 0) {
    return { ok: true, imported_count: 0, discovered_count: 0, references: refs };
  }

  const existingKeys = new Set(
    refs
      .map((ref) => String((ref && ref.source_candidate_key) || '').trim())
      .filter(Boolean)
  );
  const existingLocalRefIds = new Set(
    refs
      .filter((ref) => !ref || !ref.is_public_candidate)
      .map((ref) => String((ref && ref.id) || '').trim())
      .filter(Boolean)
  );

  const imported = [];
  for (const item of discovered) {
    const sourceRefId = String(
      (item && item.reference_id)
      || (item && item.id)
      || (item && item.reference_payload && item.reference_payload.id)
      || ''
    ).trim();
    const sourcePeerId = String((item && item.peer_id) || '').trim();
    if (sourceRefId && (!sourcePeerId || sourcePeerId === hyperwebManager.peerId) && existingLocalRefIds.has(sourceRefId)) {
      continue;
    }

    let importable = item;
    const hasPayload = !!(item && item.reference_payload && typeof item.reference_payload === 'object');
    if (!hasPayload) {
      const fetched = await hyperwebManager.importSuggestion(item);
      if (fetched && fetched.ok && fetched.imported) {
        importable = {
          ...(item || {}),
          reference_payload: fetched.imported,
          reference_id: sourceRefId || String((fetched.imported && fetched.imported.id) || ''),
        };
      } else {
        continue;
      }
    }

    const candidate = createCandidateReferenceFromPublic(importable, query);
    if (existingKeys.has(candidate.source_candidate_key)) continue;
    existingKeys.add(candidate.source_candidate_key);
    refs.unshift(candidate);
    imported.push(candidate);
  }
  setReferences(refs);
  return {
    ok: true,
    query: String(query || ''),
    discovered_count: discovered.length,
    imported_count: imported.length,
    imported,
    references: refs,
  };
}

function commitPublicCandidateReference(payload = {}) {
  const candidateId = String(payload.srId || payload.sr_id || '').trim();
  const strategy = String(payload.strategy || 'root').trim().toLowerCase();
  const targetSrId = String(payload.targetSrId || payload.target_sr_id || '').trim();
  const refs = getReferences();
  const candidateIdx = findReferenceIndex(refs, candidateId);
  if (candidateIdx < 0) return { ok: false, message: 'Public candidate reference not found.' };

  const candidate = refs[candidateIdx];
  if (!candidate || !candidate.is_public_candidate) {
    return { ok: false, message: 'Selected reference is not a public candidate.' };
  }

  const newRef = {
    ...createForkReference(candidate, {
      title: String(candidate.title || 'Imported Reference').replace(/^\[Public\]\s*/i, '').slice(0, 120),
      source_metadata: {
        ...(candidate.source_metadata || {}),
        committed_at: nowTs(),
      },
    }),
    visibility: 'private',
    is_public_candidate: false,
    source_type: 'local',
    source_peer_id: '',
    source_peer_name: '',
    source_candidate_key: '',
    is_temp_candidate: false,
    temp_imported_at: 0,
    hyperweb_payload_version: Number(candidate.hyperweb_payload_version || 1),
  };

  if (strategy === 'fork' && targetSrId) {
    const targetIdx = findReferenceIndex(refs, targetSrId);
    if (targetIdx < 0) {
      return { ok: false, message: 'Target reference for fork was not found.' };
    }
    const target = refs[targetIdx];
    newRef.parent_id = target.id;
    newRef.relation_type = 'fork';
    newRef.lineage = [target.id].concat(Array.isArray(target.lineage) ? target.lineage : []).slice(0, 100);
    target.children = Array.isArray(target.children) ? target.children : [];
    target.children.push(newRef.id);
    target.updated_at = nowTs();
  } else {
    newRef.parent_id = null;
    newRef.relation_type = 'root';
    newRef.lineage = [];
  }

  refs.unshift(newRef);
  setReferences(refs);
  return { ok: true, reference: newRef, references: refs };
}

function createApplicationMenu() {
  const isMac = process.platform === 'darwin';
  const template = [
    ...(isMac ? [{ label: app.name, submenu: [{ role: 'about' }, { type: 'separator' }, { role: 'quit' }] }] : []),
    { label: 'Edit', submenu: [{ role: 'undo' }, { role: 'redo' }, { type: 'separator' }, { role: 'cut' }, { role: 'copy' }, { role: 'paste' }, { role: 'selectAll' }] },
    { label: 'View', submenu: [{ role: 'reload' }, { role: 'forceReload' }, { type: 'separator' }, { role: 'toggleDevTools' }, { role: 'togglefullscreen' }] },
    { label: 'Window', submenu: [{ role: 'minimize' }, { role: 'close' }] },
  ];
  const menu = Menu.buildFromTemplate(template);
  Menu.setApplicationMenu(menu);
}

function createMainWindow() {
  mainWindow = new BrowserWindow({
    width: 1600,
    height: 980,
    minWidth: 1100,
    minHeight: 680,
    title: APP_NAME,
    backgroundColor: '#0f1318',
    icon: resolveWindowIconPath(),
    webPreferences: {
      preload: path.join(__dirname, 'preload.js'),
      nodeIntegration: false,
      contextIsolation: true,
      sandbox: false,
    },
    show: false,
  });

  mainWindow.loadFile(path.join(__dirname, 'renderer', 'index.html'));
  mainWindow.once('ready-to-show', () => mainWindow.show());
  mainWindow.on('closed', () => {
    mainWindow = null;
  });
}

ipcMain.handle('browser:show', (_event, bounds) => showBrowserView(bounds));
ipcMain.handle('browser:hide', () => {
  hideBrowserView();
  hideHistoryPreviewView();
  return true;
});
ipcMain.handle('browser:updateBounds', (_event, bounds) => updateBrowserBounds(bounds));
ipcMain.handle('browser:historyPreviewShow', (_event, payload) => {
  const bounds = payload && payload.bounds ? payload.bounds : payload;
  const ok = showHistoryPreviewView(bounds);
  return { ok };
});
ipcMain.handle('browser:historyPreviewHide', () => {
  hideHistoryPreviewView();
  return { ok: true };
});
ipcMain.handle('browser:historyPreviewNavigate', (_event, payload) => {
  const rawUrl = String((payload && payload.url) || '').trim();
  const url = normalizeUrl(rawUrl);
  if (!/^https?:\/\//i.test(url)) return { ok: false, message: 'A valid http(s) URL is required.' };
  const view = createHistoryPreviewViewIfNeeded();
  view.webContents.loadURL(url);
  return { ok: true, url };
});
ipcMain.handle('browser:historyPreviewUpdateBounds', (_event, payload) => {
  const bounds = payload && payload.bounds ? payload.bounds : payload;
  return { ok: updateHistoryPreviewBounds(bounds) };
});
ipcMain.handle('browser:navigate', (_event, url) => {
  const activeRuntime = getActiveRuntimeTabEntry();
  if (activeRuntime) {
    const navRes = navigateRuntimeBrowserTab(activeRuntimeBrowserTabId, url);
    return !!(navRes && navRes.ok);
  }
  const view = createBrowserViewIfNeeded();
  view.webContents.loadURL(normalizeUrl(url));
  return true;
});
ipcMain.handle('browser:reload', () => {
  const view = getOperationalBrowserView();
  if (!view || !view.webContents || view.webContents.isDestroyed()) return false;
  view.webContents.reload();
  return true;
});
ipcMain.handle('browser:back', () => {
  const view = getOperationalBrowserView();
  if (!view || !view.webContents || view.webContents.isDestroyed()) return false;
  if (!view.webContents.canGoBack()) return false;
  view.webContents.goBack();
  return true;
});
ipcMain.handle('browser:forward', () => {
  const view = getOperationalBrowserView();
  if (!view || !view.webContents || view.webContents.isDestroyed()) return false;
  if (!view.webContents.canGoForward()) return false;
  view.webContents.goForward();
  return true;
});
ipcMain.handle('browser:getZoomFactor', () => getBrowserViewZoomFactor());
ipcMain.handle('browser:setZoomFactor', (_event, factor) => setBrowserViewZoomFactor(factor));
ipcMain.handle('browser:canGoBack', () => {
  const view = getOperationalBrowserView();
  if (!view || !view.webContents || view.webContents.isDestroyed()) return false;
  return view.webContents.canGoBack();
});
ipcMain.handle('browser:canGoForward', () => {
  const view = getOperationalBrowserView();
  if (!view || !view.webContents || view.webContents.isDestroyed()) return false;
  return view.webContents.canGoForward();
});
ipcMain.handle('browser:getCurrentUrl', () => {
  const view = getOperationalBrowserView();
  if (!view || !view.webContents || view.webContents.isDestroyed()) return 'about:blank';
  try {
    return String(view.webContents.getURL() || 'about:blank');
  } catch (_) {
    return 'about:blank';
  }
});
ipcMain.handle('browser:getPageContent', async () => getPageContentFromBrowser());
ipcMain.handle('browser:openExternal', async (_event, payload) => {
  const target = String((payload && payload.url) || payload || '').trim();
  if (!target || !/^(https?:\/\/|mailto:)/i.test(target)) {
    return { ok: false, message: 'Only http(s) and mailto links are allowed.' };
  }
  try {
    await shell.openExternal(target);
    return { ok: true };
  } catch (err) {
    return { ok: false, message: String((err && err.message) || 'Unable to open external link.') };
  }
});

ipcMain.handle('tabs:create', (_event, url) => createRuntimeBrowserTab(url, true));
ipcMain.handle('tabs:switch', (_event, tabId) => switchRuntimeBrowserTab(tabId));
ipcMain.handle('tabs:close', (_event, tabId) => closeRuntimeBrowserTab(tabId));
ipcMain.handle('tabs:getAll', () => getAllRuntimeBrowserTabs());
ipcMain.handle('tabs:navigate', (_event, tabId, url) => navigateRuntimeBrowserTab(tabId, url));
ipcMain.handle('tabs:getActive', () => {
  const active = getActiveRuntimeTabEntry();
  return {
    activeTabId: String(activeRuntimeBrowserTabId || '').trim() || null,
    tab: active
      ? {
          id: String(activeRuntimeBrowserTabId || '').trim(),
          url: String(active.url || ''),
          title: String(active.title || ''),
          favicon: active.favicon || null,
          loading: !!active.loading,
          audible: !!active.audible,
          active: true,
          last_active: Number(active.last_active || 0),
        }
      : null,
  };
});

ipcMain.handle('browser:markerSetMode', (_event, enabled) => {
  markerModeEnabled = !!enabled;
  syncMarkerStateToBrowserView();
  return { ok: true, enabled: markerModeEnabled };
});

ipcMain.handle('browser:markerSetContext', (_event, payload) => {
  markerContext = {
    srId: String((payload && payload.srId) || '').trim(),
    artifactId: String((payload && payload.artifactId) || '').trim(),
  };
  syncMarkerStateToBrowserView();
  return { ok: true, context: markerContext };
});

ipcMain.handle('browser:srToggleArtifactHighlight', (_event, payload) => {
  try {
    const modeSnapshot = !!(payload && payload.marker_mode_snapshot);
    if (!markerModeEnabled && !modeSnapshot) {
      return { ok: false, added: false, removed: false, message: 'Marker mode is off.', highlight: null };
    }
    const srId = String((payload && payload.srId) || (markerContext && markerContext.srId) || '').trim();
    const artifactId = String((payload && payload.artifactId) || (markerContext && markerContext.artifactId) || '').trim();
    const startRaw = Number((payload && payload.start));
    const endRaw = Number((payload && payload.end));
    if (!srId || !artifactId) {
      return { ok: false, added: false, removed: false, message: 'srId and artifactId are required.', highlight: null };
    }
    if (!Number.isFinite(startRaw) || !Number.isFinite(endRaw)) {
      return { ok: false, added: false, removed: false, message: 'start and end must be numbers.', highlight: null };
    }

    const refs = getReferences();
    const refIdx = findReferenceIndex(refs, srId);
    if (refIdx < 0) {
      return { ok: false, added: false, removed: false, message: 'Reference not found.', highlight: null };
    }
    const artifacts = Array.isArray(refs[refIdx].artifacts) ? refs[refIdx].artifacts : [];
    const artifact = artifacts.find((item) => String((item && item.id) || '').trim() === artifactId) || null;
    if (!artifact) {
      return { ok: false, added: false, removed: false, message: 'Artifact not found.', highlight: null };
    }

    const content = String((artifact && artifact.content) || '');
    const start = Math.max(0, Math.min(Math.round(startRaw), content.length));
    const end = Math.max(start, Math.min(Math.round(endRaw), content.length));
    if (end <= start) {
      return {
        ok: false,
        added: false,
        removed: false,
        message: 'Select text inside the artifact to toggle a marker.',
        highlight: null,
      };
    }
    const derivedText = content.slice(start, end).trim();
    const selectedText = String((payload && payload.text) || '').trim();
    const text = selectedText || derivedText;
    if (!text) {
      return {
        ok: false,
        added: false,
        removed: false,
        message: 'Could not derive non-empty artifact highlight text from the selected range.',
        highlight: null,
      };
    }

    const toggleRes = toggleArtifactHighlight(refs[refIdx], {
      source: 'artifact',
      artifact_id: artifactId,
      artifact_start: start,
      artifact_end: end,
      text,
    });
    if (!toggleRes.ok) {
      return {
        ok: false,
        added: false,
        removed: false,
        message: String(toggleRes.message || 'Unable to toggle artifact highlight.'),
        highlight: null,
      };
    }

    setReferences(refs);
    syncMarkerStateToBrowserView();
    return {
      ok: true,
      added: !!toggleRes.added,
      removed: !!toggleRes.removed,
      message: String(toggleRes.message || 'Artifact highlight toggled.'),
      highlight: toggleRes.highlight || null,
    };
  } catch (err) {
    return {
      ok: false,
      added: false,
      removed: false,
      message: String((err && err.message) || 'Unable to toggle artifact highlight.'),
      highlight: null,
    };
  }
});

ipcMain.handle('browser:markerClearActive', () => {
  try {
    const markerRef = getMarkerReference();
    if (!markerRef) {
      return {
        ok: false,
        removed_count: 0,
        target: null,
        message: 'No active marker reference selected.',
      };
    }
    const refs = markerRef.refs;
    const idx = markerRef.idx;
    const ref = refs[idx];
    if (!ref || typeof ref !== 'object') {
      return {
        ok: false,
        removed_count: 0,
        target: null,
        message: 'Active marker reference is unavailable.',
      };
    }

    let clearTarget = null;
    const artifactId = String((markerContext && markerContext.artifactId) || '').trim();
    if (artifactId) {
      clearTarget = { type: 'artifact', artifact_id: artifactId };
    } else {
      let currentUrl = '';
      const view = getOperationalBrowserView();
      if (view && view.webContents && !view.webContents.isDestroyed()) {
        try {
          currentUrl = String(view.webContents.getURL() || '').trim();
        } catch (_) {
          currentUrl = '';
        }
      }
      const urlNorm = normalizeUrlForMatch(currentUrl);
      if (!urlNorm) {
        return {
          ok: false,
          removed_count: 0,
          target: null,
          message: 'No active URL to clear markers for.',
        };
      }
      clearTarget = { type: 'web', url_norm: urlNorm };
    }

    const clearRes = clearHighlightsByTarget(ref, clearTarget);
    if (!clearRes.target) {
      return {
        ok: false,
        removed_count: 0,
        target: null,
        message: 'Unable to resolve marker clear target.',
      };
    }
    if (clearRes.removed_count > 0) {
      setReferences(refs);
    }
    syncMarkerStateToBrowserView();
    const message = clearRes.removed_count > 0
      ? `Cleared ${clearRes.removed_count} marker(s) for active ${clearRes.target.type === 'artifact' ? 'artifact' : 'URL'}.`
      : `No markers found for active ${clearRes.target.type === 'artifact' ? 'artifact' : 'URL'}.`;
    return {
      ok: true,
      removed_count: clearRes.removed_count,
      target: clearRes.target,
      message,
    };
  } catch (err) {
    return {
      ok: false,
      removed_count: 0,
      target: null,
      message: String((err && err.message) || 'Unable to clear markers.'),
    };
  }
});

ipcMain.on('browser:marker:web-selection', (_event, payload) => {
  try {
    const modeSnapshot = !!(payload && payload.marker_mode_snapshot);
    if (!markerModeEnabled && !modeSnapshot) return;
    const markerRef = getMarkerReference();
    if (!markerRef) return;
    const refs = markerRef.refs;
    const idx = markerRef.idx;

    refs[idx].highlights = Array.isArray(refs[idx].highlights)
      ? refs[idx].highlights.map((item) => sanitizeHighlightEntry(item)).filter(Boolean)
      : [];

    const action = String((payload && payload.action) || '').trim().toLowerCase();
    if (action === 'partial_unmark') {
      const removeIds = new Set(
        (Array.isArray(payload && payload.remove_ids) ? payload.remove_ids : [])
          .map((id) => String(id || '').trim())
          .filter(Boolean),
      );
      if (removeIds.size > 0) {
        refs[idx].highlights = refs[idx].highlights.filter((item) => !removeIds.has(String((item && item.id) || '').trim()));
      }
      const additions = Array.isArray(payload && payload.additions) ? payload.additions : [];
      additions.forEach((item) => {
        const highlight = sanitizeHighlightEntry({
          ...(item && typeof item === 'object' ? item : {}),
          source: 'web',
          url: String((item && item.url) || (payload && payload.url) || ''),
          url_norm: String((item && item.url_norm) || (payload && payload.url_norm) || ''),
          created_at: nowTs(),
          updated_at: nowTs(),
        });
        if (!highlight || highlight.source !== 'web') return;
        refs[idx].highlights.push(highlight);
      });
    } else {
      const highlight = sanitizeHighlightEntry({
        ...(payload && typeof payload === 'object' ? payload : {}),
        source: 'web',
        created_at: nowTs(),
        updated_at: nowTs(),
      });
      if (!highlight || highlight.source !== 'web') return;
      const signature = highlightSignatureWeb(highlight);
      const matchIdx = refs[idx].highlights.findIndex((item) => highlightSignatureWeb(item) === signature);
      if (matchIdx >= 0) {
        refs[idx].highlights.splice(matchIdx, 1);
      } else {
        refs[idx].highlights.push(highlight);
      }
    }

    refs[idx].highlights = refs[idx].highlights
      .map((item) => sanitizeHighlightEntry(item))
      .filter(Boolean)
      .slice(-MAX_HIGHLIGHTS);
    refs[idx].updated_at = nowTs();
    setReferences(refs);
    syncMarkerStateToBrowserView();
  } catch (_) {
    // noop
  }
});

ipcMain.on('browser:shortcut-command', (_event, payload) => {
  const command = String((payload && payload.command) || '').trim();
  if (!SHORTCUT_COMMAND_ALLOWLIST.has(command)) return;
  if (!mainWindow || mainWindow.isDestroyed()) return;
  try {
    mainWindow.webContents.send('browser:shortcut-command', { command });
  } catch (_) {
    // noop
  }
});

function getMemoryCheckpoint(ref, checkpointId) {
  const memory = ensureReferenceMemory(ref);
  const targetId = String(checkpointId || '').trim();
  if (!targetId) return null;
  return (Array.isArray(memory.checkpoints) ? memory.checkpoints : []).find((item) => String((item && item.id) || '') === targetId) || null;
}

function applySnapshotToForkReference(targetRef, snapshot = {}) {
  if (!targetRef || typeof targetRef !== 'object') return targetRef;
  const snap = (snapshot && typeof snapshot === 'object') ? snapshot : {};
  targetRef.title = String(snap.title || targetRef.title || 'Memory Fork').slice(0, 120);
  targetRef.title_user_edited = !!snap.title_user_edited;
  targetRef.intent = String(snap.intent || '');
  targetRef.tags = Array.isArray(snap.tags) ? deepClone(snap.tags) : [];
  targetRef.tabs = Array.isArray(snap.tabs) ? deepClone(snap.tabs) : [];
  targetRef.active_tab_id = String(snap.active_tab_id || (targetRef.tabs[0] && targetRef.tabs[0].id) || '');
  targetRef.artifacts = Array.isArray(snap.artifacts) ? deepClone(snap.artifacts) : [];
  targetRef.highlights = Array.isArray(snap.highlights) ? deepClone(snap.highlights) : [];
  targetRef.context_files = Array.isArray(snap.context_files) ? deepClone(snap.context_files) : [];
  targetRef.folder_mounts = Array.isArray(snap.folder_mounts) ? deepClone(snap.folder_mounts) : [];
  targetRef.youtube_transcripts = (snap.youtube_transcripts && typeof snap.youtube_transcripts === 'object') ? deepClone(snap.youtube_transcripts) : {};
  targetRef.reference_graph = (snap.reference_graph && typeof snap.reference_graph === 'object')
    ? deepClone(snap.reference_graph)
    : { nodes: [], edges: [] };
  targetRef.agent_weights = (snap.agent_weights && typeof snap.agent_weights === 'object') ? deepClone(snap.agent_weights) : {};
  targetRef.decision_trace = Array.isArray(snap.decision_trace) ? deepClone(snap.decision_trace) : [];
  targetRef.program = String(snap.program || '');
  targetRef.skills = Array.isArray(snap.skills) ? deepClone(snap.skills) : [];
  targetRef.chat_thread = (snap.chat_thread && typeof snap.chat_thread === 'object')
    ? deepClone(snap.chat_thread)
    : { messages: [], last_message_at: null };
  targetRef.updated_at = nowTs();
  targetRef.last_used_at = nowTs();
  ensureReferenceMemory(targetRef);
  return targetRef;
}

ipcMain.handle('browser:srList', () => getReferences());

ipcMain.handle('browser:memorySetEnabled', (_event, payload) => {
  const srId = String((payload && payload.srId) || '').trim();
  const enabled = !!(payload && payload.enabled);
  const refs = getReferences();
  const idx = findReferenceIndex(refs, srId);
  if (idx < 0) return { ok: false, message: 'Reference not found.' };
  const memory = ensureReferenceMemory(refs[idx]);
  memory.enabled = enabled;
  refs[idx].updated_at = nowTs();
  if (enabled) capturePeriodicMemoryCheckpoints(refs[idx]);
  setReferences(refs, { skipMemoryCapture: true });
  return { ok: true, memory: refs[idx].memory, reference: refs[idx], references: refs };
});

ipcMain.handle('browser:memoryList', (_event, payload) => {
  const srId = String((payload && payload.srId) || '').trim();
  const refs = getReferences();
  const idx = findReferenceIndex(refs, srId);
  if (idx < 0) return { ok: false, message: 'Reference not found.' };
  const memory = ensureReferenceMemory(refs[idx]);
  const checkpoints = (Array.isArray(memory.checkpoints) ? memory.checkpoints : [])
    .sort((a, b) => Number((b && b.created_at) || 0) - Number((a && a.created_at) || 0))
    .map((item) => buildMemoryCheckpointMetadata(item));
  return { ok: true, enabled: !!memory.enabled, checkpoints };
});

ipcMain.handle('browser:memoryLoadCheckpoint', (_event, payload) => {
  const srId = String((payload && payload.srId) || '').trim();
  const checkpointId = String((payload && payload.checkpointId) || '').trim();
  const refs = getReferences();
  const idx = findReferenceIndex(refs, srId);
  if (idx < 0) return { ok: false, message: 'Reference not found.' };
  const checkpoint = getMemoryCheckpoint(refs[idx], checkpointId);
  if (!checkpoint) return { ok: false, message: 'Checkpoint not found.' };
  return {
    ok: true,
    checkpoint: buildMemoryCheckpointMetadata(checkpoint),
    snapshot: deepClone(checkpoint.snapshot || {}),
    diff: (checkpoint.diff_from_prev && typeof checkpoint.diff_from_prev === 'object')
      ? checkpoint.diff_from_prev
      : {},
  };
});

ipcMain.handle('browser:memoryPreviewDiff', (_event, payload) => {
  const srId = String((payload && payload.srId) || '').trim();
  const checkpointId = String((payload && payload.checkpointId) || '').trim();
  const against = String((payload && payload.against) || 'current').trim().toLowerCase();
  const refs = getReferences();
  const idx = findReferenceIndex(refs, srId);
  if (idx < 0) return { ok: false, message: 'Reference not found.' };
  const memory = ensureReferenceMemory(refs[idx]);
  const list = Array.isArray(memory.checkpoints) ? [...memory.checkpoints] : [];
  const checkpoint = list.find((item) => String((item && item.id) || '') === checkpointId);
  if (!checkpoint) return { ok: false, message: 'Checkpoint not found.' };
  const ordered = list.sort((a, b) => Number((b && b.created_at) || 0) - Number((a && a.created_at) || 0));
  const currentSnapshot = buildReferenceSnapshot(refs[idx]);
  let baseSnapshot = currentSnapshot;
  if (against === 'previous') {
    const cpIdx = ordered.findIndex((item) => String((item && item.id) || '') === checkpointId);
    const prev = cpIdx >= 0 ? ordered[cpIdx + 1] : null;
    baseSnapshot = prev && prev.snapshot ? prev.snapshot : {};
  }
  const diff = buildReferenceDiffSummary(baseSnapshot, checkpoint.snapshot || {});
  return { ok: true, checkpoint: buildMemoryCheckpointMetadata(checkpoint), against, diff };
});

ipcMain.handle('browser:memoryAttachDiffContext', (_event, payload) => {
  const srId = String((payload && payload.srId) || '').trim();
  const checkpointId = String((payload && payload.checkpointId) || '').trim();
  const sections = Array.isArray(payload && payload.sections) ? payload.sections : ['summary', 'diff'];
  const refs = getReferences();
  const idx = findReferenceIndex(refs, srId);
  if (idx < 0) return { ok: false, message: 'Reference not found.' };
  const checkpoint = getMemoryCheckpoint(refs[idx], checkpointId);
  if (!checkpoint) return { ok: false, message: 'Checkpoint not found.' };
  const lines = [];
  lines.push(`Memory checkpoint: ${String(checkpoint.id || '')}`);
  lines.push(`Kind: ${String(checkpoint.kind || 'periodic')}`);
  lines.push(`Timestamp: ${new Date(Number(checkpoint.created_at || 0)).toISOString()}`);
  lines.push(`Summary: ${String(checkpoint.summary || '')}`);
  if (sections.includes('diff')) {
    const diff = (checkpoint.diff_from_prev && typeof checkpoint.diff_from_prev === 'object') ? checkpoint.diff_from_prev : {};
    lines.push(`Diff: ${JSON.stringify(diff)}`);
  }
  return {
    ok: true,
    attached_context: {
      checkpoint_id: String(checkpoint.id || ''),
      kind: String(checkpoint.kind || 'periodic'),
      summary: String(checkpoint.summary || ''),
      text: lines.join('\n'),
    },
  };
});

ipcMain.handle('browser:memoryForkFromCheckpoint', (_event, payload) => {
  const srId = String((payload && payload.srId) || '').trim();
  const checkpointId = String((payload && payload.checkpointId) || '').trim();
  const titleHint = String((payload && payload.titleHint) || '').trim();
  const refs = getReferences();
  const idx = findReferenceIndex(refs, srId);
  if (idx < 0) return { ok: false, message: 'Reference not found.' };
  const checkpoint = getMemoryCheckpoint(refs[idx], checkpointId);
  if (!checkpoint) return { ok: false, message: 'Checkpoint not found.' };

  const parent = refs[idx];
  const fork = createForkReference(parent, {
    title: titleHint || `${String((checkpoint.snapshot && checkpoint.snapshot.title) || parent.title || 'Reference')} Memory Fork`,
    source_metadata: {
      fork_reason: 'memory_revival',
      memory_checkpoint_id: String(checkpoint.id || ''),
      memory_checkpoint_ts: Number(checkpoint.created_at || 0),
    },
  });
  applySnapshotToForkReference(fork, checkpoint.snapshot || {});
  parent.children = Array.isArray(parent.children) ? parent.children : [];
  parent.children.push(fork.id);
  parent.updated_at = nowTs();
  refs.unshift(fork);
  setReferences(refs);
  return {
    ok: true,
    reference: fork,
    references: refs,
    memory_source: {
      checkpoint_id: String(checkpoint.id || ''),
      checkpoint_ts: Number(checkpoint.created_at || 0),
    },
  };
});

ipcMain.handle('browser:srSetVisibility', (_event, payload) => {
  const srId = String((payload && payload.srId) || '').trim();
  const visibilityRaw = String((payload && payload.visibility) || '').trim().toLowerCase();
  const visibility = visibilityRaw === 'public' ? 'public' : 'private';
  const refs = getReferences();
  const idx = findReferenceIndex(refs, srId);
  if (idx < 0) return { ok: false, message: 'Reference not found.' };
  if (refs[idx].is_public_candidate) {
    return { ok: false, message: 'Public candidate references cannot be directly re-published.' };
  }
  if (visibility === 'public') {
    return { ok: false, message: 'Use "Publish Snapshot" to publish immutable public copies.' };
  }
  refs[idx].visibility = visibility;
  refs[idx].updated_at = nowTs();
  setReferences(refs);
  return { ok: true, reference: refs[idx], references: refs };
});

ipcMain.handle('browser:srPublishSnapshot', (_event, payload) => {
  const srId = String((payload && payload.srId) || '').trim();
  return publishSnapshotFromReference(srId);
});

ipcMain.handle('browser:srDiscoverPublicReferences', async (_event, payload) => {
  const auth = requireTtcHyperwebAuth();
  if (!auth.ok) return auth;
  const query = String((payload && payload.query) || '').trim();
  const limit = Number((payload && payload.limit) || 20);
  if (!query) return { ok: false, message: 'query is required.' };
  return importDiscoveredPublicReferences(query, limit);
});

ipcMain.handle('browser:srCommitPublicCandidate', (_event, payload) => {
  return commitPublicCandidateReference(payload || {});
});

ipcMain.handle('browser:srCreateRoot', (_event, payload) => {
  const refs = getReferences();
  const root = createReferenceBase({
    title: String((payload && payload.title) || 'New Root Reference').slice(0, 120),
    intent: String((payload && payload.intent) || ''),
    relation_type: 'root',
    current_tab: (payload && payload.current_tab) || { url: getDefaultSearchHomeUrl(), title: getDefaultSearchHomeTitle() },
  });
  refs.unshift(root);
  setReferences(refs);
  const seedTab = root.tabs && root.tabs[0] ? root.tabs[0] : null;
  if (seedTab) {
    captureCommittedHistoryFromTab(seedTab, {
      committed_at: nowTs(),
      source_sr_id: root.id,
      source_tab_id: String(seedTab.id || ''),
    }).catch(() => {});
  }
  return { ok: true, reference: root, references: refs };
});

ipcMain.handle('browser:srCreateEmptyWorkspace', (_event, payload) => {
  const title = String((payload && payload.title) || 'Untitled').slice(0, 120) || 'Untitled';
  const refs = getReferences();
  const root = createReferenceBase({
    title,
    intent: String((payload && payload.intent) || ''),
    relation_type: 'root',
    current_tab: { url: getDefaultSearchHomeUrl(), title: getDefaultSearchHomeTitle() },
  });
  root.artifacts = [
    createArtifact({
      title: 'art-1.md',
      type: 'markdown',
      content: '',
    }),
  ];
  root.updated_at = nowTs();
  refs.unshift(root);
  setReferences(refs);
  return { ok: true, reference: root, references: refs };
});

ipcMain.handle('browser:srFork', (_event, srId) => {
  const refs = getReferences();
  const idx = findReferenceIndex(refs, srId);
  if (idx < 0) return { ok: false, message: 'Reference not found.' };

  const parent = refs[idx];
  const child = createForkReference(parent, {});
  parent.children = Array.isArray(parent.children) ? parent.children : [];
  parent.children.push(child.id);
  parent.updated_at = nowTs();

  refs.unshift(child);
  setReferences(refs);

  return { ok: true, reference: child, references: refs };
});

ipcMain.handle('browser:srAddChild', (_event, srId) => {
  const refs = getReferences();
  const idx = findReferenceIndex(refs, srId);
  if (idx < 0) return { ok: false, message: 'Reference not found.' };

  const parent = refs[idx];
  const child = createReferenceBase({
    title: `${parent.title || 'Reference'} Child`,
    intent: parent.intent || '',
    parent_id: parent.id,
    lineage: [parent.id].concat(Array.isArray(parent.lineage) ? parent.lineage : []).slice(0, 100),
    relation_type: 'child',
    current_tab: parent.tabs && parent.tabs[0] ? { url: parent.tabs[0].url, title: parent.tabs[0].title } : {},
  });

  parent.children = Array.isArray(parent.children) ? parent.children : [];
  parent.children.push(child.id);
  parent.updated_at = nowTs();

  refs.unshift(child);
  setReferences(refs);

  return { ok: true, reference: child, references: refs };
});

ipcMain.handle('browser:srRename', (_event, payload) => {
  const srId = String((payload && payload.srId) || '').trim();
  const title = String((payload && payload.title) || '').trim();
  if (!srId || !title) return { ok: false, message: 'srId and title are required.' };

  const refs = getReferences();
  const idx = findReferenceIndex(refs, srId);
  if (idx < 0) return { ok: false, message: 'Reference not found.' };

  refs[idx].title = title.slice(0, 120);
  refs[idx].title_user_edited = true;
  refs[idx].updated_at = nowTs();
  setReferences(refs);

  return { ok: true, reference: refs[idx], references: refs };
});

ipcMain.handle('browser:srSetColorTag', (_event, payload) => {
  const srId = String((payload && payload.srId) || '').trim();
  if (!srId) return { ok: false, message: 'srId is required.' };

  const refs = getReferences();
  const idx = findReferenceIndex(refs, srId);
  if (idx < 0) return { ok: false, message: 'Reference not found.' };

  refs[idx].color_tag = sanitizeReferenceColorTag(payload && payload.colorTag);
  refs[idx].updated_at = nowTs();
  setReferences(refs);
  return { ok: true, reference: refs[idx], references: refs };
});

ipcMain.handle('browser:srSetPinnedRoot', (_event, payload) => {
  const srId = String((payload && payload.srId) || '').trim();
  const pinned = !!(payload && payload.pinned);
  if (!srId) return { ok: false, message: 'srId is required.' };

  const refs = getReferences();
  const idx = findReferenceIndex(refs, srId);
  if (idx < 0) return { ok: false, message: 'Reference not found.' };

  const idMap = {};
  refs.forEach((ref) => {
    const id = String((ref && ref.id) || '').trim();
    if (id) idMap[id] = ref;
  });

  if (!isPinnableRootReference(refs[idx], idMap)) {
    return { ok: false, message: 'Only top-level private references can be pinned.' };
  }

  refs[idx].pinned_root = pinned;
  refs[idx].updated_at = nowTs();
  setReferences(refs);
  return { ok: true, reference: refs[idx], references: refs };
});

ipcMain.handle('browser:srClearChatAndAutoFork', (_event, srId) => {
  const refs = getReferences();
  const idx = findReferenceIndex(refs, srId);
  if (idx < 0) return { ok: false, message: 'Reference not found.' };

  const child = createForkReference(refs[idx], {
    title: `${refs[idx].title || 'Reference'} Fork`,
    source_metadata: {
      ...(refs[idx].source_metadata || {}),
      chat_cleared_at: nowTs(),
    },
  });
  child.chat_thread = { messages: [], last_message_at: null };
  child.updated_at = nowTs();
  refs[idx].children = Array.isArray(refs[idx].children) ? refs[idx].children : [];
  refs[idx].children.push(child.id);
  refs[idx].updated_at = nowTs();
  refs.unshift(child);
  setReferences(refs);
  return { ok: true, activeReference: child, references: refs };
});

ipcMain.handle('browser:srDeleteWithSuccession', (_event, srId) => {
  const refs = getReferences();
  const idx = findReferenceIndex(refs, srId);
  if (idx < 0) return { ok: false, message: 'Reference not found.' };

  const target = refs[idx];
  const children = Array.isArray(target.children) ? [...target.children] : [];

  const filtered = refs.filter((ref) => String((ref && ref.id) || '') !== String(srId));

  filtered.forEach((ref) => {
    if (!Array.isArray(ref.children)) return;
    ref.children = ref.children.filter((childId) => String(childId) !== String(srId));
  });

  if (children.length > 0) {
    const successorId = String(children[0] || '').trim();
    const successorIdx = findReferenceIndex(filtered, successorId);
    if (successorIdx >= 0) {
      filtered[successorIdx].parent_id = target.parent_id || null;
      filtered[successorIdx].updated_at = nowTs();
    }
  }

  setReferences(filtered);
  return { ok: true, references: filtered };
});

ipcMain.handle('browser:srSearch', (_event, payload) => {
  const query = String((payload && payload.query) || '').trim();
  const topK = Number((payload && payload.top_k) || 60);
  if (!query) return { ok: true, method: 'none', results: [] };

  const refs = getReferences().filter((ref) => !ref || !ref.is_public_candidate);
  const result = scoreReferencesHybrid(query, refs, { topK });
  return {
    ok: !!(result && result.ok),
    method: String((result && result.method) || 'hybrid:local-hash-embedding-v1'),
    results: Array.isArray(result && result.results) ? result.results : [],
  };
});

ipcMain.handle('browser:srSaveInActive', (_event, payload) => {
  const activeSrId = String((payload && payload.active_sr_id) || '').trim();
  const currentTab = (payload && payload.current_tab) || {};
  const insertAfterTabId = String((payload && payload.insert_after_tab_id) || '').trim();
  const normalizedTab = createWebTab(currentTab);

  const refs = getReferences();
  const idx = findReferenceIndex(refs, activeSrId);

  if (idx < 0) {
    const root = createReferenceBase({
      title: String((payload && payload.title) || normalizedTab.title || 'Reference').slice(0, 120),
      intent: String((payload && payload.intent) || ''),
      relation_type: 'root',
      current_tab: normalizedTab,
      title_user_edited: false,
    });
    refs.unshift(root);
    setReferences(refs);
    const seedTab = root.tabs && root.tabs[0] ? root.tabs[0] : normalizedTab;
    captureCommittedHistoryFromTab(seedTab, {
      committed_at: nowTs(),
      source_sr_id: root.id,
      source_tab_id: String((seedTab && seedTab.id) || ''),
    }).catch(() => {});
    return { ok: true, action: 'root_create', reference: root, references: refs };
  }

  const ref = refs[idx];
  ref.tabs = Array.isArray(ref.tabs) ? ref.tabs : [];
  const existingTab = ref.tabs.find((tab) => String((tab && tab.url) || '') === String(normalizedTab.url || ''));
  const exists = !!existingTab;
  if (!exists) {
    if (ref.tabs.length >= MAX_BROWSER_TABS_PER_REFERENCE) {
      return { ok: false, message: `Maximum web tabs reached (${MAX_BROWSER_TABS_PER_REFERENCE}).` };
    }
    insertWebTabAdjacent(ref, normalizedTab, insertAfterTabId);
  }
  ref.active_tab_id = exists && existingTab ? String(existingTab.id || '') : normalizedTab.id;
  maybeAutoRetitleReferenceFromActiveTab(ref);
  ref.updated_at = nowTs();
  setReferences(refs);
  const tabForHistory = existingTab || normalizedTab;
  captureCommittedHistoryFromTab(tabForHistory, {
    committed_at: nowTs(),
    source_sr_id: ref.id,
    source_tab_id: String((tabForHistory && tabForHistory.id) || ''),
  }).catch(() => {});

  return { ok: true, action: exists ? 'append' : 'duplicate', reference: ref, references: refs };
});

ipcMain.handle('browser:srAddTab', (_event, payload) => {
  const srId = String((payload && payload.srId) || '').trim();
  const tab = (payload && payload.tab) || {};
  const insertAfterTabId = String((payload && payload.insert_after_tab_id) || '').trim();
  const tabKind = String((tab && tab.tab_kind) || 'web').trim().toLowerCase();
  const refs = getReferences();
  const idx = findReferenceIndex(refs, srId);
  if (idx < 0) return { ok: false, message: 'Reference not found.' };

  refs[idx].tabs = Array.isArray(refs[idx].tabs) ? refs[idx].tabs : [];
  if (tabKind === 'skills') {
    const skillRes = ensureSingleSkillsTab(refs[idx]);
    if (skillRes && skillRes.tab) {
      refs[idx].active_tab_id = skillRes.tab.id;
      refs[idx].updated_at = nowTs();
      setReferences(refs);
      return { ok: true, tab: skillRes.tab, reference: refs[idx], references: refs, deduped: !skillRes.created };
    }
    return { ok: false, message: 'Unable to create skills tab.' };
  }

  if (tabKind === 'files') {
    const filesRes = ensureSingleFilesTab(refs[idx]);
    if (filesRes && filesRes.tab) {
      refs[idx].active_tab_id = filesRes.tab.id;
      refs[idx].updated_at = nowTs();
      setReferences(refs);
      return { ok: true, tab: filesRes.tab, reference: refs[idx], references: refs, deduped: !filesRes.created };
    }
    return { ok: false, message: 'Unable to create files tab.' };
  }

  const webCount = refs[idx].tabs.filter((item) => String((item && item.tab_kind) || 'web').trim().toLowerCase() === 'web').length;
  if (webCount >= MAX_BROWSER_TABS_PER_REFERENCE) {
    return { ok: false, message: `Maximum web tabs reached (${MAX_BROWSER_TABS_PER_REFERENCE}).` };
  }

  const nextTab = createWebTab(tab);
  insertWebTabAdjacent(refs[idx], nextTab, insertAfterTabId);
  refs[idx].active_tab_id = nextTab.id;
  maybeAutoRetitleReferenceFromActiveTab(refs[idx]);
  refs[idx].updated_at = nowTs();
  setReferences(refs);
  captureCommittedHistoryFromTab(nextTab, {
    committed_at: nowTs(),
    source_sr_id: refs[idx].id,
    source_tab_id: String(nextTab.id || ''),
  }).catch(() => {});

  return { ok: true, reference: refs[idx], references: refs };
});

ipcMain.handle('browser:srSetActiveTab', (_event, payload) => {
  const srId = String((payload && payload.srId) || '').trim();
  const tabId = String((payload && payload.tabId) || '').trim();
  const refs = getReferences();
  const idx = findReferenceIndex(refs, srId);
  if (idx < 0) return { ok: false, message: 'Reference not found.' };

  const tabs = Array.isArray(refs[idx].tabs) ? refs[idx].tabs : [];
  const found = tabs.find((tab) => String((tab && tab.id) || '') === tabId);
  if (!found) return { ok: false, message: 'Tab not found.' };

  refs[idx].active_tab_id = tabId;
  refs[idx].updated_at = nowTs();
  setReferences(refs);

  return { ok: true, reference: refs[idx], references: refs };
});

ipcMain.handle('browser:srPatchTab', (_event, payload) => {
  const srId = String((payload && payload.srId) || '').trim();
  const tabId = String((payload && payload.tabId) || '').trim();
  const patch = (payload && payload.patch && typeof payload.patch === 'object') ? payload.patch : {};
  const refs = getReferences();
  const idx = findReferenceIndex(refs, srId);
  if (idx < 0) return { ok: false, message: 'Reference not found.' };

  const tabs = Array.isArray(refs[idx].tabs) ? refs[idx].tabs : [];
  const tabIdx = tabs.findIndex((tab) => String((tab && tab.id) || '') === tabId);
  if (tabIdx < 0) return { ok: false, message: 'Tab not found.' };

  tabs[tabIdx] = {
    ...tabs[tabIdx],
    ...patch,
    updated_at: nowTs(),
  };
  refs[idx].tabs = tabs;
  maybeAutoRetitleReferenceFromActiveTab(refs[idx]);
  refs[idx].updated_at = nowTs();
  setReferences(refs);
  return { ok: true, reference: refs[idx], references: refs };
});

ipcMain.handle('browser:srRemoveTab', (_event, payload) => {
  const srId = String((payload && payload.srId) || '').trim();
  const tabId = String((payload && payload.tabId) || '').trim();
  const refs = getReferences();
  const idx = findReferenceIndex(refs, srId);
  if (idx < 0) return { ok: false, message: 'Reference not found.' };

  const tabs = Array.isArray(refs[idx].tabs) ? refs[idx].tabs : [];
  const filtered = tabs.filter((tab) => String((tab && tab.id) || '') !== tabId);
  refs[idx].tabs = filtered;
  if (String(refs[idx].active_tab_id || '') === tabId) {
    refs[idx].active_tab_id = filtered[0] ? filtered[0].id : null;
  }
  refs[idx].updated_at = nowTs();
  setReferences(refs);

  return { ok: true, reference: refs[idx], references: refs };
});

ipcMain.handle('browser:srOpenVizTab', (_event, payload) => {
  const srId = String((payload && payload.srId) || '').trim();
  if (!srId) return { ok: false, message: 'srId is required.' };
  if (findReferenceIndex(getReferences(), srId) < 0) return { ok: false, message: 'Reference not found.' };
  return {
    ok: false,
    message: 'Visualization tabs are deprecated. Create a markdown/html artifact instead.',
    deprecated: true,
  };
});

ipcMain.handle('browser:resolveArtifactAsset', (_event, payload) => {
  const srId = String((payload && payload.srId) || '').trim();
  const uri = String((payload && payload.uri) || '').trim();
  return resolveArtifactAssetUri(srId, uri);
});

ipcMain.handle('browser:saveArtifactImage', async (_event, payload) => {
  const srId = String((payload && payload.srId) || '').trim();
  const sourceUrl = String((payload && payload.sourceUrl) || '').trim();
  const suggestedName = String((payload && payload.suggestedName) || '').trim();
  return saveArtifactImageForReference(srId, sourceUrl, suggestedName);
});

ipcMain.handle('browser:srUpsertArtifact', (_event, payload) => {
  const srId = String((payload && payload.srId) || '').trim();
  const artifactInput = (payload && payload.artifact) || {};
  const refs = getReferences();
  const idx = findReferenceIndex(refs, srId);
  if (idx < 0) return { ok: false, message: 'Reference not found.' };

  refs[idx].artifacts = Array.isArray(refs[idx].artifacts) ? refs[idx].artifacts : [];
  const artifact = createArtifact(artifactInput);

  const existingIdx = refs[idx].artifacts.findIndex((a) => String((a && a.id) || '') === artifact.id);
  if (existingIdx >= 0) {
    const existing = refs[idx].artifacts[existingIdx];
    refs[idx].artifacts[existingIdx] = {
      ...existing,
      ...artifact,
      created_at: existing.created_at,
      updated_at: nowTs(),
    };
  } else {
    refs[idx].artifacts.push(artifact);
  }
  refs[idx].updated_at = nowTs();
  setReferences(refs);

  const fresh = refs[idx].artifacts.find((a) => String((a && a.id) || '') === artifact.id) || artifact;
  return { ok: true, artifact: fresh, reference: refs[idx], references: refs };
});

ipcMain.handle('browser:srGetArtifact', (_event, payload) => {
  const srId = String((payload && payload.srId) || '').trim();
  const artifactId = String((payload && payload.artifactId) || '').trim();
  const refs = getReferences();
  const idx = findReferenceIndex(refs, srId);
  if (idx < 0) return { ok: false, message: 'Reference not found.' };

  refs[idx].artifacts = Array.isArray(refs[idx].artifacts) ? refs[idx].artifacts : [];
  const artifact = refs[idx].artifacts.find((item) => String((item && item.id) || '') === artifactId);
  if (!artifact) return { ok: false, message: 'Artifact not found.' };

  return { ok: true, artifact };
});

ipcMain.handle('browser:srDeleteArtifact', (_event, payload) => {
  const srId = String((payload && payload.srId) || '').trim();
  const artifactId = String((payload && payload.artifactId) || '').trim();
  const refs = getReferences();
  const idx = findReferenceIndex(refs, srId);
  if (idx < 0) return { ok: false, message: 'Reference not found.' };

  const prev = Array.isArray(refs[idx].artifacts) ? refs[idx].artifacts : [];
  const next = prev.filter((artifact) => String((artifact && artifact.id) || '') !== artifactId);
  if (next.length === prev.length) return { ok: false, message: 'Artifact not found.' };

  refs[idx].artifacts = next;
  refs[idx].updated_at = nowTs();
  setReferences(refs);

  return { ok: true, reference: refs[idx], references: refs };
});

ipcMain.handle('browser:srAppendChatMessage', (_event, payload) => {
  const srId = String((payload && payload.srId) || '').trim();
  const role = String((payload && payload.role) || '').trim();
  const text = String((payload && payload.text) || '').trim();
  if (!srId || !role || !text) return { ok: false, message: 'srId, role, text are required.' };

  const refs = getReferences();
  const idx = findReferenceIndex(refs, srId);
  if (idx < 0) return { ok: false, message: 'Reference not found.' };

  appendChatMessage(refs[idx], role, text);
  refs[idx].updated_at = nowTs();
  setReferences(refs);

  return { ok: true, reference: refs[idx] };
});

ipcMain.handle('browser:srGetChatThread', (_event, srId) => {
  const refs = getReferences();
  const idx = findReferenceIndex(refs, String(srId || '').trim());
  if (idx < 0) return { ok: false, message: 'Reference not found.' };

  const messages = refs[idx].chat_thread && Array.isArray(refs[idx].chat_thread.messages)
    ? refs[idx].chat_thread.messages
    : [];
  return { ok: true, messages };
});

ipcMain.handle('browser:srUpdateAgentWeights', (_event, payload) => {
  const srId = String((payload && payload.srId) || '').trim();
  const weights = (payload && payload.weights && typeof payload.weights === 'object') ? payload.weights : {};
  const refs = getReferences();
  const idx = findReferenceIndex(refs, srId);
  if (idx < 0) return { ok: false, message: 'Reference not found.' };

  refs[idx].agent_weights = {
    ...(refs[idx].agent_weights && typeof refs[idx].agent_weights === 'object' ? refs[idx].agent_weights : {}),
    ...weights,
    updated_at: nowTs(),
  };
  refs[idx].updated_at = nowTs();
  setReferences(refs);

  return { ok: true, reference: refs[idx], references: refs };
});

ipcMain.handle('browser:srAppendDecisionTrace', (_event, payload) => {
  const srId = String((payload && payload.srId) || '').trim();
  const step = (payload && payload.step && typeof payload.step === 'object') ? payload.step : null;
  if (!step) return { ok: false, message: 'step is required.' };

  const refs = getReferences();
  const idx = findReferenceIndex(refs, srId);
  if (idx < 0) return { ok: false, message: 'Reference not found.' };

  refs[idx].decision_trace = Array.isArray(refs[idx].decision_trace) ? refs[idx].decision_trace : [];
  refs[idx].decision_trace.push(step);
  refs[idx].decision_trace = refs[idx].decision_trace.slice(-DECISION_TRACE_MAX_STEPS);
  appendDecisionTraceGraph(refs[idx], step);
  refs[idx].updated_at = nowTs();
  setReferences(refs);

  return { ok: true, reference: refs[idx], references: refs };
});

ipcMain.handle('browser:srApplyDiffOp', (_event, payload) => {
  const diffOp = (payload && typeof payload === 'object') ? payload : {};
  const targetKind = String((diffOp && diffOp.target_kind) || '').trim().toLowerCase();
  const srId = String((diffOp && diffOp.reference_id) || '').trim();
  if (!srId || !targetKind) return { ok: false, message: 'reference_id and target_kind are required.' };

  const refs = getReferences();
  const idx = findReferenceIndex(refs, srId);
  if (idx < 0) return { ok: false, message: 'Reference not found.' };

  let result;
  if (targetKind === 'artifact') {
    result = applyArtifactDiff(refs[idx], diffOp);
  } else if (targetKind === 'context_file') {
    result = applyContextFileDiff(refs[idx], diffOp);
  } else {
    result = { ok: false, message: `Unsupported diff target_kind: ${targetKind}` };
  }

  if (result.ok) {
    setReferences(refs);
  }

  return {
    ...result,
    reference: refs[idx],
    references: refs,
  };
});

ipcMain.handle('browser:srListPendingDiffOps', () => {
  return { ok: true, diff_ops: [] };
});

ipcMain.handle('browser:srAddContextFile', (_event, payload) => {
  try {
    const srId = String((payload && payload.srId) || '').trim();
    const absolutePath = String((payload && payload.absolutePath) || '').trim();
    if (!srId || !absolutePath) {
      return { ok: false, message: 'srId and absolutePath are required.' };
    }

    const ext = path.extname(absolutePath).toLowerCase();
    if (!['.txt', '.md'].includes(ext)) {
      return { ok: false, message: 'Only .txt and .md files are supported.' };
    }

    if (!fs.existsSync(absolutePath)) {
      return { ok: false, message: 'Selected file does not exist.' };
    }

    const stat = fs.statSync(absolutePath);
    if (!stat.isFile()) {
      return { ok: false, message: 'Selected path is not a file.' };
    }
    if (stat.size > CONTEXT_FILE_MAX_BYTES) {
      return { ok: false, message: 'Context file exceeds 1MB limit.' };
    }

    const refs = getReferences();
    const idx = findReferenceIndex(refs, srId);
    if (idx < 0) return { ok: false, message: 'Reference not found.' };

    const raw = fs.readFileSync(absolutePath, 'utf8');
    const hash = crypto.createHash('sha256').update(raw, 'utf8').digest('hex');
    refs[idx].context_files = Array.isArray(refs[idx].context_files) ? refs[idx].context_files : [];
    const already = refs[idx].context_files.find((file) => String((file && file.content_hash) || '') === hash);
    if (already) {
      return { ok: true, file: already, deduped: true, references: refs };
    }

    const fileId = makeId('ctx');
    const outDir = path.join(app.getPath('userData'), 'semantic_references', srId, 'context_files');
    fs.mkdirSync(outDir, { recursive: true });
    const safeName = path.basename(absolutePath).replace(/[^a-zA-Z0-9._-]/g, '_');
    const outPath = path.join(outDir, `${fileId}_${safeName}`);
    fs.writeFileSync(outPath, raw, 'utf8');

    const summary = raw.slice(0, 1200).split('\n').slice(0, 12).join(' ').trim();
    const item = {
      id: fileId,
      source_type: 'external_context_file',
      original_name: path.basename(absolutePath),
      relative_path: path.basename(absolutePath),
      stored_path: outPath,
      mime_type: ext === '.md' ? 'text/markdown' : 'text/plain',
      size_bytes: stat.size,
      content_hash: hash,
      ingest_status: 'ready',
      summary: summary.length > 360 ? `${summary.slice(0, 360)}...` : summary,
      read_only: false,
      created_at: nowTs(),
      updated_at: nowTs(),
    };
    refs[idx].context_files.push(item);
    const filesTabRes = ensureSingleFilesTab(refs[idx]);
    refs[idx].updated_at = nowTs();
    setReferences(refs);
    return {
      ok: true,
      file: item,
      files_tab: filesTabRes && filesTabRes.tab ? filesTabRes.tab : null,
      files_tab_created: !!(filesTabRes && filesTabRes.created),
      references: refs,
    };
  } catch (err) {
    return { ok: false, message: err.message || 'Failed to import context file.' };
  }
});

ipcMain.handle('browser:srUpsertYouTubeTranscript', async (_event, payload) => {
  const srId = String((payload && payload.srId) || '').trim();
  if (!srId) return { ok: false, message: 'srId is required.' };

  const refs = getReferences();
  const idx = findReferenceIndex(refs, srId);
  if (idx < 0) return { ok: false, message: 'Reference not found.' };

  const rawUrl = String((payload && payload.url) || '').trim();
  const videoId = String((payload && payload.video_id) || extractYouTubeVideoIdFromUrl(rawUrl) || '').trim();
  if (!videoId) return { ok: false, message: 'Could not resolve YouTube video id.' };

  refs[idx].youtube_transcripts = sanitizeYouTubeTranscriptMap(refs[idx].youtube_transcripts);
  refs[idx].context_files = Array.isArray(refs[idx].context_files) ? refs[idx].context_files : [];
  const currentRecord = refs[idx].youtube_transcripts[videoId] || null;
  const now = nowTs();

  let transcriptFullText = String((payload && (payload.transcript_full_text || payload.transcript_text)) || '').trim();
  let transcriptText = transcriptFullText.slice(0, YOUTUBE_TRANSCRIPT_MAX_CHARS).trim();
  let fetchedPayload = null;
  const hasExplicitStatus = !!(payload && payload.status && typeof payload.status === 'object');
  if (!transcriptText && !hasExplicitStatus) {
    fetchedPayload = await fetchYouTubeTranscriptViaTimedtext(rawUrl, videoId);
    if (fetchedPayload && fetchedPayload.ok) {
      transcriptFullText = String(fetchedPayload.transcript_full_text || fetchedPayload.transcript_text || '').trim();
      transcriptText = transcriptFullText.slice(0, YOUTUBE_TRANSCRIPT_MAX_CHARS).trim();
    }
  }

  let nextRecord = null;
  if (transcriptText) {
    const payloadTranscriptChars = Number(payload && payload.transcript_char_count);
    const transcriptCharCount = Number.isFinite(payloadTranscriptChars)
      ? Math.max(0, Math.round(payloadTranscriptChars))
      : transcriptFullText.length;
    nextRecord = sanitizeYouTubeTranscriptRecord({
      ...(currentRecord || {}),
      video_id: videoId,
      url: rawUrl || (currentRecord && currentRecord.url) || '',
      title: String((payload && payload.title) || (fetchedPayload && fetchedPayload.title) || (currentRecord && currentRecord.title) || ''),
      language: String((payload && payload.language) || (fetchedPayload && fetchedPayload.language) || (currentRecord && currentRecord.language) || ''),
      transcript_text: transcriptText,
      summary: String((payload && payload.summary) || (fetchedPayload && fetchedPayload.summary) || summarizeYouTubeTranscriptText(transcriptFullText || transcriptText)),
      transcript_char_count: transcriptCharCount || transcriptText.length,
      transcript_truncated: !!(payload && payload.transcript_truncated) || transcriptCharCount > transcriptText.length,
      source: String((payload && payload.source) || (fetchedPayload && fetchedPayload.source) || (currentRecord && currentRecord.source) || 'youtube_captions'),
      updated_at: now,
      status: { state: 'ready', retry_after: null },
    }, videoId);
  } else {
    const statusInput = (payload && payload.status && typeof payload.status === 'object')
      ? payload.status
      : {
        state: 'error',
        error_code: String((fetchedPayload && fetchedPayload.error_code) || 'transcript_unavailable'),
        message: String((fetchedPayload && fetchedPayload.message) || 'Transcript unavailable.'),
        retry_after: now + YOUTUBE_TRANSCRIPT_RETRY_COOLDOWN_MS,
      };
    const retryAfterInput = Number(statusInput.retry_after);
    const retryAfter = Number.isFinite(retryAfterInput)
      ? Math.max(0, Math.round(retryAfterInput))
      : now + YOUTUBE_TRANSCRIPT_RETRY_COOLDOWN_MS;
    nextRecord = sanitizeYouTubeTranscriptRecord({
      ...(currentRecord || {}),
      video_id: videoId,
      url: rawUrl || (currentRecord && currentRecord.url) || '',
      title: String((payload && payload.title) || (currentRecord && currentRecord.title) || ''),
      language: String((payload && payload.language) || (currentRecord && currentRecord.language) || ''),
      transcript_text: '',
      summary: String((payload && payload.summary) || (currentRecord && currentRecord.summary) || ''),
      source: String((payload && payload.source) || (currentRecord && currentRecord.source) || 'youtube_captions'),
      updated_at: now,
      status: {
        state: 'error',
        error_code: String(statusInput.error_code || 'transcript_error'),
        message: String(statusInput.message || 'Transcript unavailable.'),
        retry_after: retryAfter,
      },
    }, videoId);
  }

  if (!nextRecord) return { ok: false, message: 'Failed to sanitize transcript payload.' };
  refs[idx].youtube_transcripts[videoId] = nextRecord;
  refs[idx].youtube_transcripts = sanitizeYouTubeTranscriptMap(refs[idx].youtube_transcripts);

  if (String((nextRecord.status && nextRecord.status.state) || '') === 'ready') {
    const existingContext = refs[idx].context_files.find((item) => {
      if (!item || typeof item !== 'object') return false;
      return String(item.video_id || '') === videoId || String(item.id || '') === `ctx_yt_${videoId}`;
    });
    const synthetic = buildSyntheticYouTubeContextFile(
      srId,
      videoId,
      nextRecord,
      transcriptFullText || transcriptText,
      existingContext || null,
    );
    if (synthetic) {
      const existingIdx = refs[idx].context_files.findIndex((item) => {
        if (!item || typeof item !== 'object') return false;
        return String(item.video_id || '') === videoId || String(item.id || '') === `ctx_yt_${videoId}`;
      });
      if (existingIdx >= 0) refs[idx].context_files[existingIdx] = synthetic;
      else refs[idx].context_files.push(synthetic);
    }
  }
  pruneYouTubeTranscriptContextFiles(refs[idx]);
  refs[idx].updated_at = now;
  setReferences(refs);

  return {
    ok: true,
    record: refs[idx].youtube_transcripts[videoId] || nextRecord,
    reference: refs[idx],
    references: refs,
  };
});

ipcMain.handle('browser:srMountFolder', async (_event, payload) => {
  const srId = String((payload && payload.srId) || '').trim();
  const refs = getReferences();
  const idx = findReferenceIndex(refs, srId);
  if (idx < 0) return { ok: false, message: 'Reference not found.' };

  let targetFolder = String((payload && payload.absolutePath) || '').trim();
  if (!targetFolder) {
    const pick = await dialog.showOpenDialog(mainWindow, {
      title: 'Choose a folder to mount as reference context',
      properties: ['openDirectory'],
    });
    if (pick.canceled || !Array.isArray(pick.filePaths) || pick.filePaths.length === 0) {
      return { ok: false, canceled: true, message: 'Folder selection canceled.' };
    }
    targetFolder = String(pick.filePaths[0] || '').trim();
  }

  if (!targetFolder || !fs.existsSync(targetFolder)) {
    return { ok: false, message: 'Folder path does not exist.' };
  }

  const mountId = makeId('mount');
  const ingest = indexFolderAsContext(targetFolder, {
    maxFiles: 250,
    maxFileBytes: 256 * 1024,
  });

  refs[idx].folder_mounts = Array.isArray(refs[idx].folder_mounts) ? refs[idx].folder_mounts : [];
  refs[idx].context_files = Array.isArray(refs[idx].context_files) ? refs[idx].context_files : [];

  const mount = {
    id: mountId,
    absolute_path: ingest.root_path,
    read_only: true,
    indexed_at: nowTs(),
    file_count: ingest.files.length,
    skipped_count: ingest.skipped_count,
    truncated: ingest.truncated,
  };
  refs[idx].folder_mounts.push(mount);

  const dedupe = new Set(refs[idx].context_files.map((file) => `${file.content_hash || ''}:${file.relative_path || ''}`));

  const imported = [];
  ingest.files.forEach((file) => {
    const key = `${file.content_hash || ''}:${file.relative_path || ''}`;
    if (dedupe.has(key)) return;
    dedupe.add(key);

    const item = {
      id: makeId('ctx'),
      source_type: 'folder_mount',
      mount_id: mountId,
      original_name: path.basename(file.relative_path),
      relative_path: file.relative_path,
      stored_path: file.absolute_path,
      mime_type: file.mime_type,
      size_bytes: file.size_bytes,
      content_hash: file.content_hash,
      summary: file.summary,
      read_only: true,
      created_at: nowTs(),
      updated_at: nowTs(),
    };
    refs[idx].context_files.push(item);
    imported.push(item);
  });

  const filesTabRes = ensureSingleFilesTab(refs[idx]);

  refs[idx].updated_at = nowTs();
  setReferences(refs);

  return {
    ok: true,
    mount,
    files_tab: filesTabRes && filesTabRes.tab ? filesTabRes.tab : null,
    files_tab_created: !!(filesTabRes && filesTabRes.created),
    imported_count: imported.length,
    imported,
    reference: refs[idx],
    references: refs,
  };
});

ipcMain.handle('browser:srReindexFolderMount', (_event, payload) => {
  const srId = String((payload && payload.srId) || '').trim();
  const mountId = String((payload && payload.mountId) || '').trim();
  if (!srId || !mountId) return { ok: false, message: 'srId and mountId are required.' };

  const refs = getReferences();
  const idx = findReferenceIndex(refs, srId);
  if (idx < 0) return { ok: false, message: 'Reference not found.' };

  refs[idx].folder_mounts = Array.isArray(refs[idx].folder_mounts) ? refs[idx].folder_mounts : [];
  refs[idx].context_files = Array.isArray(refs[idx].context_files) ? refs[idx].context_files : [];
  const mount = refs[idx].folder_mounts.find((item) => String((item && item.id) || '') === mountId);
  if (!mount) return { ok: false, message: 'Mount not found.' };

  const targetFolder = String((mount && mount.absolute_path) || '').trim();
  if (!targetFolder || !fs.existsSync(targetFolder)) {
    return { ok: false, message: 'Mounted folder path no longer exists.' };
  }

  const ingest = indexFolderAsContext(targetFolder, {
    maxFiles: 250,
    maxFileBytes: 256 * 1024,
  });

  refs[idx].context_files = refs[idx].context_files.filter((file) => String((file && file.mount_id) || '') !== mountId);
  const dedupe = new Set(refs[idx].context_files.map((file) => `${file.content_hash || ''}:${file.relative_path || ''}`));
  const imported = [];

  ingest.files.forEach((file) => {
    const key = `${file.content_hash || ''}:${file.relative_path || ''}`;
    if (dedupe.has(key)) return;
    dedupe.add(key);
    const item = {
      id: makeId('ctx'),
      source_type: 'folder_mount',
      mount_id: mountId,
      original_name: path.basename(file.relative_path),
      relative_path: file.relative_path,
      stored_path: file.absolute_path,
      mime_type: file.mime_type,
      size_bytes: file.size_bytes,
      content_hash: file.content_hash,
      summary: file.summary,
      read_only: true,
      created_at: nowTs(),
      updated_at: nowTs(),
    };
    refs[idx].context_files.push(item);
    imported.push(item);
  });

  mount.indexed_at = nowTs();
  mount.file_count = ingest.files.length;
  mount.skipped_count = ingest.skipped_count;
  mount.truncated = ingest.truncated;

  const filesTabRes = ensureSingleFilesTab(refs[idx]);
  refs[idx].updated_at = nowTs();
  setReferences(refs);

  return {
    ok: true,
    mount,
    files_tab: filesTabRes && filesTabRes.tab ? filesTabRes.tab : null,
    files_tab_created: !!(filesTabRes && filesTabRes.created),
    imported_count: imported.length,
    imported,
    reference: refs[idx],
    references: refs,
  };
});

ipcMain.handle('browser:srUnmountFolder', (_event, payload) => {
  const srId = String((payload && payload.srId) || '').trim();
  const mountId = String((payload && payload.mountId) || '').trim();
  if (!srId || !mountId) return { ok: false, message: 'srId and mountId are required.' };

  const refs = getReferences();
  const idx = findReferenceIndex(refs, srId);
  if (idx < 0) return { ok: false, message: 'Reference not found.' };

  refs[idx].folder_mounts = Array.isArray(refs[idx].folder_mounts) ? refs[idx].folder_mounts : [];
  refs[idx].context_files = Array.isArray(refs[idx].context_files) ? refs[idx].context_files : [];
  const prevMounts = refs[idx].folder_mounts.length;
  refs[idx].folder_mounts = refs[idx].folder_mounts.filter((item) => String((item && item.id) || '') !== mountId);
  if (refs[idx].folder_mounts.length === prevMounts) {
    return { ok: false, message: 'Mount not found.' };
  }

  refs[idx].context_files = refs[idx].context_files.filter((file) => String((file && file.mount_id) || '') !== mountId);
  const filesTabRes = ensureSingleFilesTab(refs[idx]);
  refs[idx].updated_at = nowTs();
  setReferences(refs);

  return {
    ok: true,
    files_tab: filesTabRes && filesTabRes.tab ? filesTabRes.tab : null,
    files_tab_created: !!(filesTabRes && filesTabRes.created),
    reference: refs[idx],
    references: refs,
  };
});

ipcMain.handle('browser:srListContextFiles', (_event, srId) => {
  const refs = getReferences();
  const idx = findReferenceIndex(refs, String(srId || '').trim());
  if (idx < 0) return [];
  return Array.isArray(refs[idx].context_files) ? refs[idx].context_files : [];
});

ipcMain.handle('browser:srGetContextFilePreview', (_event, payload) => {
  const srId = String((payload && payload.srId) || '').trim();
  const fileId = String((payload && payload.fileId) || '').trim();

  const refs = getReferences();
  const idx = findReferenceIndex(refs, srId);
  if (idx < 0) return { ok: false, message: 'Reference not found.' };

  const files = Array.isArray(refs[idx].context_files) ? refs[idx].context_files : [];
  const item = files.find((file) => String((file && file.id) || '') === fileId);
  if (!item) return { ok: false, message: 'Context file not found.' };

  const targetPath = String(item.stored_path || '').trim();
  if (!targetPath || !fs.existsSync(targetPath)) {
    return { ok: false, message: 'Context file path is unavailable.' };
  }

  try {
    const raw = fs.readFileSync(targetPath, 'utf8');
    return { ok: true, preview: raw.slice(0, 4000), file: item };
  } catch (err) {
    return { ok: false, message: err.message || 'Unable to read context file.' };
  }
});

ipcMain.handle('browser:srRemoveContextFile', (_event, payload) => {
  const srId = String((payload && payload.srId) || '').trim();
  const fileId = String((payload && payload.fileId) || '').trim();
  const refs = getReferences();
  const idx = findReferenceIndex(refs, srId);
  if (idx < 0) return { ok: false, message: 'Reference not found.' };

  const prev = Array.isArray(refs[idx].context_files) ? refs[idx].context_files : [];
  const next = prev.filter((file) => String((file && file.id) || '') !== fileId);
  if (next.length === prev.length) return { ok: false, message: 'Context file not found.' };

  refs[idx].context_files = next;
  refs[idx].updated_at = nowTs();
  setReferences(refs);

  return { ok: true, reference: refs[idx], references: refs };
});

ipcMain.handle('browser:srGetProgram', (_event, payload) => {
  const srId = String((payload && payload.srId) || '').trim();
  if (!srId) return { ok: false, message: 'srId is required.' };
  const refs = getReferences();
  const idx = findReferenceIndex(refs, srId);
  if (idx < 0) return { ok: false, message: 'Reference not found.' };
  return {
    ok: true,
    program: String((refs[idx] && refs[idx].program) || ''),
  };
});

ipcMain.handle('browser:srSetProgram', (_event, payload) => {
  const srId = String((payload && payload.srId) || '').trim();
  const program = String((payload && payload.program) || '');
  if (!srId) return { ok: false, message: 'srId is required.' };
  const refs = getReferences();
  const idx = findReferenceIndex(refs, srId);
  if (idx < 0) return { ok: false, message: 'Reference not found.' };
  refs[idx].program = program;
  refs[idx].updated_at = nowTs();
  setReferences(refs);
  return {
    ok: true,
    program,
    reference: refs[idx],
    references: refs,
  };
});

ipcMain.handle('browser:pythonCheck', async () => {
  const runtimes = await getPythonRuntimeResolver().diagnostics({ bypassCache: true });
  const tool = runtimes && runtimes.tool && typeof runtimes.tool === 'object' ? runtimes.tool : {};
  return {
    ok: !!tool.ok,
    version: String(tool.version || ''),
    message: String(tool.message || ''),
    source: String(tool.source || ''),
    python_bin: String(tool.python_bin || ''),
    runtimes: {
      tool,
      viz: (runtimes && runtimes.viz && typeof runtimes.viz === 'object') ? runtimes.viz : {},
    },
  };
});

ipcMain.handle('browser:pythonExec', async (_event, payload) => {
  const srId = String((payload && payload.srId) || '').trim();
  const code = String((payload && payload.code) || '');
  if (!srId) return { ok: false, message: 'srId is required.' };
  if (findReferenceIndex(getReferences(), srId) < 0) return { ok: false, message: 'Reference not found.' };
  const runRes = await runPythonForReference(srId, code, Math.min(CHAT_REQUEST_TIMEOUT_MS, 45_000), {}, 'tool');
  return {
    ok: !!(runRes && runRes.ok),
    stdout: String((runRes && runRes.stdout) || ''),
    stderr: String((runRes && runRes.stderr) || ''),
    png_base64: String((runRes && runRes.png_base64) || ''),
    png_path: String((runRes && runRes.png_path) || ''),
    execution_id: String((runRes && runRes.execution_id) || ''),
    timed_out: !!(runRes && runRes.timed_out),
    exit_code: Number((runRes && runRes.exit_code) || 0),
  };
});

ipcMain.handle('browser:pipInstall', async (_event, payload) => {
  const srId = String((payload && payload.srId) || '').trim();
  const packages = Array.isArray(payload && payload.packages) ? payload.packages : [];
  if (!srId) return { ok: false, message: 'srId is required.' };
  if (findReferenceIndex(getReferences(), srId) < 0) return { ok: false, message: 'Reference not found.' };
  if (app.isPackaged) {
    return {
      ok: false,
      installed: [],
      rejected: [],
      stdout: '',
      stderr: PACKAGED_PYTHON_IMMUTABLE_MESSAGE,
      timed_out: false,
      allowlist: [],
      message: PACKAGED_PYTHON_IMMUTABLE_MESSAGE,
    };
  }
  const runtime = await getPythonRuntimeResolver().resolve('tool');
  const res = await installAllowedPackages({
    packages,
    pythonBin: String((runtime && runtime.python_bin) || 'python3').trim() || 'python3',
    cwd: app.getPath('userData'),
    timeoutMs: 120_000,
  });
  return {
    ok: !!(res && res.ok),
    installed: Array.isArray(res && res.installed) ? res.installed : [],
    rejected: Array.isArray(res && res.rejected) ? res.rejected : [],
    stdout: String((res && res.stdout) || ''),
    stderr: String((res && res.stderr) || ''),
    timed_out: !!(res && res.timed_out),
    allowlist: Array.isArray(res && res.allowlist) ? res.allowlist : [],
  };
});

ipcMain.handle('browser:srListSkills', (_event, payload) => {
  const srId = String((payload && payload.srId) || '').trim();
  if (!srId) return { ok: false, message: 'srId is required.' };
  const refs = getReferences();
  const idx = findReferenceIndex(refs, srId);
  if (idx < 0) return { ok: false, message: 'Reference not found.' };
  const out = listSkillsForReference(refs[idx]);
  return {
    ok: true,
    ...out,
  };
});

ipcMain.handle('browser:srSaveSkill', (_event, payload) => {
  const srId = String((payload && payload.srId) || '').trim();
  const scope = String((payload && payload.scope) || 'local').trim().toLowerCase() === 'global' ? 'global' : 'local';
  const skill = (payload && payload.skill && typeof payload.skill === 'object') ? payload.skill : {};
  const res = upsertSkillForReference(srId, skill, scope);
  if (!res || !res.ok) return { ok: false, message: (res && res.message) || 'Unable to save skill.' };
  const refs = getReferences();
  const idx = findReferenceIndex(refs, srId);
  if (idx >= 0) {
    const skillTabRes = ensureSingleSkillsTab(refs[idx]);
    if (skillTabRes && skillTabRes.created) {
      refs[idx].updated_at = nowTs();
      setReferences(refs);
    }
  }
  return {
    ok: true,
    skill: res.skill,
    linked_skills: res.linked_skills,
    references: getReferences(),
  };
});

ipcMain.handle('browser:srDeleteSkill', (_event, payload) => {
  const srId = String((payload && payload.srId) || '').trim();
  const skillId = String((payload && payload.skillId) || '').trim();
  const scope = String((payload && payload.scope) || 'local').trim().toLowerCase() === 'global' ? 'global' : 'local';
  const res = deleteSkillForReference(srId, skillId, scope);
  if (!res || !res.ok) return { ok: false, message: (res && res.message) || 'Unable to delete skill.' };
  return {
    ok: true,
    references: res.references || getReferences(),
  };
});

ipcMain.handle('browser:srRunSkill', async (_event, payload) => {
  const srId = String((payload && payload.srId) || '').trim();
  const skillId = String((payload && payload.skillId) || '').trim();
  const scope = String((payload && payload.scope) || 'local').trim().toLowerCase() === 'global' ? 'global' : 'local';
  const args = (payload && payload.args && typeof payload.args === 'object') ? payload.args : null;
  if (!srId || !skillId) return { ok: false, message: 'srId and skillId are required.' };
  const runRes = await runSkillForReference(srId, skillId, scope, args);
  const pythonArtifact = buildPythonArtifactPendingOutput(srId, 'Skill Output', runRes);
  return {
    ok: !!(runRes && runRes.ok),
    stdout: String((runRes && runRes.stdout) || ''),
    stderr: String((runRes && runRes.stderr) || ''),
    png_base64: String((runRes && runRes.png_base64) || ''),
    png_path: String((runRes && runRes.png_path) || ''),
    execution_id: String((runRes && runRes.execution_id) || ''),
    timed_out: !!(runRes && runRes.timed_out),
    skill: runRes && runRes.skill ? runRes.skill : null,
    pending_artifacts: (pythonArtifact && pythonArtifact.artifact) ? [pythonArtifact.artifact] : [],
    pending_workspace_tabs: (pythonArtifact && pythonArtifact.workspace_tab) ? [pythonArtifact.workspace_tab] : [],
  };
});

ipcMain.handle('browser:chatStart', async (_event, payload) => {
  const input = (payload && typeof payload === 'object') ? payload : {};
  const srId = String(input.sr_id || input.srId || '').trim();
  const message = String(input.message || '').trim();
  const requestId = String(input.request_id || makeId('req')).trim();

  if (!srId) return { ok: false, message: 'sr_id is required.' };
  if (!message) return { ok: false, message: 'message is required.' };
  if (!requestId) return { ok: false, message: 'request_id is required.' };
  if (findReferenceIndex(getReferences(), srId) < 0) return { ok: false, message: 'Active reference not found.' };
  if (activeChatRequests.has(requestId)) {
    return { ok: false, message: 'request_id is already active.' };
  }

  const timeout = setTimeout(() => {
    const active = activeChatRequests.get(requestId);
    if (!active) return;
    try {
      active.abortController.abort(new Error('Request timed out.'));
    } catch (_) {
      // noop
    }
  }, CHAT_REQUEST_TIMEOUT_MS);
  const abortController = new AbortController();
  activeChatRequests.set(requestId, { abortController, timeout, srId });

  (async () => {
    let statusSeq = 0;
    try {
      const response = await executePathAChat({
        ...input,
        sr_id: srId,
        request_id: requestId,
      }, {
        signal: abortController.signal,
        timeoutMs: CHAT_REQUEST_TIMEOUT_MS,
        onDelta: (deltaText) => {
          const text = String(deltaText || '');
          if (!text) return;
          sendBrowserEvent('browser:chatStream', {
            request_id: requestId,
            phase: 'delta',
            delta_text: text,
          });
        },
        onStatus: (statusPayload) => {
          const status = (statusPayload && typeof statusPayload === 'object') ? statusPayload : {};
          const text = String(status.text || '').trim();
          if (!text) return;
          statusSeq += 1;
          sendBrowserEvent('browser:chatStream', {
            request_id: requestId,
            sr_id: srId,
            phase: 'status',
            seq: statusSeq,
            state: String(status.state || 'info').trim().toLowerCase() || 'info',
            source: String(status.source || 'agent').trim().toLowerCase() || 'agent',
            tool_name: String(status.tool_name || '').trim(),
            text,
            meta: (status.meta && typeof status.meta === 'object') ? status.meta : {},
          });
        },
      });
      sendBrowserEvent('browser:chatStream', {
        request_id: requestId,
        phase: 'final',
        ...response,
      });
    } catch (err) {
      const aborted = abortController.signal.aborted;
      const messageText = aborted
        ? 'Request canceled.'
        : String((err && err.message) || 'Lumino request failed.');
      sendBrowserEvent('browser:chatStream', {
        request_id: requestId,
        phase: 'error',
        message: messageText,
      });
    } finally {
      const active = activeChatRequests.get(requestId);
      if (active && active.timeout) clearTimeout(active.timeout);
      activeChatRequests.delete(requestId);
    }
  })();

  return { ok: true, request_id: requestId };
});

ipcMain.handle('browser:chatCancel', async (_event, payload) => {
  const requestId = String((payload && payload.request_id) || payload || '').trim();
  if (!requestId) return { ok: false, message: 'request_id is required.' };
  const active = activeChatRequests.get(requestId);
  if (!active) return { ok: false, message: 'Request not found.', request_id: requestId };
  try {
    active.abortController.abort(new Error('Request canceled.'));
  } catch (_) {
    // noop
  }
  return { ok: true, request_id: requestId };
});

ipcMain.handle('browser:chat', async (_event, payload) => {
  const input = (payload && typeof payload === 'object') ? payload : {};
  const srId = String(input.sr_id || input.srId || '').trim();
  const requestId = String(input.request_id || makeId('req')).trim();

  try {
    const response = await executePathAChat({
      ...input,
      sr_id: srId,
      request_id: requestId,
    }, {
      timeoutMs: CHAT_REQUEST_TIMEOUT_MS,
    });
    return response;
  } catch (err) {
    return {
      message: String((err && err.message) || 'Lumino request failed.'),
      request_id: requestId,
      sr_id: srId,
      pending_artifacts: [],
      pending_weight_updates: [],
      pending_decision_traces: [],
      pending_workspace_tabs: [],
      pending_diff_ops: [],
      pending_hyperweb_queries: [],
      pending_hyperweb_suggestions: [],
    };
  }
});

ipcMain.handle('browser:crawlerStart', async (_event, payload) => {
  const input = (payload && typeof payload === 'object') ? payload : {};
  const srId = String(input.sr_id || input.srId || '').trim();
  if (!srId) return { ok: false, message: 'sr_id is required.' };
  if (findReferenceIndex(getReferences(), srId) < 0) {
    return { ok: false, message: 'Reference not found.' };
  }
  const sourceType = String(input.source_type || input.sourceType || 'web').trim().toLowerCase() === 'local'
    ? 'local'
    : 'web';
  const crawlConfig = sanitizeCrawlerSettings(input);
  const startRes = await luminoCrawler.start({
    sr_id: srId,
    source_type: sourceType,
    url: String(input.url || '').trim(),
    absolute_path: String(input.absolute_path || input.absolutePath || '').trim(),
    depth: crawlConfig.depth,
    page_cap: crawlConfig.page_cap,
    mode: crawlConfig.mode,
    markdown_first: crawlConfig.markdown_first,
    robots_policy: crawlConfig.robots_policy,
  });
  if (!startRes || !startRes.ok) {
    return { ok: false, message: String((startRes && startRes.message) || 'Unable to start crawler job.') };
  }
  return {
    ok: true,
    job: startRes.job,
  };
});

ipcMain.handle('browser:crawlerStatus', (_event, payload) => {
  const input = (payload && typeof payload === 'object') ? payload : {};
  const jobId = String(input.job_id || input.jobId || '').trim();
  const srId = String(input.sr_id || input.srId || '').trim();
  if (jobId) return luminoCrawler.getStatus(jobId);

  const statusRes = luminoCrawler.getStatus('');
  if (!statusRes || !statusRes.ok) {
    return { ok: false, message: String((statusRes && statusRes.message) || 'Crawler status unavailable.') };
  }
  const jobs = Array.isArray(statusRes.jobs) ? statusRes.jobs : [];
  const filtered = srId
    ? jobs.filter((job) => String((job && job.sr_id) || '').trim() === srId)
    : jobs;
  return {
    ok: true,
    jobs: filtered,
    count: filtered.length,
  };
});

ipcMain.handle('browser:crawlerStop', (_event, payload) => {
  const input = (payload && typeof payload === 'object') ? payload : {};
  const explicitJobId = String(input.job_id || input.jobId || '').trim();
  const srId = String(input.sr_id || input.srId || '').trim();
  const targetJobId = explicitJobId || String(((getLatestCrawlerJobForReference(srId) || {}).id) || '').trim();
  if (!targetJobId) {
    return { ok: false, message: srId ? 'No crawler job found for this reference.' : 'No crawler job found.' };
  }
  const stopRes = luminoCrawler.stop(targetJobId);
  if (!stopRes || !stopRes.ok) {
    return { ok: false, message: String((stopRes && stopRes.message) || 'Unable to stop crawler job.') };
  }
  return {
    ok: true,
    job: stopRes.job,
  };
});

ipcMain.handle('browser:providerSetKey', (_event, payload) => {
  const provider = String((payload && payload.provider) || '').trim().toLowerCase();
  const apiKey = String((payload && payload.apiKey) || '');
  if (!PROVIDERS.includes(provider)) return { ok: false, message: 'Unsupported provider.' };
  return upsertProviderPrimaryCompatibility(provider, apiKey);
});

ipcMain.handle('browser:providerDeleteKey', (_event, payload) => {
  const provider = String((payload && payload.provider) || '').trim().toLowerCase();
  if (!PROVIDERS.includes(provider)) return { ok: false, message: 'Unsupported provider.' };
  return deleteProviderKeyProfileEntry({ provider });
});

ipcMain.handle('browser:providerListConfigured', () => {
  const state = buildProviderKeysState();
  if (!state || !state.ok) {
    return {
      ok: false,
      message: (state && state.message) || 'Provider key status unavailable.',
      providers: [],
    };
  }
  return {
    ok: true,
    providers: (state.providers || []).map((entry) => ({
      provider: String((entry && entry.provider) || ''),
      configured: !!(entry && entry.configured),
    })),
  };
});

ipcMain.handle('browser:providerKeysList', () => {
  return buildProviderKeysState();
});

ipcMain.handle('browser:providerKeyUpsert', (_event, payload) => {
  return upsertProviderKeyProfile(payload || {});
});

ipcMain.handle('browser:providerKeyDelete', (_event, payload) => {
  return deleteProviderKeyProfileEntry(payload || {});
});

ipcMain.handle('browser:providerKeySetPrimary', (_event, payload) => {
  return setProviderPrimaryKeyProfile(payload || {});
});

ipcMain.handle('browser:providerListModels', async (_event, payload) => {
  const provider = String((payload && payload.provider) || '').trim().toLowerCase();
  const keyId = normalizeProviderKeyId((payload && payload.keyId) || (payload && payload.key_id) || '');
  if (!PROVIDERS.includes(provider)) return { ok: false, message: 'Unsupported provider.' };
  let apiKey = '';
  let resolvedKeyId = '';
  if (provider === 'lmstudio') {
    const creds = resolveProviderRuntimeCredentials(provider, readSettings(), keyId);
    if (!creds || !creds.ok) {
      return { ok: false, message: (creds && creds.message) || 'LM Studio configuration is invalid.' };
    }
    apiKey = String(creds.apiKey || '');
    resolvedKeyId = String(creds.key_id || '');
  } else {
    const keyRes = resolveProviderApiKey(provider, keyId);
    if (!keyRes || !keyRes.ok || !keyRes.apiKey) {
      return { ok: false, message: (keyRes && keyRes.message) || 'API key is not configured for this provider.' };
    }
    apiKey = String(keyRes.apiKey || '');
    resolvedKeyId = String(keyRes.key_id || '');
  }
  const modelRes = await fetchProviderModels(provider, apiKey);
  if (!modelRes || !modelRes.ok) return modelRes;
  return {
    ...modelRes,
    key_id: resolvedKeyId,
  };
});

ipcMain.handle('browser:importFromBrowser', (_event, payload) => {
  const source = String((payload && payload.source) || '').trim().toLowerCase();
  if (!['chrome', 'safari'].includes(source)) {
    return { ok: false, message: 'Unsupported browser source.' };
  }
  return importBrowserDataToReference(source);
});

ipcMain.handle('browser:requestDefaultBrowser', () => {
  return requestDefaultBrowser();
});

ipcMain.handle('browser:openDefaultBrowserSettings', () => {
  return openDefaultBrowserSettings();
});

ipcMain.handle('browser:getPreferences', () => {
  return { ok: true, ...readSettings() };
});

ipcMain.handle('browser:telegramStatus', async () => {
  const settings = readSettings();
  const tokenRef = String((settings && settings.telegram_bot_token_ref) || '').trim();
  const tokenConfigured = tokenRef ? !!getSecureSecretStore().hasSecret(tokenRef) : false;
  const runtime = telegramService && typeof telegramService.status === 'function'
    ? telegramService.status()
    : { ok: true, running: false };
  const usersRes = getOrchestratorUsersStore().listUsers();
  const activeUsers = Array.isArray(usersRes && usersRes.users) ? usersRes.users : [];
  return {
    ok: true,
    enabled: !!settings.telegram_enabled,
    token_ref: tokenRef,
    token_configured: tokenConfigured,
    active_user_count: activeUsers.length,
    poll_interval_sec: Number(settings.telegram_poll_interval_sec || 2),
    runtime,
  };
});

ipcMain.handle('browser:telegramSetToken', async (_event, payload) => {
  const token = String((payload && payload.token) || '').trim();
  if (!token) return { ok: false, message: 'token is required.' };
  const current = readSettings();
  const existingRef = String((current && current.telegram_bot_token_ref) || '').trim();
  const setRes = setSecretValueByRef(existingRef, token, TELEGRAM_SECRET_REF_PREFIX);
  if (!setRes || !setRes.ok) {
    return { ok: false, message: String((setRes && setRes.message) || 'Unable to save Telegram token.') };
  }
  const updated = writeSettings({
    telegram_bot_token_ref: String(setRes.ref || '').trim(),
    telegram_enabled: true,
  });
  const runtimeRes = await applySettingsRuntimeEffects(current, updated);
  return {
    ok: true,
    token_ref: String(setRes.ref || '').trim(),
    settings: updated,
    applied_runtime: runtimeRes,
  };
});

ipcMain.handle('browser:telegramClearToken', async () => {
  const current = readSettings();
  const tokenRef = String((current && current.telegram_bot_token_ref) || '').trim();
  const clearRes = clearSecretValueByRef(tokenRef);
  if (!clearRes || !clearRes.ok) {
    return { ok: false, message: String((clearRes && clearRes.message) || 'Unable to clear Telegram token.') };
  }
  const updated = writeSettings({
    telegram_bot_token_ref: '',
    telegram_enabled: false,
  });
  const runtimeRes = await applySettingsRuntimeEffects(current, updated);
  return {
    ok: true,
    settings: updated,
    applied_runtime: runtimeRes,
  };
});

ipcMain.handle('browser:telegramTestMessage', async (_event, payload) => {
  const settings = readSettings();
  const enableRes = await ensureTelegramRuntime(settings);
  if (!enableRes || !enableRes.ok || !telegramService) {
    return { ok: false, message: String((enableRes && enableRes.message) || 'Telegram runtime is unavailable.') };
  }
  const requestedChat = String((payload && payload.chat_id) || '').trim();
  const usersRes = getOrchestratorUsersStore().listUsers();
  const users = Array.isArray(usersRes && usersRes.users) ? usersRes.users : [];
  const chatId = requestedChat || String((((users[0] || {}).chat_id) || '')).trim();
  if (!chatId) return { ok: false, message: 'chat_id is required (or register one chat via /hello).' };
  const text = String((payload && payload.text) || 'Subgrapher test ping from Settings.').trim();
  const sendRes = await telegramService.sendMessage(chatId, text, { disable_web_page_preview: true });
  if (!sendRes || !sendRes.ok) {
    return { ok: false, message: String((sendRes && sendRes.message) || 'Unable to send Telegram test message.') };
  }
  return { ok: true, chat_id: chatId };
});

ipcMain.handle('browser:orchestratorUsersList', () => {
  return getOrchestratorUsersStore().listUsers();
});

ipcMain.handle('browser:orchestratorUserRevoke', (_event, payload) => {
  const input = (payload && typeof payload === 'object') ? payload : {};
  const chatId = String(input.chat_id || input.chatId || '').trim();
  if (!chatId) return { ok: false, message: 'chat_id is required.' };
  const usersStore = getOrchestratorUsersStore();
  const revokeRes = usersStore.revokeByChatId(chatId);
  if (!revokeRes || !revokeRes.ok) {
    return { ok: false, message: String((revokeRes && revokeRes.message) || 'Unable to revoke user.') };
  }
  setTelegramRegistrationPending(chatId, false);
  const clearRes = getPathBExecutor().clearConversation(chatId);
  const prefsStore = getOrchestratorPreferencesStore();
  const removedUserId = String(((revokeRes.user || {}).user_id) || '').trim().toLowerCase();
  prefsStore.delete(chatId.toLowerCase());
  if (removedUserId) prefsStore.delete(removedUserId);
  const jobsStore = getOrchestratorJobsStore();
  const listRes = jobsStore.listJobs({ include_deleted: false, created_by_chat_id: chatId });
  const jobs = Array.isArray(listRes && listRes.jobs) ? listRes.jobs : [];
  let revokedJobs = 0;
  jobs.forEach((job) => {
    const jobId = String((job && job.id) || '').trim();
    if (!jobId) return;
    const del = jobsStore.deleteJob(jobId);
    if (del && del.ok) revokedJobs += 1;
  });
  return {
    ok: true,
    chat_id: chatId,
    user: revokeRes.user || null,
    path_b_context_cleared: !!(clearRes && clearRes.ok),
    revoked_jobs: revokedJobs,
  };
});

ipcMain.handle('browser:lmstudioTokenStatus', () => {
  const settings = readSettings();
  const tokenRef = String((settings && settings.lmstudio_token_ref) || '').trim();
  const tokenConfigured = tokenRef ? !!getSecureSecretStore().hasSecret(tokenRef) : false;
  return {
    ok: true,
    token_ref: tokenRef,
    token_configured: tokenConfigured,
  };
});

ipcMain.handle('browser:lmstudioSetToken', async (_event, payload) => {
  const token = String((payload && payload.token) || '').trim();
  if (!token) return { ok: false, message: 'token is required.' };
  const current = readSettings();
  const existingRef = String((current && current.lmstudio_token_ref) || '').trim();
  const setRes = setSecretValueByRef(existingRef, token, LMSTUDIO_SECRET_REF_PREFIX);
  if (!setRes || !setRes.ok) {
    return { ok: false, message: String((setRes && setRes.message) || 'Unable to save LM Studio token.') };
  }
  const updated = writeSettings({ lmstudio_token_ref: String(setRes.ref || '').trim() });
  const runtimeRes = await applySettingsRuntimeEffects(current, updated);
  return {
    ok: true,
    token_ref: String(setRes.ref || '').trim(),
    settings: updated,
    applied_runtime: runtimeRes,
  };
});

ipcMain.handle('browser:lmstudioClearToken', async () => {
  const current = readSettings();
  const tokenRef = String((current && current.lmstudio_token_ref) || '').trim();
  const clearRes = clearSecretValueByRef(tokenRef);
  if (!clearRes || !clearRes.ok) {
    return { ok: false, message: String((clearRes && clearRes.message) || 'Unable to clear LM Studio token.') };
  }
  const updated = writeSettings({ lmstudio_token_ref: '' });
  const runtimeRes = await applySettingsRuntimeEffects(current, updated);
  return {
    ok: true,
    settings: updated,
    applied_runtime: runtimeRes,
  };
});

ipcMain.handle('browser:orchestratorWebKeyStatus', () => {
  const settings = readSettings();
  const keyRef = String((settings && settings.orchestrator_web_provider_key_ref) || '').trim();
  const keyConfigured = keyRef ? !!getSecureSecretStore().hasSecret(keyRef) : false;
  return {
    ok: true,
    key_ref: keyRef,
    key_configured: keyConfigured,
  };
});

ipcMain.handle('browser:orchestratorWebSetKey', async (_event, payload) => {
  const key = String((payload && payload.key) || '').trim();
  if (!key) return { ok: false, message: 'key is required.' };
  const current = readSettings();
  const existingRef = String((current && current.orchestrator_web_provider_key_ref) || '').trim();
  const setRes = setSecretValueByRef(existingRef, key, ORCHESTRATOR_WEB_SECRET_REF_PREFIX);
  if (!setRes || !setRes.ok) {
    return { ok: false, message: String((setRes && setRes.message) || 'Unable to save web provider key.') };
  }
  const updated = writeSettings({ orchestrator_web_provider_key_ref: String(setRes.ref || '').trim() });
  const runtimeRes = await applySettingsRuntimeEffects(current, updated);
  return {
    ok: true,
    key_ref: String(setRes.ref || '').trim(),
    settings: updated,
    applied_runtime: runtimeRes,
  };
});

ipcMain.handle('browser:orchestratorWebClearKey', async () => {
  const current = readSettings();
  const keyRef = String((current && current.orchestrator_web_provider_key_ref) || '').trim();
  const clearRes = clearSecretValueByRef(keyRef);
  if (!clearRes || !clearRes.ok) {
    return { ok: false, message: String((clearRes && clearRes.message) || 'Unable to clear web provider key.') };
  }
  const updated = writeSettings({ orchestrator_web_provider_key_ref: '' });
  const runtimeRes = await applySettingsRuntimeEffects(current, updated);
  return {
    ok: true,
    settings: updated,
    applied_runtime: runtimeRes,
  };
});

ipcMain.handle('browser:orchestratorJobList', (_event, payload) => {
  const input = (payload && typeof payload === 'object') ? payload : {};
  return getOrchestratorJobsStore().listJobs({
    include_deleted: !!input.include_deleted,
    created_by_chat_id: String(input.created_by_chat_id || '').trim(),
  });
});

ipcMain.handle('browser:orchestratorJobCreate', (_event, payload) => {
  const input = (payload && typeof payload === 'object') ? payload : {};
  return getOrchestratorJobsStore().createJob(input);
});

ipcMain.handle('browser:orchestratorJobEdit', (_event, payload) => {
  const input = (payload && typeof payload === 'object') ? payload : {};
  const jobId = String(input.id || input.job_id || '').trim();
  const patch = (input.patch && typeof input.patch === 'object') ? input.patch : input;
  return getOrchestratorJobsStore().editJob(jobId, patch);
});

ipcMain.handle('browser:orchestratorJobPause', (_event, payload) => {
  const input = (payload && typeof payload === 'object') ? payload : {};
  const jobId = String(input.id || input.job_id || payload || '').trim();
  return getOrchestratorJobsStore().pauseJob(jobId);
});

ipcMain.handle('browser:orchestratorJobResume', (_event, payload) => {
  const input = (payload && typeof payload === 'object') ? payload : {};
  const jobId = String(input.id || input.job_id || payload || '').trim();
  return getOrchestratorJobsStore().resumeJob(jobId);
});

ipcMain.handle('browser:orchestratorJobDelete', (_event, payload) => {
  const input = (payload && typeof payload === 'object') ? payload : {};
  const jobId = String(input.id || input.job_id || payload || '').trim();
  return getOrchestratorJobsStore().deleteJob(jobId);
});

ipcMain.handle('browser:historyList', (_event, payload) => {
  return queryHistoryEntries(payload || {});
});

ipcMain.handle('browser:historyGet', (_event, payload) => {
  const historyId = String((payload && payload.history_id) || payload || '').trim();
  return getHistoryEntryById(historyId);
});

ipcMain.handle('browser:historyDelete', (_event, payload) => {
  const historyId = String((payload && payload.history_id) || payload || '').trim();
  return deleteHistoryEntryById(historyId);
});

ipcMain.handle('browser:historyClear', (_event, payload) => {
  const phrase = String((payload && payload.phrase) || '').trim();
  return clearHistoryEntries(phrase);
});

ipcMain.handle('browser:historySemanticMap', (_event, payload) => {
  return buildHistorySemanticMap(payload || {});
});

ipcMain.handle('browser:updatePreferences', async (_event, payload) => {
  const current = readSettings();
  const patch = pickEditableSettingsPatch(payload || {});
  const updated = writeSettings(patch);
  const appliedRuntime = await applySettingsRuntimeEffects(current, updated);
  return {
    ok: true,
    settings: updated,
    applied_runtime: appliedRuntime,
  };
});

ipcMain.handle('browser:settingsDiagnostics', async () => {
  const settings = readSettings();
  const trustcommons = getTrustCommonsStatus();
  const hyperweb = hyperwebManager.getStatus();
  const identity = getHyperwebIdentityDiagnostics();
  return {
    ok: true,
    settings,
    trustcommons,
    hyperweb,
    hyperweb_identity: identity,
  };
});

ipcMain.handle('browser:settingsDangerResetHyperwebIdentity', async (_event, payload) => {
  const phrase = String((payload && payload.phrase) || '').trim().toUpperCase();
  if (phrase !== confirmTypedResetPhrase('RESET')) {
    return { ok: false, message: 'Type RESET to confirm.' };
  }
  keychain.deleteSecret(HYPERWEB_IDENTITY_PRIVATE_KEY_ACCOUNT, { service: HYPERWEB_IDENTITY_SERVICE });
  const state = ensureHyperwebSocialState();
  state.identity = createDefaultHyperwebSocialState().identity;
  writeHyperwebSocialState(state);
  hyperwebSocialState = state;
  const nextIdentity = ensureHyperwebIdentity();
  return {
    ok: true,
    new_identity: nextIdentity && nextIdentity.identity ? nextIdentity.identity : {},
  };
});

ipcMain.handle('browser:settingsDangerClearHyperwebSocialCache', async (_event, payload) => {
  const phrase = String((payload && payload.phrase) || '').trim().toUpperCase();
  if (phrase !== confirmTypedResetPhrase('RESET')) {
    return { ok: false, message: 'Type RESET to confirm.' };
  }
  const existing = ensureHyperwebSocialState();
  const preservedIdentity = existing && existing.identity ? existing.identity : createDefaultHyperwebSocialState().identity;
  const next = createDefaultHyperwebSocialState();
  next.identity = preservedIdentity;
  writeHyperwebSocialState(next);
  hyperwebSocialState = next;
  return { ok: true };
});

ipcMain.handle('browser:settingsDangerResetTrustCommonsLink', async (_event, payload) => {
  const phrase = String((payload && payload.phrase) || '').trim().toUpperCase();
  if (phrase !== confirmTypedResetPhrase('RESET')) {
    return { ok: false, message: 'Type RESET to confirm.' };
  }
  const current = readSettings();
  cachedTrustCommonsSyncSecret = '';
  cachedTrustCommonsSyncAccount = '';
  ephemeralTrustCommonsSyncSecret = '';
  const syncAccount = getTrustCommonsSyncAccount(current);
  keychain.deleteSecret(syncAccount, { service: TRUSTCOMMONS_SYNC_SECRET_SERVICE });
  const next = writeSettings({
    trustcommons_bootstrap_complete: false,
    trustcommons_identity_id: '',
    trustcommons_display_name: '',
    trustcommons_bootstrap_at: 0,
  });
  trustCommonsRuntime.bootstrapComplete = false;
  trustCommonsRuntime.identity = null;
  trustCommonsRuntime.connected = false;
  trustCommonsRuntime.lastError = '';
  trustCommonsSyncBridge.stop().catch(() => {});
  hyperwebManager.disconnect();
  return {
    ok: true,
    settings: next,
  };
});

ipcMain.handle('browser:setLuminoSelection', (_event, payload) => {
  const provider = String((payload && payload.provider) || '').trim().toLowerCase();
  const model = String((payload && payload.model) || '').trim();
  if (!PROVIDERS.includes(provider)) {
    return { ok: false, message: 'Unsupported provider.' };
  }
  const updated = writeSettings({
    lumino_last_provider: provider,
    lumino_last_model: model,
  });
  return {
    ok: true,
    lumino_last_provider: String((updated && updated.lumino_last_provider) || 'openai'),
    lumino_last_model: String((updated && updated.lumino_last_model) || ''),
  };
});

ipcMain.handle('browser:setDefaultSearchEngine', (_event, payload) => {
  const engine = String((payload && payload.engine) || '').trim().toLowerCase();
  if (!['google', 'bing', 'ddg'].includes(engine)) {
    return { ok: false, message: 'Unsupported search engine.' };
  }
  const updated = writeSettings({ default_search_engine: engine });
  return { ok: true, ...updated };
});

ipcMain.handle('browser:trustCommonsStatus', () => {
  return getTrustCommonsStatus();
});

ipcMain.handle('browser:trustCommonsConnect', async () => {
  const connected = await connectTrustCommonsAndHyperweb({ launchApp: true });
  return connected;
});

ipcMain.handle('browser:hyperwebStatus', async () => {
  const authenticated = isTtcHyperwebAuthenticated();
  await hyperwebManager.refreshLocalPublicIndex();
  return {
    ok: true,
    ttc_authenticated: authenticated,
    ...hyperwebManager.getStatus(),
  };
});

ipcMain.handle('browser:hyperwebConnect', async () => {
  const auth = requireTtcHyperwebAuth();
  if (!auth.ok) return auth;
  const trust = await connectTrustCommonsAndHyperweb({ launchApp: false });
  const status = hyperwebManager.getStatus();
  return {
    ok: !!(trust && trust.ok),
    degraded: !!(trust && trust.degraded),
    message: (trust && trust.message) || '',
    trustcommons: trust && trust.status ? trust.status : getTrustCommonsStatus(),
    ...status,
  };
});

ipcMain.handle('browser:hyperwebDisconnect', () => {
  const auth = requireTtcHyperwebAuth();
  if (!auth.ok) return auth;
  const res = hyperwebManager.disconnect();
  trustCommonsRuntime.connected = false;
  return { ok: true, ...res.status };
});

ipcMain.handle('browser:hyperwebQuery', async (_event, payload) => {
  const auth = requireTtcHyperwebAuth();
  if (!auth.ok) return auth;
  const query = String((payload && payload.query) || '').trim();
  const limit = Number((payload && payload.limit) || 25);
  if (!query) return { ok: false, message: 'query is required.' };

  const trustStatus = getTrustCommonsStatus();
  if (!trustStatus.bootstrap_complete) {
    ensureTrustCommonsBootstrap();
  }

  const status = hyperwebManager.getStatus();
  const connectedPeers = Number((status && status.peer_count) || 0);
  const result = await hyperwebManager.query(query, {
    limit,
    timeout_ms: connectedPeers > 0 ? 2800 : 180,
  });
  return {
    ok: !!(result && result.ok),
    ...result,
  };
});

ipcMain.handle('browser:hyperwebImportSuggestion', async (_event, payload) => {
  const auth = requireTtcHyperwebAuth();
  if (!auth.ok) return auth;
  const suggestion = payload && payload.suggestion ? payload.suggestion : payload;
  const importRes = await hyperwebManager.importSuggestion(suggestion || {});
  if (!importRes || !importRes.ok || !importRes.imported) {
    return { ok: false, message: (importRes && importRes.message) || 'Unable to import Hyperweb suggestion.' };
  }
  return importPublicReferenceAsPrivateCopy({
    source_type: 'public_snapshot',
    reference_payload: importRes.imported,
    peer_id: String((suggestion && suggestion.peer_id) || ''),
    peer_name: String((suggestion && suggestion.peer_name) || ''),
    snapshot_id: String((suggestion && suggestion.snapshot_id) || ''),
  });
});

ipcMain.handle('browser:hyperwebSocialStatus', async () => {
  const auth = requireTtcHyperwebAuth();
  if (!auth.ok) return auth;
  const state = ensureHyperwebSocialState();
  const status = hyperwebManager.getStatus();
  return {
    ok: true,
    identity: state.identity,
    peer_relations: Array.isArray(state.peer_relations) ? state.peer_relations : [],
    known_peers: state.known_peers || {},
    peers: hyperwebManager.listPeers(),
    connected: !!(status && status.connected),
    peer_count: Number((status && status.peer_count) || 0),
  };
});

ipcMain.handle('browser:hyperwebListPeers', async () => {
  const auth = requireTtcHyperwebAuth();
  if (!auth.ok) return auth;
  const state = ensureHyperwebSocialState();
  return {
    ok: true,
    peers: hyperwebManager.listPeers(),
    known_peers: state.known_peers || {},
  };
});

ipcMain.handle('browser:hyperwebCreateInvite', async () => {
  const auth = requireTtcHyperwebAuth();
  if (!auth.ok) return auth;
  const invite = createHyperwebInvite();
  if (!invite || !invite.ok) return { ok: false, message: 'Unable to create invite.' };
  return invite;
});

ipcMain.handle('browser:hyperwebAcceptInvite', async (_event, payload) => {
  const auth = requireTtcHyperwebAuth();
  if (!auth.ok) return auth;
  const token = String((payload && payload.token) || '').trim();
  const res = acceptHyperwebInviteToken(token);
  if (!res || !res.ok) return res;
  const state = ensureHyperwebSocialState();
  const identity = state.identity || {};
  hyperwebManager.broadcastProtocol({
    type: 'hyperweb:invite_handshake',
    payload: {
      fingerprint: String(identity.fingerprint || '').toUpperCase(),
      alias: String(identity.display_alias || ''),
      pubkey: String(identity.pubkey || ''),
      addresses: [],
    },
    ts: nowTs(),
  });
  return {
    ...res,
    social_status: {
      identity,
      peer_relations: state.peer_relations,
      known_peers: state.known_peers,
    },
  };
});

ipcMain.handle('browser:hyperwebPostCreate', async (_event, payload) => {
  const auth = requireTtcHyperwebAuth();
  if (!auth.ok) return auth;
  const body = String((payload && payload.body) || '').trim();
  if (!body) return { ok: false, message: 'Post body is required.' };
  const author = getLocalHyperwebAuthor();
  const event = makeSignedSocialEvent('social_post', {
    post_id: makeId('hwpost'),
    body: body.slice(0, 5000),
    author_fingerprint: String(author.peer_id || '').toUpperCase(),
    author_alias: String(author.peer_name || ''),
    created_at: nowTs(),
  });
  const res = appendHyperwebSocialEvent(event);
  if (!res || !res.ok) return { ok: false, message: 'Unable to append post event.' };
  return buildHyperwebFeed({});
});

ipcMain.handle('browser:hyperwebPostReply', async (_event, payload) => {
  const auth = requireTtcHyperwebAuth();
  if (!auth.ok) return auth;
  const postId = String((payload && payload.post_id) || '').trim();
  const body = String((payload && payload.body) || '').trim();
  if (!postId) return { ok: false, message: 'post_id is required.' };
  if (!body) return { ok: false, message: 'Reply body is required.' };
  const state = ensureHyperwebSocialState();
  if (!state.posts_by_id[postId]) return { ok: false, message: 'Post not found.' };
  const author = getLocalHyperwebAuthor();
  const event = makeSignedSocialEvent('social_reply', {
    reply_id: makeId('hwreply'),
    post_id: postId,
    body: body.slice(0, 5000),
    author_fingerprint: String(author.peer_id || '').toUpperCase(),
    author_alias: String(author.peer_name || ''),
    created_at: nowTs(),
  });
  const res = appendHyperwebSocialEvent(event);
  if (!res || !res.ok) return { ok: false, message: 'Unable to append reply event.' };
  return buildHyperwebFeed({});
});

ipcMain.handle('browser:hyperwebVoteSet', async (_event, payload) => {
  const auth = requireTtcHyperwebAuth();
  if (!auth.ok) return auth;
  const targetId = String((payload && payload.target_id) || '').trim();
  const value = Number((payload && payload.value) || 0);
  if (!targetId) return { ok: false, message: 'target_id is required.' };
  if (value !== 1 && value !== -1) return { ok: false, message: 'value must be 1 or -1.' };
  const author = getLocalHyperwebAuthor();
  const event = makeSignedSocialEvent('social_vote', {
    target_id: targetId,
    actor_fingerprint: String(author.peer_id || '').toUpperCase(),
    value,
    updated_at: nowTs(),
  });
  const res = appendHyperwebSocialEvent(event);
  if (!res || !res.ok) return { ok: false, message: 'Unable to append vote event.' };
  return buildHyperwebFeed({});
});

ipcMain.handle('browser:hyperwebReportTarget', async (_event, payload) => {
  const auth = requireTtcHyperwebAuth();
  if (!auth.ok) return auth;
  const targetId = String((payload && payload.target_id) || '').trim();
  const targetKind = String((payload && payload.target_kind) || 'post').trim().toLowerCase();
  const reason = String((payload && payload.reason) || '').trim();
  if (!targetId) return { ok: false, message: 'target_id is required.' };
  return appendHyperwebReport(targetId, targetKind, reason);
});

ipcMain.handle('browser:hyperwebDeleteSnapshot', async (_event, payload) => {
  const auth = requireTtcHyperwebAuth();
  if (!auth.ok) return auth;
  const snapshotId = String((payload && payload.snapshot_id) || '').trim();
  return deletePublishedSnapshot(snapshotId);
});

ipcMain.handle('browser:hyperwebFeedQuery', async (_event, payload) => {
  const auth = requireTtcHyperwebAuth();
  if (!auth.ok) return auth;
  const authorFingerprint = String((payload && payload.author_fingerprint) || '').trim();
  return buildHyperwebFeed({ author_fingerprint: authorFingerprint });
});

ipcMain.handle('browser:hyperwebProfileQuery', async (_event, payload) => {
  const auth = requireTtcHyperwebAuth();
  if (!auth.ok) return auth;
  const authorFingerprint = String((payload && payload.author_fingerprint) || '').trim();
  return queryHyperwebProfile(authorFingerprint);
});

ipcMain.handle('browser:hyperwebResetFilter', async () => {
  const auth = requireTtcHyperwebAuth();
  if (!auth.ok) return auth;
  return { ok: true };
});

ipcMain.handle('browser:hyperwebMembersList', async () => {
  const auth = requireTtcHyperwebAuth();
  return {
    ok: true,
    members: listHyperwebMembers(),
    unauthenticated: !auth.ok,
    message: auth.ok ? '' : String(auth.message || ''),
  };
});

ipcMain.handle('browser:hyperwebShareReference', async (_event, payload) => {
  const auth = requireTtcHyperwebAuth();
  if (!auth.ok) return auth;
  const srId = String((payload && payload.sr_id) || '').trim();
  const recipientIds = Array.isArray(payload && payload.recipient_ids) ? payload.recipient_ids : [];
  return createPrivateShare(srId, recipientIds);
});

ipcMain.handle('browser:hyperwebListShares', async () => {
  const auth = requireTtcHyperwebAuth();
  if (!auth.ok) return auth;
  return listPrivateSharesForCurrentUser();
});

ipcMain.handle('browser:hyperwebAcceptShareWrite', async (_event, payload) => {
  const auth = requireTtcHyperwebAuth();
  if (!auth.ok) return auth;
  const shareId = String((payload && payload.share_id) || '').trim();
  return updateRecipientShareStatus(shareId, 'write_accepted');
});

ipcMain.handle('browser:hyperwebDeclineShareWrite', async (_event, payload) => {
  const auth = requireTtcHyperwebAuth();
  if (!auth.ok) return auth;
  const shareId = String((payload && payload.share_id) || '').trim();
  return updateRecipientShareStatus(shareId, 'declined');
});

ipcMain.handle('browser:hyperwebRevokeShare', async (_event, payload) => {
  const auth = requireTtcHyperwebAuth();
  if (!auth.ok) return auth;
  const shareId = String((payload && payload.share_id) || '').trim();
  return revokeShareAccess(shareId);
});

ipcMain.handle('browser:hyperwebDeleteShare', async (_event, payload) => {
  const auth = requireTtcHyperwebAuth();
  if (!auth.ok) return auth;
  const shareId = String((payload && payload.share_id) || '').trim();
  return deletePrivateShare(shareId);
});

ipcMain.handle('browser:hyperwebListSharedRooms', async () => {
  const auth = requireTtcHyperwebAuth();
  if (!auth.ok) return auth;
  return listSharedRoomsForCurrentUser();
});

ipcMain.handle('browser:hyperwebOpenSharedRoom', async (_event, payload) => {
  const auth = requireTtcHyperwebAuth();
  if (!auth.ok) return auth;
  const roomId = String((payload && payload.room_id) || '').trim();
  return openSharedRoomForCurrentUser(roomId);
});

ipcMain.handle('browser:hyperwebCollabApplyUpdate', async (_event, payload) => {
  const auth = requireTtcHyperwebAuth();
  if (!auth.ok) return auth;
  const roomId = String((payload && payload.room_id) || '').trim();
  const update = (payload && payload.update && typeof payload.update === 'object') ? payload.update : {};
  return applySharedRoomUpdate(roomId, update);
});

ipcMain.handle('browser:hyperwebReferenceSearch', async (_event, payload) => {
  const auth = requireTtcHyperwebAuth();
  if (!auth.ok) return auth;
  const query = String((payload && payload.query) || '').trim();
  const limit = Number((payload && payload.limit) || 40);
  const authorFingerprint = String((payload && payload.author_fingerprint) || '').trim();
  return runHyperwebReferenceSearch(query, {
    limit,
    author_fingerprint: authorFingerprint,
  });
});

ipcMain.handle('browser:hyperwebImportReference', async (_event, payload) => {
  const auth = requireTtcHyperwebAuth();
  if (!auth.ok) return auth;
  const item = (payload && payload.item && typeof payload.item === 'object') ? payload.item : {};
  const isSnapshot = (
    String(item.source_type || '').toLowerCase() === 'public_snapshot'
    || !!String(item.snapshot_id || '').trim()
  );
  if (isSnapshot) {
    return importPublicReferenceAsPrivateCopy(item);
  }
  const importRes = await hyperwebManager.importSuggestion(item || {});
  if (!importRes || !importRes.ok || !importRes.imported) {
    return { ok: false, message: (importRes && importRes.message) || 'Unable to import Hyperweb reference.' };
  }
  return importPublicReferenceAsPrivateCopy({
    ...item,
    source_type: 'public_snapshot',
    reference_payload: importRes.imported,
  });
});

ipcMain.handle('browser:hyperwebPostSearch', async (_event, payload) => {
  const auth = requireTtcHyperwebAuth();
  if (!auth.ok) return auth;
  const query = String((payload && payload.query) || '').trim();
  const limit = Number((payload && payload.limit) || 40);
  const authorFingerprint = String((payload && payload.author_fingerprint) || '').trim();
  return runHyperwebPostSearch(query, {
    limit,
    author_fingerprint: authorFingerprint,
  });
});

function extractInviteTokenFromUrl(rawUrl = '') {
  const text = String(rawUrl || '').trim();
  if (!text) return '';
  try {
    const parsed = new URL(text);
    if (String(parsed.protocol || '').replace(':', '') !== HYPERWEB_INVITE_PROTO) return '';
    const route = String(parsed.hostname || '').trim().toLowerCase();
    if (route !== HYPERWEB_INVITE_ROUTE) return '';
    return String(parsed.searchParams.get('token') || '').trim();
  } catch (_) {
    return '';
  }
}

function extractInviteTokenFromArgv(argv = []) {
  const args = Array.isArray(argv) ? argv : [];
  for (const arg of args) {
    const token = extractInviteTokenFromUrl(String(arg || '').trim());
    if (token) return token;
  }
  return '';
}

function consumePendingInviteTokenIfAny() {
  const token = String(pendingInviteToken || '').trim();
  if (!token) return null;
  pendingInviteToken = '';
  return acceptHyperwebInviteToken(token);
}

const gotSingleInstanceLock = app.requestSingleInstanceLock();
if (!gotSingleInstanceLock) {
  app.quit();
} else {
  app.on('second-instance', (_event, argv) => {
    const token = extractInviteTokenFromArgv(argv);
    if (token) pendingInviteToken = token;
    if (mainWindow) {
      if (mainWindow.isMinimized()) mainWindow.restore();
      mainWindow.focus();
    }
  });
  app.on('open-url', (event, urlText) => {
    if (event && typeof event.preventDefault === 'function') event.preventDefault();
    const token = extractInviteTokenFromUrl(urlText);
    if (token) pendingInviteToken = token;
  });
}

if (gotSingleInstanceLock) app.whenReady().then(() => {
  app.setName(APP_NAME);
  app.setAsDefaultProtocolClient(HYPERWEB_INVITE_PROTO);
  setDockIconIfAvailable();
  createApplicationMenu();
  ensureReferences();
  ensureHyperwebSocialState();
  syncPublicFeedWithSnapshots();
  const launchInviteToken = extractInviteTokenFromArgv(process.argv);
  if (launchInviteToken) pendingInviteToken = launchInviteToken;
  consumePendingInviteTokenIfAny();
  hyperwebManager.setPublicIndexProvider(() => getPublicReferencesForHyperweb());
  const settings = readSettings();
  hyperwebManager.setRelayUrl(settings.hyperweb_relay_url || DEFAULT_HYPERWEB_RELAY_URL);
  hyperwebManager.setEnabled(!!settings.hyperweb_enabled);
  applySettingsRuntimeEffects(settings, settings).catch((err) => {
    console.warn('[runtime] settings effects failed:', String((err && err.message) || err));
  });
  ensureTrustCommonsBootstrap();
  cleanupStaleSandboxes(app.getPath('userData'), { maxAgeMs: 48 * 60 * 60 * 1000 });
  getPythonRuntimeResolver().diagnostics({ bypassCache: true }).then((result) => {
    const tool = result && result.tool && typeof result.tool === 'object' ? result.tool : {};
    const viz = result && result.viz && typeof result.viz === 'object' ? result.viz : {};
    if (!tool.ok) {
      console.warn('[python] tool runtime unavailable:', tool.message || 'check failed');
    }
    if (!viz.ok) {
      console.warn('[python] viz runtime unavailable:', viz.message || 'check failed');
    }
  }).catch((err) => {
    console.warn('[python] check error:', String((err && err.message) || err));
  });
  ensureTrustCommonsSyncBridge()
    .then((res) => {
      if (!res || !res.ok) {
        trustCommonsRuntime.lastError = String((res && res.message) || 'Trust Commons local sync bridge unavailable.');
      }
    })
    .catch((err) => {
      trustCommonsRuntime.lastError = String((err && err.message) || 'Trust Commons local sync bridge unavailable.');
    });
  hyperwebManager.refreshLocalPublicIndex().catch(() => {});
  createMainWindow();
  if (!memorySemanticTimer) {
    memorySemanticTimer = setInterval(() => {
      if (!mainWindow || mainWindow.isDestroyed()) return;
      if (!mainWindow.isFocused()) return;
      runMemorySemanticEvaluation();
    }, MEMORY_SEMANTIC_INTERVAL_MS);
  }
  runMemorySemanticEvaluation();

  if (settings.hyperweb_enabled) {
    connectTrustCommonsAndHyperweb({ launchApp: false }).catch(() => {});
  }

  app.on('activate', () => {
    if (BrowserWindow.getAllWindows().length === 0) createMainWindow();
  });
});

app.on('window-all-closed', () => {
  if (process.platform !== 'darwin') app.quit();
});

app.on('before-quit', () => {
  if (memorySemanticTimer) {
    clearInterval(memorySemanticTimer);
    memorySemanticTimer = null;
  }
  if (orchestratorScheduler) {
    try {
      orchestratorScheduler.stop();
    } catch (_) {
      // noop
    }
  }
  if (telegramService) {
    telegramService.stop().catch(() => {});
  }
  hyperwebManager.disconnect();
  trustCommonsSyncBridge.stop().catch(() => {});
});
