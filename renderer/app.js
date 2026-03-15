/* global window */

const electronApi = window.electronAPI || null;
const api = electronApi && electronApi.browser;
const PYTHON_WINDOWS_DOWNLOAD_URL = 'https://www.python.org/downloads/windows/';

const state = {
  references: [],
  activeSrId: null,
  searchQuery: '',
  markerMode: false,
  browserCurrentUrl: 'about:blank',
  browserCurrentTitle: 'Untitled',
  lastWebSrId: null,
  lastWebTabId: null,
  activeSurface: {
    kind: 'web',
    tabId: null,
    artifactId: null,
    filesTabId: null,
    skillsTabId: null,
    mailTabId: null,
  },
  artifactSaveTimer: null,
  artifactPreviewSeq: 0,
  artifactSplitByRef: {},
  artifactZoomByRef: {},
  artifactCarouselIndexByArtifact: {},
  artifactResolvedImages: [],
  artifactUnresolvedImages: [],
  artifactActiveImageIndex: 0,
  artifactActiveArtifactId: '',
  artifactViewModeByArtifact: {},
  htmlArtifactRuntime: {
    artifactId: '',
    running: false,
    stale: false,
    objectUrl: '',
    iframeEl: null,
    sourceContent: '',
  },
  suppressArtifactInput: false,
  artifactMarkerSelectionTimer: null,
  artifactMarkerSelectionSignature: '',
  artifactMarkerSelectionAt: 0,
  diffQueueByRef: new Map(),
  selectedProvider: 'openai',
  selectedModel: '',
  providerModelsRequestSeq: 0,
  providerKeysState: { providers: [] },
  providerKeyStatusByProvider: {},
  providerKeyModal: {
    provider: 'openai',
    keyId: '',
    label: '',
    setPrimary: true,
    fromSettings: false,
  },
  onboardingComplete: false,
  referenceCollapsed: {},
  referenceExpandedSrId: null,
  referenceInlineRename: { srId: null, draft: '' },
  referenceSearchMatchedIds: new Set(),
  referenceColorFilter: { mode: 'all', selected: new Set() },
  referenceAutoOnly: false,
  referenceColorPicker: { openSrId: null },
  activeArtifactByRef: new Map(),
  activeFilesByRef: new Map(),
  activeSkillsByRef: new Map(),
  activeMailByRef: new Map(),
  workspaceBrowserTabMap: new Map(),
  hyperwebStatus: null,
  hyperwebSuggestions: [],
  audibleByRef: new Map(),
  activeChatRequestId: null,
  activeChatRequestSrId: null,
  streamingAssistant: null,
  chatUserNodeByRequest: new Map(),
  chatStatusByRequest: new Map(),
  appView: 'workspace',
  zenMode: false,
  hyperwebFeed: [],
  hyperwebFilterFingerprint: '',
  hyperwebActiveTab: 'feed',
  hyperwebReferenceResults: [],
  hyperwebIdentity: null,
  hyperwebChatMode: 'room',
  hyperwebChatPeerId: '',
  hyperwebChatRoomId: '',
  hyperwebChatMessages: [],
  hyperwebChatConversations: [],
  hyperwebChatRooms: [],
  hyperwebChatMembers: [],
  hyperwebChatThreadId: '',
  hyperwebChatThreadPolicy: { retention: 'off' },
  hyperwebChatActivePresence: null,
  hyperwebChatLivePeerCount: 0,
  hyperwebChatPendingFile: null,
  hyperwebReferenceExpandedKeys: new Set(),
  hyperwebPostSearchQuery: '',
  hyperwebSplitRatio: 0.5,
  publishSnapshotTargetId: '',
  shareReferenceTargetId: '',
  shareMemberDirectory: [],
  shareMemberSearchQuery: '',
  shareRecipientSelection: new Set(),
  sharesActiveTab: 'incoming',
  privateIncomingShares: [],
  privateOutgoingShares: [],
  privateSharedRooms: [],
  privateActiveRoomId: '',
  privateActiveRoom: null,
  privateRoomEditorSaveTimer: null,
  settingsDraft: null,
  settingsPersisted: null,
  referenceRanking: {
    enabled: false,
    enabledAt: 0,
    lastRankedAt: 0,
    orderIds: [],
    scoreById: {},
  },
  visibleReferenceOrder: [],
  settingsDirty: false,
  settingsValidationErrors: {},
  settingsDiagnostics: null,
  settingsTrustedPeers: [],
  mailStatus: null,
  mailUnreadCount: 0,
  hyperwebUnreadCount: 0,
  mailAccounts: [],
  mailboxesByAccount: new Map(),
  mailSearchQueryByRef: new Map(),
  mailSearchResultsByRef: new Map(),
  mailSelectedSourceIdsByRef: new Map(),
  mailCommittedSelectedSourceIdsByRef: new Map(),
  mailSelectedViewByRef: new Map(),
  mailPreviewByRef: new Map(),
  mailComposerByRef: new Map(),
  mailNavByRef: new Map(),
  appDataProtectionStatus: null,
  settingsSaveState: '',
  telegramRuntimeStatus: null,
  orchestratorUsers: [],
  orchestratorUsersLoading: false,
  lmstudioTokenConfigured: null,
  orchestratorWebKeyConfigured: null,
  settingsLmstudioModels: [],
  settingsAbstractionStatus: null,
  settingsRagStatus: null,
  historyEntries: [],
  historySearchQuery: '',
  historySelectedId: '',
  historyDetailView: 'preview',
  historyMapPoints: [],
  historyMapBounds: null,
  historyMapClusters: [],
  historyHoveredPointId: '',
  memoryReplay: {
    active: false,
    lane: 'all',
    checkpoints: [],
    filtered: [],
    index: -1,
    activeCheckpointId: '',
    virtualReference: null,
    playing: false,
    timer: null,
  },
  referenceActivationSeq: 0,
  chatPanelCollapsed: false,
  chatPanelWidth: 252,
};

const PROVIDERS = ['openai', 'cerebras', 'google', 'anthropic', 'lmstudio'];
const REFERENCE_COLOR_TAGS = ['c1', 'c2', 'c3', 'c4', 'c5'];
const BROWSER_URL_PLACEHOLDER_WEB = 'type anything to search or enter URL...';
const BROWSER_URL_PLACEHOLDER_ARTIFACT = 'Commands: /add /create /rename/<name> /rm';
const BROWSER_URL_PLACEHOLDER_FILES = 'Files tab active: enter URL/search to open web tab';
const BROWSER_URL_PLACEHOLDER_SKILLS = 'Skills tab active: enter URL/search to open web tab';
const BROWSER_MARKER_MODE_KEY = 'subgrapher_browser_marker_mode_v1';
const BROWSER_MARKER_HOLD_MS = 650;
const HYPERWEB_SPLIT_RATIO_KEY = 'subgrapher_hyperweb_split_ratio_v1';
const ZEN_MODE_KEY = 'subgrapher_zen_mode_v1';
const UI_ZOOM_KEY = 'subgrapher_ui_zoom_v1';
const CHAT_PANEL_WIDTH_KEY = 'subgrapher_chat_panel_width_v1';
const CHAT_PANEL_COLLAPSED_KEY = 'subgrapher_chat_panel_collapsed_v1';
const ARTIFACT_SPLIT_BY_REF_KEY = 'subgrapher_artifact_split_by_ref_v1';
const ARTIFACT_ZOOM_BY_REF_KEY = 'subgrapher_artifact_zoom_by_ref_v1';
const ARTIFACT_VIEW_MODE_CODE = 'code';
const ARTIFACT_VIEW_MODE_PREVIEW = 'preview';
const ARTIFACT_DEFAULT_SPLIT_RATIO = 0.52;
const ARTIFACT_DEFAULT_ZOOM = 1.0;
const ARTIFACT_ZOOM_STEP = 0.1;
const ARTIFACT_MIN_SPLIT_RATIO = 0.2;
const ARTIFACT_MAX_SPLIT_RATIO = 0.8;
const ARTIFACT_MIN_ZOOM = 0.3;
const ARTIFACT_MAX_ZOOM = 3.0;
const BROWSER_VIEW_ZOOM_STEP = 0.1;
const BROWSER_VIEW_MIN_ZOOM = 0.25;
const BROWSER_VIEW_MAX_ZOOM = 5;
const UI_ZOOM_STEP = 0.1;
const UI_MIN_ZOOM = 0.5;
const UI_MAX_ZOOM = 3.0;
const IMAGE_ANALYSIS_PROMPT_DEFAULT = 'Describe the image in details and write the context.';
const RAG_EMBEDDING_SOURCE_DEFAULT = 'lmstudio';
const RAG_EMBEDDING_MODEL_DEFAULT = 'text-embedding-nomic-embed-text-v1.5';
const RAG_TOP_K_DEFAULT = 8;
const HISTORY_DEFAULT_MAX_ENTRIES = 5000;
const GLOBAL_MAIL_VIEW_ID = '__global_mail__';
const MAIL_SMART_FOLDER_LABELS = {
  inbox: 'Inbox',
  unread: 'Unread',
  sent: 'Sent',
  drafts: 'Drafts',
  archive: 'Archive',
  trash: 'Trash',
  junk: 'Junk',
};
const HYPERWEB_LAST_SEEN_KEY = 'subgrapher_hyperweb_last_seen_v1';
let passiveNoticeTimer = null;
let hyperwebChatRefreshPromise = null;
let hyperwebChatRefreshQueuedMode = '';

function e(id) {
  return document.getElementById(id);
}

function escapeHtml(value) {
  return String(value || '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#039;');
}

function deepClone(value) {
  return JSON.parse(JSON.stringify(value));
}

function normalizeInlineText(value) {
  return String(value || '').replace(/\s+/g, ' ').trim();
}

function formatAgo(ts) {
  const t = Number(ts || 0);
  if (!Number.isFinite(t) || t <= 0) return '';
  const diffSec = Math.max(0, Math.floor((Date.now() - t) / 1000));
  if (diffSec < 60) return `${diffSec}s ago`;
  if (diffSec < 3600) return `${Math.floor(diffSec / 60)}m ago`;
  if (diffSec < 86400) return `${Math.floor(diffSec / 3600)}h ago`;
  return `${Math.floor(diffSec / 86400)}d ago`;
}

function clamp(value, min, max) {
  return Math.max(min, Math.min(max, value));
}

function mergeHyperwebChatRefreshMode(currentMode = '', nextMode = '') {
  const rank = { '': 0, history: 1, incremental: 2, full: 3 };
  return (rank[nextMode] || 0) > (rank[currentMode] || 0) ? nextMode : currentMode;
}

function resolveHyperwebChatRefreshMode(payload = {}) {
  const eventName = String((payload && payload.event) || '').trim().toLowerCase();
  const inboxKind = String((payload && payload.kind) || '').trim().toLowerCase();
  const socialType = String((payload && payload.social_type) || '').trim().toLowerCase();
  if (eventName === 'ack') return 'history';
  if (eventName === 'message') return 'incremental';
  if (eventName === 'inbox_entry') {
    if (inboxKind === 'dm_message') return 'incremental';
    if (inboxKind === 'delivery_ack' || inboxKind === 'read_ack') return 'history';
    return '';
  }
  if (eventName === 'social_sync') {
    if (socialType === 'social_chat_message_private') return 'incremental';
    if (socialType === 'social_chat_receipt' || socialType === 'social_chat_delete' || socialType === 'social_thread_delete' || socialType === 'social_thread_policy') {
      return 'history';
    }
    return '';
  }
  return 'full';
}

async function runHyperwebChatRefresh(mode = 'full') {
  if (mode === 'incremental') {
    await Promise.all([
      refreshHyperwebChatConversations(),
      refreshHyperwebChatHistory(),
    ]);
    return;
  }
  if (mode === 'history') {
    await refreshHyperwebChatHistory();
    return;
  }
  await refreshHyperwebChatData();
}

function scheduleHyperwebChatRefresh(mode = 'full') {
  const nextMode = String(mode || 'full').trim().toLowerCase();
  if (!nextMode) return Promise.resolve();
  if (hyperwebChatRefreshPromise) {
    hyperwebChatRefreshQueuedMode = mergeHyperwebChatRefreshMode(hyperwebChatRefreshQueuedMode, nextMode);
    return hyperwebChatRefreshPromise;
  }
  hyperwebChatRefreshPromise = (async () => {
    let modeToRun = nextMode;
    while (modeToRun) {
      hyperwebChatRefreshQueuedMode = '';
      await runHyperwebChatRefresh(modeToRun);
      modeToRun = hyperwebChatRefreshQueuedMode;
    }
  })().finally(() => {
    hyperwebChatRefreshPromise = null;
    hyperwebChatRefreshQueuedMode = '';
  });
  return hyperwebChatRefreshPromise;
}

function snapHyperwebRatio(rawRatio) {
  const ratio = clamp(Number(rawRatio || 0.5), 0.2, 0.8);
  const points = [0.2, 0.5, 0.8];
  const threshold = 0.035;
  for (const point of points) {
    if (Math.abs(ratio - point) <= threshold) return point;
  }
  return ratio;
}

function loadHyperwebSplitRatio() {
  try {
    const raw = localStorage.getItem(HYPERWEB_SPLIT_RATIO_KEY);
    if (!raw) return 0.5;
    return snapHyperwebRatio(Number(raw));
  } catch (_) {
    return 0.5;
  }
}

function loadHyperwebLastSeenAt() {
  try {
    const raw = window.localStorage.getItem(HYPERWEB_LAST_SEEN_KEY);
    const value = Number(raw || 0);
    return Number.isFinite(value) ? value : 0;
  } catch (_) {
    return 0;
  }
}

function saveHyperwebLastSeenAt(ts = Date.now()) {
  try {
    window.localStorage.setItem(HYPERWEB_LAST_SEEN_KEY, String(Number(ts || Date.now())));
  } catch (_) {
    // noop
  }
}

function renderTopbarBadges() {
  const mailBadge = e('mail-open-badge');
  const hyperwebBadge = e('hyperweb-open-badge');
  const mailCount = Math.max(0, Number(state.mailUnreadCount || 0));
  const hyperwebCount = Math.max(0, Number(state.hyperwebUnreadCount || 0));
  if (mailBadge) {
    mailBadge.textContent = String(mailCount);
    mailBadge.classList.toggle('hidden', mailCount <= 0);
  }
  if (hyperwebBadge) {
    hyperwebBadge.textContent = String(hyperwebCount);
    hyperwebBadge.classList.toggle('hidden', hyperwebCount <= 0);
  }
}

async function refreshTopbarBadges() {
  if (api.mailSearchLocalThreads) {
    const unreadRes = await api.mailSearchLocalThreads('', 500, true, '', '', '', 'unread');
    const unreadItems = (unreadRes && unreadRes.ok && Array.isArray(unreadRes.items)) ? unreadRes.items : [];
    state.mailUnreadCount = unreadItems.reduce((sum, item) => sum + Math.max(0, Number((item && item.unread_count) || 0)), 0);
  }
  if (api.hyperwebChatConversations) {
    const conversationsRes = await api.hyperwebChatConversations();
    const conversations = (conversationsRes && conversationsRes.ok && Array.isArray(conversationsRes.conversations))
      ? conversationsRes.conversations
      : [];
    const dmUnread = conversations.reduce((sum, item) => sum + Math.max(0, Number((item && item.unread_count) || 0)), 0);
    let publicUnread = 0;
    if (api.hyperwebFeedQuery) {
      const feedRes = await api.hyperwebFeedQuery('');
      const posts = (feedRes && feedRes.ok && Array.isArray(feedRes.posts)) ? feedRes.posts : [];
      const lastSeenAt = loadHyperwebLastSeenAt();
      const myId = String((((state.hyperwebIdentity || {}).fingerprint) || '')).trim().toUpperCase();
      posts.forEach((post) => {
        const postTs = Number((post && post.created_at) || 0);
        const authorFp = String((post && post.author_fingerprint) || '').trim().toUpperCase();
        if (postTs > lastSeenAt && authorFp && authorFp !== myId) publicUnread += 1;
        (Array.isArray(post && post.replies) ? post.replies : []).forEach((reply) => {
          const replyTs = Number((reply && reply.created_at) || 0);
          const replyAuthor = String((reply && reply.author_fingerprint) || '').trim().toUpperCase();
          if (replyTs > lastSeenAt && replyAuthor && replyAuthor !== myId) publicUnread += 1;
        });
      });
    }
    state.hyperwebUnreadCount = dmUnread + publicUnread;
  }
  renderTopbarBadges();
}

function persistHyperwebSplitRatio(value) {
  try {
    localStorage.setItem(HYPERWEB_SPLIT_RATIO_KEY, String(value));
  } catch (_) {
    // noop
  }
}

function loadZenModePreference() {
  try {
    return localStorage.getItem(ZEN_MODE_KEY) === '1';
  } catch (_) {
    return false;
  }
}

function persistZenModePreference(enabled) {
  try {
    localStorage.setItem(ZEN_MODE_KEY, enabled ? '1' : '0');
  } catch (_) {
    // noop
  }
}

function applyZenModeUi() {
  document.body.classList.toggle('zen-mode', !!state.zenMode);
}

function loadChatPanelWidthPreference() {
  try {
    const raw = Number(localStorage.getItem(CHAT_PANEL_WIDTH_KEY));
    if (!Number.isFinite(raw)) return 252;
    return clamp(Math.round(raw), 252, 420);
  } catch (_) {
    return 252;
  }
}

function persistChatPanelWidthPreference(width) {
  try {
    localStorage.setItem(CHAT_PANEL_WIDTH_KEY, String(clamp(Math.round(width), 252, 420)));
  } catch (_) {
    // noop
  }
}

function loadChatPanelCollapsedPreference() {
  return false;
}

function persistChatPanelCollapsedPreference(collapsed) {
  try {
    localStorage.setItem(CHAT_PANEL_COLLAPSED_KEY, '0');
  } catch (_) {
    // noop
  }
}

function applyChatPanelState() {
  state.chatPanelCollapsed = false;
  document.body.classList.remove('chat-collapsed');
  const width = clamp(Number(state.chatPanelWidth || 252), 252, 420);
  document.body.style.setProperty('--left-panel-width', `${width}px`);
  document.body.style.setProperty('--right-panel-width', `${width}px`);
  ['workspace-right-rail-toggle-btn'].forEach((id) => {
    const node = e(id);
    if (!node) return;
    node.setAttribute('aria-expanded', 'true');
  });
  const mobileToggle = e('workspace-right-rail-toggle-btn');
  if (mobileToggle) mobileToggle.textContent = 'Lumino';
}

function setChatPanelCollapsed(collapsed, options = {}) {
  state.chatPanelCollapsed = false;
  applyChatPanelState();
  if (!options.skipPersist) persistChatPanelCollapsedPreference(false);
  void syncActiveSurface();
}

function setHistoryDetailView(viewName) {
  const next = String(viewName || 'preview').trim().toLowerCase() === 'map' ? 'map' : 'preview';
  state.historyDetailView = next;
  e('history-view-preview-btn')?.classList.toggle('active', next === 'preview');
  e('history-view-map-btn')?.classList.toggle('active', next === 'map');
  drawHistorySemanticMap();
}

function closeTopbarMenu() {
  e('app-tools-menu')?.classList.add('hidden');
  e('app-tools-btn')?.setAttribute('aria-expanded', 'false');
}

function toggleTopbarMenu() {
  const menu = e('app-tools-menu');
  const button = e('app-tools-btn');
  if (!menu || !button) return;
  const willOpen = menu.classList.contains('hidden');
  menu.classList.toggle('hidden', !willOpen);
  button.setAttribute('aria-expanded', willOpen ? 'true' : 'false');
}

function loadUiZoomPreference() {
  try {
    const raw = localStorage.getItem(UI_ZOOM_KEY);
    if (!raw) return null;
    const parsed = Number(raw);
    if (!Number.isFinite(parsed) || parsed <= 0) return null;
    return clamp(parsed, UI_MIN_ZOOM, UI_MAX_ZOOM);
  } catch (_) {
    return null;
  }
}

function persistUiZoomPreference(zoom) {
  try {
    const next = clamp(Number(zoom || 1), UI_MIN_ZOOM, UI_MAX_ZOOM);
    localStorage.setItem(UI_ZOOM_KEY, String(next));
  } catch (_) {
    // noop
  }
}

function loadArtifactViewPreferences() {
  try {
    const splitRaw = localStorage.getItem(ARTIFACT_SPLIT_BY_REF_KEY);
    const parsedSplit = splitRaw ? JSON.parse(splitRaw) : {};
    if (parsedSplit && typeof parsedSplit === 'object') {
      state.artifactSplitByRef = parsedSplit;
    }
  } catch (_) {
    state.artifactSplitByRef = {};
  }

  try {
    const zoomRaw = localStorage.getItem(ARTIFACT_ZOOM_BY_REF_KEY);
    const parsedZoom = zoomRaw ? JSON.parse(zoomRaw) : {};
    if (parsedZoom && typeof parsedZoom === 'object') {
      state.artifactZoomByRef = parsedZoom;
    }
  } catch (_) {
    state.artifactZoomByRef = {};
  }
}

function persistArtifactViewPreferences() {
  try {
    localStorage.setItem(ARTIFACT_SPLIT_BY_REF_KEY, JSON.stringify(state.artifactSplitByRef || {}));
    localStorage.setItem(ARTIFACT_ZOOM_BY_REF_KEY, JSON.stringify(state.artifactZoomByRef || {}));
  } catch (_) {
    // noop
  }
}

function getArtifactSplitRatioForReference(srId) {
  const key = String(srId || '').trim();
  const value = Number((state.artifactSplitByRef && state.artifactSplitByRef[key]) || ARTIFACT_DEFAULT_SPLIT_RATIO);
  return clamp(value, ARTIFACT_MIN_SPLIT_RATIO, ARTIFACT_MAX_SPLIT_RATIO);
}

function setArtifactSplitRatioForReference(srId, ratio, options = {}) {
  const key = String(srId || '').trim();
  if (!key) return;
  const next = clamp(Number(ratio || ARTIFACT_DEFAULT_SPLIT_RATIO), ARTIFACT_MIN_SPLIT_RATIO, ARTIFACT_MAX_SPLIT_RATIO);
  state.artifactSplitByRef = state.artifactSplitByRef || {};
  state.artifactSplitByRef[key] = next;
  if (!options.skipPersist) persistArtifactViewPreferences();
}

function getArtifactZoomForReference(srId) {
  const key = String(srId || '').trim();
  const value = Number((state.artifactZoomByRef && state.artifactZoomByRef[key]) || ARTIFACT_DEFAULT_ZOOM);
  return clamp(value, ARTIFACT_MIN_ZOOM, ARTIFACT_MAX_ZOOM);
}

function setArtifactZoomForReference(srId, zoom, options = {}) {
  const key = String(srId || '').trim();
  if (!key) return;
  const next = clamp(Number(zoom || ARTIFACT_DEFAULT_ZOOM), ARTIFACT_MIN_ZOOM, ARTIFACT_MAX_ZOOM);
  state.artifactZoomByRef = state.artifactZoomByRef || {};
  state.artifactZoomByRef[key] = next;
  if (!options.skipPersist) persistArtifactViewPreferences();
}

function getArtifactCarouselIndex(artifactId) {
  const key = String(artifactId || '').trim();
  if (!key) return 0;
  const raw = Number((state.artifactCarouselIndexByArtifact && state.artifactCarouselIndexByArtifact[key]) || 0);
  if (!Number.isFinite(raw) || raw < 0) return 0;
  return Math.floor(raw);
}

function setArtifactCarouselIndex(artifactId, index) {
  const key = String(artifactId || '').trim();
  if (!key) return;
  state.artifactCarouselIndexByArtifact = state.artifactCarouselIndexByArtifact || {};
  state.artifactCarouselIndexByArtifact[key] = Math.max(0, Math.floor(Number(index) || 0));
}

function applyHyperwebSplitRatio(rawRatio, options = {}) {
  const snapped = snapHyperwebRatio(rawRatio);
  state.hyperwebSplitRatio = snapped;
  const grid = e('hyperweb-grid');
  if (grid) {
    if (state.hyperwebActiveTab === 'feed' || state.hyperwebActiveTab === 'refs') {
      grid.style.gridTemplateColumns = '';
    } else {
      const left = Math.round(snapped * 1000);
      const right = Math.round((1 - snapped) * 1000);
      grid.style.gridTemplateColumns = `minmax(300px, ${left}fr) 8px minmax(300px, ${right}fr)`;
    }
  }
  if (!options.skipPersist) persistHyperwebSplitRatio(snapped);
}

function referenceResultKey(item) {
  const key = String((item && item.reference_key) || '').trim();
  if (key) return key;
  const peer = String((item && item.peer_id) || '').trim();
  const refId = String((item && item.reference_id) || '').trim();
  return `${peer}:${refId}:${String((item && item.title) || '').trim()}`;
}

function makeActiveSurface(kind, patch = {}) {
  return {
    kind: String(kind || 'web'),
    tabId: null,
    artifactId: null,
    filesTabId: null,
    skillsTabId: null,
    mailTabId: null,
    ...(patch && typeof patch === 'object' ? patch : {}),
  };
}

function getReferenceById(srId) {
  const id = String(srId || '').trim();
  return state.references.find((ref) => String((ref && ref.id) || '') === id) || null;
}

function ensureReferencesArray() {
  if (!Array.isArray(state.references)) state.references = [];
}

function normalizeReferenceSortText(value = '') {
  return String(value || '').trim().toLowerCase().replace(/\s+/g, ' ');
}

function compareReferencesAlphabetically(a, b) {
  const aTitle = normalizeReferenceSortText((a && a.title) || '') || normalizeReferenceSortText((a && a.intent) || '');
  const bTitle = normalizeReferenceSortText((b && b.title) || '') || normalizeReferenceSortText((b && b.intent) || '');
  if (aTitle !== bTitle) return aTitle.localeCompare(bTitle);
  const aIntent = normalizeReferenceSortText((a && a.intent) || '');
  const bIntent = normalizeReferenceSortText((b && b.intent) || '');
  if (aIntent !== bIntent) return aIntent.localeCompare(bIntent);
  return String((a && a.id) || '').localeCompare(String((b && b.id) || ''));
}

function compareReferencesByRankOrAlphabetical(a, b) {
  const ranking = (state.referenceRanking && typeof state.referenceRanking === 'object') ? state.referenceRanking : {};
  if (!ranking.enabled) return compareReferencesAlphabetically(a, b);
  const orderIds = Array.isArray(ranking.orderIds) ? ranking.orderIds : [];
  const orderMap = ranking.orderMap instanceof Map
    ? ranking.orderMap
    : new Map(orderIds.map((id, index) => [String(id || '').trim(), index]));
  const aIndex = orderMap.has(String((a && a.id) || '').trim()) ? Number(orderMap.get(String((a && a.id) || '').trim())) : -1;
  const bIndex = orderMap.has(String((b && b.id) || '').trim()) ? Number(orderMap.get(String((b && b.id) || '').trim())) : -1;
  if (aIndex >= 0 && bIndex >= 0 && aIndex !== bIndex) return aIndex - bIndex;
  return compareReferencesAlphabetically(a, b);
}

function getReferenceRankingPayloadOrderMap(payload = {}) {
  const orderIds = Array.isArray(payload.order_ids) ? payload.order_ids.map((id) => String(id || '').trim()).filter(Boolean) : [];
  return new Map(orderIds.map((id, index) => [id, index]));
}

async function refreshReferenceRankingState(options = {}) {
  if (!api || typeof api.referenceRankingState !== 'function') return null;
  let res = null;
  try {
    res = await api.referenceRankingState({ ensure_fresh: !!options.ensureFresh });
  } catch (_) {
    res = null;
  }
  if (!res || !res.ok) return null;
  state.referenceRanking = {
    enabled: !!res.enabled,
    enabledAt: Number(res.enabled_at || 0),
    lastRankedAt: Number(res.last_ranked_at || 0),
    orderIds: Array.isArray(res.order_ids) ? res.order_ids.map((id) => String(id || '').trim()).filter(Boolean) : [],
    orderMap: getReferenceRankingPayloadOrderMap(res),
    scoreById: (res.scores && typeof res.scores === 'object') ? { ...res.scores } : {},
  };
  return state.referenceRanking;
}

function buildReferenceRankingInteractionPayload(action, options = {}) {
  const payload = {
    action: String(action || '').trim().toLowerCase(),
    sr_id: String((options && options.srId) || state.activeSrId || '').trim(),
  };
  const ref = getReferenceById(payload.sr_id) || getActiveReference();
  const useLiveWebContext = String((state.activeSurface && state.activeSurface.kind) || 'web') === 'web';
  const fallbackWeb = useLiveWebContext && ref ? getActiveWebTab(ref) : null;
  const browserTitle = String((options && options.browserTitle) || (fallbackWeb && fallbackWeb.title) || (useLiveWebContext ? state.browserCurrentTitle : '') || '').trim();
  const browserUrl = String((options && options.browserUrl) || (fallbackWeb && fallbackWeb.url) || (useLiveWebContext ? state.browserCurrentUrl : '') || '').trim();
  if (browserTitle) payload.browser_title = browserTitle;
  if (browserUrl) payload.browser_url = browserUrl;
  const chatPrompt = String((options && options.chatPrompt) || '').trim();
  if (chatPrompt) payload.chat_prompt = chatPrompt;
  return payload;
}

async function noteReferenceRankingInteraction(action, options = {}) {
  if (!api || typeof api.referenceRankingRecord !== 'function') return null;
  const payload = buildReferenceRankingInteractionPayload(action, options);
  if (!payload.action || !payload.sr_id) return null;
  let res = null;
  try {
    res = await api.referenceRankingRecord(payload);
  } catch (_) {
    res = null;
  }
  if (!res || !res.ok) return null;
  state.referenceRanking = {
    enabled: !!res.enabled,
    enabledAt: Number(res.enabled_at || 0),
    lastRankedAt: Number(res.last_ranked_at || 0),
    orderIds: Array.isArray(res.order_ids) ? res.order_ids.map((id) => String(id || '').trim()).filter(Boolean) : [],
    orderMap: getReferenceRankingPayloadOrderMap(res),
    scoreById: (res.scores && typeof res.scores === 'object') ? { ...res.scores } : {},
  };
  return res;
}

function repairActiveReferenceSelection() {
  ensureReferencesArray();
  const refs = state.references;
  const activeId = String(state.activeSrId || '').trim();
  const hasActive = !!(activeId && refs.some((ref) => String((ref && ref.id) || '') === activeId));
  if (hasActive) return false;
  const fallbackId = String(((refs[0] && refs[0].id) || '')).trim();
  state.activeSrId = fallbackId || null;
  if (fallbackId) {
    const fallbackRef = getReferenceById(fallbackId);
    state.activeSurface = restoreSurfaceForReference(fallbackRef);
    rememberSurfaceForReference(fallbackId, state.activeSurface);
  }
  return true;
}

async function refreshAndRepairActiveReferenceSelection() {
  const list = await api.srList();
  state.references = Array.isArray(list) ? list : [];
  const changed = repairActiveReferenceSelection();
  if (changed) {
    renderReferences();
    renderWorkspaceTabs();
    renderContextFiles();
    renderDiffPanel();
  }
  return !!String(state.activeSrId || '').trim();
}

function getActiveReference() {
  if (
    state.memoryReplay
    && state.memoryReplay.active
    && state.memoryReplay.virtualReference
    && String((state.memoryReplay.virtualReference && state.memoryReplay.virtualReference.id) || '') === String(state.activeSrId || '')
  ) {
    return state.memoryReplay.virtualReference;
  }
  return getReferenceById(state.activeSrId);
}

function getLiveActiveReference() {
  return getReferenceById(state.activeSrId);
}

function isMemoryReplayActive() {
  return !!(state.memoryReplay && state.memoryReplay.active);
}

function blockIfMemoryReplay(message = 'Memory replay is read-only. Exit memory mode to edit.') {
  if (!isMemoryReplayActive()) return false;
  showPassiveNotification(message);
  return true;
}

function rememberSurfaceForReference(srId, surface = state.activeSurface) {
  const refId = String(srId || '').trim();
  if (!refId || !surface || typeof surface !== 'object') return;
  const kind = String(surface.kind || '').trim().toLowerCase();
  if (kind === 'artifact' && surface.artifactId) {
    state.activeArtifactByRef.set(refId, String(surface.artifactId));
    state.activeFilesByRef.delete(refId);
    state.activeSkillsByRef.delete(refId);
    state.activeMailByRef.delete(refId);
    return;
  }
  if (kind === 'files' && surface.filesTabId) {
    state.activeFilesByRef.set(refId, String(surface.filesTabId));
    state.activeArtifactByRef.delete(refId);
    state.activeSkillsByRef.delete(refId);
    state.activeMailByRef.delete(refId);
    return;
  }
  if (kind === 'skills' && surface.skillsTabId) {
    state.activeSkillsByRef.set(refId, String(surface.skillsTabId));
    state.activeArtifactByRef.delete(refId);
    state.activeFilesByRef.delete(refId);
    state.activeMailByRef.delete(refId);
    return;
  }
  if (kind === 'mail' && surface.mailTabId) {
    state.activeMailByRef.set(refId, String(surface.mailTabId));
    state.activeArtifactByRef.delete(refId);
    state.activeFilesByRef.delete(refId);
    state.activeSkillsByRef.delete(refId);
    return;
  }
  state.activeArtifactByRef.delete(refId);
  state.activeFilesByRef.delete(refId);
  state.activeSkillsByRef.delete(refId);
  state.activeMailByRef.delete(refId);
}

function restoreSurfaceForReference(ref) {
  const srId = String((ref && ref.id) || '').trim();
  if (!srId || !ref) return makeActiveSurface('web');
  const artifacts = Array.isArray(ref.artifacts) ? ref.artifacts : [];
  const tabs = Array.isArray(ref.tabs) ? ref.tabs : [];

  const artifactId = String(state.activeArtifactByRef.get(srId) || '').trim();
  if (artifactId && artifacts.some((artifact) => String((artifact && artifact.id) || '') === artifactId)) {
    return makeActiveSurface('artifact', { artifactId });
  }

  const filesTabId = String(state.activeFilesByRef.get(srId) || '').trim();
  if (filesTabId && tabs.some((tab) => (
    String((tab && tab.id) || '') === filesTabId
    && String((tab && tab.tab_kind) || '').trim().toLowerCase() === 'files'
  ))) {
    return makeActiveSurface('files', { filesTabId });
  }

  const skillsTabId = String(state.activeSkillsByRef.get(srId) || '').trim();
  if (skillsTabId && tabs.some((tab) => (
    String((tab && tab.id) || '') === skillsTabId
    && String((tab && tab.tab_kind) || '').trim().toLowerCase() === 'skills'
  ))) {
    return makeActiveSurface('skills', { skillsTabId });
  }

  const mailTabId = String(state.activeMailByRef.get(srId) || '').trim();
  if (mailTabId && tabs.some((tab) => (
    String((tab && tab.id) || '') === mailTabId
    && String((tab && tab.tab_kind) || '').trim().toLowerCase() === 'mail'
  ))) {
    return makeActiveSurface('mail', { mailTabId });
  }

  const activeWeb = getActiveWebTab(ref);
  if (activeWeb && activeWeb.id) {
    return makeActiveSurface('web', { tabId: String(activeWeb.id || '').trim() });
  }
  return makeActiveSurface('web');
}

function setStatusText(nodeId, text) {
  const node = e(nodeId);
  if (!node) return;
  node.textContent = String(text || '');
}

function showPassiveNotification(message, timeoutMs = 3200) {
  const text = String(message || '').trim();
  if (!text) return;
  const node = e('active-ref-meta');
  if (!node) return;
  node.textContent = text;
  if (passiveNoticeTimer) clearTimeout(passiveNoticeTimer);
  passiveNoticeTimer = setTimeout(() => {
    passiveNoticeTimer = null;
    updateActiveReferenceMeta();
  }, Math.max(1200, Number(timeoutMs) || 3200));
}

function clearMemoryReplayTimer() {
  if (state.memoryReplay && state.memoryReplay.timer) {
    clearInterval(state.memoryReplay.timer);
    state.memoryReplay.timer = null;
  }
}

function formatMemoryTime(ts) {
  const t = Number(ts || 0);
  if (!Number.isFinite(t) || t <= 0) return '';
  try {
    return new Date(t).toLocaleString();
  } catch (_) {
    return String(t);
  }
}

function dedupeMemoryReplayCheckpoints(list = []) {
  const deduped = [];
  let previousKey = '';
  (Array.isArray(list) ? list : []).forEach((item) => {
    if (!item || typeof item !== 'object') return;
    const kind = String(item.kind || 'periodic').trim().toLowerCase();
    const hash = String(item.snapshot_hash || '').trim();
    const key = hash ? `${kind}:${hash}` : '';
    if (key && key === previousKey) return;
    deduped.push(item);
    previousKey = key;
  });
  return deduped;
}

function applyMemoryReplayFilter() {
  const lane = String((state.memoryReplay && state.memoryReplay.lane) || 'all').trim().toLowerCase();
  const list = Array.isArray(state.memoryReplay && state.memoryReplay.checkpoints) ? state.memoryReplay.checkpoints : [];
  const activeCheckpointId = String((state.memoryReplay && state.memoryReplay.activeCheckpointId) || '').trim();
  let filtered = list;
  if (lane === 'periodic') filtered = list.filter((item) => String((item && item.kind) || '').toLowerCase() === 'periodic');
  if (lane === 'semantic') filtered = list.filter((item) => String((item && item.kind) || '').toLowerCase() === 'semantic');
  filtered = dedupeMemoryReplayCheckpoints(filtered);
  state.memoryReplay.filtered = filtered;
  if (state.memoryReplay.playing && filtered.length <= 1) {
    state.memoryReplay.playing = false;
    clearMemoryReplayTimer();
  }
  if (!filtered.length) {
    state.memoryReplay.index = -1;
    state.memoryReplay.activeCheckpointId = '';
    state.memoryReplay.virtualReference = null;
    return;
  }
  const checkpointIndex = activeCheckpointId
    ? filtered.findIndex((item) => String((item && item.id) || '') === activeCheckpointId)
    : -1;
  if (checkpointIndex >= 0) {
    state.memoryReplay.index = checkpointIndex;
  } else if (state.memoryReplay.index < 0 || state.memoryReplay.index >= filtered.length) {
    state.memoryReplay.index = filtered.length - 1;
  }
  const cp = filtered[state.memoryReplay.index] || null;
  state.memoryReplay.activeCheckpointId = cp ? String(cp.id || '') : '';
}

function renderMemoryRail() {
  const rail = e('memory-rail');
  const memoryBtn = e('browser-memory-btn');
  const panel = e('workspace-panel');
  if (!rail || !memoryBtn || !panel) return;
  const active = isMemoryReplayActive();
  rail.classList.toggle('hidden', !active);
  memoryBtn.classList.toggle('memory-active', active);
  panel.classList.toggle('memory-replay-mode', active);
  const lane = String((state.memoryReplay && state.memoryReplay.lane) || 'all');
  e('memory-lane-all-btn')?.classList.toggle('active', lane === 'all');
  e('memory-lane-periodic-btn')?.classList.toggle('active', lane === 'periodic');
  e('memory-lane-semantic-btn')?.classList.toggle('active', lane === 'semantic');

  const filtered = Array.isArray(state.memoryReplay.filtered) ? state.memoryReplay.filtered : [];
  const idx = Number(state.memoryReplay.index || 0);
  const cp = filtered[idx] || null;
  const positionNode = e('memory-position');
  const labelNode = e('memory-checkpoint-label');
  const prevBtn = e('memory-prev-btn');
  const nextBtn = e('memory-next-btn');
  const playBtn = e('memory-play-btn');
  if (positionNode) {
    positionNode.textContent = filtered.length ? `${idx + 1} / ${filtered.length}` : '0 / 0';
  }
  if (labelNode) {
    labelNode.textContent = cp
      ? `${String(cp.kind || 'periodic')} · ${formatMemoryTime(cp.created_at)} · ${String(cp.summary || '')}`
      : 'No checkpoint';
  }
  const hasCheckpoints = filtered.length > 0;
  const atStart = !hasCheckpoints || idx <= 0;
  const atEnd = !hasCheckpoints || idx >= filtered.length - 1;
  if (prevBtn) prevBtn.disabled = !active || atStart;
  if (nextBtn) nextBtn.disabled = !active || atEnd;
  if (playBtn) {
    playBtn.textContent = state.memoryReplay.playing ? 'Pause' : 'Play';
    playBtn.disabled = !active || filtered.length <= 1 || (!state.memoryReplay.playing && atEnd);
  }
}

async function refreshMemoryReplayList() {
  const srId = String(state.activeSrId || '').trim();
  if (!srId) return false;
  const res = await api.memoryList(srId);
  if (!res || !res.ok) {
    showPassiveNotification((res && res.message) ? res.message : 'Unable to load memory list.');
    return false;
  }
  state.memoryReplay.checkpoints = Array.isArray(res.checkpoints) ? res.checkpoints : [];
  applyMemoryReplayFilter();
  return true;
}

async function loadMemoryCheckpointByIndex(index) {
  const srId = String(state.activeSrId || '').trim();
  const filtered = Array.isArray(state.memoryReplay.filtered) ? state.memoryReplay.filtered : [];
  if (!srId || !filtered.length) return;
  const nextIndex = Math.max(0, Math.min(filtered.length - 1, Number(index || 0)));
  const cp = filtered[nextIndex];
  if (!cp) return;
  const res = await api.memoryLoadCheckpoint(srId, String(cp.id || ''));
  if (!res || !res.ok || !res.snapshot) {
    showPassiveNotification((res && res.message) ? res.message : 'Unable to load memory checkpoint.');
    return;
  }
  state.memoryReplay.index = nextIndex;
  state.memoryReplay.activeCheckpointId = String(cp.id || '');
  state.memoryReplay.virtualReference = {
    ...deepClone(getLiveActiveReference() || {}),
    ...deepClone(res.snapshot || {}),
    id: srId,
  };
  renderMemoryRail();
  renderWorkspaceTabs();
  renderContextFiles();
  renderDiffPanel();
  await loadChatThread();
  await syncActiveSurface();
}

async function enterMemoryReplay() {
  if (!state.activeSrId) {
    showPassiveNotification('Select a reference first.');
    return;
  }
  const srId = String(state.activeSrId || '').trim();
  const enableRes = await api.memorySetEnabled(srId, true);
  if (!enableRes || !enableRes.ok) {
    showPassiveNotification((enableRes && enableRes.message) ? enableRes.message : 'Unable to enable memory.');
    return;
  }
  state.references = enableRes.references || state.references;
  state.memoryReplay.active = true;
  state.memoryReplay.lane = 'all';
  state.memoryReplay.index = -1;
  state.memoryReplay.activeCheckpointId = '';
  state.memoryReplay.virtualReference = null;
  await refreshMemoryReplayList();
  renderMemoryRail();
  setChatBusy(false);
  if (state.memoryReplay.filtered.length > 0) {
    await loadMemoryCheckpointByIndex(state.memoryReplay.filtered.length - 1);
  } else {
    showPassiveNotification('No memory checkpoints available yet for this reference.');
  }
}

async function exitMemoryReplay() {
  clearMemoryReplayTimer();
  state.memoryReplay.active = false;
  state.memoryReplay.playing = false;
  state.memoryReplay.checkpoints = [];
  state.memoryReplay.filtered = [];
  state.memoryReplay.index = -1;
  state.memoryReplay.activeCheckpointId = '';
  state.memoryReplay.virtualReference = null;
  renderMemoryRail();
  setChatBusy(false);
  renderWorkspaceTabs();
  renderContextFiles();
  renderDiffPanel();
  await loadChatThread();
  await syncActiveSurface();
}

async function stepMemoryReplay(delta) {
  const filtered = Array.isArray(state.memoryReplay.filtered) ? state.memoryReplay.filtered : [];
  if (!filtered.length) return;
  const current = Number(state.memoryReplay.index || 0);
  const next = Math.max(0, Math.min(filtered.length - 1, current + Number(delta || 0)));
  if (next === current) {
    if (state.memoryReplay.playing) {
      state.memoryReplay.playing = false;
      clearMemoryReplayTimer();
      renderMemoryRail();
    }
    return;
  }
  await loadMemoryCheckpointByIndex(next);
}

async function setMemoryReplayLane(lane) {
  state.memoryReplay.lane = String(lane || 'all').trim().toLowerCase();
  applyMemoryReplayFilter();
  renderMemoryRail();
  if (state.memoryReplay.filtered.length > 0) {
    await loadMemoryCheckpointByIndex(Math.max(0, Math.min(state.memoryReplay.index, state.memoryReplay.filtered.length - 1)));
  } else {
    state.memoryReplay.virtualReference = null;
    renderWorkspaceTabs();
    renderContextFiles();
    renderDiffPanel();
    await loadChatThread();
    await syncActiveSurface();
  }
}

function loadReferenceCollapsedState() {
  try {
    const raw = localStorage.getItem('subgrapher_reference_tree_collapsed_v1');
    const parsed = raw ? JSON.parse(raw) : {};
    if (parsed && typeof parsed === 'object') {
      state.referenceCollapsed = parsed;
    }
  } catch (_) {
    state.referenceCollapsed = {};
  }
}

function persistReferenceCollapsedState() {
  try {
    localStorage.setItem('subgrapher_reference_tree_collapsed_v1', JSON.stringify(state.referenceCollapsed || {}));
  } catch (_) {
    // noop
  }
}

function toggleReferenceInlineExpansion(srId = '') {
  const targetId = String(srId || '').trim();
  if (!targetId) return;
  state.referenceExpandedSrId = state.referenceExpandedSrId === targetId ? null : targetId;
  renderReferences();
}

function loadBrowserMarkerModePreference() {
  try {
    return localStorage.getItem(BROWSER_MARKER_MODE_KEY) === '1';
  } catch (_) {
    return false;
  }
}

function persistBrowserMarkerModePreference(enabled) {
  try {
    localStorage.setItem(BROWSER_MARKER_MODE_KEY, enabled ? '1' : '0');
  } catch (_) {
    // noop
  }
}

function updateBrowserMarkerModeUi() {
  const btn = e('browser-marker-mode-btn');
  if (!btn) return;
  btn.textContent = `Marker: ${state.markerMode ? 'On' : 'Off'}`;
  btn.classList.toggle('marker-mode-active', !!state.markerMode);
  btn.setAttribute('aria-pressed', state.markerMode ? 'true' : 'false');
  btn.title = `Marker mode: ${state.markerMode ? 'On' : 'Off'}`;
}

async function setBrowserMarkerMode(enabled, options = {}) {
  state.markerMode = !!enabled;
  if (!state.markerMode) {
    if (state.artifactMarkerSelectionTimer) {
      clearTimeout(state.artifactMarkerSelectionTimer);
      state.artifactMarkerSelectionTimer = null;
    }
    state.artifactMarkerSelectionSignature = '';
    state.artifactMarkerSelectionAt = 0;
  }
  if (options.persist !== false) {
    persistBrowserMarkerModePreference(state.markerMode);
  }
  updateBrowserMarkerModeUi();
  try {
    await api.markerSetMode(state.markerMode);
  } catch (_) {
    // marker mode sync is best-effort
  }
}

function getArtifactMarkerSelection(input) {
  if (!input || typeof input.value !== 'string') return null;
  const rawStart = Number(input.selectionStart);
  const rawEnd = Number(input.selectionEnd);
  if (!Number.isFinite(rawStart) || !Number.isFinite(rawEnd)) return null;
  const minRaw = Math.min(rawStart, rawEnd);
  const maxRaw = Math.max(rawStart, rawEnd);
  const start = Math.max(0, Math.min(Math.round(minRaw), input.value.length));
  const end = Math.max(start, Math.min(Math.round(maxRaw), input.value.length));
  if (end <= start) return null;
  const text = String(input.value.slice(start, end) || '').trim();
  if (!text) return null;
  return { start, end, text };
}

function isDuplicateArtifactMarkerSelection(srId, artifactId, selection, cooldownMs = 220) {
  const signature = [
    String(srId || '').trim(),
    String(artifactId || '').trim(),
    Number(selection && selection.start),
    Number(selection && selection.end),
    String((selection && selection.text) || ''),
  ].join('|');
  const now = Date.now();
  const isDuplicate = (
    signature
    && signature === String(state.artifactMarkerSelectionSignature || '')
    && (now - Number(state.artifactMarkerSelectionAt || 0)) < cooldownMs
  );
  state.artifactMarkerSelectionSignature = signature;
  state.artifactMarkerSelectionAt = now;
  return isDuplicate;
}

async function toggleActiveArtifactMarkerSelection() {
  if (!state.markerMode) return;
  if (!api || typeof api.srToggleArtifactHighlight !== 'function') return;
  if (!state.activeSrId || state.activeSurface.kind !== 'artifact') return;
  const artifactId = String(state.activeSurface.artifactId || '').trim();
  if (!artifactId) return;
  const input = e('artifact-input');
  if (!input) return;
  const selection = getArtifactMarkerSelection(input);
  if (!selection) return;
  if (isDuplicateArtifactMarkerSelection(state.activeSrId, artifactId, selection)) return;
  try {
    const res = await api.srToggleArtifactHighlight(state.activeSrId, artifactId, {
      start: selection.start,
      end: selection.end,
      text: selection.text,
      marker_mode_snapshot: true,
    });
    if (res && res.ok) {
      state.references = res.references || state.references;
      if (typeof input.setSelectionRange === 'function') {
        input.setSelectionRange(selection.end, selection.end);
      }
      renderArtifactHighlightLayer();
      if (res.added) showPassiveNotification('Artifact marker added.');
      else if (res.removed) showPassiveNotification('Artifact marker removed.');
    }
  } catch (_) {
    // marker toggle is best-effort
  }
}

function scheduleToggleActiveArtifactMarkerSelection(delayMs = 0) {
  if (!state.markerMode || state.activeSurface.kind !== 'artifact') return;
  if (state.artifactMarkerSelectionTimer) {
    clearTimeout(state.artifactMarkerSelectionTimer);
    state.artifactMarkerSelectionTimer = null;
  }
  state.artifactMarkerSelectionTimer = setTimeout(() => {
    state.artifactMarkerSelectionTimer = null;
    void toggleActiveArtifactMarkerSelection();
  }, Math.max(0, Math.round(Number(delayMs) || 0)));
}

function getActiveArtifactHighlights() {
  if (state.activeSurface.kind !== 'artifact') return [];
  const ref = getActiveReference();
  const artifactId = String(state.activeSurface.artifactId || '').trim();
  if (!ref || !artifactId) return [];
  return (Array.isArray(ref.highlights) ? ref.highlights : [])
    .filter((item) => item && String(item.source || '').trim().toLowerCase() === 'artifact'
      && String(item.artifact_id || '').trim() === artifactId)
    .map((item) => ({
      start: Number(item.artifact_start),
      end: Number(item.artifact_end),
    }))
    .filter((item) => Number.isFinite(item.start) && Number.isFinite(item.end) && item.end > item.start)
    .sort((a, b) => a.start - b.start);
}

function renderArtifactHighlightLayer() {
  const pane = e('artifact-text-pane');
  const input = e('artifact-input');
  const layer = e('artifact-highlight-layer');
  if (!pane || !input || !layer) return;
  if (state.activeSurface.kind !== 'artifact') {
    pane.classList.remove('marker-visualized');
    pane.classList.remove('marker-selecting');
    layer.innerHTML = '';
    return;
  }
  const content = String(input.value || '');
  const highlights = getActiveArtifactHighlights();
  const useLayer = !!(state.markerMode || highlights.length);
  const rawStart = Number(input.selectionStart);
  const rawEnd = Number(input.selectionEnd);
  const hasActiveSelection = (
    document.activeElement === input
    && Number.isFinite(rawStart)
    && Number.isFinite(rawEnd)
    && Math.abs(rawEnd - rawStart) > 0
  );
  pane.classList.toggle('marker-visualized', useLayer);
  pane.classList.toggle('marker-selecting', hasActiveSelection);
  if (!useLayer) {
    layer.innerHTML = '';
    return;
  }
  let cursor = 0;
  let html = '';
  highlights.forEach((entry) => {
    const start = clamp(Math.round(entry.start), 0, content.length);
    const end = clamp(Math.round(entry.end), start, content.length);
    if (end <= start || start < cursor) return;
    html += escapeHtml(content.slice(cursor, start));
    html += `<mark>${escapeHtml(content.slice(start, end))}</mark>`;
    cursor = end;
  });
  html += escapeHtml(content.slice(cursor));
  layer.innerHTML = html || '&#8203;';
  layer.scrollTop = input.scrollTop;
  layer.scrollLeft = input.scrollLeft;
}

function normalizeReferenceSearchQuery(raw) {
  return String(raw || '').trim();
}

function updateReferenceSearchControls() {
  const clearBtn = e('reference-search-clear-btn');
  if (!clearBtn) return;
  clearBtn.style.visibility = state.searchQuery ? 'visible' : 'hidden';
}

function sanitizeReferenceColorTag(value) {
  const tag = String(value || '').trim().toLowerCase();
  return REFERENCE_COLOR_TAGS.includes(tag) ? tag : '';
}

function resetReferenceColorFilter() {
  state.referenceColorFilter = { mode: 'all', selected: new Set() };
}

function updateReferenceColorFilterUi() {
  const row = e('reference-color-filter-row');
  if (!row) return;
  const mode = String((state.referenceColorFilter && state.referenceColorFilter.mode) || 'all');
  const selected = (state.referenceColorFilter && state.referenceColorFilter.selected instanceof Set)
    ? state.referenceColorFilter.selected
    : new Set();
  row.querySelectorAll('button[data-color-filter]').forEach((button) => {
    const filter = String(button.getAttribute('data-color-filter') || '').trim().toLowerCase();
    const active = filter === 'all'
      ? mode === 'all'
      : (mode === 'colors' && selected.has(filter));
    button.classList.toggle('active', active);
  });
}

function normalizeProviderKeyId(value) {
  return String(value || '')
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9._-]+/g, '-')
    .replace(/^-+/g, '')
    .replace(/-+$/g, '')
    .slice(0, 80);
}

function getProviderStateEntry(provider) {
  const target = String(provider || '').trim().toLowerCase();
  const entries = Array.isArray(state.providerKeysState && state.providerKeysState.providers)
    ? state.providerKeysState.providers
    : [];
  return entries.find((entry) => String((entry && entry.provider) || '') === target) || null;
}

function getProviderPrimaryKeyId(provider) {
  const entry = getProviderStateEntry(provider);
  return normalizeProviderKeyId(entry && entry.primary_key_id);
}

function getProviderKeyLabel(provider, keyId) {
  const targetKeyId = normalizeProviderKeyId(keyId);
  const entry = getProviderStateEntry(provider);
  const keys = Array.isArray(entry && entry.keys) ? entry.keys : [];
  const found = keys.find((item) => normalizeProviderKeyId(item && item.key_id) === targetKeyId);
  return String((found && found.label) || targetKeyId || '').trim();
}

function getProviderKeyStatusText(provider) {
  const target = String(provider || '').trim().toLowerCase();
  return String((state.providerKeyStatusByProvider && state.providerKeyStatusByProvider[target]) || '').trim();
}

function setProviderKeyStatusText(provider, message) {
  const target = String(provider || '').trim().toLowerCase();
  if (!state.providerKeyStatusByProvider || typeof state.providerKeyStatusByProvider !== 'object') {
    state.providerKeyStatusByProvider = {};
  }
  state.providerKeyStatusByProvider[target] = String(message || '');
  const node = e(`settings-provider-key-status-${target}`);
  if (node) node.textContent = state.providerKeyStatusByProvider[target];
}

function getSelectedProvider() {
  const select = e('provider-select');
  const provider = String((select && select.value) || state.selectedProvider || 'openai').trim().toLowerCase();
  if (!PROVIDERS.includes(provider)) return 'openai';
  return provider;
}

function getSelectedModel() {
  const select = e('provider-model-select');
  const model = String((select && select.value) || state.selectedModel || '').trim();
  return model;
}

async function persistLuminoSelection(provider = getSelectedProvider(), model = getSelectedModel()) {
  const targetProvider = String(provider || '').trim().toLowerCase();
  const targetModel = String(model || '').trim();
  if (!api || typeof api.setLuminoSelection !== 'function') return null;
  if (!PROVIDERS.includes(targetProvider)) return null;
  try {
    return await api.setLuminoSelection(targetProvider, targetModel);
  } catch (_) {
    return null;
  }
}

function renderModelDropdown(models, options = {}) {
  const select = e('provider-model-select');
  if (!select) return;
  const list = Array.isArray(models) ? models.filter(Boolean) : [];
  const preserveOnEmpty = options.preserveOnEmpty !== false;
  const previous = options.forceModel ? '' : getSelectedModel();
  const chosen = String(options.forceModel || previous || list[0] || '').trim();

  if (list.length === 0) {
    const fallbackModel = preserveOnEmpty
      ? String(options.forceModel || previous || state.selectedModel || '').trim()
      : '';
    if (fallbackModel) {
      select.innerHTML = `<option value="${escapeHtml(fallbackModel)}">${escapeHtml(fallbackModel)} (saved)</option>`;
      select.value = fallbackModel;
      state.selectedModel = fallbackModel;
    } else {
      select.innerHTML = '<option value=\"\">No models loaded</option>';
      select.value = '';
      state.selectedModel = '';
    }
    return;
  }

  select.innerHTML = list.map((model) => `<option value=\"${escapeHtml(model)}\">${escapeHtml(model)}</option>`).join('');
  if (chosen && list.includes(chosen)) {
    select.value = chosen;
  } else {
    select.value = list[0];
  }
  state.selectedModel = String(select.value || '').trim();
}

async function fetchModelsForProvider(provider, options = {}) {
  const targetProvider = String(provider || '').trim().toLowerCase();
  if (!PROVIDERS.includes(targetProvider)) return null;
  const targetKeyId = normalizeProviderKeyId(options.keyId || options.key_id || '');
  const statusId = options.statusId || 'provider-status';
  const applyToMain = options.applyToMain !== false;
  const mainRequestSeq = applyToMain ? (state.providerModelsRequestSeq += 1) : 0;
  const previousModel = String(state.selectedModel || '').trim();
  const keyLabel = targetKeyId ? getProviderKeyLabel(targetProvider, targetKeyId) : '';
  setStatusText(statusId, `Fetching models from ${targetProvider}${keyLabel ? ` (${keyLabel})` : ''}...`);
  const res = await api.providerListModels(targetProvider, targetKeyId);
  const isStaleMainResponse = applyToMain
    && (
      mainRequestSeq !== state.providerModelsRequestSeq
      || getSelectedProvider() !== targetProvider
    );
  if (isStaleMainResponse) {
    return Array.isArray(res && res.models) ? res.models : null;
  }
  if (!res || !res.ok) {
    setStatusText(statusId, (res && res.message) ? res.message : `Unable to fetch models for ${targetProvider}.`);
    return null;
  }
  const models = Array.isArray(res.models) ? res.models : [];
  if (applyToMain) {
    renderModelDropdown(models, { forceModel: options.forceModel || '' });
  }
  if (applyToMain && options.persistSelection !== false) {
    await persistLuminoSelection(targetProvider, getSelectedModel() || previousModel);
  }
  const suffix = res.fallback ? ' (fallback list)' : '';
  const keySuffix = keyLabel ? ` via ${keyLabel}` : '';
  if (statusId === 'provider-status') {
    await refreshProviderStatus({ reload: false });
  } else {
    setStatusText(statusId, `Loaded ${models.length} model(s) for ${targetProvider}${keySuffix}${suffix}.`);
  }
  return models;
}

function getActiveWebTab(ref) {
  if (!ref || !Array.isArray(ref.tabs)) return null;
  const webTabs = ref.tabs.filter((tab) => String((tab && tab.tab_kind) || 'web') === 'web');
  if (!webTabs.length) return null;
  const surfaceTabId = state.activeSurface.kind === 'web' ? String(state.activeSurface.tabId || '') : '';
  const preferredId = surfaceTabId || String(ref.active_tab_id || '');
  const preferred = webTabs.find((tab) => String((tab && tab.id) || '') === preferredId);
  return preferred || webTabs[0];
}

function getActiveFilesTab(ref) {
  if (!ref || !Array.isArray(ref.tabs)) return null;
  const filesTabs = ref.tabs.filter((tab) => String((tab && tab.tab_kind) || '').trim().toLowerCase() === 'files');
  if (!filesTabs.length) return null;
  const surfaceTabId = state.activeSurface.kind === 'files'
    ? String(state.activeSurface.filesTabId || '')
    : '';
  const preferredId = surfaceTabId || String(ref.active_tab_id || '');
  const preferred = filesTabs.find((tab) => String((tab && tab.id) || '') === preferredId);
  return preferred || filesTabs[0];
}

function getNormalizedFilesViewState(tab) {
  const raw = (tab && tab.files_view_state && typeof tab.files_view_state === 'object')
    ? tab.files_view_state
    : {};
  const scope = String(raw.scope || 'all').trim().toLowerCase();
  if (scope === 'mount') {
    return {
      scope: 'mount',
      mount_id: String(raw.mount_id || '').trim(),
      file_id: '',
    };
  }
  if (scope === 'context_file') {
    return {
      scope: 'context_file',
      mount_id: '',
      file_id: String(raw.file_id || '').trim(),
    };
  }
  return {
    scope: 'all',
    mount_id: '',
    file_id: '',
  };
}

function getActiveSkillsTab(ref) {
  if (!ref || !Array.isArray(ref.tabs)) return null;
  const skillsTabs = ref.tabs.filter((tab) => String((tab && tab.tab_kind) || '').trim().toLowerCase() === 'skills');
  if (!skillsTabs.length) return null;
  const surfaceTabId = state.activeSurface.kind === 'skills'
    ? String(state.activeSurface.skillsTabId || '')
    : '';
  const preferredId = surfaceTabId || String(ref.active_tab_id || '');
  const preferred = skillsTabs.find((tab) => String((tab && tab.id) || '') === preferredId);
  return preferred || skillsTabs[0];
}

function getActiveMailTab(ref) {
  if (!ref || !Array.isArray(ref.tabs)) return null;
  const mailTabs = ref.tabs.filter((tab) => String((tab && tab.tab_kind) || '').trim().toLowerCase() === 'mail');
  if (!mailTabs.length) return null;
  const surfaceTabId = state.activeSurface.kind === 'mail'
    ? String(state.activeSurface.mailTabId || '')
    : '';
  const preferredId = surfaceTabId || String(ref.active_tab_id || '');
  const preferred = mailTabs.find((tab) => String((tab && tab.id) || '') === preferredId);
  return preferred || mailTabs[0];
}

function getNextArtifactName(ref) {
  const artifacts = Array.isArray(ref && ref.artifacts) ? ref.artifacts : [];
  const used = new Set();
  artifacts.forEach((artifact) => {
    const title = String((artifact && artifact.title) || '').trim().toLowerCase();
    const match = title.match(/^art-(\d+)\.md$/);
    if (!match) return;
    const idx = Number(match[1]);
    if (Number.isFinite(idx) && idx > 0) used.add(idx);
  });
  let next = 1;
  while (used.has(next)) next += 1;
  return `art-${next}.md`;
}

function normalizeUrlForComparison(url) {
  const raw = String(url || '').trim();
  if (!raw) return '';
  try {
    const parsed = new URL(raw);
    let pathname = parsed.pathname || '/';
    if (pathname.length > 1 && pathname.endsWith('/')) pathname = pathname.slice(0, -1);
    return `${parsed.protocol}//${parsed.host}${pathname}${parsed.search}`;
  } catch (_) {
    return raw.toLowerCase();
  }
}

function normalizeSearchEngineValue(value) {
  const engine = String(value || '').trim().toLowerCase();
  return ['ddg', 'google', 'bing'].includes(engine) ? engine : 'ddg';
}

function getConfiguredDefaultSearchEngine() {
  const select = e('default-search-engine-select');
  if (select && select.value) return normalizeSearchEngineValue(select.value);
  if (state.settingsDraft && state.settingsDraft.default_search_engine) {
    return normalizeSearchEngineValue(state.settingsDraft.default_search_engine);
  }
  if (state.settingsPersisted && state.settingsPersisted.default_search_engine) {
    return normalizeSearchEngineValue(state.settingsPersisted.default_search_engine);
  }
  return 'ddg';
}

function getDefaultSearchHomeUrl(engine = getConfiguredDefaultSearchEngine()) {
  const target = normalizeSearchEngineValue(engine);
  if (target === 'google') return 'https://www.google.com/';
  if (target === 'bing') return 'https://www.bing.com/';
  return 'https://duckduckgo.com/';
}

function getDefaultSearchHomeTitle(engine = getConfiguredDefaultSearchEngine()) {
  const target = normalizeSearchEngineValue(engine);
  if (target === 'google') return 'Google';
  if (target === 'bing') return 'Bing';
  return 'DuckDuckGo';
}

function isBlankNavigationUrl(url) {
  const value = String(url || '').trim().toLowerCase();
  return !value || value === 'about:blank';
}

function hasRuntimeTabsApi() {
  return !!(
    window.electronAPI
    && window.electronAPI.tabs
    && typeof window.electronAPI.tabs.create === 'function'
    && typeof window.electronAPI.tabs.switch === 'function'
    && typeof window.electronAPI.tabs.navigate === 'function'
  );
}

function workspaceTabCacheKey(srId, tabId) {
  return `${String(srId || '').trim()}:${String(tabId || '').trim()}`;
}

async function ensureWorkspaceWebTabActive(srId, tab) {
  const refId = String(srId || '').trim();
  const semanticTabId = String((tab && tab.id) || '').trim();
  const semanticUrl = String((tab && tab.url) || '').trim();
  if (!hasRuntimeTabsApi() || !refId || !semanticTabId || !semanticUrl) return { ok: false, fallback: true };

  const key = workspaceTabCacheKey(refId, semanticTabId);
  const mappedRuntimeTabId = String(state.workspaceBrowserTabMap.get(key) || '').trim();
  const targetCanonical = normalizeUrlForComparison(semanticUrl);
  if (mappedRuntimeTabId) {
    const allTabs = await window.electronAPI.tabs.getAll().catch(() => []);
    const mappedRuntimeTab = Array.isArray(allTabs)
      ? allTabs.find((item) => String((item && item.id) || '').trim() === mappedRuntimeTabId)
      : null;
    if (mappedRuntimeTab) {
      const runtimeCanonical = normalizeUrlForComparison(String((mappedRuntimeTab && mappedRuntimeTab.url) || '').trim());
      const hasCanonicalMismatch = !!(targetCanonical && runtimeCanonical && targetCanonical !== runtimeCanonical);
      if (hasCanonicalMismatch) {
        state.workspaceBrowserTabMap.delete(key);
        await closeRuntimeTabIfUnmapped(mappedRuntimeTabId);
      } else {
        const switched = await window.electronAPI.tabs.switch(mappedRuntimeTabId).catch(() => null);
        if (switched && switched.ok) {
          return { ok: true, runtimeTabId: mappedRuntimeTabId, reused: true };
        }
        state.workspaceBrowserTabMap.delete(key);
        await closeRuntimeTabIfUnmapped(mappedRuntimeTabId);
      }
    } else {
      state.workspaceBrowserTabMap.delete(key);
    }
  }

  const created = await window.electronAPI.tabs.create(semanticUrl).catch(() => null);
  if (created && created.ok && created.tabId) {
    const runtimeTabId = String(created.tabId || '').trim();
    if (runtimeTabId) state.workspaceBrowserTabMap.set(key, runtimeTabId);
    return { ok: true, runtimeTabId, reused: false };
  }
  return { ok: false, fallback: true };
}

async function closeRuntimeTabIfUnmapped(runtimeTabId) {
  const targetId = String(runtimeTabId || '').trim();
  if (!targetId || !hasRuntimeTabsApi()) return;
  const stillMapped = Array.from(state.workspaceBrowserTabMap.values())
    .some((id) => String(id || '').trim() === targetId);
  if (stillMapped) return;
  await window.electronAPI.tabs.close(targetId).catch(() => null);
}

async function navigateActiveRuntimeTab(url) {
  const target = String(url || '').trim();
  if (!target) return false;
  if (hasRuntimeTabsApi()) {
    const active = await window.electronAPI.tabs.getActive().catch(() => null);
    const runtimeTabId = String((active && active.activeTabId) || '').trim();
    if (runtimeTabId) {
      const navRes = await window.electronAPI.tabs.navigate(runtimeTabId, target).catch(() => null);
      if (navRes && navRes.ok) return true;
    }
  }
  await api.navigate(target);
  return true;
}

async function detachWorkspaceWebTabMapping(srId, tabId) {
  const key = workspaceTabCacheKey(srId, tabId);
  const runtimeTabId = String(state.workspaceBrowserTabMap.get(key) || '').trim();
  if (!runtimeTabId) return;
  state.workspaceBrowserTabMap.delete(key);
  await closeRuntimeTabIfUnmapped(runtimeTabId);
}

async function pruneWorkspaceWebTabMappings(references = state.references) {
  if (!hasRuntimeTabsApi()) return;
  const refs = Array.isArray(references) ? references : [];
  const validKeys = new Set();
  refs.forEach((ref) => {
    const refId = String((ref && ref.id) || '').trim();
    if (!refId) return;
    const tabs = Array.isArray(ref && ref.tabs) ? ref.tabs : [];
    tabs.forEach((tab) => {
      const kind = String((tab && tab.tab_kind) || 'web').trim().toLowerCase();
      if (kind !== 'web') return;
      const tabId = String((tab && tab.id) || '').trim();
      if (!tabId) return;
      validKeys.add(workspaceTabCacheKey(refId, tabId));
    });
  });

  const staleKeys = Array.from(state.workspaceBrowserTabMap.keys())
    .filter((key) => !validKeys.has(String(key || '').trim()));
  for (const key of staleKeys) {
    const runtimeTabId = String(state.workspaceBrowserTabMap.get(key) || '').trim();
    state.workspaceBrowserTabMap.delete(key);
    if (!runtimeTabId) continue;
    await closeRuntimeTabIfUnmapped(runtimeTabId);
  }
}

function isUncommittedCurrentUrl(ref, url) {
  const needle = normalizeUrlForComparison(url);
  if (!needle || !ref) return false;
  const tabs = Array.isArray(ref.tabs)
    ? ref.tabs.filter((tab) => String((tab && tab.tab_kind) || 'web').trim().toLowerCase() === 'web')
    : [];
  return !tabs.some((tab) => normalizeUrlForComparison(tab && tab.url) === needle);
}

function setUncommittedActionCue(active) {
  const panel = e('workspace-panel');
  const chip = e('workspace-status-chip');
  if (!panel) return;
  panel.classList.toggle('browser-has-uncommitted', !!active);
  if (chip) chip.classList.toggle('hidden', !active);
}

async function refreshUncommittedActionCue() {
  if (!state.activeSrId) {
    setUncommittedActionCue(false);
    return;
  }
  if (state.activeSurface.kind !== 'web') {
    setUncommittedActionCue(false);
    return;
  }
  const ref = getActiveReference();
  if (!ref) {
    setUncommittedActionCue(false);
    return;
  }
  let liveUrl = '';
  try {
    liveUrl = String((await api.getCurrentUrl()) || '').trim();
  } catch (_) {
    liveUrl = '';
  }
  if (!liveUrl) liveUrl = String(state.browserCurrentUrl || '').trim();
  if (!liveUrl) {
    setUncommittedActionCue(false);
    return;
  }
  setUncommittedActionCue(isUncommittedCurrentUrl(ref, liveUrl));
}

function extractYouTubeVideoId(rawUrl) {
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

function getYouTubeTranscriptRecordForUrl(ref, rawUrl) {
  if (!ref || typeof ref !== 'object') return null;
  const videoId = extractYouTubeVideoId(rawUrl);
  if (!videoId) return null;
  const map = (ref.youtube_transcripts && typeof ref.youtube_transcripts === 'object') ? ref.youtube_transcripts : {};
  const record = map[videoId];
  return (record && typeof record === 'object') ? record : null;
}

function shouldFetchYouTubeTranscript(ref, rawUrl) {
  const videoId = extractYouTubeVideoId(rawUrl);
  if (!videoId) return { ok: false, reason: 'not_youtube', videoId: '' };
  const current = getYouTubeTranscriptRecordForUrl(ref, rawUrl);
  const transcriptChars = Number(current && current.transcript_char_count);
  const transcriptPreview = String((current && current.transcript_text) || '').trim();
  const hasTranscript = !!transcriptPreview || (Number.isFinite(transcriptChars) && transcriptChars > 0);
  if (current && String((current.status && current.status.state) || '').toLowerCase() === 'ready' && hasTranscript) {
    return { ok: false, reason: 'already_cached', videoId };
  }
  const retryAfter = Number(current && current.status ? current.status.retry_after : null);
  if (current && String((current.status && current.status.state) || '').toLowerCase() === 'error' && Number.isFinite(retryAfter) && retryAfter > Date.now()) {
    return { ok: false, reason: 'cooldown_active', videoId };
  }
  return { ok: true, reason: 'fetch_needed', videoId };
}

async function maybeQueueYouTubeTranscriptIngestion(srId, rawUrl, title = '', options = {}) {
  const referenceId = String(srId || '').trim();
  const targetUrl = String(rawUrl || '').trim();
  if (!referenceId || !targetUrl || !api.srUpsertYouTubeTranscript) {
    return { ok: false, skipped: true, reason: 'missing_input' };
  }

  const ref = getReferenceById(referenceId);
  const fetchPlan = shouldFetchYouTubeTranscript(ref, targetUrl);
  if (!fetchPlan.ok) return { ok: false, skipped: true, reason: fetchPlan.reason, videoId: fetchPlan.videoId || '' };

  const res = await api.srUpsertYouTubeTranscript(referenceId, {
    url: targetUrl,
    video_id: fetchPlan.videoId || '',
    title: String(title || ''),
    source: 'youtube_timedtext',
  });
  if (res && res.ok && Array.isArray(res.references)) {
    state.references = res.references;
    if (String(state.activeSrId || '') === referenceId) {
      renderContextFiles();
      renderFilesPanel();
    }
  } else if (options.silentFailure !== true) {
    showPassiveNotification((res && res.message) || 'YouTube transcript ingestion failed.');
  }
  return res || { ok: false, skipped: false, reason: 'upsert_failed' };
}

async function buildCurrentBrowserTabPayload() {
  const page = await api.getPageContent().catch(() => null);
  const currentUrl = (page && page.success && page.data && page.data.url)
    ? String(page.data.url || '').trim()
    : String(state.browserCurrentUrl || '').trim();
  const currentTitle = (page && page.success && page.data && page.data.title)
    ? String(page.data.title || '').trim()
    : String(state.browserCurrentTitle || '').trim();
  if (!currentUrl) return null;
  return {
    url: currentUrl,
    title: currentTitle || currentUrl || 'Untitled',
  };
}

function parseUrlBarCommand(rawInput) {
  const raw = String(rawInput || '').trim();
  if (!raw.startsWith('/')) return null;
  if (/^\/add$/i.test(raw)) return { type: 'create_artifact' };
  if (/^\/create(?:\/.*)?$/i.test(raw)) return { type: 'create_artifact' };
  const renameMatch = raw.match(/^\/rename\/(.+)$/i);
  if (renameMatch) return { type: 'rename_artifact', name: String(renameMatch[1] || '').trim() };
  if (/^\/rm$/i.test(raw)) return { type: 'delete_artifact' };
  return null;
}

async function syncUrlBarForActiveSurface() {
  const input = e('browser-url-input');
  if (!input) return;
  const ref = getActiveReference();

  if (!ref) {
    input.placeholder = BROWSER_URL_PLACEHOLDER_WEB;
    input.value = String(state.browserCurrentUrl || '').trim();
    return;
  }

  if (state.activeSurface.kind === 'artifact') {
    input.value = '';
    input.placeholder = BROWSER_URL_PLACEHOLDER_ARTIFACT;
    setUncommittedActionCue(false);
    return;
  }

  if (state.activeSurface.kind === 'files') {
    input.value = '';
    input.placeholder = BROWSER_URL_PLACEHOLDER_FILES;
    setUncommittedActionCue(false);
    return;
  }

  if (state.activeSurface.kind === 'skills') {
    input.value = '';
    input.placeholder = BROWSER_URL_PLACEHOLDER_SKILLS;
    setUncommittedActionCue(false);
    return;
  }

  input.placeholder = BROWSER_URL_PLACEHOLDER_WEB;
  const activeWeb = getActiveWebTab(ref);
  const nextUrl = String(state.browserCurrentUrl || (activeWeb && activeWeb.url) || '').trim();
  input.value = nextUrl;
}

async function ensureActiveWebTabForNavigation(target) {
  const url = String(target || '').trim();
  if (!url) return null;

  let ref = getActiveReference();
  if (!ref) {
    const createRes = await api.srSaveInActive({
      active_sr_id: null,
      current_tab: { url, title: url },
      title: 'Untitled',
      no_change_policy: 'duplicate_tab',
    });
    if (!(createRes && createRes.ok && createRes.reference && createRes.reference.id)) return null;
    state.references = createRes.references || state.references;
    state.activeSrId = String(createRes.reference.id || '').trim();
    ref = getActiveReference();
  }

  if (!ref) return null;

  let webTab = getActiveWebTab(ref);
  if (!webTab) {
    const addRes = await api.srAddTab(state.activeSrId, { url: 'about:blank', title: 'New Web Tab' });
    if (!(addRes && addRes.ok)) return null;
    state.references = addRes.references || state.references;
    ref = getActiveReference();
    webTab = getActiveWebTab(ref);
  }
  if (!webTab) return null;

  const setRes = await api.srSetActiveTab(state.activeSrId, webTab.id);
  if (setRes && setRes.ok) {
    state.references = setRes.references || state.references;
  }
  state.activeSurface = makeActiveSurface('web', { tabId: String(webTab.id || '').trim() });
  rememberSurfaceForReference(state.activeSrId, state.activeSurface);
  return webTab;
}

async function convertActiveWebTabToArtifact() {
  if (!state.activeSrId) {
    showPassiveNotification('Select a reference first.');
    return true;
  }
  const ref = getActiveReference();
  if (!ref) {
    showPassiveNotification('Select a reference first.');
    return true;
  }

  const artifactTitle = getNextArtifactName(ref);
  const artifactId = `artifact_${Date.now()}_${Math.random().toString(36).slice(2, 9)}`;
  const upsertRes = await api.srUpsertArtifact(state.activeSrId, {
    id: artifactId,
    type: 'markdown',
    title: artifactTitle,
    content: '',
    mime_type: 'text/markdown',
    open_in_tab: true,
  });
  if (!upsertRes || !upsertRes.ok || !upsertRes.artifact) {
    showPassiveNotification((upsertRes && upsertRes.message) || 'Unable to create artifact.');
    return true;
  }

  state.references = upsertRes.references || state.references;
  state.activeSurface = makeActiveSurface('artifact', {
    artifactId: String((upsertRes.artifact && upsertRes.artifact.id) || artifactId),
  });
  rememberSurfaceForReference(state.activeSrId, state.activeSurface);
  renderReferences();
  renderWorkspaceTabs();
  renderMemoryRail();
  renderContextFiles();
  renderDiffPanel();
  await syncActiveSurface();
  await syncUrlBarForActiveSurface();
  void noteReferenceRankingInteraction('artifact_create', { srId: state.activeSrId });
  return true;
}

async function renameActiveArtifactFromCommand(nameInput) {
  if (!state.activeSrId) {
    showPassiveNotification('Use /rename/<name> on an active artifact tab.');
    return true;
  }
  if (state.activeSurface.kind !== 'artifact' || !state.activeSurface.artifactId) {
    showPassiveNotification('Use /rename/<name> on an active artifact tab.');
    return true;
  }
  let nextName = String(nameInput || '').trim();
  if (!nextName) {
    showPassiveNotification('Provide a name: /rename/<name>.');
    return true;
  }
  try {
    nextName = decodeURIComponent(nextName);
  } catch (_) {
    // keep raw value
  }

  const ref = getActiveReference();
  const artifactId = String(state.activeSurface.artifactId || '').trim();
  const currentArtifact = Array.isArray(ref && ref.artifacts)
    ? ref.artifacts.find((item) => String((item && item.id) || '') === artifactId)
    : null;
  if (!currentArtifact) {
    showPassiveNotification('Active artifact not found.');
    return true;
  }

  const editorInput = e('artifact-input');
  const liveContent = (state.activeSurface.kind === 'artifact' && editorInput)
    ? String(editorInput.value || '')
    : String((currentArtifact && currentArtifact.content) || '');
  const upsertRes = await api.srUpsertArtifact(state.activeSrId, {
    ...currentArtifact,
    id: artifactId,
    title: nextName,
    content: liveContent,
    open_in_tab: true,
  });
  if (!upsertRes || !upsertRes.ok) {
    showPassiveNotification((upsertRes && upsertRes.message) || 'Unable to rename artifact.');
    return true;
  }

  state.references = upsertRes.references || state.references;
  renderReferences();
  renderWorkspaceTabs();
  renderContextFiles();
  renderDiffPanel();
  await syncActiveSurface();
  await syncUrlBarForActiveSurface();
  void noteReferenceRankingInteraction('artifact_edit', { srId: state.activeSrId });
  return true;
}

async function removeActiveArtifactFromCommand() {
  if (!state.activeSrId) {
    showPassiveNotification('Use /rm on an active artifact tab.');
    return true;
  }
  if (state.activeSurface.kind !== 'artifact' || !state.activeSurface.artifactId) {
    showPassiveNotification('Use /rm on an active artifact tab.');
    return true;
  }

  const artifactId = String(state.activeSurface.artifactId || '').trim();
  const refBeforeDelete = getActiveReference();
  const artifactIdsBeforeDelete = Array.isArray(refBeforeDelete && refBeforeDelete.artifacts)
    ? refBeforeDelete.artifacts.map((artifact) => String((artifact && artifact.id) || '').trim()).filter(Boolean)
    : [];
  const deletedArtifactIndex = artifactIdsBeforeDelete.indexOf(artifactId);
  const deleteRes = await api.srDeleteArtifact(state.activeSrId, artifactId);
  if (!deleteRes || !deleteRes.ok) {
    showPassiveNotification((deleteRes && deleteRes.message) || 'Unable to delete artifact.');
    return true;
  }

  state.references = deleteRes.references || state.references;
  const refAfterDelete = getActiveReference();
  const remainingArtifactIds = Array.isArray(refAfterDelete && refAfterDelete.artifacts)
    ? refAfterDelete.artifacts.map((artifact) => String((artifact && artifact.id) || '').trim()).filter(Boolean)
    : [];
  const fallbackIndex = deletedArtifactIndex >= 0
    ? Math.min(deletedArtifactIndex, Math.max(0, remainingArtifactIds.length - 1))
    : 0;
  const fallbackArtifactId = String(remainingArtifactIds[fallbackIndex] || '').trim();
  if (fallbackArtifactId) {
    state.activeSurface = makeActiveSurface('artifact', { artifactId: fallbackArtifactId });
  } else {
    const fallbackWebTab = getActiveWebTab(refAfterDelete);
    if (fallbackWebTab && fallbackWebTab.id) {
      const setRes = await api.srSetActiveTab(state.activeSrId, fallbackWebTab.id);
      if (setRes && setRes.ok) state.references = setRes.references || state.references;
      state.activeSurface = makeActiveSurface('web', { tabId: String(fallbackWebTab.id || '').trim() });
    } else {
      state.activeSurface = makeActiveSurface('web');
    }
  }
  rememberSurfaceForReference(state.activeSrId, state.activeSurface);

  renderReferences();
  renderWorkspaceTabs();
  renderContextFiles();
  renderDiffPanel();
  await syncActiveSurface();
  await syncUrlBarForActiveSurface();
  void noteReferenceRankingInteraction('artifact_delete', { srId: state.activeSrId });
  return true;
}

async function navigateFromActiveVizContext(rawInput) {
  const target = String(rawInput || '').trim();
  if (!target) return false;

  const webTab = await ensureActiveWebTabForNavigation(target);
  if (!webTab || !webTab.id) {
    await navigateActiveRuntimeTab(target);
    return true;
  }

  renderWorkspaceTabs();
  await syncActiveSurface();
  await navigateActiveRuntimeTab(target);
  state.browserCurrentUrl = target;
  await syncUrlBarForActiveSurface();
  await refreshUncommittedActionCue();
  return true;
}

async function maybeHandleUrlBarCommand(rawInput) {
  const parsed = parseUrlBarCommand(rawInput);
  if (!parsed) return false;
  if (parsed.type === 'create_artifact') return convertActiveWebTabToArtifact();
  if (parsed.type === 'rename_artifact') return renameActiveArtifactFromCommand(parsed.name);
  if (parsed.type === 'delete_artifact') return removeActiveArtifactFromCommand();
  return false;
}

async function handleBrowserUrlInputSubmit(rawInput) {
  const inputValue = String(rawInput || '').trim();
  if (!inputValue) return false;

  const handledCommand = await maybeHandleUrlBarCommand(inputValue);
  if (handledCommand) return true;

  const webTab = await ensureActiveWebTabForNavigation(inputValue);
  if (!webTab || !webTab.id) {
    await navigateActiveRuntimeTab(inputValue);
    state.browserCurrentUrl = inputValue;
    await syncUrlBarForActiveSurface();
    await refreshUncommittedActionCue();
    return true;
  }

  renderWorkspaceTabs();
  await syncActiveSurface();
  await navigateActiveRuntimeTab(inputValue);
  state.browserCurrentUrl = inputValue;
  await syncUrlBarForActiveSurface();
  await refreshUncommittedActionCue();
  return true;
}

function buildLuminoScopeRefs(activeSrId) {
  const refs = Array.isArray(state.references) ? state.references : [];
  const idMap = new Map();
  refs.forEach((ref) => {
    const id = String((ref && ref.id) || '').trim();
    if (id) idMap.set(id, ref);
  });

  const active = idMap.get(String(activeSrId || '').trim());
  if (!active) return [];

  const allowed = new Set([String(active.id)]);
  const parentId = String((active.parent_id || '')).trim();
  if (parentId && idMap.has(parentId)) allowed.add(parentId);

  const children = Array.isArray(active.children) ? active.children : [];
  children.forEach((childId) => {
    const id = String(childId || '').trim();
    if (id && idMap.has(id)) allowed.add(id);
  });

  return refs.filter((ref) => allowed.has(String((ref && ref.id) || '').trim()));
}

async function activateReferenceSurface(srId, surface = null) {
  const next = String(srId || '').trim();
  if (!next) return false;
  const activationSeq = Number(state.referenceActivationSeq || 0) + 1;
  state.referenceActivationSeq = activationSeq;
  if (isMemoryReplayActive() && String(state.activeSrId || '') !== next) {
    clearMemoryReplayTimer();
    state.memoryReplay.active = false;
    state.memoryReplay.playing = false;
    state.memoryReplay.checkpoints = [];
    state.memoryReplay.filtered = [];
    state.memoryReplay.index = -1;
    state.memoryReplay.activeCheckpointId = '';
    state.memoryReplay.virtualReference = null;
    renderMemoryRail();
  }
  const previous = String(state.activeSrId || '').trim();
  if (previous) {
    rememberSurfaceForReference(previous, state.activeSurface);
  }
  state.activeSrId = next;
  state.audibleByRef.clear();
  if (surface && typeof surface === 'object') {
    state.activeSurface = makeActiveSurface(String(surface.kind || 'web'), surface);
  } else {
    state.activeSurface = restoreSurfaceForReference(getReferenceById(next));
  }
  rememberSurfaceForReference(next, state.activeSurface);
  renderReferences();
  renderWorkspaceTabs();
  renderContextFiles();
  renderDiffPanel();
  await loadChatThread();
  if (activationSeq !== Number(state.referenceActivationSeq || 0) || String(state.activeSrId || '') !== next) return false;
  loadProgramEditorForActiveReference();
  await syncActiveSurface();
  if (activationSeq !== Number(state.referenceActivationSeq || 0) || String(state.activeSrId || '') !== next) return false;
  await refreshUncommittedActionCue();
  void noteReferenceRankingInteraction('activate_reference', { srId: next });
  return true;
}

function setActiveReference(srId) {
  void activateReferenceSurface(srId);
}

function updateActiveReferenceMeta() {
  const meta = e('active-ref-meta');
  const ref = getActiveReference();
  if (!meta) return;
  if (!ref) {
    meta.textContent = 'No active reference';
    return;
  }
  const tabCount = Array.isArray(ref.tabs) ? ref.tabs.length : 0;
  const artifactCount = Array.isArray(ref.artifacts) ? ref.artifacts.length : 0;
  const contextCount = Array.isArray(ref.context_files) ? ref.context_files.length : 0;
  const mailCount = Array.isArray(ref.mail_threads) ? ref.mail_threads.length : 0;
  meta.textContent = `${tabCount} tab(s) · ${artifactCount} artifact(s) · ${contextCount} context file(s) · ${mailCount} mail thread(s)`;
}

async function commitInlineRename(srId, nextTitle) {
  const title = String(nextTitle || '').trim();
  if (!srId || !title) {
    state.referenceInlineRename = { srId: null, draft: '' };
    renderReferences();
    return;
  }
  const res = await api.srRename(srId, title);
  state.referenceInlineRename = { srId: null, draft: '' };
  if (!res || !res.ok) {
    window.alert((res && res.message) || 'Unable to rename reference.');
    renderReferences();
    return;
  }
  state.references = res.references || await api.srList();
  renderReferences();
  renderWorkspaceTabs();
}

function renderReferences() {
  const list = e('references-list');
  if (!list) return;

  const query = normalizeReferenceSearchQuery(state.searchQuery);
  state.searchQuery = query;
  updateReferenceSearchControls();
  updateReferenceColorFilterUi();
  const autoOnlyToggle = e('reference-auto-only-toggle');
  if (autoOnlyToggle) autoOnlyToggle.checked = !!state.referenceAutoOnly;

  const allRefs = Array.isArray(state.references) ? state.references : [];
  const colorFilterMode = String((state.referenceColorFilter && state.referenceColorFilter.mode) || 'all');
  const colorFilterSelected = (state.referenceColorFilter && state.referenceColorFilter.selected instanceof Set)
    ? state.referenceColorFilter.selected
    : new Set();
  const colorMatches = (ref) => {
    if (colorFilterMode !== 'colors') return true;
    const tag = sanitizeReferenceColorTag(ref && ref.color_tag);
    return colorFilterSelected.has(tag);
  };
  const refs = allRefs.filter((ref) => {
    if (!colorMatches(ref)) return false;
    if (!state.referenceAutoOnly) return true;
    return String((ref && ref.agent_meta && ref.agent_meta.created_by) || '').trim().toLowerCase() === 'lumino_b';
  });
  const idMap = {};
  const childrenMap = {};
  refs.forEach((ref) => {
    const id = String((ref && ref.id) || '').trim();
    if (!id) return;
    idMap[id] = ref;
    childrenMap[id] = [];
  });
  refs.forEach((ref) => {
    const id = String((ref && ref.id) || '').trim();
    const parentId = String((ref && ref.parent_id) || '').trim();
    if (!id || !parentId) return;
    if (childrenMap[parentId]) childrenMap[parentId].push(ref);
  });

  Object.keys(childrenMap).forEach((parentId) => {
    childrenMap[parentId].sort(compareReferencesByRankOrAlphabetical);
  });

  const roots = refs
    .filter((ref) => {
      const parentId = String((ref && ref.parent_id) || '').trim();
      return !parentId || !idMap[parentId];
    })
    .sort(compareReferencesByRankOrAlphabetical);
  const pinnedRoots = roots.filter((ref) => !!(ref && ref.pinned_root));
  const unpinnedRoots = roots.filter((ref) => !(ref && ref.pinned_root));

  let matchedIds = new Set();
  if (query) {
    matchedIds = new Set(
      refs
        .filter((ref) => {
          const blob = [
            ref.title,
            ref.intent,
            ...(Array.isArray(ref.tags) ? ref.tags : []),
            ...(Array.isArray(ref.artifacts) ? ref.artifacts.map((artifact) => `${artifact.title || ''} ${artifact.content || ''}`) : []),
          ].join(' ').toLowerCase();
          return blob.includes(query.toLowerCase());
        })
        .map((ref) => String((ref && ref.id) || '').trim())
        .filter(Boolean)
    );
  }
  state.referenceSearchMatchedIds = matchedIds;

  const visibleIds = new Set();
  if (query) {
    matchedIds.forEach((matchId) => {
      let cursor = matchId;
      while (cursor && idMap[cursor] && !visibleIds.has(cursor)) {
        visibleIds.add(cursor);
        const parentId = String((idMap[cursor] && idMap[cursor].parent_id) || '').trim();
        if (!parentId || !idMap[parentId]) break;
        cursor = parentId;
      }
    });
  }

  if (!roots.length) {
    state.visibleReferenceOrder = [];
    if (!allRefs.length) {
      list.innerHTML = '<div class="references-empty">No saved references yet.</div>';
    } else {
      list.innerHTML = '<div class="references-empty">No references match this filter.</div>';
    }
    updateActiveReferenceMeta();
    return;
  }

  const renderNode = (ref, level) => {
    const srId = String((ref && ref.id) || '').trim();
    if (!srId) return '';
    if (query && !visibleIds.has(srId)) return '';
    const children = Array.isArray(childrenMap[srId]) ? childrenMap[srId] : [];
    const renderedChildren = children.map((child) => renderNode(child, level + 1)).filter(Boolean);
    const hasChildren = renderedChildren.length > 0;
    const collapsed = query ? false : !!state.referenceCollapsed[srId];
    const tabCount = Array.isArray(ref.tabs) ? ref.tabs.length : 0;
    const isCandidate = !!(ref && ref.is_public_candidate);
    const subtitleParts = [ref.relation_type || 'root', `${tabCount} tab(s)`];
    const isActive = srId === String(state.activeSrId || '');
    // A reference is a root node if it has no parent OR its parent is not in the visible map
    // (handles orphaned roots whose parent was deleted)
    const refParentId = String((ref && ref.parent_id) || '').trim();
    const isRoot = !refParentId || !idMap[refParentId];
    const isPinnable = isRoot && !isCandidate;
    const isSearchHit = query && matchedIds.has(srId);
    const isInlineRename = state.referenceInlineRename.srId === srId;
    const isAudible = !!state.audibleByRef.get(srId);
    const isExpanded = String(state.referenceExpandedSrId || '') === srId;
    const colorTag = sanitizeReferenceColorTag(ref && ref.color_tag);
    const isColorPickerOpen = String((state.referenceColorPicker && state.referenceColorPicker.openSrId) || '') === srId;
    const agentMeta = (ref && ref.agent_meta && typeof ref.agent_meta === 'object') ? ref.agent_meta : {};
    const isAuto = String(agentMeta.created_by || '').trim().toLowerCase() === 'lumino_b';
    const autoStatusRaw = String(agentMeta.status || '').trim().toLowerCase();
    const autoStatus = ['pending', 'active', 'failed'].includes(autoStatusRaw) ? autoStatusRaw : 'active';
    if (isCandidate) subtitleParts.push('public candidate');
    const subtitle = subtitleParts.join(' · ');
    const actionButtons = `
      ${isPinnable ? `<button data-pin="${escapeHtml(srId)}">${ref.pinned_root ? 'Unpin' : 'Pin'}</button>` : ''}
      ${isCandidate
        ? `
          <button data-commit-root="${escapeHtml(srId)}">Commit Root</button>
          <button data-commit-fork="${escapeHtml(srId)}">Commit Fork</button>
        `
        : `
          <button data-fork="${escapeHtml(srId)}">Fork</button>
          <button data-rename="${escapeHtml(srId)}">Rename</button>
          <button data-publish-snapshot="${escapeHtml(srId)}">Publish Snapshot</button>
          <button data-share-privately="${escapeHtml(srId)}">Share Privately</button>
        `
      }
      <button class="danger" data-remove="${escapeHtml(srId)}">Remove</button>
    `;
    const titleText = String(ref.title || ref.intent || '').trim() || (isRoot ? 'Untitled root' : 'Untitled branch');
    const titleMarkup = isInlineRename
      ? `<input type="text" class="reference-inline-rename-input" data-action="rename-input" data-sr-id="${escapeHtml(srId)}" maxlength="120" value="${escapeHtml(state.referenceInlineRename.draft || ref.title || 'Untitled')}" />`
      : `
        <div class="reference-title">
          <button type="button" class="reference-color-dot" data-action="toggle-color-picker" data-sr-id="${escapeHtml(srId)}" data-color-tag="${escapeHtml(colorTag)}" title="Set reference color"></button>
          <span class="reference-title-text" title="${escapeHtml(titleText)}">${escapeHtml(titleText)}</span>
          ${isAuto ? `<span class="reference-inline-flag ${escapeHtml(autoStatus)}">auto</span>` : ''}
          ${isAudible ? '<span class="reference-inline-flag">audio</span>' : ''}
        </div>
        ${isColorPickerOpen
          ? `
            <div class="reference-color-picker" data-color-picker="${escapeHtml(srId)}">
              <button type="button" data-action="set-color-tag" data-sr-id="${escapeHtml(srId)}" data-color-tag="">Default</button>
              <button type="button" class="swatch" data-action="set-color-tag" data-sr-id="${escapeHtml(srId)}" data-color-tag="c1" title="Color 1"></button>
              <button type="button" class="swatch" data-action="set-color-tag" data-sr-id="${escapeHtml(srId)}" data-color-tag="c2" title="Color 2"></button>
              <button type="button" class="swatch" data-action="set-color-tag" data-sr-id="${escapeHtml(srId)}" data-color-tag="c3" title="Color 3"></button>
              <button type="button" class="swatch" data-action="set-color-tag" data-sr-id="${escapeHtml(srId)}" data-color-tag="c4" title="Color 4"></button>
              <button type="button" class="swatch" data-action="set-color-tag" data-sr-id="${escapeHtml(srId)}" data-color-tag="c5" title="Color 5"></button>
            </div>
          `
          : ''
        }
      `;

    return `
      <div class="reference-tree-node" data-sr-id="${escapeHtml(srId)}" data-level="${level}">
        <div class="reference-item-row" data-level="${level}">
          <button type="button" class="reference-tree-caret ${hasChildren ? '' : 'placeholder'}" data-action="toggle-collapse" data-sr-id="${escapeHtml(srId)}">${hasChildren ? (collapsed ? '▸' : '▾') : ''}</button>
          <div class="reference-item ${isActive ? 'active' : ''} ${isExpanded ? 'expanded' : ''} ${isSearchHit ? 'search-hit' : ''} ${colorTag ? `color-${escapeHtml(colorTag)}` : ''}" data-ref-id="${escapeHtml(srId)}">
            ${titleMarkup}
            <div class="reference-sub">${escapeHtml(subtitle)}</div>
            <div class="reference-actions">${actionButtons}</div>
          </div>
        </div>
        ${hasChildren ? `<div class="reference-children ${collapsed ? 'collapsed' : ''}" data-parent-id="${escapeHtml(srId)}">${renderedChildren.join('')}</div>` : ''}
      </div>
    `;
  };

  const renderSection = (title, sectionRoots) => {
    if (!Array.isArray(sectionRoots) || sectionRoots.length === 0) return '';
    const content = sectionRoots.map((root) => renderNode(root, 0)).filter(Boolean).join('');
    if (!content) return '';
    return `
      <section class="reference-section" data-reference-section="${escapeHtml(title.toLowerCase())}">
        <div class="reference-section-label">${escapeHtml(title)}</div>
        <div class="reference-section-body">${content}</div>
      </section>
    `;
  };
  const treeMarkup = [
    renderSection('Pinned', pinnedRoots),
    renderSection('Unpinned', unpinnedRoots),
  ].filter(Boolean).join('');
  list.innerHTML = treeMarkup || '<div class="references-empty">No references match this query.</div>';
  state.visibleReferenceOrder = Array.from(list.querySelectorAll('.reference-tree-node[data-sr-id]'))
    .filter((node) => !!(node && (node.offsetParent || node.getClientRects().length)))
    .map((node) => String(node.getAttribute('data-sr-id') || '').trim())
    .filter(Boolean);

  list.querySelectorAll('button[data-action="toggle-collapse"]').forEach((button) => {
    button.addEventListener('click', (event) => {
      event.preventDefault();
      event.stopPropagation();
      const srId = String(button.getAttribute('data-sr-id') || '').trim();
      if (!srId) return;
      state.referenceCollapsed[srId] = !state.referenceCollapsed[srId];
      persistReferenceCollapsedState();
      renderReferences();
    });
  });

  let pendingReferenceClick = null;
  list.querySelectorAll('.reference-item').forEach((node) => {
    node.addEventListener('click', async (event) => {
      if (event.target && event.target.closest('button')) return;
      if (event.target && event.target.closest('input[data-action="rename-input"]')) return;
      const srId = String(node.getAttribute('data-ref-id') || '').trim();
      if (!srId) return;
      if (pendingReferenceClick) {
        window.clearTimeout(pendingReferenceClick.timerId);
        pendingReferenceClick = null;
      }
      pendingReferenceClick = {
        srId,
        timerId: window.setTimeout(() => {
          toggleReferenceInlineExpansion(srId);
          pendingReferenceClick = null;
        }, 220),
      };
    });
    node.addEventListener('dblclick', async (event) => {
      if (event.target && event.target.closest('button')) return;
      if (event.target && event.target.closest('input[data-action="rename-input"]')) return;
      const srId = String(node.getAttribute('data-ref-id') || '').trim();
      if (!srId) return;
      if (pendingReferenceClick && pendingReferenceClick.srId === srId) {
        window.clearTimeout(pendingReferenceClick.timerId);
        pendingReferenceClick = null;
      }
      state.referenceExpandedSrId = srId;
      setActiveReference(srId);
    });
  });

  list.querySelectorAll('input[data-action="rename-input"]').forEach((input) => {
    input.addEventListener('input', () => {
      state.referenceInlineRename.draft = String(input.value || '');
    });
    input.addEventListener('click', (event) => event.stopPropagation());
    input.addEventListener('keydown', async (event) => {
      const srId = String(input.getAttribute('data-sr-id') || '').trim();
      if (!srId) return;
      if (event.key === 'Enter') {
        event.preventDefault();
        await commitInlineRename(srId, input.value);
      } else if (event.key === 'Escape') {
        event.preventDefault();
        state.referenceInlineRename = { srId: null, draft: '' };
        renderReferences();
      }
    });
    input.addEventListener('blur', () => {
      const srId = String(input.getAttribute('data-sr-id') || '').trim();
      commitInlineRename(srId, input.value);
    });
  });

  list.querySelectorAll('button[data-pin]').forEach((button) => {
    button.addEventListener('click', async (event) => {
      event.preventDefault();
      event.stopPropagation();
      const srId = String(button.getAttribute('data-pin') || '').trim();
      const ref = getReferenceById(srId);
      if (!srId || !ref) return;
      const res = await api.srSetPinnedRoot(srId, !ref.pinned_root);
      if (!res || !res.ok) {
        window.alert((res && res.message) || 'Unable to update pin status.');
        return;
      }
      state.references = res.references || await api.srList();
      renderReferences();
    });
  });

  list.querySelectorAll('button[data-action="toggle-color-picker"]').forEach((button) => {
    button.addEventListener('click', (event) => {
      event.preventDefault();
      event.stopPropagation();
      const srId = String(button.getAttribute('data-sr-id') || '').trim();
      if (!srId) return;
      const openSrId = String((state.referenceColorPicker && state.referenceColorPicker.openSrId) || '').trim();
      state.referenceColorPicker.openSrId = openSrId === srId ? null : srId;
      renderReferences();
    });
  });

  list.querySelectorAll('button[data-action="set-color-tag"]').forEach((button) => {
    button.addEventListener('click', async (event) => {
      event.preventDefault();
      event.stopPropagation();
      const srId = String(button.getAttribute('data-sr-id') || '').trim();
      const colorTag = sanitizeReferenceColorTag(button.getAttribute('data-color-tag'));
      if (!srId || !api.srSetColorTag) return;
      const res = await api.srSetColorTag(srId, colorTag);
      if (!res || !res.ok) {
        window.alert((res && res.message) || 'Unable to update reference color.');
        return;
      }
      state.referenceColorPicker.openSrId = null;
      state.references = res.references || await api.srList();
      renderReferences();
    });
  });

  list.querySelectorAll('button[data-fork]').forEach((button) => {
    button.addEventListener('click', async (event) => {
      event.preventDefault();
      event.stopPropagation();
      const srId = String(button.getAttribute('data-fork') || '').trim();
      if (!srId) return;
      const res = await api.srFork(srId);
      if (!res || !res.ok || !res.reference) {
        window.alert((res && res.message) || 'Unable to fork reference.');
        return;
      }
      state.references = res.references || await api.srList();
      setActiveReference(res.reference.id);
    });
  });

  list.querySelectorAll('button[data-rename]').forEach((button) => {
    button.addEventListener('click', (event) => {
      event.preventDefault();
      event.stopPropagation();
      const srId = String(button.getAttribute('data-rename') || '').trim();
      const ref = getReferenceById(srId);
      if (!srId || !ref) return;
      state.referenceInlineRename = { srId, draft: String(ref.title || '') };
      renderReferences();
      const node = list.querySelector(`input[data-action="rename-input"][data-sr-id="${srId}"]`);
      if (node) {
        node.focus();
        node.select();
      }
    });
  });

  list.querySelectorAll('button[data-remove]').forEach((button) => {
    button.addEventListener('click', async (event) => {
      event.preventDefault();
      event.stopPropagation();
      const srId = String(button.getAttribute('data-remove') || '').trim();
      if (!srId) return;
      const ok = window.confirm('Remove this reference and keep its descendants with succession?');
      if (!ok) return;
      const res = await api.srDeleteWithSuccession(srId);
      if (!res || !res.ok) {
        window.alert((res && res.message) || 'Unable to remove reference.');
        return;
      }
      const nextIds = new Set(
        (Array.isArray(res.references) ? res.references : [])
          .map((item) => String((item && item.id) || '').trim())
          .filter(Boolean)
      );
      Array.from(state.activeArtifactByRef.keys()).forEach((key) => {
        if (!nextIds.has(String(key || ''))) state.activeArtifactByRef.delete(key);
      });
      Array.from(state.activeFilesByRef.keys()).forEach((key) => {
        if (!nextIds.has(String(key || ''))) state.activeFilesByRef.delete(key);
      });
      state.references = res.references || await api.srList();
      await pruneWorkspaceWebTabMappings(state.references);
      if (!state.references.find((item) => String((item && item.id) || '') === String(state.activeSrId || ''))) {
        state.activeSrId = state.references[0] ? state.references[0].id : null;
      }
      renderReferences();
      renderWorkspaceTabs();
      renderContextFiles();
      renderDiffPanel();
      await syncActiveSurface();
    });
  });

  list.querySelectorAll('button[data-publish-snapshot]').forEach((button) => {
    button.addEventListener('click', async (event) => {
      event.stopPropagation();
      const srId = String(button.getAttribute('data-publish-snapshot') || '').trim();
      if (!srId) return;
      await openPublishSnapshotModal(srId);
    });
  });

  list.querySelectorAll('button[data-share-privately]').forEach((button) => {
    button.addEventListener('click', async (event) => {
      event.stopPropagation();
      const srId = String(button.getAttribute('data-share-privately') || '').trim();
      if (!srId) return;
      await openShareReferenceModal(srId);
    });
  });

  list.querySelectorAll('button[data-commit-root]').forEach((button) => {
    button.addEventListener('click', async (event) => {
      event.stopPropagation();
      const srId = String(button.getAttribute('data-commit-root') || '').trim();
      if (!srId) return;
      const res = await api.srCommitPublicCandidate(srId, 'root', null);
      if (!res || !res.ok) {
        window.alert((res && res.message) || 'Unable to commit public reference as root.');
        return;
      }
      state.references = res.references || await api.srList();
      setActiveReference(res.reference && res.reference.id ? res.reference.id : state.activeSrId);
    });
  });

  list.querySelectorAll('button[data-commit-fork]').forEach((button) => {
    button.addEventListener('click', async (event) => {
      event.stopPropagation();
      const srId = String(button.getAttribute('data-commit-fork') || '').trim();
      const targetSrId = String(state.activeSrId || '').trim();
      if (!srId || !targetSrId) return;
      const res = await api.srCommitPublicCandidate(srId, 'fork', targetSrId);
      if (!res || !res.ok) {
        window.alert((res && res.message) || 'Unable to commit public reference as fork.');
        return;
      }
      state.references = res.references || await api.srList();
      setActiveReference(res.reference && res.reference.id ? res.reference.id : state.activeSrId);
    });
  });

  updateActiveReferenceMeta();
}

async function publishSnapshotFromModal() {
  const srId = String(state.publishSnapshotTargetId || '').trim();
  if (!srId || !api.srPublishSnapshot) {
    closePublishSnapshotModal();
    return;
  }
  const res = await api.srPublishSnapshot(srId);
  if (!res || !res.ok) {
    setStatusText('publish-snapshot-meta', (res && res.message) ? res.message : 'Unable to publish snapshot.');
    return;
  }
  closePublishSnapshotModal({ skipSurfaceSync: true });
  showPassiveNotification('Snapshot published');
  if (res.references) {
    state.references = res.references;
    renderReferences();
    renderWorkspaceTabs();
  }
  await openHyperwebPage();
  await setHyperwebSurfaceTab('refs');
  const snapshotId = String((res.snapshot && res.snapshot.snapshot_id) || '').trim();
  if (snapshotId) {
    setStatusText('hyperweb-ref-status', `Snapshot published: ${snapshotId}`);
  }
}

async function sendPrivateShareFromModal() {
  const srId = String(state.shareReferenceTargetId || '').trim();
  const recipients = Array.from(state.shareRecipientSelection.values()).filter(Boolean);
  if (!srId || !api.hyperwebShareReference) {
    closeShareReferenceModal();
    return;
  }
  if (!recipients.length) {
    setStatusText('shares-status-line', 'Select at least one trusted peer.');
    return;
  }
  const res = await api.hyperwebShareReference(srId, recipients);
  if (!res || !res.ok) {
    setStatusText('shares-status-line', (res && res.message) ? res.message : 'Unable to send private share.');
    return;
  }
  const inviteUrl = String((res && res.share && res.share.invite_url) || '').trim();
  const inviteMessage = String((res && res.share && res.share.invite_message) || '').trim();
  const inviteDm = (res && res.share && res.share.invite_dm && typeof res.share.invite_dm === 'object')
    ? res.share.invite_dm
    : null;
  const attemptedDm = Number((inviteDm && inviteDm.attempted) || 0);
  const deliveredDm = Number((inviteDm && inviteDm.delivered) || 0);
  const dmSuffix = attemptedDm > 0 ? ` Invite delivered ${deliveredDm}/${attemptedDm}.` : '';
  if (inviteUrl) {
    const clipboardText = inviteMessage || inviteUrl;
    try {
      if (navigator && navigator.clipboard && navigator.clipboard.writeText) {
        await navigator.clipboard.writeText(clipboardText);
      } else {
        window.prompt('Copy invite message:', clipboardText);
      }
      setStatusText('shares-status-line', `Private share sent. Invite message copied to clipboard.${dmSuffix}`);
    } catch (_) {
      window.prompt('Copy invite message:', clipboardText);
    }
  }
  closeShareReferenceModal({ skipSurfaceSync: true });
  await openPrivateSharesPage();
  setSharesTab('outgoing');
  if (!inviteUrl) setStatusText('shares-status-line', `Private share sent.${dmSuffix}`);
}

function renderWorkspaceTabs() {
  const holder = e('workspace-tabs');
  repairActiveReferenceSelection();
  const ref = getActiveReference();
  const replayMode = isMemoryReplayActive();
  if (!holder) return;
  if (!ref) {
    holder.innerHTML = '';
    return;
  }

  const tabs = Array.isArray(ref.tabs) ? ref.tabs : [];
  const webTabs = tabs.filter((tab) => String((tab && tab.tab_kind) || 'web') === 'web');
  const filesTabs = tabs.filter((tab) => String((tab && tab.tab_kind) || '').trim().toLowerCase() === 'files');
  const skillsTabs = tabs.filter((tab) => String((tab && tab.tab_kind) || '').trim().toLowerCase() === 'skills');
  const mailTabs = tabs.filter((tab) => String((tab && tab.tab_kind) || '').trim().toLowerCase() === 'mail');
  const artifacts = Array.isArray(ref.artifacts) ? ref.artifacts : [];

  const activeWeb = getActiveWebTab(ref);
  const activeWebId = activeWeb ? String(activeWeb.id) : '';

  holder.innerHTML = `
    <button id="workspace-add-tab-btn" class="workspace-add-btn" type="button" title="Add tab">+</button>
    <div id="workspace-add-menu" class="workspace-add-menu hidden">
      <button type="button" data-workspace-add-action="web">+ web</button>
      <button type="button" data-workspace-add-action="md">+ md</button>
      <button type="button" data-workspace-add-action="folder">+ folder</button>
      <button type="button" data-workspace-add-action="mail">+ mail</button>
      <button type="button" data-workspace-add-action="skills">+ skills</button>
    </div>
    ${webTabs.map((tab) => `
      <div class="workspace-tab ${(state.activeSurface.kind === 'web' && String(state.activeSurface.tabId || activeWebId) === String(tab.id || '')) ? 'active' : ''}" data-kind="web" data-tab-id="${escapeHtml(tab.id)}">
        <span class="workspace-tab-label-wrap"><span class="workspace-tab-label" title="${escapeHtml(tab.title || tab.url || 'Web Tab')}">${escapeHtml(tab.title || tab.url || 'Web Tab')}</span></span>
        <button data-close-web="${escapeHtml(tab.id)}" title="Close web tab">×</button>
      </div>
    `).join('')}
    ${filesTabs.map((tab) => `
      <div class="workspace-tab ${(state.activeSurface.kind === 'files' && String(state.activeSurface.filesTabId || '') === String(tab.id || '')) ? 'active' : ''}" data-kind="files" data-tab-id="${escapeHtml(tab.id)}">
        <span class="workspace-tab-label-wrap"><span class="workspace-tab-label" title="${escapeHtml(tab.title || 'Files')}">${escapeHtml(tab.title || 'Files')}</span></span>
        <button data-close-files="${escapeHtml(tab.id)}" title="Close files tab">×</button>
      </div>
    `).join('')}
    ${skillsTabs.map((tab) => `
      <div class="workspace-tab ${(state.activeSurface.kind === 'skills' && String(state.activeSurface.skillsTabId || '') === String(tab.id || '')) ? 'active' : ''}" data-kind="skills" data-tab-id="${escapeHtml(tab.id)}">
        <span class="workspace-tab-label-wrap"><span class="workspace-tab-label" title="${escapeHtml(tab.title || 'Skills')}">${escapeHtml(tab.title || 'Skills')}</span></span>
        <button data-close-skills="${escapeHtml(tab.id)}" title="Close skills tab">×</button>
      </div>
    `).join('')}
    ${mailTabs.map((tab) => `
      <div class="workspace-tab ${(state.activeSurface.kind === 'mail' && String(state.activeSurface.mailTabId || '') === String(tab.id || '')) ? 'active' : ''}" data-kind="mail" data-tab-id="${escapeHtml(tab.id)}">
        <span class="workspace-tab-label-wrap"><span class="workspace-tab-label" title="${escapeHtml(tab.title || 'Mail')}">${escapeHtml(tab.title || 'Mail')}</span></span>
        <button data-close-mail="${escapeHtml(tab.id)}" title="Close mail tab">×</button>
      </div>
    `).join('')}
    ${artifacts.map((artifact) => `
      <div class="workspace-tab ${(state.activeSurface.kind === 'artifact' && String(state.activeSurface.artifactId || '') === String(artifact.id || '')) ? 'active' : ''}" data-kind="artifact" data-artifact-id="${escapeHtml(artifact.id)}">
        <span class="workspace-tab-label-wrap"><span class="workspace-tab-label" title="${escapeHtml(artifact.title || 'Artifact')}">${escapeHtml(artifact.title || 'Artifact')}</span></span>
        <button data-delete-artifact="${escapeHtml(artifact.id)}" title="Delete artifact">×</button>
      </div>
    `).join('')}
  `;

  const addBtn = e('workspace-add-tab-btn');
  const addMenu = e('workspace-add-menu');
  if (addBtn && addMenu) {
    addBtn.disabled = replayMode;
    const hideAddMenu = async () => {
      const wasOpen = !addMenu.classList.contains('hidden');
      addMenu.classList.add('hidden');
      addMenu.style.visibility = '';
      addMenu.style.pointerEvents = '';
      addMenu.style.left = '';
      addMenu.style.top = '';
      if (wasOpen) await syncActiveSurface();
    };
    addBtn.addEventListener('click', async (event) => {
      if (replayMode) {
        event.preventDefault();
        showPassiveNotification('Memory replay is read-only.');
        return;
      }
      event.preventDefault();
      event.stopPropagation();
      const willOpen = addMenu.classList.contains('hidden');
      if (!willOpen) {
        await hideAddMenu();
        return;
      }
      addMenu.classList.remove('hidden');
      addMenu.style.visibility = '';
      addMenu.style.pointerEvents = '';
      try {
        await api.hide();
      } catch (_) {
        // noop
      }
    });
    addMenu.querySelectorAll('button[data-workspace-add-action]').forEach((button) => {
      button.addEventListener('click', async (event) => {
        if (replayMode) {
          event.preventDefault();
          event.stopPropagation();
          showPassiveNotification('Memory replay is read-only.');
          return;
        }
        event.preventDefault();
        event.stopPropagation();
        const action = String(button.getAttribute('data-workspace-add-action') || '').trim().toLowerCase();
        await hideAddMenu();
        if (action === 'web') {
          const homeUrl = getDefaultSearchHomeUrl();
          const homeTitle = getDefaultSearchHomeTitle();
          const currentUrl = homeUrl;
          const currentTitle = homeTitle;
          if (!state.activeSrId) {
            const createRes = await api.srSaveInActive({
              active_sr_id: null,
              current_tab: { url: currentUrl, title: currentTitle },
              title: 'Untitled',
              no_change_policy: 'duplicate_tab',
            });
            if (createRes && createRes.ok && createRes.reference) {
              state.references = createRes.references || state.references;
              setActiveReference(createRes.reference.id);
            }
            return;
          }
          const res = await api.srAddTab(state.activeSrId, { url: currentUrl, title: currentTitle });
          if (res && res.ok) {
            state.references = res.references || state.references;
            state.activeSurface = makeActiveSurface('web');
            rememberSurfaceForReference(state.activeSrId, state.activeSurface);
            renderReferences();
            renderWorkspaceTabs();
            await syncActiveSurface();
            void noteReferenceRankingInteraction('add_tab', {
              srId: state.activeSrId,
              browserTitle: currentTitle,
              browserUrl: currentUrl,
            });
          }
          return;
        }
        if (action === 'md') {
          await convertActiveWebTabToArtifact();
          return;
        }
        if (action === 'folder') {
          await mountFolderToActiveReference();
          return;
        }
        if (action === 'mail') {
          if (!state.activeSrId) {
            showPassiveNotification('Select a reference first.');
            return;
          }
          const res = await api.srAddTab(state.activeSrId, { tab_kind: 'mail', title: 'Mail' });
          if (res && res.ok) {
            state.references = res.references || await api.srList();
            const tabId = String(((res && res.tab && res.tab.id) || '')).trim();
            if (tabId) {
              state.activeSurface = makeActiveSurface('mail', { mailTabId: tabId });
              rememberSurfaceForReference(state.activeSrId, state.activeSurface);
            }
            renderReferences();
            renderWorkspaceTabs();
            renderContextFiles();
            renderDiffPanel();
            await syncActiveSurface();
          }
          return;
        }
        if (action === 'skills') {
          if (!state.activeSrId) {
            showPassiveNotification('Select a reference first.');
            return;
          }
          const res = await api.srAddTab(state.activeSrId, { tab_kind: 'skills', title: 'Skills' });
          if (res && res.ok) {
            state.references = res.references || await api.srList();
            const tabId = String(((res && res.tab && res.tab.id) || '')).trim();
            if (tabId) {
              state.activeSurface = makeActiveSurface('skills', { skillsTabId: tabId });
              rememberSurfaceForReference(state.activeSrId, state.activeSurface);
            }
            renderReferences();
            renderWorkspaceTabs();
            renderContextFiles();
            renderDiffPanel();
            await syncActiveSurface();
            void noteReferenceRankingInteraction('add_tab', { srId: state.activeSrId, browserTitle: 'Skills' });
          }
        }
      });
    });

  }

  holder.querySelectorAll('.workspace-tab[data-kind="web"]').forEach((node) => {
    node.addEventListener('click', async (event) => {
      if (event.target && event.target.matches('button[data-close-web]')) return;
      const tabId = String(node.getAttribute('data-tab-id') || '').trim();
      if (!tabId) return;
      if (state.activeSurface.kind === 'web' && String(state.activeSurface.tabId || '') === tabId) return;
      if (replayMode) {
        state.activeSurface = makeActiveSurface('web', { tabId });
        renderWorkspaceTabs();
        await syncActiveSurface();
        return;
      }
      const res = await api.srSetActiveTab(state.activeSrId, tabId);
      if (res && res.ok) {
        state.references = res.references || state.references;
        state.activeSurface = makeActiveSurface('web', { tabId });
        rememberSurfaceForReference(state.activeSrId, state.activeSurface);
        renderWorkspaceTabs();
        await syncActiveSurface();
      }
    });
  });

  holder.querySelectorAll('button[data-close-web]').forEach((btn) => {
    btn.addEventListener('click', async (event) => {
      event.stopPropagation();
      if (replayMode) {
        showPassiveNotification('Memory replay is read-only.');
        return;
      }
      const tabId = String(btn.getAttribute('data-close-web') || '').trim();
      if (!tabId) return;
      const res = await api.srRemoveTab(state.activeSrId, tabId);
      if (res && res.ok) {
        await detachWorkspaceWebTabMapping(state.activeSrId, tabId);
        state.references = res.references || state.references;
        state.activeSurface = makeActiveSurface('web');
        rememberSurfaceForReference(state.activeSrId, state.activeSurface);
        renderWorkspaceTabs();
        await syncActiveSurface();
        void noteReferenceRankingInteraction('remove_tab', { srId: state.activeSrId });
      }
    });
  });

  holder.querySelectorAll('.workspace-tab[data-kind="files"]').forEach((node) => {
    node.addEventListener('click', async (event) => {
      if (event.target && event.target.matches('button[data-close-files]')) return;
      const tabId = String(node.getAttribute('data-tab-id') || '').trim();
      if (!tabId || !state.activeSrId) return;
      if (replayMode) {
        state.activeSurface = makeActiveSurface('files', { filesTabId: tabId });
        renderWorkspaceTabs();
        await syncActiveSurface();
        return;
      }
      const res = await api.srSetActiveTab(state.activeSrId, tabId);
      if (res && res.ok) {
        state.references = res.references || state.references;
      } else {
        state.references = await api.srList();
      }
      state.activeSurface = makeActiveSurface('files', { filesTabId: tabId });
      rememberSurfaceForReference(state.activeSrId, state.activeSurface);
      renderWorkspaceTabs();
      await syncActiveSurface();
    });
  });

  holder.querySelectorAll('button[data-close-files]').forEach((btn) => {
    btn.addEventListener('click', async (event) => {
      event.stopPropagation();
      if (replayMode) {
        showPassiveNotification('Memory replay is read-only.');
        return;
      }
      const tabId = String(btn.getAttribute('data-close-files') || '').trim();
      if (!tabId || !state.activeSrId) return;
      const wasActive = state.activeSurface.kind === 'files'
        && String(state.activeSurface.filesTabId || '') === tabId;
      const res = await api.srRemoveTab(state.activeSrId, tabId);
      if (!res || !res.ok) return;
      state.references = res.references || state.references;
      if (wasActive) {
        state.activeSurface = makeActiveSurface('web');
        rememberSurfaceForReference(state.activeSrId, state.activeSurface);
      }
      renderWorkspaceTabs();
      await syncActiveSurface();
      void noteReferenceRankingInteraction('remove_tab', { srId: state.activeSrId });
    });
  });

  holder.querySelectorAll('.workspace-tab[data-kind="skills"]').forEach((node) => {
    node.addEventListener('click', async (event) => {
      if (event.target && event.target.matches('button[data-close-skills]')) return;
      const tabId = String(node.getAttribute('data-tab-id') || '').trim();
      if (!tabId || !state.activeSrId) return;
      if (replayMode) {
        state.activeSurface = makeActiveSurface('skills', { skillsTabId: tabId });
        renderWorkspaceTabs();
        await syncActiveSurface();
        return;
      }
      const res = await api.srSetActiveTab(state.activeSrId, tabId);
      if (res && res.ok) {
        state.references = res.references || state.references;
      } else {
        state.references = await api.srList();
      }
      state.activeSurface = makeActiveSurface('skills', { skillsTabId: tabId });
      rememberSurfaceForReference(state.activeSrId, state.activeSurface);
      renderWorkspaceTabs();
      await syncActiveSurface();
    });
  });

  holder.querySelectorAll('.workspace-tab[data-kind="mail"]').forEach((node) => {
    node.addEventListener('click', async (event) => {
      if (event.target && event.target.matches('button[data-close-mail]')) return;
      const tabId = String(node.getAttribute('data-tab-id') || '').trim();
      if (!tabId || !state.activeSrId) return;
      if (replayMode) {
        state.activeSurface = makeActiveSurface('mail', { mailTabId: tabId });
        renderWorkspaceTabs();
        await syncActiveSurface();
        return;
      }
      const res = await api.srSetActiveTab(state.activeSrId, tabId);
      state.references = (res && res.ok) ? (res.references || state.references) : await api.srList();
      state.activeSurface = makeActiveSurface('mail', { mailTabId: tabId });
      rememberSurfaceForReference(state.activeSrId, state.activeSurface);
      renderWorkspaceTabs();
      await syncActiveSurface();
    });
  });

  holder.querySelectorAll('button[data-close-mail]').forEach((btn) => {
    btn.addEventListener('click', async (event) => {
      event.stopPropagation();
      if (replayMode) {
        showPassiveNotification('Memory replay is read-only.');
        return;
      }
      const tabId = String(btn.getAttribute('data-close-mail') || '').trim();
      if (!tabId || !state.activeSrId) return;
      const wasActive = state.activeSurface.kind === 'mail'
        && String(state.activeSurface.mailTabId || '') === tabId;
      const res = await api.srRemoveTab(state.activeSrId, tabId);
      if (!res || !res.ok) return;
      clearMailStateBucket(tabId);
      state.references = res.references || state.references;
      if (wasActive) {
        state.activeSurface = makeActiveSurface('web');
        rememberSurfaceForReference(state.activeSrId, state.activeSurface);
      }
      renderWorkspaceTabs();
      await syncActiveSurface();
    });
  });

  holder.querySelectorAll('button[data-close-skills]').forEach((btn) => {
    btn.addEventListener('click', async (event) => {
      event.stopPropagation();
      if (replayMode) {
        showPassiveNotification('Memory replay is read-only.');
        return;
      }
      const tabId = String(btn.getAttribute('data-close-skills') || '').trim();
      if (!tabId || !state.activeSrId) return;
      const wasActive = state.activeSurface.kind === 'skills'
        && String(state.activeSurface.skillsTabId || '') === tabId;
      const res = await api.srRemoveTab(state.activeSrId, tabId);
      if (!res || !res.ok) return;
      state.references = res.references || state.references;
      if (wasActive) {
        state.activeSurface = makeActiveSurface('web');
        rememberSurfaceForReference(state.activeSrId, state.activeSurface);
      }
      renderWorkspaceTabs();
      await syncActiveSurface();
      void noteReferenceRankingInteraction('remove_tab', { srId: state.activeSrId });
    });
  });

  holder.querySelectorAll('.workspace-tab[data-kind="artifact"]').forEach((node) => {
    node.addEventListener('click', async (event) => {
      if (event.target && event.target.matches('button[data-delete-artifact]')) return;
      const artifactId = String(node.getAttribute('data-artifact-id') || '').trim();
      if (!artifactId) return;
      state.activeSurface = makeActiveSurface('artifact', { artifactId });
      rememberSurfaceForReference(state.activeSrId, state.activeSurface);
      renderWorkspaceTabs();
      await syncActiveSurface();
    });
  });

  holder.querySelectorAll('button[data-delete-artifact]').forEach((btn) => {
    btn.addEventListener('click', async (event) => {
      event.stopPropagation();
      if (replayMode) {
        showPassiveNotification('Memory replay is read-only.');
        return;
      }
      const artifactId = String(btn.getAttribute('data-delete-artifact') || '').trim();
      if (!artifactId) return;
      const priorActiveSurface = state.activeSurface && typeof state.activeSurface === 'object'
        ? { ...state.activeSurface }
        : makeActiveSurface('web');
      const activeArtifactId = String((priorActiveSurface && priorActiveSurface.artifactId) || '').trim();
      const deletingActiveArtifact = String((priorActiveSurface && priorActiveSurface.kind) || '') === 'artifact'
        && activeArtifactId === artifactId;
      const artifactIdsBeforeDelete = artifacts.map((artifact) => String((artifact && artifact.id) || '').trim()).filter(Boolean);
      const deletedArtifactIndex = artifactIdsBeforeDelete.indexOf(artifactId);
      const res = await api.srDeleteArtifact(state.activeSrId, artifactId);
      if (res && res.ok) {
        state.references = res.references || state.references;
        const updatedRef = getActiveReference();
        const remainingArtifacts = Array.isArray(updatedRef && updatedRef.artifacts) ? updatedRef.artifacts : [];
        const remainingArtifactIds = remainingArtifacts.map((artifact) => String((artifact && artifact.id) || '').trim()).filter(Boolean);
        if (!deletingActiveArtifact) {
          if (
            String((priorActiveSurface && priorActiveSurface.kind) || '') === 'artifact'
            && activeArtifactId
            && remainingArtifactIds.includes(activeArtifactId)
          ) {
            state.activeSurface = makeActiveSurface('artifact', { artifactId: activeArtifactId });
          } else {
            state.activeSurface = priorActiveSurface;
          }
        } else {
          const fallbackIndex = deletedArtifactIndex >= 0 ? Math.min(deletedArtifactIndex, Math.max(0, remainingArtifactIds.length - 1)) : 0;
          const fallbackArtifactId = String(remainingArtifactIds[fallbackIndex] || '').trim();
          if (fallbackArtifactId) {
            state.activeSurface = makeActiveSurface('artifact', { artifactId: fallbackArtifactId });
          } else {
            const fallbackWeb = getActiveWebTab(updatedRef);
            state.activeSurface = fallbackWeb && fallbackWeb.id
              ? makeActiveSurface('web', { tabId: String(fallbackWeb.id || '').trim() })
              : makeActiveSurface('web');
          }
        }
        rememberSurfaceForReference(state.activeSrId, state.activeSurface);
        renderReferences();
        renderWorkspaceTabs();
        syncActiveSurface();
        void noteReferenceRankingInteraction('artifact_delete', { srId: state.activeSrId });
      }
    });
  });

  holder.querySelectorAll('.workspace-tab').forEach((tabNode) => {
    const label = tabNode.querySelector('.workspace-tab-label');
    if (!label) return;
    const wrap = tabNode.querySelector('.workspace-tab-label-wrap');
    if (!wrap) return;
    const scrollDistance = Math.max(0, label.scrollWidth - wrap.clientWidth);
    if (scrollDistance > 8) {
      tabNode.setAttribute('data-overflow', '1');
      tabNode.style.setProperty('--scroll-distance', `${scrollDistance}px`);
    } else {
      tabNode.removeAttribute('data-overflow');
      tabNode.style.removeProperty('--scroll-distance');
    }
  });

  if (!document.__workspaceAddMenuCloseBound) {
    document.addEventListener('click', async (event) => {
      const menu = e('workspace-add-menu');
      const btn = e('workspace-add-tab-btn');
      const target = event.target;
      if (!menu) return;
      if (menu.contains(target)) return;
      if (btn && btn.contains(target)) return;
      const wasOpen = !menu.classList.contains('hidden');
      menu.classList.add('hidden');
      if (wasOpen) await syncActiveSurface();
    });
    document.__workspaceAddMenuCloseBound = true;
  }
  if (!document.__workspaceAddMenuViewportBound) {
    const reposition = () => {};
    window.addEventListener('resize', reposition);
    window.addEventListener('scroll', reposition, true);
    document.__workspaceAddMenuViewportBound = true;
  }
}

async function computeBrowserBounds() {
  const container = e('browser-view-container');
  if (!container) return null;
  const rect = container.getBoundingClientRect();
  if (rect.width <= 4 || rect.height <= 4) return null;
  const scale = getUiZoomScaleForEmbeddedBounds();
  return {
    x: Math.max(0, Math.round(rect.x * scale)),
    y: Math.max(0, Math.round(rect.y * scale)),
    width: Math.max(1, Math.round(rect.width * scale)),
    height: Math.max(1, Math.round(rect.height * scale)),
  };
}

async function computeHistoryPreviewBounds() {
  const container = e('history-preview-container');
  if (!container) return null;
  const rect = container.getBoundingClientRect();
  if (rect.width <= 4 || rect.height <= 4) return null;
  const scale = getUiZoomScaleForEmbeddedBounds();
  return {
    x: Math.max(0, Math.round(rect.x * scale)),
    y: Math.max(0, Math.round(rect.y * scale)),
    width: Math.max(1, Math.round(rect.width * scale)),
    height: Math.max(1, Math.round(rect.height * scale)),
  };
}

function getUiZoomScaleForEmbeddedBounds() {
  if (!electronApi || typeof electronApi.getZoomFactor !== 'function') return 1;
  const zoom = Number(electronApi.getZoomFactor());
  if (!Number.isFinite(zoom) || zoom <= 0) return 1;
  return zoom;
}

async function syncHistoryPreviewBounds() {
  if (state.appView !== 'history') return;
  drawHistorySemanticMap();
  const bounds = await computeHistoryPreviewBounds();
  if (!bounds) return;
  if (typeof api.historyPreviewUpdateBounds !== 'function') return;
  await api.historyPreviewUpdateBounds(bounds);
}

function hideNonWebSurfaces() {
  e('artifact-editor')?.classList.add('hidden');
  e('files-panel')?.classList.add('hidden');
  e('skills-panel')?.classList.add('hidden');
  e('mail-panel')?.classList.add('hidden');
}

function isModalOpen(id) {
  const overlay = e(id);
  if (!overlay) return false;
  return !overlay.classList.contains('hidden');
}

function isWorkspaceAddMenuOpen() {
  const menu = e('workspace-add-menu');
  return !!(menu && !menu.classList.contains('hidden'));
}

function hasBlockingOverlay() {
  return (
    isWorkspaceAddMenuOpen()
    || isModalOpen('context-preview-overlay')
    || isModalOpen('onboarding-overlay')
    || isModalOpen('about-overlay')
    || isModalOpen('publish-snapshot-overlay')
    || isModalOpen('share-reference-overlay')
    || isModalOpen('provider-key-overlay')
  );
}

function closeContextPreviewModal(options = {}) {
  const overlay = e('context-preview-overlay');
  const title = e('context-preview-title');
  const meta = e('context-preview-meta');
  const imageWrap = e('context-preview-image-wrap');
  const image = e('context-preview-image');
  const body = e('context-preview-body');
  const status = e('context-preview-status');
  const openPathBtn = e('context-preview-open-path-btn');
  if (overlay) overlay.classList.add('hidden');
  if (title) title.textContent = 'Context Preview';
  if (meta) meta.textContent = '';
  if (imageWrap) imageWrap.classList.add('hidden');
  if (image) {
    image.src = '';
    image.alt = 'Context preview image';
  }
  if (body) body.textContent = '';
  if (status) status.textContent = '';
  if (openPathBtn) {
    openPathBtn.dataset.filePath = '';
    openPathBtn.disabled = true;
  }
  if (!options.skipSurfaceSync) {
    void syncActiveSurface();
  }
}

function buildContextPreviewMeta(preview = {}) {
  const file = (preview && preview.file && typeof preview.file === 'object') ? preview.file : {};
  const parts = [];
  const relativePath = String(file.relative_path || '').trim();
  const mimeType = String(preview.mime_type || file.mime_type || '').trim();
  const sizeBytes = Number(file.size_bytes || 0);
  if (relativePath) parts.push(relativePath);
  if (mimeType) parts.push(mimeType);
  if (Number.isFinite(sizeBytes) && sizeBytes > 0) {
    parts.push(`${sizeBytes.toLocaleString()} bytes`);
  }
  return parts.join(' · ');
}

function buildContextPreviewBody(preview = {}) {
  const mode = String(preview.preview_mode || 'text').trim().toLowerCase();
  const summary = String(preview.summary || '').trim();
  const content = String(preview.preview || '').replace(/\u0000/g, '').trim();
  const looksScrambled = (text) => {
    const value = String(text || '').trim();
    if (!value) return false;
    const suspicious = (value.match(/[^\x09\x0A\x0D\x20-\x7E]/g) || []).length;
    const ratio = suspicious / Math.max(1, value.length);
    return ratio > 0.12;
  };
  if (mode === 'binary') {
    const rows = [];
    const notice = String(preview.message || 'Binary format detected.').trim();
    rows.push(notice || 'Binary format detected.');
    if (summary && !looksScrambled(summary)) {
      rows.push('');
      rows.push(`Index summary: ${summary}`);
    }
    rows.push('');
    if (content) {
      rows.push('Extracted text fragments:');
      rows.push(content);
    } else {
      rows.push('No extracted text fragments were available.');
    }
    return rows.join('\n');
  }
  if (content) return content;
  if (summary) return summary;
  return '(empty file)';
}

function openContextPreviewModal(preview = {}) {
  const overlay = e('context-preview-overlay');
  const title = e('context-preview-title');
  const meta = e('context-preview-meta');
  const imageWrap = e('context-preview-image-wrap');
  const image = e('context-preview-image');
  const body = e('context-preview-body');
  const status = e('context-preview-status');
  const openPathBtn = e('context-preview-open-path-btn');
  if (!overlay || !title || !meta || !body || !status || !openPathBtn) return;

  const file = (preview && preview.file && typeof preview.file === 'object') ? preview.file : {};
  const titleText = String(
    preview.title
    || file.original_name
    || file.relative_path
    || 'Context Preview',
  ).trim() || 'Context Preview';
  const bodyText = buildContextPreviewBody(preview);
  const mode = String(preview.preview_mode || 'text').trim().toLowerCase();
  const message = String(preview.message || '').trim();
  const pathValue = String(file.stored_path || '').trim();
  const imageUrl = String(preview.image_url || '').trim();
  const hasRenderableImage = mode === 'image' && !!imageUrl;

  title.textContent = titleText;
  meta.textContent = buildContextPreviewMeta(preview);
  if (imageWrap && image) {
    if (hasRenderableImage) {
      image.src = imageUrl;
      image.alt = titleText || 'Context preview image';
      imageWrap.classList.remove('hidden');
    } else {
      image.src = '';
      image.alt = 'Context preview image';
      imageWrap.classList.add('hidden');
    }
  }
  body.textContent = bodyText;
  status.textContent = message || (mode === 'binary'
    ? 'Binary preview mode.'
    : (hasRenderableImage
      ? `Image preview loaded · ${bodyText.length.toLocaleString()} character(s) of extracted context.`
      : `Showing ${bodyText.length.toLocaleString()} character(s).`));
  openPathBtn.dataset.filePath = pathValue;
  openPathBtn.disabled = !pathValue;

  overlay.classList.remove('hidden');
  void syncActiveSurface();
}

async function previewContextFileById(fileId = '') {
  const targetFileId = String(fileId || '').trim();
  if (!targetFileId || !state.activeSrId) return;
  const preview = await api.srGetContextFilePreview(state.activeSrId, targetFileId);
  if (!preview || !preview.ok) {
    window.alert((preview && preview.message) || 'Unable to preview file.');
    return;
  }
  openContextPreviewModal(preview);
}

function isEditableKeyboardTarget(target) {
  if (!target || typeof target !== 'object') return false;
  if (target.isContentEditable) return true;
  const tagName = String(target.tagName || '').toLowerCase();
  return tagName === 'input' || tagName === 'textarea' || tagName === 'select';
}

function getFocusedEditableElement() {
  const active = document.activeElement;
  return isEditableKeyboardTarget(active) ? active : null;
}

function clearActiveDomSelection() {
  try {
    const selection = window.getSelection ? window.getSelection() : null;
    if (selection && typeof selection.removeAllRanges === 'function') selection.removeAllRanges();
  } catch (_) {
    // noop
  }
}

function blurFocusedEditableAndClearSelection() {
  const active = getFocusedEditableElement();
  if (!active) {
    clearActiveDomSelection();
    return false;
  }
  if (typeof active.selectionStart === 'number' && typeof active.selectionEnd === 'number' && typeof active.setSelectionRange === 'function') {
    const end = Number(active.selectionEnd || 0);
    try {
      active.setSelectionRange(end, end);
    } catch (_) {
      // noop
    }
  }
  clearActiveDomSelection();
  if (typeof active.blur === 'function') active.blur();
  return true;
}

function getReferenceTransportOrder() {
  return Array.isArray(state.visibleReferenceOrder) ? state.visibleReferenceOrder.map((id) => String(id || '').trim()).filter(Boolean) : [];
}

function getWorkspaceTransportItems(ref = getActiveReference()) {
  if (!ref) return [];
  const tabs = Array.isArray(ref.tabs) ? ref.tabs : [];
  const webTabs = tabs.filter((tab) => String((tab && tab.tab_kind) || 'web').trim().toLowerCase() === 'web');
  const filesTabs = tabs.filter((tab) => String((tab && tab.tab_kind) || '').trim().toLowerCase() === 'files');
  const skillsTabs = tabs.filter((tab) => String((tab && tab.tab_kind) || '').trim().toLowerCase() === 'skills');
  const mailTabs = tabs.filter((tab) => String((tab && tab.tab_kind) || '').trim().toLowerCase() === 'mail');
  const artifacts = Array.isArray(ref.artifacts) ? ref.artifacts : [];
  return [
    ...webTabs.map((tab) => ({ kind: 'web', id: String((tab && tab.id) || '').trim() })),
    ...filesTabs.map((tab) => ({ kind: 'files', id: String((tab && tab.id) || '').trim() })),
    ...skillsTabs.map((tab) => ({ kind: 'skills', id: String((tab && tab.id) || '').trim() })),
    ...mailTabs.map((tab) => ({ kind: 'mail', id: String((tab && tab.id) || '').trim() })),
    ...artifacts.map((artifact) => ({ kind: 'artifact', id: String((artifact && artifact.id) || '').trim() })),
  ].filter((item) => item.id);
}

function getActiveWorkspaceTransportIndex(items = getWorkspaceTransportItems()) {
  const list = Array.isArray(items) ? items : [];
  const activeWebId = state.activeSurface.kind === 'web'
    ? String(state.activeSurface.tabId || (getActiveWebTab(getActiveReference()) && getActiveWebTab(getActiveReference()).id) || '').trim()
    : '';
  const activeIdByKind = {
    web: activeWebId,
    files: String(state.activeSurface.filesTabId || '').trim(),
    skills: String(state.activeSurface.skillsTabId || '').trim(),
    mail: String(state.activeSurface.mailTabId || '').trim(),
    artifact: String(state.activeSurface.artifactId || '').trim(),
  };
  return list.findIndex((item) => item.kind === String(state.activeSurface.kind || '').trim() && item.id === activeIdByKind[item.kind]);
}

async function activateWorkspaceTransportItem(item) {
  const next = (item && typeof item === 'object') ? item : null;
  if (!next || !next.id || !state.activeSrId) return false;
  const replayMode = isMemoryReplayActive();
  if (next.kind === 'web') {
    if (replayMode) {
      state.activeSurface = makeActiveSurface('web', { tabId: next.id });
      renderWorkspaceTabs();
      await syncActiveSurface();
      return true;
    }
    const res = await api.srSetActiveTab(state.activeSrId, next.id);
    if (!res || !res.ok) return false;
    state.references = res.references || state.references;
    state.activeSurface = makeActiveSurface('web', { tabId: next.id });
    rememberSurfaceForReference(state.activeSrId, state.activeSurface);
    renderWorkspaceTabs();
    await syncActiveSurface();
    return true;
  }
  if (next.kind === 'files') {
    if (replayMode) {
      state.activeSurface = makeActiveSurface('files', { filesTabId: next.id });
      renderWorkspaceTabs();
      await syncActiveSurface();
      return true;
    }
    const res = await api.srSetActiveTab(state.activeSrId, next.id);
    state.references = (res && res.ok) ? (res.references || state.references) : await api.srList();
    state.activeSurface = makeActiveSurface('files', { filesTabId: next.id });
    rememberSurfaceForReference(state.activeSrId, state.activeSurface);
    renderWorkspaceTabs();
    await syncActiveSurface();
    return true;
  }
  if (next.kind === 'skills') {
    if (replayMode) {
      state.activeSurface = makeActiveSurface('skills', { skillsTabId: next.id });
      renderWorkspaceTabs();
      await syncActiveSurface();
      return true;
    }
    const res = await api.srSetActiveTab(state.activeSrId, next.id);
    state.references = (res && res.ok) ? (res.references || state.references) : await api.srList();
    state.activeSurface = makeActiveSurface('skills', { skillsTabId: next.id });
    rememberSurfaceForReference(state.activeSrId, state.activeSurface);
    renderWorkspaceTabs();
    await syncActiveSurface();
    return true;
  }
  if (next.kind === 'mail') {
    if (replayMode) {
      state.activeSurface = makeActiveSurface('mail', { mailTabId: next.id });
      renderWorkspaceTabs();
      await syncActiveSurface();
      return true;
    }
    const res = await api.srSetActiveTab(state.activeSrId, next.id);
    state.references = (res && res.ok) ? (res.references || state.references) : await api.srList();
    state.activeSurface = makeActiveSurface('mail', { mailTabId: next.id });
    rememberSurfaceForReference(state.activeSrId, state.activeSurface);
    renderWorkspaceTabs();
    await syncActiveSurface();
    return true;
  }
  if (next.kind === 'artifact') {
    state.activeSurface = makeActiveSurface('artifact', { artifactId: next.id });
    rememberSurfaceForReference(state.activeSrId, state.activeSurface);
    renderWorkspaceTabs();
    await syncActiveSurface();
    return true;
  }
  return false;
}

async function closeActiveWorkspaceTab() {
  if (!state.activeSrId) return false;
  if (isMemoryReplayActive()) {
    showPassiveNotification('Memory replay is read-only.');
    return true;
  }

  const refBeforeClose = getActiveReference();
  if (!refBeforeClose) return false;

  if (state.activeSurface.kind === 'artifact') {
    return removeActiveArtifactFromCommand();
  }

  if (state.activeSurface.kind === 'files') {
    const tabId = String(state.activeSurface.filesTabId || '').trim();
    if (!tabId) return false;
    const res = await api.srRemoveTab(state.activeSrId, tabId);
    if (!res || !res.ok) return false;
    state.references = res.references || state.references;
    state.activeSurface = makeActiveSurface('web');
    rememberSurfaceForReference(state.activeSrId, state.activeSurface);
    renderWorkspaceTabs();
    await syncActiveSurface();
    void noteReferenceRankingInteraction('remove_tab', { srId: state.activeSrId });
    return true;
  }

  if (state.activeSurface.kind === 'skills') {
    const tabId = String(state.activeSurface.skillsTabId || '').trim();
    if (!tabId) return false;
    const res = await api.srRemoveTab(state.activeSrId, tabId);
    if (!res || !res.ok) return false;
    state.references = res.references || state.references;
    state.activeSurface = makeActiveSurface('web');
    rememberSurfaceForReference(state.activeSrId, state.activeSurface);
    renderWorkspaceTabs();
    await syncActiveSurface();
    void noteReferenceRankingInteraction('remove_tab', { srId: state.activeSrId });
    return true;
  }

  const activeWebTab = getActiveWebTab(refBeforeClose);
  const tabId = String((state.activeSurface.tabId || (activeWebTab && activeWebTab.id) || '')).trim();
  if (!tabId) return false;
  const res = await api.srRemoveTab(state.activeSrId, tabId);
  if (!res || !res.ok) return false;
  await detachWorkspaceWebTabMapping(state.activeSrId, tabId);
  state.references = res.references || state.references;
  state.activeSurface = makeActiveSurface('web');
  rememberSurfaceForReference(state.activeSrId, state.activeSurface);
  renderWorkspaceTabs();
  await syncActiveSurface();
  void noteReferenceRankingInteraction('remove_tab', { srId: state.activeSrId });
  return true;
}

async function stepReferenceTransport(delta) {
  const order = getReferenceTransportOrder();
  if (!order.length) return false;
  const currentIndex = order.indexOf(String(state.activeSrId || '').trim());
  const fallbackIndex = delta > 0 ? -1 : order.length;
  const baseIndex = currentIndex >= 0 ? currentIndex : fallbackIndex;
  const nextIndex = baseIndex + (delta > 0 ? 1 : -1);
  if (nextIndex < 0 || nextIndex >= order.length) return false;
  await activateReferenceSurface(order[nextIndex]);
  return true;
}

async function stepWorkspaceTabTransport(delta) {
  const items = getWorkspaceTransportItems();
  if (!items.length) return false;
  const currentIndex = getActiveWorkspaceTransportIndex(items);
  const fallbackIndex = delta > 0 ? -1 : items.length;
  const baseIndex = currentIndex >= 0 ? currentIndex : fallbackIndex;
  const nextIndex = baseIndex + (delta > 0 ? 1 : -1);
  if (nextIndex < 0 || nextIndex >= items.length) return false;
  return activateWorkspaceTransportItem(items[nextIndex]);
}

async function focusLuminoInputFromShortcut() {
  if (state.appView !== 'workspace') return false;
  if (window.innerWidth <= 980) {
    document.body.classList.add('mobile-right-open');
    document.body.classList.remove('mobile-left-open');
    e('workspace-right-rail-toggle-btn')?.setAttribute('aria-expanded', 'true');
  }
  if (state.chatPanelCollapsed) setChatPanelCollapsed(false);
  const input = e('chat-input');
  if (!input) return false;
  input.focus();
  if (typeof input.select === 'function' && String(input.value || '').trim()) input.select();
  return true;
}

function parseShortcutCommandFromKeyboardEvent(event) {
  if (!event) return '';
  const key = String(event.key || '');
  const code = String(event.code || '');
  const lowerKey = key.toLowerCase();

  const hasCtrlOrMeta = !!(event.ctrlKey || event.metaKey);
  if (hasCtrlOrMeta && !event.altKey) {
    if (!event.shiftKey) {
      if (key === 'ArrowUp') return 'reference_prev';
      if (key === 'ArrowDown') return 'reference_next';
      if (key === 'ArrowLeft') return 'workspace_tab_prev';
      if (key === 'ArrowRight') return 'workspace_tab_next';
      if (lowerKey === 'w' || code === 'KeyW') return 'workspace_tab_close';
    }
    if (key === '0' || code === 'Digit0' || code === 'Numpad0') return 'web_zoom_reset';
    if (key === '+' || key === '=' || code === 'NumpadAdd') return 'web_zoom_in';
    if (key === '-' || key === '_' || code === 'NumpadSubtract') return 'web_zoom_out';
    return '';
  }

  if (event.shiftKey && !event.ctrlKey && !event.metaKey && !event.altKey) {
    if (key === '0' || key === ')' || code === 'Digit0' || code === 'Numpad0') return 'ui_zoom_reset';
    if (key === '+' || key === '=' || code === 'NumpadAdd') return 'ui_zoom_in';
    if (key === '-' || key === '_' || code === 'NumpadSubtract') return 'ui_zoom_out';
    if (lowerKey === 'z' || code === 'KeyZ') return 'toggle_zen';
  }
  if (!event.ctrlKey && !event.metaKey && !event.altKey && !event.shiftKey) {
    if (lowerKey === 'l' || code === 'KeyL') return 'focus_lumino';
  }
  return '';
}

async function executeShortcutCommand(command) {
  const cmd = String(command || '').trim().toLowerCase();
  if (!cmd) return;

  if (cmd === 'toggle_zen') {
    await toggleZenMode();
    showPassiveNotification(`Zen mode ${state.zenMode ? 'on' : 'off'}.`, 1000);
    return;
  }

  if (cmd === 'reference_prev') {
    await stepReferenceTransport(-1);
    return;
  }

  if (cmd === 'reference_next') {
    await stepReferenceTransport(1);
    return;
  }

  if (cmd === 'workspace_tab_prev') {
    await stepWorkspaceTabTransport(-1);
    return;
  }

  if (cmd === 'workspace_tab_next') {
    await stepWorkspaceTabTransport(1);
    return;
  }

  if (cmd === 'workspace_tab_close') {
    await closeActiveWorkspaceTab();
    return;
  }

  if (cmd === 'focus_lumino') {
    await focusLuminoInputFromShortcut();
    return;
  }

  if (cmd === 'web_zoom_in' || cmd === 'web_zoom_out' || cmd === 'web_zoom_reset') {
    if (state.appView !== 'workspace' || state.activeSurface.kind !== 'web') return;
    let nextZoom = 1;
    if (cmd === 'web_zoom_in' || cmd === 'web_zoom_out') {
      const current = Number(await api.getZoomFactor());
      const base = Number.isFinite(current) && current > 0 ? current : 1;
      nextZoom = cmd === 'web_zoom_in'
        ? clamp(base + BROWSER_VIEW_ZOOM_STEP, BROWSER_VIEW_MIN_ZOOM, BROWSER_VIEW_MAX_ZOOM)
        : clamp(base - BROWSER_VIEW_ZOOM_STEP, BROWSER_VIEW_MIN_ZOOM, BROWSER_VIEW_MAX_ZOOM);
    }
    const res = await api.setZoomFactor(nextZoom);
    if (!res || !res.ok) return;
    showPassiveNotification(`Web zoom ${Math.round(Number(res.zoom || nextZoom) * 100)}%`, 1000);
    return;
  }

  if (cmd === 'ui_zoom_in' || cmd === 'ui_zoom_out' || cmd === 'ui_zoom_reset') {
    if (!electronApi || typeof electronApi.getZoomFactor !== 'function' || typeof electronApi.setZoomFactor !== 'function') return;
    let nextZoom = 1;
    if (cmd === 'ui_zoom_in' || cmd === 'ui_zoom_out') {
      const current = Number(electronApi.getZoomFactor());
      const base = Number.isFinite(current) && current > 0 ? current : 1;
      nextZoom = cmd === 'ui_zoom_in'
        ? clamp(base + UI_ZOOM_STEP, UI_MIN_ZOOM, UI_MAX_ZOOM)
        : clamp(base - UI_ZOOM_STEP, UI_MIN_ZOOM, UI_MAX_ZOOM);
    }
    const res = electronApi.setZoomFactor(nextZoom);
    if (res && res.ok === false) return;
    const applied = Number((res && res.zoom) || electronApi.getZoomFactor() || nextZoom);
    if (Number.isFinite(applied) && applied > 0) {
      persistUiZoomPreference(applied);
    }
    await resyncEmbeddedViewBoundsAfterUiZoom();
    showPassiveNotification(`UI zoom ${Math.round(applied * 100)}%`, 1000);
  }
}

async function waitForUiZoomRelayout() {
  await new Promise((resolve) => {
    requestAnimationFrame(() => {
      requestAnimationFrame(resolve);
    });
  });
}

async function resyncEmbeddedViewBoundsAfterUiZoom() {
  await waitForUiZoomRelayout();
  if (state.appView === 'workspace' && state.activeSurface.kind === 'web') {
    const bounds = await computeBrowserBounds();
    if (bounds) {
      await api.updateBounds(bounds);
    }
    return;
  }
  if (state.appView === 'history') {
    await syncHistoryPreviewBounds();
  }
}

function bindBrowserZoomShortcuts() {
  if (document.__browserZoomShortcutsBound) return;
  document.addEventListener('keydown', async (event) => {
    if (!event || event.defaultPrevented) return;
    const key = String(event.key || '');
    if (
      key === 'Escape'
      && !event.ctrlKey
      && !event.metaKey
      && !event.altKey
      && !event.shiftKey
      && blurFocusedEditableAndClearSelection()
    ) {
      event.preventDefault();
      event.stopPropagation();
      return;
    }
    if (
      key === 'Escape'
      && state.zenMode
      && !event.ctrlKey
      && !event.metaKey
      && !event.altKey
      && !event.shiftKey
      && !hasBlockingOverlay()
      && !getFocusedEditableElement()
    ) {
      event.preventDefault();
      event.stopPropagation();
      await setZenMode(false);
      showPassiveNotification('Zen mode off.', 1000);
      return;
    }
    if (hasBlockingOverlay()) return;
    if (getFocusedEditableElement()) return;
    const command = parseShortcutCommandFromKeyboardEvent(event);
    if (!command) return;
    event.preventDefault();
    event.stopPropagation();
    await executeShortcutCommand(command);
  }, true);
  if (api && typeof api.onShortcutCommand === 'function') {
    api.onShortcutCommand((payload) => {
      const command = String((payload && payload.command) || '').trim().toLowerCase();
      if (!command) return;
      if (hasBlockingOverlay()) return;
      void executeShortcutCommand(command);
    });
  }
  document.__browserZoomShortcutsBound = true;
}

function extractMarkdownImageTokens(markdownText) {
  const text = String(markdownText || '');
  const tokens = [];
  const re = /!\[([^\]]*)\]\(([^)]+)\)/g;
  let match;
  while ((match = re.exec(text)) !== null) {
    const alt = String(match[1] || '').trim();
    let uri = String(match[2] || '').trim();
    if (!uri) continue;
    if (uri.startsWith('<') && uri.endsWith('>')) {
      uri = uri.slice(1, -1).trim();
    }
    uri = uri
      .replace(/\s+"[^"]*"\s*$/, '')
      .replace(/\s+'[^']*'\s*$/, '')
      .trim();
    if (!uri) continue;
    tokens.push({ alt, uri });
  }
  return tokens;
}

function applyArtifactSplitLayout(ratio) {
  const shell = e('artifact-viewer-shell');
  const imagePane = e('artifact-image-pane');
  if (!shell || !imagePane) return;
  if (!shell.classList.contains('artifact-mode-image-text')) return;
  const shellRect = shell.getBoundingClientRect();
  if (!shellRect || shellRect.height <= 0) return;
  const split = clamp(Number(ratio || ARTIFACT_DEFAULT_SPLIT_RATIO), ARTIFACT_MIN_SPLIT_RATIO, ARTIFACT_MAX_SPLIT_RATIO);
  const height = Math.round(shellRect.height * split);
  imagePane.style.flex = `0 0 ${Math.max(120, height)}px`;
}

function applyArtifactModeLayout() {
  const shell = e('artifact-viewer-shell');
  const imagePane = e('artifact-image-pane');
  const splitter = e('artifact-splitter-horizontal');
  const textPane = e('artifact-text-pane');
  if (!shell || !imagePane || !splitter || !textPane) return;

  const hasResolved = Array.isArray(state.artifactResolvedImages) && state.artifactResolvedImages.length > 0;
  shell.classList.toggle('artifact-mode-text-only', !hasResolved);
  shell.classList.toggle('artifact-mode-image-text', hasResolved);
  imagePane.classList.toggle('hidden', !hasResolved);
  splitter.classList.toggle('hidden', !hasResolved);
  if (!hasResolved) {
    imagePane.style.flex = '0 0 auto';
    textPane.style.flex = '1 1 auto';
    return;
  }

  textPane.style.flex = '1 1 auto';
  const ratio = getArtifactSplitRatioForReference(state.activeSrId);
  applyArtifactSplitLayout(ratio);
}

function renderActiveArtifactImage() {
  const img = e('artifact-image');
  const status = e('artifact-image-status');
  const carousel = e('artifact-carousel');
  const carouselIndex = e('artifact-carousel-index');
  const prevBtn = e('artifact-carousel-prev-btn');
  const nextBtn = e('artifact-carousel-next-btn');
  const zoomOutBtn = e('artifact-zoom-out-btn');
  const zoomInBtn = e('artifact-zoom-in-btn');
  const saveBtn = e('artifact-save-image-btn');
  const zoom = getArtifactZoomForReference(state.activeSrId);

  if (!img || !status || !carousel || !carouselIndex || !prevBtn || !nextBtn || !zoomOutBtn || !zoomInBtn || !saveBtn) return;
  if (!Array.isArray(state.artifactResolvedImages) || state.artifactResolvedImages.length === 0) {
    img.removeAttribute('src');
    status.textContent = state.artifactUnresolvedImages.length > 0
      ? `${state.artifactUnresolvedImages.length} unresolved image link(s); showing text-only mode.`
      : 'No markdown images found.';
    carousel.classList.add('hidden');
    zoomOutBtn.disabled = true;
    zoomInBtn.disabled = true;
    saveBtn.disabled = true;
    return;
  }

  const total = state.artifactResolvedImages.length;
  const idx = clamp(Number(state.artifactActiveImageIndex || 0), 0, total - 1);
  state.artifactActiveImageIndex = idx;
  const active = state.artifactResolvedImages[idx];
  img.alt = String((active && active.alt) || 'artifact image');
  img.src = String((active && active.resolved_url) || '');
  img.style.transform = `scale(${zoom.toFixed(2)})`;

  const unresolvedCount = Number((state.artifactUnresolvedImages && state.artifactUnresolvedImages.length) || 0);
  status.textContent = `Image ${idx + 1}/${total} · zoom ${Math.round(zoom * 100)}%${unresolvedCount > 0 ? ` · ${unresolvedCount} unresolved` : ''}`;
  carousel.classList.toggle('hidden', total <= 1);
  carouselIndex.textContent = `${idx + 1} / ${total}`;
  prevBtn.disabled = total <= 1;
  nextBtn.disabled = total <= 1;
  zoomOutBtn.disabled = zoom <= ARTIFACT_MIN_ZOOM + 0.0001;
  zoomInBtn.disabled = zoom >= ARTIFACT_MAX_ZOOM - 0.0001;
  saveBtn.disabled = !String((active && active.resolved_url) || '').trim();
}

function cycleArtifactImage(step) {
  if (!Array.isArray(state.artifactResolvedImages) || state.artifactResolvedImages.length <= 1) return;
  const total = state.artifactResolvedImages.length;
  const current = Number(state.artifactActiveImageIndex || 0);
  const next = (current + Number(step || 0) + total) % total;
  state.artifactActiveImageIndex = next;
  setArtifactCarouselIndex(state.artifactActiveArtifactId, next);
  renderActiveArtifactImage();
}

function adjustArtifactImageZoom(delta) {
  const srId = String(state.activeSrId || '').trim();
  if (!srId) return;
  const current = getArtifactZoomForReference(srId);
  const next = clamp(current + Number(delta || 0), ARTIFACT_MIN_ZOOM, ARTIFACT_MAX_ZOOM);
  setArtifactZoomForReference(srId, next);
  renderActiveArtifactImage();
}

function buildSuggestedImageName() {
  const active = Array.isArray(state.artifactResolvedImages)
    ? state.artifactResolvedImages[state.artifactActiveImageIndex]
    : null;
  const fallback = 'artifact-image.png';
  const source = String((active && active.source_uri) || '').trim();
  const fromPath = source.split('/').pop() || '';
  const plain = fromPath.split('?')[0].trim();
  const clean = plain.replace(/[^a-zA-Z0-9._-]/g, '_');
  if (clean && clean.includes('.')) return clean;
  return fallback;
}

async function saveActiveArtifactImage() {
  const srId = String(state.activeSrId || '').trim();
  if (!srId) return;
  if (!Array.isArray(state.artifactResolvedImages) || state.artifactResolvedImages.length === 0) return;
  const active = state.artifactResolvedImages[state.artifactActiveImageIndex];
  const sourceUrl = String((active && active.resolved_url) || '').trim();
  if (!sourceUrl) {
    showPassiveNotification('No active image to save.');
    return;
  }
  const suggestedName = buildSuggestedImageName();
  const res = await api.saveArtifactImage(srId, sourceUrl, suggestedName);
  if (!res || !res.ok) {
    const message = String((res && res.message) || 'Unable to save image.');
    if (message && message.toLowerCase() !== 'save canceled') showPassiveNotification(message);
    return;
  }
  showPassiveNotification(`Image saved: ${String(res.saved_path || '').trim() || suggestedName}`);
}

async function refreshArtifactVisualState(markdownText, artifactId) {
  const status = e('artifact-image-status');
  if (!status) return;
  if (state.activeSurface.kind !== 'artifact') return;

  const srId = String(state.activeSrId || '').trim();
  if (!srId) {
    state.artifactResolvedImages = [];
    state.artifactUnresolvedImages = [];
    applyArtifactModeLayout();
    renderActiveArtifactImage();
    return;
  }

  const requestSeq = Number(state.artifactPreviewSeq || 0) + 1;
  state.artifactPreviewSeq = requestSeq;
  status.textContent = 'Resolving markdown image links...';

  const tokens = extractMarkdownImageTokens(markdownText);
  const resolvedImages = [];
  const unresolvedImages = [];

  for (const token of tokens) {
    let resolved = { ok: false, resolved_url: '', reason: 'resolve_failed' };
    try {
      resolved = await api.resolveArtifactAsset(srId, token.uri);
    } catch (_) {
      resolved = { ok: false, resolved_url: '', reason: 'resolver_unavailable' };
    }
    if (requestSeq !== state.artifactPreviewSeq) return;

    if (resolved && resolved.ok && String(resolved.resolved_url || '').trim()) {
      resolvedImages.push({
        alt: String(token.alt || '').trim() || 'artifact image',
        source_uri: String(token.uri || '').trim(),
        resolved_url: String(resolved.resolved_url || '').trim(),
        reason: String(resolved.reason || '').trim(),
      });
    } else {
      unresolvedImages.push({
        alt: String(token.alt || '').trim() || 'artifact image',
        source_uri: String(token.uri || '').trim(),
        reason: String((resolved && resolved.reason) || 'unknown_error'),
      });
    }
  }

  if (requestSeq !== state.artifactPreviewSeq) return;
  state.artifactResolvedImages = resolvedImages;
  state.artifactUnresolvedImages = unresolvedImages;
  state.artifactActiveArtifactId = String(artifactId || '').trim();

  if (resolvedImages.length > 0) {
    const remembered = getArtifactCarouselIndex(state.artifactActiveArtifactId);
    state.artifactActiveImageIndex = clamp(remembered, 0, resolvedImages.length - 1);
  } else {
    state.artifactActiveImageIndex = 0;
  }

  applyArtifactModeLayout();
  renderActiveArtifactImage();
}

function normalizeArtifactType(value) {
  const type = String(value || '').trim().toLowerCase();
  return type === 'html' ? 'html' : 'markdown';
}

function getArtifactViewMode(artifactId, artifactType) {
  const normalizedType = normalizeArtifactType(artifactType);
  if (normalizedType !== 'html') return ARTIFACT_VIEW_MODE_CODE;
  const key = String(artifactId || '').trim();
  const raw = key ? String((state.artifactViewModeByArtifact && state.artifactViewModeByArtifact[key]) || '') : '';
  return raw === ARTIFACT_VIEW_MODE_CODE ? ARTIFACT_VIEW_MODE_CODE : ARTIFACT_VIEW_MODE_PREVIEW;
}

function setArtifactViewMode(artifactId, nextMode) {
  const key = String(artifactId || '').trim();
  if (!key) return;
  const mode = String(nextMode || '').trim().toLowerCase() === ARTIFACT_VIEW_MODE_PREVIEW
    ? ARTIFACT_VIEW_MODE_PREVIEW
    : ARTIFACT_VIEW_MODE_CODE;
  state.artifactViewModeByArtifact = state.artifactViewModeByArtifact || {};
  state.artifactViewModeByArtifact[key] = mode;
}

function renderHtmlRuntimePlaceholder(message) {
  const host = e('artifact-html-runtime-host');
  if (!host) return;
  host.innerHTML = `<div class="artifact-html-placeholder">${escapeHtml(String(message || '').trim() || 'Preview unavailable. Click Refresh to rerender.')}</div>`;
}

function resetHtmlArtifactCodeViews(ref = getActiveReference()) {
  const artifacts = Array.isArray(ref && ref.artifacts) ? ref.artifacts : [];
  artifacts.forEach((artifact) => {
    if (normalizeArtifactType(artifact && artifact.type) !== 'html') return;
    const artifactId = String((artifact && artifact.id) || '').trim();
    if (!artifactId) return;
    setArtifactViewMode(artifactId, ARTIFACT_VIEW_MODE_PREVIEW);
  });
}

function getActiveHtmlRuntimeIframe() {
  const runtime = (state.htmlArtifactRuntime && typeof state.htmlArtifactRuntime === 'object')
    ? state.htmlArtifactRuntime
    : null;
  if (!runtime || !runtime.running) return null;
  const iframe = runtime.iframeEl;
  if (iframe && iframe.isConnected) return iframe;
  const host = e('artifact-html-runtime-host');
  if (!host) return null;
  const fallback = host.querySelector('iframe.artifact-html-iframe');
  if (!fallback) return null;
  state.htmlArtifactRuntime.iframeEl = fallback;
  return fallback;
}

function focusActiveHtmlRuntime(_reason = '') {
  const iframe = getActiveHtmlRuntimeIframe();
  if (!iframe) return false;
  try {
    iframe.focus();
  } catch (_) {
    // noop
  }
  try {
    if (iframe.contentWindow && typeof iframe.contentWindow.focus === 'function') {
      iframe.contentWindow.focus();
    }
  } catch (_) {
    // noop
  }
  return true;
}

function shouldPreferRuntimeFocusAfterChat() {
  if (!state || state.activeSurface.kind !== 'artifact') return false;
  const artifactId = String(state.activeSurface.artifactId || '').trim();
  if (!artifactId) return false;
  const runtime = (state.htmlArtifactRuntime && typeof state.htmlArtifactRuntime === 'object')
    ? state.htmlArtifactRuntime
    : null;
  if (!runtime || !runtime.running) return false;
  if (String(runtime.artifactId || '') !== artifactId) return false;
  return getArtifactViewMode(artifactId, 'html') === ARTIFACT_VIEW_MODE_PREVIEW;
}

function stopHtmlArtifactRuntime(options = {}) {
  const opts = (options && typeof options === 'object') ? options : {};
  const runtime = (state.htmlArtifactRuntime && typeof state.htmlArtifactRuntime === 'object')
    ? state.htmlArtifactRuntime
    : {
      artifactId: '', running: false, stale: false, objectUrl: '', iframeEl: null, sourceContent: '',
    };
  if (runtime.objectUrl) {
    try {
      URL.revokeObjectURL(runtime.objectUrl);
    } catch (_) {
      // noop
    }
  }
  const host = e('artifact-html-runtime-host');
  if (host) host.innerHTML = '';
  state.htmlArtifactRuntime = {
    artifactId: opts.preserveArtifactId ? String(runtime.artifactId || '') : '',
    running: false,
    stale: false,
    objectUrl: '',
    iframeEl: null,
    sourceContent: '',
  };
}

function updateArtifactRuntimeControls(artifact) {
  const safeArtifact = (artifact && typeof artifact === 'object') ? artifact : {};
  const artifactId = String((safeArtifact && safeArtifact.id) || '').trim();
  const artifactType = normalizeArtifactType((safeArtifact && safeArtifact.type) || 'markdown');
  const isHtml = artifactType === 'html';
  const mode = getArtifactViewMode(artifactId, artifactType);
  const runtime = (state.htmlArtifactRuntime && typeof state.htmlArtifactRuntime === 'object')
    ? state.htmlArtifactRuntime
    : {
      artifactId: '', running: false, stale: false, objectUrl: '', iframeEl: null, sourceContent: '',
    };
  const runtimeForArtifact = isHtml && runtime.running && String(runtime.artifactId || '') === artifactId;

  const chip = e('artifact-type-chip');
  const codeBtn = e('artifact-mode-code-btn');
  const refreshBtn = e('artifact-run-refresh-btn');
  const runtimeStatus = e('artifact-runtime-status');
  const textPane = e('artifact-text-pane');
  const previewPane = e('artifact-html-preview-pane');

  if (chip) {
    chip.textContent = isHtml ? 'HTML' : 'Markdown';
    chip.classList.toggle('artifact-type-html', isHtml);
  }
  if (codeBtn) {
    codeBtn.classList.toggle('hidden', !isHtml);
    codeBtn.classList.toggle('active', isHtml && mode === ARTIFACT_VIEW_MODE_CODE);
    codeBtn.disabled = !isHtml;
  }
  if (refreshBtn) {
    refreshBtn.classList.toggle('hidden', !isHtml);
    refreshBtn.disabled = !isHtml;
  }
  if (runtimeStatus) {
    if (!isHtml) {
      runtimeStatus.textContent = 'Markdown artifact';
    } else if (!runtimeForArtifact) {
      runtimeStatus.textContent = 'HTML preview loading';
    } else {
      runtimeStatus.textContent = 'HTML preview live';
    }
  }

  if (textPane) textPane.classList.toggle('hidden', isHtml && mode === ARTIFACT_VIEW_MODE_PREVIEW);
  if (previewPane) previewPane.classList.toggle('hidden', !isHtml || mode !== ARTIFACT_VIEW_MODE_PREVIEW);
}

function startHtmlArtifactRuntime(artifact, htmlContent) {
  const safeArtifact = (artifact && typeof artifact === 'object') ? artifact : {};
  const artifactId = String((safeArtifact && safeArtifact.id) || '').trim();
  if (!artifactId) return false;
  const host = e('artifact-html-runtime-host');
  if (!host) return false;
  stopHtmlArtifactRuntime({ preserveArtifactId: true });
  const source = String(htmlContent || '');
  const blob = new Blob([source], { type: 'text/html' });
  const url = URL.createObjectURL(blob);
  const iframe = document.createElement('iframe');
  iframe.className = 'artifact-html-iframe';
  iframe.setAttribute('tabindex', '0');
  iframe.setAttribute('sandbox', 'allow-scripts allow-forms allow-pointer-lock allow-downloads');
  iframe.addEventListener('load', () => {
    const runtime = (state.htmlArtifactRuntime && typeof state.htmlArtifactRuntime === 'object')
      ? state.htmlArtifactRuntime
      : null;
    if (!runtime || !runtime.running) return;
    if (String(runtime.artifactId || '') !== artifactId) return;
    focusActiveHtmlRuntime('runtime-load');
  }, { once: true });
  iframe.src = url;
  host.innerHTML = '';
  host.appendChild(iframe);
  state.htmlArtifactRuntime = {
    artifactId,
    running: true,
    stale: false,
    objectUrl: url,
    iframeEl: iframe,
    sourceContent: source,
  };
  focusActiveHtmlRuntime('runtime-start');
  return true;
}

function ensureHtmlArtifactRuntime(artifact, htmlContent, options = {}) {
  const safeArtifact = (artifact && typeof artifact === 'object') ? artifact : {};
  const artifactId = String((safeArtifact && safeArtifact.id) || '').trim();
  if (!artifactId || normalizeArtifactType(safeArtifact.type) !== 'html') return false;
  const source = String(htmlContent || safeArtifact.content || '');
  const runtime = (state.htmlArtifactRuntime && typeof state.htmlArtifactRuntime === 'object')
    ? state.htmlArtifactRuntime
    : null;
  const shouldRestart = !!(
    options.force
    || !runtime
    || !runtime.running
    || String(runtime.artifactId || '') !== artifactId
    || String(runtime.sourceContent || '') !== source
  );
  if (shouldRestart) {
    if (!startHtmlArtifactRuntime(safeArtifact, source)) {
      showPassiveNotification('Unable to refresh HTML preview.');
      return false;
    }
  }
  updateArtifactRuntimeControls(safeArtifact);
  if (options.focus) focusActiveHtmlRuntime(options.focusReason || 'runtime-sync');
  return true;
}

function refreshActiveHtmlArtifactRuntime() {
  const ref = getActiveReference();
  if (!ref || state.activeSurface.kind !== 'artifact') return;
  const artifactId = String(state.activeSurface.artifactId || '').trim();
  const artifact = (Array.isArray(ref.artifacts) ? ref.artifacts : []).find((item) => String((item && item.id) || '') === artifactId);
  if (!artifact || normalizeArtifactType(artifact.type) !== 'html') return;
  const input = e('artifact-input');
  const source = String((input && typeof input.value === 'string') ? input.value : (artifact.content || ''));
  ensureHtmlArtifactRuntime(artifact, source, { force: true, focus: true, focusReason: 'runtime-refresh-button' });
}

async function showWebSurface(tab) {
  if (!tab) return;
  resetHtmlArtifactCodeViews();
  stopHtmlArtifactRuntime();
  hideNonWebSurfaces();
  e('browser-placeholder')?.classList.add('hidden');

  const bounds = await computeBrowserBounds();
  const ref = getActiveReference();
  const activeSrId = String(state.activeSrId || '').trim();
  const semanticTabId = String((tab && tab.id) || '').trim();
  const canUseRuntimeTabs = hasRuntimeTabsApi() && !!activeSrId && !!semanticTabId;
  if (canUseRuntimeTabs) {
    const runtimeRes = await ensureWorkspaceWebTabActive(activeSrId, tab);
    if (runtimeRes && runtimeRes.ok) {
      if (bounds) {
        await api.show(bounds);
        await api.updateBounds(bounds);
      }
      let runtimeUrl = '';
      try {
        runtimeUrl = String((await api.getCurrentUrl()) || '').trim();
      } catch (_) {
        runtimeUrl = '';
      }
      state.browserCurrentUrl = runtimeUrl || String((tab && tab.url) || '').trim();
      state.lastWebSrId = activeSrId || null;
      state.lastWebTabId = semanticTabId || null;
      const input = e('browser-url-input');
      if (input) input.value = String(state.browserCurrentUrl || '');
      await syncUrlBarForActiveSurface();
      await api.markerSetContext({ srId: state.activeSrId, artifactId: null });
      await refreshUncommittedActionCue();
      return;
    }
  }

  if (bounds) {
    await api.show(bounds);
    await api.updateBounds(bounds);
  }

  const sameReferenceSession = activeSrId && String(state.lastWebSrId || '') === activeSrId;
  const sameTabSession = semanticTabId && String(state.lastWebTabId || '') === semanticTabId;
  let liveUrl = '';
  try {
    liveUrl = String((await api.getCurrentUrl()) || '').trim();
  } catch (_) {
    liveUrl = '';
  }
  const targetNorm = normalizeUrlForComparison((tab && tab.url) || '');
  const liveNorm = normalizeUrlForComparison(liveUrl || '');
  const liveMatchesTarget = !!(targetNorm && liveNorm && targetNorm === liveNorm);
  const preserveRuntimeUrl = !!(
    liveMatchesTarget
    || (
      sameReferenceSession
      && sameTabSession
      && ref
      && liveUrl
      && isUncommittedCurrentUrl(ref, liveUrl)
    )
  );
  if (!preserveRuntimeUrl) {
    await api.navigate(tab.url || 'about:blank');
  } else {
    state.browserCurrentUrl = liveUrl;
  }
  state.lastWebSrId = activeSrId || null;
  state.lastWebTabId = semanticTabId || null;

  const input = e('browser-url-input');
  if (input) input.value = preserveRuntimeUrl ? String(liveUrl || '') : String(tab.url || '');
  await syncUrlBarForActiveSurface();

  await api.markerSetContext({ srId: state.activeSrId, artifactId: null });
  await refreshUncommittedActionCue();
}

async function showArtifactSurface(artifactId) {
  const ref = getActiveReference();
  if (!ref) return;
  const artifact = (Array.isArray(ref.artifacts) ? ref.artifacts : []).find((item) => String((item && item.id) || '') === String(artifactId || ''));
  if (!artifact) return;
  const artifactType = normalizeArtifactType(artifact.type);

  await api.hide();
  e('browser-placeholder')?.classList.add('hidden');
  e('files-panel')?.classList.add('hidden');
  e('skills-panel')?.classList.add('hidden');
  e('mail-panel')?.classList.add('hidden');

  const editor = e('artifact-editor');
  const title = e('artifact-title');
  const input = e('artifact-input');
  if (!editor || !title || !input) return;

  editor.classList.remove('hidden');
  title.textContent = artifact.title || 'Artifact';

  state.suppressArtifactInput = true;
  input.value = String(artifact.content || '');
  input.readOnly = isMemoryReplayActive();
  state.suppressArtifactInput = false;
  if (artifactType === 'markdown') {
    if (state.htmlArtifactRuntime && state.htmlArtifactRuntime.running) {
      stopHtmlArtifactRuntime();
    }
    await refreshArtifactVisualState(String(artifact.content || ''), artifact.id);
  } else {
    state.artifactResolvedImages = [];
    state.artifactUnresolvedImages = [];
    state.artifactActiveImageIndex = 0;
    state.artifactActiveArtifactId = String(artifact.id || '').trim();
    applyArtifactModeLayout();
    renderActiveArtifactImage();
    ensureHtmlArtifactRuntime(artifact, String(artifact.content || ''), { focus: getArtifactViewMode(artifact.id, artifactType) !== ARTIFACT_VIEW_MODE_CODE, focusReason: 'show-artifact-surface' });
  }
  updateArtifactRuntimeControls(artifact);
  renderArtifactHighlightLayer();

  const status = e('artifact-status');
  if (status) status.textContent = 'Saved';
  await syncUrlBarForActiveSurface();

  await api.markerSetContext({ srId: state.activeSrId, artifactId: artifact.id });
}

function renderFilesPanel() {
  const body = e('files-body');
  const status = e('files-status');
  const ref = getActiveReference();
  const replayMode = isMemoryReplayActive();
  if (!body || !status) return;
  if (!ref) {
    status.textContent = 'No active reference';
    body.innerHTML = '<div class="muted">Select a reference to manage mounted folders.</div>';
    return;
  }

  const activeFilesTab = getActiveFilesTab(ref);
  const filesViewState = getNormalizedFilesViewState(activeFilesTab);
  const mounts = Array.isArray(ref.folder_mounts) ? ref.folder_mounts : [];
  const files = Array.isArray(ref.context_files) ? ref.context_files : [];
  let visibleMounts = mounts;
  let visibleFiles = files;
  let mountHeading = 'Mounted Folders';
  let filesHeading = 'Indexed Context Files';

  if (filesViewState.scope === 'mount' && filesViewState.mount_id) {
    visibleMounts = mounts.filter((mount) => String((mount && mount.id) || '').trim() === filesViewState.mount_id);
    visibleFiles = files.filter((file) => String((file && file.mount_id) || '').trim() === filesViewState.mount_id);
    mountHeading = 'Mounted Folder';
    filesHeading = 'Indexed Files';
  } else if (filesViewState.scope === 'context_file' && filesViewState.file_id) {
    visibleMounts = [];
    visibleFiles = files.filter((file) => String((file && file.id) || '').trim() === filesViewState.file_id);
    filesHeading = 'Added Context';
  }

  status.textContent = `${visibleMounts.length} mount(s) · ${visibleFiles.length} file(s)`;

  const mountMarkup = visibleMounts.length
    ? visibleMounts.map((mount) => {
      const pathText = escapeHtml(String((mount && mount.absolute_path) || ''));
      const count = Number((mount && mount.file_count) || 0);
      const skipped = Number((mount && mount.skipped_count) || 0);
      const reasonCounts = (mount && mount.skip_reason_counts && typeof mount.skip_reason_counts === 'object')
        ? mount.skip_reason_counts
        : {};
      const reasonLabelMap = {
        too_large: 'too large',
        unsupported_extension: 'unsupported type',
        read_failed: 'read failed',
        stat_failed: 'stat failed',
      };
      const reasonParts = Object.entries(reasonCounts)
        .map(([reason, rawCount]) => ({ reason: String(reason || '').trim(), count: Number(rawCount || 0) }))
        .filter((entry) => entry.reason && Number.isFinite(entry.count) && entry.count > 0)
        .map((entry) => `${reasonLabelMap[entry.reason] || entry.reason}: ${entry.count}`);
      const skipReasonMarkup = reasonParts.length > 0
        ? `<div class="files-item-sub muted small">Skip reasons: ${escapeHtml(reasonParts.join(', '))}</div>`
        : '';
      return `
        <div class="files-item" data-mount-id="${escapeHtml(String((mount && mount.id) || ''))}">
          <div class="files-item-title">${pathText || '(missing path)'}</div>
          <div class="files-item-sub">${count} indexed · ${skipped} skipped</div>
          ${skipReasonMarkup}
          <div class="files-item-actions">
            <button data-files-reindex="${escapeHtml(String((mount && mount.id) || ''))}" ${replayMode ? 'disabled' : ''}>Reindex</button>
            <button data-files-unmount="${escapeHtml(String((mount && mount.id) || ''))}" ${replayMode ? 'disabled' : ''}>Unmount</button>
          </div>
        </div>
      `;
    }).join('')
    : (
      filesViewState.scope === 'mount'
        ? '<div class="muted small">This mounted folder is no longer available.</div>'
        : '<div class="muted small">No folder mounts yet. Use workspace + and choose + folder.</div>'
    );

  const contextMarkup = visibleFiles.length
    ? visibleFiles.slice(0, 240).map((file) => {
      const fileId = String((file && file.id) || '').trim();
      const name = escapeHtml(String((file && file.relative_path) || (file && file.original_name) || 'context.txt'));
      const summary = escapeHtml(String((file && file.summary) || ''));
      const source = escapeHtml(String((file && file.source_type) || 'context'));
      return `
        <div class="files-item" data-file-id="${escapeHtml(fileId)}">
          <div class="files-item-title">${name}</div>
          <div class="files-item-sub">${source}</div>
          <div class="files-item-sub">${summary}</div>
          <div class="files-item-actions">
            <button data-files-preview="${escapeHtml(fileId)}">Preview</button>
            <button data-files-remove="${escapeHtml(fileId)}" ${replayMode ? 'disabled' : ''}>Remove</button>
          </div>
        </div>
      `;
    }).join('')
    : (
      filesViewState.scope === 'context_file'
        ? '<div class="muted small">This added context file is no longer available.</div>'
        : '<div class="muted small">No indexed context files.</div>'
    );

  body.innerHTML = [
    visibleMounts.length > 0 || filesViewState.scope !== 'context_file'
      ? `
        <div class="files-block">
          <h4>${mountHeading}</h4>
          ${mountMarkup}
        </div>
      `
      : '',
    `
      <div class="files-block">
        <h4>${filesHeading}</h4>
        ${contextMarkup}
      </div>
    `,
  ].join('');

  body.querySelectorAll('button[data-files-reindex]').forEach((button) => {
    button.addEventListener('click', async () => {
      if (blockIfMemoryReplay()) return;
      const mountId = String(button.getAttribute('data-files-reindex') || '').trim();
      if (!mountId || !state.activeSrId) return;
      const res = await api.srReindexFolderMount(state.activeSrId, mountId);
      if (!res || !res.ok) {
        window.alert((res && res.message) || 'Unable to reindex mount.');
        return;
      }
      state.references = res.references || await api.srList();
      const filesTabId = String((res && res.files_tab && res.files_tab.id) || '').trim();
      if (filesTabId) {
        state.activeSurface = makeActiveSurface('files', { filesTabId });
        rememberSurfaceForReference(state.activeSrId, state.activeSurface);
      }
      renderReferences();
      renderWorkspaceTabs();
      renderContextFiles();
      renderFilesPanel();
      renderDiffPanel();
      const indexed = Number((res && res.mount && res.mount.file_count) || 0);
      const skipped = Number((res && res.mount && res.mount.skipped_count) || 0);
      showPassiveNotification(`Reindex complete: ${indexed} indexed, ${skipped} skipped.`);
      await syncActiveSurface();
    });
  });

  body.querySelectorAll('button[data-files-unmount]').forEach((button) => {
    button.addEventListener('click', async () => {
      if (blockIfMemoryReplay()) return;
      const mountId = String(button.getAttribute('data-files-unmount') || '').trim();
      if (!mountId || !state.activeSrId) return;
      const ok = window.confirm('Unmount this folder and remove indexed context files from this reference?');
      if (!ok) return;
      const res = await api.srUnmountFolder(state.activeSrId, mountId);
      if (!res || !res.ok) {
        window.alert((res && res.message) || 'Unable to unmount folder.');
        return;
      }
      state.references = res.references || await api.srList();
      const removedTabIds = Array.isArray(res && res.removed_tab_ids) ? res.removed_tab_ids.map((id) => String(id || '').trim()) : [];
      if (state.activeSurface.kind === 'files' && removedTabIds.includes(String(state.activeSurface.filesTabId || '').trim())) {
        state.activeSurface = makeActiveSurface('web');
        rememberSurfaceForReference(state.activeSrId, state.activeSurface);
      }
      renderReferences();
      renderWorkspaceTabs();
      renderContextFiles();
      renderFilesPanel();
      renderDiffPanel();
      await syncActiveSurface();
    });
  });

  body.querySelectorAll('button[data-files-preview]').forEach((button) => {
    button.addEventListener('click', async () => {
      const fileId = String(button.getAttribute('data-files-preview') || '').trim();
      await previewContextFileById(fileId);
    });
  });

  body.querySelectorAll('button[data-files-remove]').forEach((button) => {
    button.addEventListener('click', async () => {
      if (blockIfMemoryReplay()) return;
      const fileId = String(button.getAttribute('data-files-remove') || '').trim();
      if (!fileId || !state.activeSrId) return;
      const res = await api.srRemoveContextFile(state.activeSrId, fileId);
      if (!res || !res.ok) {
        window.alert((res && res.message) || 'Unable to remove context file.');
        return;
      }
      state.references = res.references || await api.srList();
      const removedTabIds = Array.isArray(res && res.removed_tab_ids) ? res.removed_tab_ids.map((id) => String(id || '').trim()) : [];
      if (state.activeSurface.kind === 'files' && removedTabIds.includes(String(state.activeSurface.filesTabId || '').trim())) {
        state.activeSurface = makeActiveSurface('web');
        rememberSurfaceForReference(state.activeSrId, state.activeSurface);
      }
      renderReferences();
      renderWorkspaceTabs();
      renderContextFiles();
      renderFilesPanel();
      renderDiffPanel();
      await syncActiveSurface();
      void noteReferenceRankingInteraction('context_file_remove', { srId: state.activeSrId });
    });
  });
}

async function showFilesSurface(tabId) {
  const ref = getActiveReference();
  if (!ref) return;
  resetHtmlArtifactCodeViews(ref);
  stopHtmlArtifactRuntime();
  const tab = (Array.isArray(ref.tabs) ? ref.tabs : []).find((item) => String((item && item.id) || '') === String(tabId || ''));
  if (!tab || String((tab && tab.tab_kind) || '').trim().toLowerCase() !== 'files') return;

  await api.hide();
  e('browser-placeholder')?.classList.add('hidden');
  e('artifact-editor')?.classList.add('hidden');
  e('skills-panel')?.classList.add('hidden');
  e('mail-panel')?.classList.add('hidden');
  const panel = e('files-panel');
  if (panel) panel.classList.remove('hidden');
  renderFilesPanel();
  await syncUrlBarForActiveSurface();
  await api.markerSetContext({ srId: state.activeSrId, artifactId: null });
}

async function renderSkillsPanel() {
  const body = e('skills-body');
  const status = e('skills-status');
  const ref = getActiveReference();
  if (!body || !status) return;
  if (!ref || !state.activeSrId) {
    status.textContent = 'No active reference';
    body.innerHTML = '<div class="muted">Select a reference to manage skills.</div>';
    return;
  }

  if (isMemoryReplayActive()) {
    const linked = Array.isArray(ref.skills) ? ref.skills : [];
    status.textContent = `${linked.length} replay skill(s) · read-only`;
    body.innerHTML = `
      <div class="skills-block">
        <h4>Linked Skills (Replay)</h4>
        ${linked.length
          ? linked.map((skill) => `
            <div class="skill-item">
              <div class="skill-item-title">${escapeHtml(String(skill.name || skill.id || 'skill'))}</div>
              <div class="skill-item-sub">${escapeHtml(String(skill.description || ''))}</div>
            </div>
          `).join('')
          : '<div class="muted small">No linked skills in this memory checkpoint.</div>'}
      </div>
    `;
    return;
  }

  const listRes = await api.srListSkills(state.activeSrId);
  if (!listRes || !listRes.ok) {
    status.textContent = 'Skill list unavailable';
    body.innerHTML = `<div class="muted">${escapeHtml((listRes && listRes.message) || 'Unable to load skills.')}</div>`;
    return;
  }

  const linked = Array.isArray(listRes.linked_skills) ? listRes.linked_skills : [];
  const localSkills = Array.isArray(listRes.local_skills) ? listRes.local_skills : [];
  const globalSkills = Array.isArray(listRes.global_skills) ? listRes.global_skills : [];
  status.textContent = `${linked.length} linked · ${localSkills.length} local · ${globalSkills.length} global`;

  const linkedMarkup = linked.length
    ? linked.map((skill) => `
      <div class="skill-item" data-skill-id="${escapeHtml(String(skill.id || ''))}" data-skill-scope="${escapeHtml(String(skill.scope || 'local'))}">
        <div class="skill-item-title">${escapeHtml(String(skill.name || 'skill'))}</div>
        <div class="skill-item-sub">${escapeHtml(String(skill.description || ''))}</div>
        <details>
          <summary class="small muted">Code</summary>
          <pre>${escapeHtml(String(skill.code || ''))}</pre>
        </details>
        <div class="skill-item-actions">
          <button data-skill-run="${escapeHtml(String(skill.id || ''))}" data-skill-run-scope="${escapeHtml(String(skill.scope || 'local'))}">Run</button>
          <button data-skill-delete="${escapeHtml(String(skill.id || ''))}" data-skill-delete-scope="${escapeHtml(String(skill.scope || 'local'))}">Delete</button>
        </div>
      </div>
    `).join('')
    : '<div class="muted small">No linked skills for this reference yet.</div>';

  body.innerHTML = `
    <div class="skills-block">
      <h4>Linked Skills</h4>
      ${linkedMarkup}
    </div>
    <div class="skills-block">
      <h4>Save New Skill</h4>
      <div class="skill-form">
        <input id="skill-save-name" type="text" placeholder="Skill name" />
        <input id="skill-save-description" type="text" placeholder="Description (optional)" />
        <textarea id="skill-save-code" placeholder="Python code"></textarea>
        <div class="skill-form-row">
          <select id="skill-save-scope">
            <option value="local">Local</option>
            <option value="global">Global</option>
          </select>
          <button id="skill-save-btn">Save Skill</button>
        </div>
        <div id="skill-save-status" class="muted small"></div>
      </div>
    </div>
  `;

  body.querySelectorAll('button[data-skill-run]').forEach((button) => {
    button.addEventListener('click', async () => {
      const skillId = String(button.getAttribute('data-skill-run') || '').trim();
      const scope = String(button.getAttribute('data-skill-run-scope') || 'local').trim().toLowerCase();
      if (!skillId || !state.activeSrId) return;
      const runRes = await api.srRunSkill(state.activeSrId, skillId, scope, {});
      if (!runRes || !runRes.ok) {
        showPassiveNotification((runRes && runRes.stderr) || (runRes && runRes.message) || 'Skill run failed.');
        return;
      }
      if (Array.isArray(runRes.pending_workspace_tabs) && runRes.pending_workspace_tabs.length > 0) {
        await applyPendingUpdates({ pending_workspace_tabs: runRes.pending_workspace_tabs }, state.activeSrId);
      }
      showPassiveNotification('Skill executed.');
    });
  });

  body.querySelectorAll('button[data-skill-delete]').forEach((button) => {
    button.addEventListener('click', async () => {
      const skillId = String(button.getAttribute('data-skill-delete') || '').trim();
      const scope = String(button.getAttribute('data-skill-delete-scope') || 'local').trim().toLowerCase();
      if (!skillId || !state.activeSrId) return;
      const deleteRes = await api.srDeleteSkill(state.activeSrId, skillId, scope);
      if (!deleteRes || !deleteRes.ok) {
        showPassiveNotification((deleteRes && deleteRes.message) || 'Unable to delete skill.');
        return;
      }
      state.references = deleteRes.references || await api.srList();
      renderReferences();
      renderWorkspaceTabs();
      await renderSkillsPanel();
    });
  });

  const saveBtn = e('skill-save-btn');
  if (saveBtn) {
    saveBtn.addEventListener('click', async () => {
      const name = String((e('skill-save-name') && e('skill-save-name').value) || '').trim();
      const description = String((e('skill-save-description') && e('skill-save-description').value) || '').trim();
      const code = String((e('skill-save-code') && e('skill-save-code').value) || '');
      const scope = String((e('skill-save-scope') && e('skill-save-scope').value) || 'local').trim().toLowerCase();
      const statusNode = e('skill-save-status');
      if (!name || !code.trim()) {
        if (statusNode) statusNode.textContent = 'Name and code are required.';
        return;
      }
      const saveRes = await api.srSaveSkill(state.activeSrId, { name, description, code }, scope);
      if (!saveRes || !saveRes.ok) {
        if (statusNode) statusNode.textContent = (saveRes && saveRes.message) || 'Unable to save skill.';
        return;
      }
      if (statusNode) statusNode.textContent = `Saved "${name}".`;
      if (e('skill-save-code')) e('skill-save-code').value = '';
      state.references = saveRes.references || await api.srList();
      renderReferences();
      renderWorkspaceTabs();
      await renderSkillsPanel();
    });
  }
}

async function showSkillsSurface(tabId) {
  const ref = getActiveReference();
  if (!ref) return;
  resetHtmlArtifactCodeViews(ref);
  stopHtmlArtifactRuntime();
  const tab = (Array.isArray(ref.tabs) ? ref.tabs : []).find((item) => String((item && item.id) || '') === String(tabId || ''));
  if (!tab || String((tab && tab.tab_kind) || '').trim().toLowerCase() !== 'skills') return;

  await api.hide();
  e('browser-placeholder')?.classList.add('hidden');
  e('artifact-editor')?.classList.add('hidden');
  e('files-panel')?.classList.add('hidden');
  e('mail-panel')?.classList.add('hidden');
  const panel = e('skills-panel');
  if (panel) panel.classList.remove('hidden');
  await renderSkillsPanel();
  await syncUrlBarForActiveSurface();
  await api.markerSetContext({ srId: state.activeSrId, artifactId: null });
}

function getMailSearchState(srId) {
  const refId = resolveMailStateId(srId);
  return {
    query: String(state.mailSearchQueryByRef.get(refId) || '').trim(),
    results: Array.isArray(state.mailSearchResultsByRef.get(refId)) ? state.mailSearchResultsByRef.get(refId) : [],
    selected: state.mailSelectedSourceIdsByRef.get(refId) instanceof Set
      ? state.mailSelectedSourceIdsByRef.get(refId)
      : new Set(),
  };
}

function getMailNavState(srId) {
  const refId = resolveMailStateId(srId);
  const current = state.mailNavByRef.get(refId);
  if (current && typeof current === 'object') return current;
  const firstAccount = Array.isArray(state.mailAccounts) && state.mailAccounts.length ? state.mailAccounts[0] : null;
  return {
    account_id: String((firstAccount && firstAccount.id) || '').trim(),
    mailbox_path: '',
    smart_view: 'inbox',
  };
}

function setMailNavState(srId, patch = {}) {
  const refId = resolveMailStateId(srId);
  const next = {
    ...getMailNavState(refId),
    ...(patch && typeof patch === 'object' ? patch : {}),
  };
  state.mailNavByRef.set(refId, next);
  return next;
}

function setMailSelectedSource(srId, sourceId, checked) {
  const refId = resolveMailStateId(srId);
  const current = state.mailSelectedSourceIdsByRef.get(refId) instanceof Set
    ? new Set(state.mailSelectedSourceIdsByRef.get(refId))
    : new Set();
  if (checked) current.add(String(sourceId || '').trim());
  else current.delete(String(sourceId || '').trim());
  state.mailSelectedSourceIdsByRef.set(refId, current);
}

function getCommittedMailThreadIds(srId, results = []) {
  const refId = resolveMailStateId(srId);
  const committed = state.mailCommittedSelectedSourceIdsByRef.get(refId) instanceof Set
    ? state.mailCommittedSelectedSourceIdsByRef.get(refId)
    : new Set();
  const availableIds = new Set((Array.isArray(results) ? results : []).map((item) => String((item && item.id) || '').trim()).filter(Boolean));
  return Array.from(committed).filter((threadId) => availableIds.has(threadId));
}

function setCommittedMailThreadIds(srId, threadIds = []) {
  const refId = resolveMailStateId(srId);
  const next = new Set((Array.isArray(threadIds) ? threadIds : []).map((item) => String(item || '').trim()).filter(Boolean));
  state.mailCommittedSelectedSourceIdsByRef.set(refId, next);
}

function isMailSelectedViewEnabled(srId) {
  return !!state.mailSelectedViewByRef.get(resolveMailStateId(srId));
}

function setMailSelectedView(srId, enabled) {
  const refId = resolveMailStateId(srId);
  if (!refId) return;
  if (enabled) state.mailSelectedViewByRef.set(refId, true);
  else state.mailSelectedViewByRef.delete(refId);
}

function clearMailSelection(srId) {
  const refId = resolveMailStateId(srId);
  state.mailSelectedSourceIdsByRef.set(refId, new Set());
  state.mailCommittedSelectedSourceIdsByRef.set(refId, new Set());
  state.mailSelectedViewByRef.delete(refId);
}

function clearMailStateBucket(srId) {
  const refId = resolveMailStateId(srId);
  state.mailSearchQueryByRef.delete(refId);
  state.mailSearchResultsByRef.delete(refId);
  state.mailSelectedSourceIdsByRef.delete(refId);
  state.mailCommittedSelectedSourceIdsByRef.delete(refId);
  state.mailSelectedViewByRef.delete(refId);
  state.mailPreviewByRef.delete(refId);
  state.mailComposerByRef.delete(refId);
  state.mailNavByRef.delete(refId);
}

function getSelectedMailThreadIds(srId, results = []) {
  const selected = getMailSearchState(srId).selected;
  const availableIds = new Set((Array.isArray(results) ? results : []).map((item) => String((item && item.id) || '').trim()).filter(Boolean));
  return Array.from(selected).filter((threadId) => availableIds.has(threadId));
}

async function deleteMailThreads(threadIds = []) {
  const ids = Array.isArray(threadIds) ? threadIds.map((item) => String(item || '').trim()).filter(Boolean) : [];
  if (!ids.length) return { ok: false, deletedCount: 0, failed: [] };
  const failed = [];
  let deletedCount = 0;
  for (const threadId of ids) {
    const res = await api.mailDeleteThread(threadId);
    if (!res || !res.ok) {
      failed.push({
        threadId,
        message: (res && res.message) || 'Unable to move thread to trash.',
      });
      continue;
    }
    deletedCount += 1;
  }
  return {
    ok: failed.length === 0,
    deletedCount,
    failed,
  };
}

function removeMailThreadsFromLocalState(threadIds = []) {
  const ids = new Set((Array.isArray(threadIds) ? threadIds : []).map((item) => String(item || '').trim()).filter(Boolean));
  if (!ids.size) return;
  state.mailSearchResultsByRef.forEach((results, key) => {
    const list = Array.isArray(results) ? results : [];
    state.mailSearchResultsByRef.set(key, list.filter((item) => !ids.has(String((item && item.id) || '').trim())));
  });
  state.mailPreviewByRef.forEach((preview, key) => {
    const previewThreadId = String((preview && preview.thread_id) || '').trim();
    if (previewThreadId && ids.has(previewThreadId)) state.mailPreviewByRef.delete(key);
  });
}

function resolveMailThreadCounterparty(item = {}) {
  const accountEmail = String((item && item.account_email) || '').trim().toLowerCase();
  const participants = Array.isArray(item && item.participants) ? item.participants : [];
  const others = participants
    .map((value) => String(value || '').trim())
    .filter(Boolean)
    .filter((value) => value.toLowerCase() !== accountEmail);
  if (others.length > 0) {
    if (others.length === 1) return others[0];
    return `${others[0]} +${others.length - 1}`;
  }
  const sender = String((item && item.from) || '').trim();
  return sender || 'Unknown sender';
}

function getMailPreviewState(srId) {
  return state.mailPreviewByRef.get(resolveMailStateId(srId)) || null;
}

function getMailComposerState(srId) {
  const refId = resolveMailStateId(srId);
  const current = state.mailComposerByRef.get(refId);
  if (current && typeof current === 'object') return current;
  const accounts = Array.isArray(state.mailAccounts) ? state.mailAccounts : [];
  return {
    open: false,
    mode: 'compose',
    account_id: String(((accounts[0] || {}).id) || '').trim(),
    to: '',
    cc: '',
    bcc: '',
    subject: '',
    body_text: '',
    in_reply_to: '',
    references: [],
    attachments: [],
    draft_key: '',
  };
}

function setMailComposerState(srId, patch = {}) {
  const refId = resolveMailStateId(srId);
  const current = getMailComposerState(refId);
  const next = {
    ...current,
    ...(patch && typeof patch === 'object' ? patch : {}),
  };
  state.mailComposerByRef.set(refId, next);
  return next;
}

function closeMailComposer(srId) {
  const refId = resolveMailStateId(srId);
  const current = getMailComposerState(refId);
  state.mailComposerByRef.set(refId, { ...current, open: false });
}

function setMailPreviewState(srId, next) {
  const refId = resolveMailStateId(srId);
  if (!refId) return;
  if (!next) {
    state.mailPreviewByRef.delete(refId);
    return;
  }
  state.mailPreviewByRef.set(refId, next);
}

async function loadMailSourcePreview(srId, source) {
  const res = await api.mailPreviewSource(source || {});
  if (!res || !res.ok) return null;
  const preview = res.preview || null;
  if (!preview) return null;
  setMailPreviewState(srId, {
    kind: 'search',
    source_id: String((source && source.source_id) || '').trim(),
    preview,
  });
  return preview;
}

function resolveMailStateId(srId) {
  const raw = String(srId || '').trim();
  return raw || GLOBAL_MAIL_VIEW_ID;
}

function getActiveWorkspaceMailStateId() {
  if (state.activeSurface && state.activeSurface.kind === 'mail' && state.activeSurface.mailTabId) {
    return String(state.activeSurface.mailTabId || '').trim();
  }
  const ref = getActiveReference();
  const activeMailTab = getActiveMailTab(ref);
  return String((activeMailTab && activeMailTab.id) || '').trim();
}

function normalizeMailUiWhitespace(value = '') {
  return String(value || '').replace(/\s+/g, ' ').trim();
}

const MAIL_WINDOWS_1252_REVERSE_MAP = new Map([
  [0x20ac, 0x80],
  [0x201a, 0x82],
  [0x0192, 0x83],
  [0x201e, 0x84],
  [0x2026, 0x85],
  [0x2020, 0x86],
  [0x2021, 0x87],
  [0x02c6, 0x88],
  [0x2030, 0x89],
  [0x0160, 0x8a],
  [0x2039, 0x8b],
  [0x0152, 0x8c],
  [0x017d, 0x8e],
  [0x2018, 0x91],
  [0x2019, 0x92],
  [0x201c, 0x93],
  [0x201d, 0x94],
  [0x2022, 0x95],
  [0x2013, 0x96],
  [0x2014, 0x97],
  [0x02dc, 0x98],
  [0x2122, 0x99],
  [0x0161, 0x9a],
  [0x203a, 0x9b],
  [0x0153, 0x9c],
  [0x017e, 0x9e],
  [0x0178, 0x9f],
]);

function scoreMailMojibake(value = '') {
  const text = String(value || '');
  if (!text) return 0;
  const suspicious = text.match(/(?:Ã.|Â.|â[\u0080-\u00bf€™œ\u009d€¢–—…]|ðŸ|ï¿½)/g) || [];
  const replacement = text.match(/\uFFFD/g) || [];
  return (suspicious.length * 3) + (replacement.length * 5);
}

function repairLikelyMailMojibake(value = '') {
  const text = String(value || '');
  if (!text || !/(?:Ã.|Â.|â[\u0080-\u00ff]|ðŸ|ï¿½)/.test(text) || typeof TextDecoder !== 'function') return text;
  const bytes = [];
  for (const char of text) {
    const code = char.codePointAt(0);
    if (code <= 0xff) {
      bytes.push(code);
      continue;
    }
    const mapped = MAIL_WINDOWS_1252_REVERSE_MAP.get(code);
    if (typeof mapped !== 'number') return text;
    bytes.push(mapped);
  }
  try {
    const candidate = new TextDecoder('utf-8', { fatal: false }).decode(Uint8Array.from(bytes));
    return scoreMailMojibake(candidate) < scoreMailMojibake(text) ? candidate : text;
  } catch (_) {
    return text;
  }
}

function decodeMailText(value = '') {
  let text = repairLikelyMailMojibake(value);
  const replacements = [
    ['â€™', '\''],
    ['â€˜', '\''],
    ['â€œ', '"'],
    ['â€\u009d', '"'],
    ['â€¢', '•'],
    ['â€“', '-'],
    ['â€”', '-'],
    ['â€¦', '...'],
    ['Â', ' '],
  ];
  replacements.forEach(([from, to]) => {
    text = text.split(from).join(to);
  });
  if (typeof document !== 'undefined' && document && typeof document.createElement === 'function') {
    const textarea = document.createElement('textarea');
    textarea.innerHTML = text;
    text = textarea.value;
  }
  return text;
}

function stripMailHtmlForDisplay(value = '') {
  return decodeMailText(
    String(value || '')
      .replace(/<style[\s\S]*?<\/style>/gi, ' ')
      .replace(/<script[\s\S]*?<\/script>/gi, ' ')
      .replace(/<(br|\/p|\/div|\/li|\/tr|\/h[1-6])>/gi, '\n')
      .replace(/<li[^>]*>/gi, '- ')
      .replace(/<[^>]+>/g, ' ')
      .replace(/&nbsp;/gi, ' ')
      .replace(/&amp;/gi, '&')
      .replace(/&lt;/gi, '<')
      .replace(/&gt;/gi, '>')
      .replace(/&quot;/gi, '"')
      .replace(/&apos;/gi, '\'')
  )
    .replace(/\r\n/g, '\n')
    .replace(/\r/g, '\n');
}

function normalizeMailSnippet(value = '') {
  const text = decodeMailText(value)
    .replace(/\r\n/g, '\n')
    .replace(/\r/g, '\n');
  const lines = text
    .split('\n')
    .map((line) => String(line || '').trim())
    .filter(Boolean);
  const kept = [];
  for (const line of lines) {
    if (/^on .+ wrote:$/i.test(line)) break;
    if (/^(from|to|cc|bcc|date|subject):/i.test(line)) continue;
    if (/^>+/.test(line)) continue;
    kept.push(line);
    if (kept.join(' ').length >= 220) break;
  }
  return normalizeMailUiWhitespace(kept.join(' '))
    .replace(/\s*https?:\/\/\S+/gi, '')
    .trim()
    .slice(0, 220);
}

function formatMailFolderLabel(mailbox = {}) {
  const role = String((mailbox && mailbox.special_use) || '').trim().toLowerCase();
  if (MAIL_SMART_FOLDER_LABELS[role]) return MAIL_SMART_FOLDER_LABELS[role];
  const raw = String((mailbox && (mailbox.name || mailbox.path)) || 'Mailbox').trim();
  const tail = raw.split('/').pop() || raw;
  return tail
    .replace(/[_-]+/g, ' ')
    .replace(/\s+/g, ' ')
    .trim()
    .replace(/\b\w/g, (ch) => ch.toUpperCase());
}

function buildMailNavItems(mailboxes = []) {
  const smartItems = ['inbox', 'unread', 'sent', 'drafts', 'archive', 'trash'].map((key) => ({
    key,
    label: MAIL_SMART_FOLDER_LABELS[key],
    type: 'smart',
  }));
  const seen = new Set(smartItems.map((item) => `smart:${item.key}`));
  const seenMailboxRoles = new Set();
  const mailboxItems = [];
  (Array.isArray(mailboxes) ? mailboxes : []).forEach((mailbox) => {
    const role = String((mailbox && mailbox.special_use) || '').trim().toLowerCase();
    const path = String((mailbox && mailbox.path) || '').trim();
    if (!path) return;
    if (role && MAIL_SMART_FOLDER_LABELS[role] && role !== 'junk') return;
    if (role === 'junk') {
      if (seenMailboxRoles.has(role)) return;
      seenMailboxRoles.add(role);
    }
    if (!role && path.toLowerCase() === 'inbox') return;
    const key = `mailbox:${path.toLowerCase()}`;
    if (seen.has(key)) return;
    seen.add(key);
    mailboxItems.push({
      key: path,
      label: formatMailFolderLabel(mailbox),
      type: 'mailbox',
    });
  });
  return smartItems.concat(mailboxItems);
}

function renderMailPreviewMarkup(preview) {
  if (!preview) return '<div class="muted small">Select a thread above to preview it.</div>';
  const formatAddressLine = (label, values) => {
    const items = Array.isArray(values) ? values.map((item) => String(item || '').trim()).filter(Boolean) : [];
    if (!items.length) return '';
    return `
      <div class="mail-meta-line">
        <span class="mail-meta-label">${escapeHtml(label)}</span>
        <span>${escapeHtml(items.join(', '))}</span>
      </div>
    `;
  };
  const formatMailBodyForDisplay = (value) => {
    const text = decodeMailText(value).replace(/\r\n/g, '\n').replace(/\r/g, '\n').trim();
    if (!text) return '<div class="muted small">No readable body content.</div>';
    const lines = text.split('\n');
    const out = [];
    let paragraph = [];
    let quote = [];
    const flushParagraph = () => {
      if (!paragraph.length) return;
      out.push(`<p>${escapeHtml(paragraph.join(' '))}</p>`);
      paragraph = [];
    };
    const flushQuote = () => {
      if (!quote.length) return;
      out.push(`<blockquote class="mail-quote-block">${escapeHtml(quote.join('\n'))}</blockquote>`);
      quote = [];
    };
    lines.forEach((rawLine) => {
      const line = String(rawLine || '');
      const trimmed = line.trim();
      const isQuoteBoundary = /^On .+ wrote:$/i.test(trimmed);
      const isQuoted = /^>+/.test(trimmed);
      if (!trimmed) {
        flushParagraph();
        flushQuote();
        return;
      }
      if (isQuoteBoundary || isQuoted) {
        flushParagraph();
        quote.push(trimmed);
        return;
      }
      if (/^(from|to|cc|bcc|date|subject):/i.test(trimmed) && quote.length) {
        quote.push(trimmed);
        return;
      }
      flushQuote();
      paragraph.push(trimmed);
    });
    flushParagraph();
    flushQuote();
    return out.join('');
  };
  const attachmentsMarkup = Array.isArray(preview.attachments) && preview.attachments.length
    ? `
      <div class="mail-attachment-list">
        ${preview.attachments.map((attachment) => `
          <div class="mail-attachment">
            <span>${escapeHtml(String((attachment && attachment.file_name) || 'attachment'))}</span>
            <span class="muted small">${escapeHtml(String((attachment && attachment.mime_type) || 'file'))}</span>
          </div>
        `).join('')}
      </div>
    `
    : '';
  const meta = [
    String((preview && preview.account_name) || '').trim(),
    String((preview && preview.mailbox_name) || '').trim(),
    String((preview && preview.sent_at) || '').trim(),
  ].filter(Boolean).join(' · ');
  const unread = !!(preview && preview.is_unread);
  const bodyValue = String(
    (preview && preview.body_text)
    || stripMailHtmlForDisplay(preview && preview.body_html)
    || (preview && preview.snippet)
    || ''
  );
  return `
    <div class="mail-message ${unread ? 'unread' : ''}">
      <div class="mail-message-head">
        <div class="mail-row-title">${escapeHtml(String((preview && preview.subject) || 'Message'))}</div>
        <div class="mail-row-sub muted small">${escapeHtml(meta)}</div>
        ${formatAddressLine('From', [String((preview && preview.from) || '').trim()])}
        ${formatAddressLine('To', preview && preview.to)}
        ${formatAddressLine('Cc', preview && preview.cc)}
      </div>
      <div class="mail-message-body-wrap">
        <div class="mail-message-body">${formatMailBodyForDisplay(bodyValue)}</div>
      </div>
      ${attachmentsMarkup}
    </div>
  `;
}

function orderMailThreadMessages(messages = []) {
  return (Array.isArray(messages) ? messages.slice() : []).sort((a, b) => {
    const sentDiff = Number((b && b.sent_ts) || 0) - Number((a && a.sent_ts) || 0);
    if (sentDiff !== 0) return sentDiff;
    return Number((b && b.uid) || 0) - Number((a && a.uid) || 0);
  });
}

async function runMailSearch(srId, query) {
  const refId = String(srId || '').trim();
  const nav = getMailNavState(refId);
  state.mailSearchQueryByRef.set(refId, String(query || '').trim());
  const res = await api.mailSearchLocalThreads(
    String(query || '').trim(),
    80,
    true,
    '',
    nav.account_id || '',
    nav.mailbox_path || '',
    nav.smart_view || '',
  );
  state.mailSearchResultsByRef.set(refId, (res && Array.isArray(res.items)) ? res.items : []);
  state.mailStatus = await api.mailStatus();
  setMailPreviewState(srId, null);
  return res;
}

async function refreshVisibleMailStoreState(viewId, accountId = '') {
  const targetViewId = String(viewId || '').trim();
  if (!targetViewId) return;
  const targetAccountId = String(accountId || '').trim();
  const nav = getMailNavState(targetViewId);
  const activeAccountId = String(nav.account_id || '').trim();
  if (targetAccountId && activeAccountId && activeAccountId !== targetAccountId) return;
  const searchState = getMailSearchState(targetViewId);
  const previewState = getMailPreviewState(targetViewId);
  const previewThreadId = String((previewState && previewState.thread_id) || '').trim();
  await runMailSearch(targetViewId, searchState.query || '');
  if (!previewThreadId) return;
  const previewRes = await api.mailPreviewSource(previewThreadId);
  if (previewRes && previewRes.ok && previewRes.thread) {
    setMailPreviewState(targetViewId, { thread_id: previewThreadId, thread: previewRes.thread });
  }
}

async function openMailThreadPreview(viewId, threadId) {
  const id = String(threadId || '').trim();
  if (!id) return null;
  let res = await api.mailPreviewSource(id);
  if (!res || !res.ok || !res.thread) return null;
  let thread = res.thread;
  const hasUnread = Array.isArray(thread.messages) && thread.messages.some((message) => !!message.is_unread);
  if (hasUnread) {
    const updateRes = await api.mailUpdateThreadState(id, { is_unread: false });
    if (updateRes && updateRes.ok) {
      await runMailSearch(viewId, getMailSearchState(viewId).query || '');
      res = await api.mailPreviewSource(id);
      if (res && res.ok && res.thread) thread = res.thread;
    }
  }
  setMailPreviewState(viewId, { thread_id: id, thread });
  return thread;
}

function buildReplyDraftFromPreview(previewState = null) {
  const thread = previewState && previewState.thread ? previewState.thread : null;
  const messages = Array.isArray(thread && thread.messages) ? thread.messages : [];
  const last = messages.length ? messages[messages.length - 1] : null;
  const subject = String((last && last.subject) || (thread && thread.subject) || '').trim();
  const normalizedSubject = /^re:/i.test(subject) ? subject : `Re: ${subject}`;
  const references = messages.map((message) => String((message && message.message_id_header) || '').trim()).filter(Boolean);
  const quoted = String((last && last.body_text) || '').trim()
    .split('\n')
    .map((line) => `> ${line}`)
    .join('\n');
  return {
    open: true,
    mode: 'reply',
    account_id: String((thread && thread.account_id) || (last && last.account_id) || '').trim(),
    to: String((last && last.from) || '').trim(),
    cc: '',
    bcc: '',
    subject: normalizedSubject.trim(),
    body_text: quoted ? `\n\n${quoted}` : '',
    in_reply_to: String((last && last.message_id_header) || '').trim(),
    references,
    attachments: [],
    draft_key: '',
  };
}

async function renderMailPanel() {
  const body = e('mail-body');
  const status = e('mail-status');
  const ref = getActiveReference();
  if (!body || !status) return;
  if (!ref || !state.activeSrId) {
    status.textContent = 'No active reference';
    body.innerHTML = '<div class="muted">Select a reference to manage mail.</div>';
    return;
  }
  const activeMailTab = getActiveMailTab(ref);
  const mailStateId = String((activeMailTab && activeMailTab.id) || '').trim();
  if (!activeMailTab || !mailStateId) {
    status.textContent = 'No active mail tab';
    body.innerHTML = '<div class="muted">Open a mail tab for this reference.</div>';
    return;
  }
  const tabViewState = activeMailTab && activeMailTab.mail_view_state && typeof activeMailTab.mail_view_state === 'object'
    ? activeMailTab.mail_view_state
    : {};
  if (!state.mailSearchQueryByRef.has(mailStateId) && tabViewState.query) {
    state.mailSearchQueryByRef.set(mailStateId, String(tabViewState.query || '').trim());
  }
  if (!state.mailNavByRef.has(mailStateId)) {
    state.mailNavByRef.set(mailStateId, {
      account_id: String(tabViewState.account_id || '').trim(),
      mailbox_path: String(tabViewState.mailbox_path || '').trim(),
      smart_view: String(tabViewState.smart_view || 'inbox').trim() || 'inbox',
    });
  }

  const listRes = await api.srListMailThreads(state.activeSrId);
  const attachedThreads = (listRes && listRes.ok && Array.isArray(listRes.threads)) ? listRes.threads : [];
  const searchState = getMailSearchState(mailStateId);
  const nav = getMailNavState(mailStateId);
  const query = searchState.query;
  let results = searchState.results;
  const selectedSources = searchState.selected;
  let previewState = getMailPreviewState(mailStateId);
  const composer = getMailComposerState(mailStateId);
  const mailStatus = state.mailStatus || await api.mailStatus();
  state.mailStatus = mailStatus || null;
  const accountOptions = Array.isArray(state.mailAccounts) ? state.mailAccounts : [];
  const activeAccountId = nav.account_id || String(((accountOptions[0] || {}).id) || '').trim();
  if (!nav.account_id && activeAccountId) setMailNavState(mailStateId, { account_id: activeAccountId });
  const activeMailboxes = state.mailboxesByAccount.get(activeAccountId) || [];
  if (!Array.isArray(results) || results.length === 0) {
    const res = await api.mailSearchLocalThreads(query, 80, true, '', activeAccountId, nav.mailbox_path || '', nav.smart_view || '');
    results = (res && Array.isArray(res.items)) ? res.items : [];
    state.mailSearchResultsByRef.set(mailStateId, results);
  }

  const selectedThreadIds = getSelectedMailThreadIds(mailStateId, results);
  const committedThreadIds = getCommittedMailThreadIds(mailStateId, results);
  const committedThreadIdSet = new Set(committedThreadIds);
  const selectionMode = committedThreadIds.length > 0 && isMailSelectedViewEnabled(mailStateId);
  const visibleResults = selectionMode
    ? results.filter((item) => committedThreadIdSet.has(String((item && item.id) || '').trim()))
    : results;
  if (previewState && previewState.thread_id && selectionMode && !committedThreadIdSet.has(String(previewState.thread_id || '').trim())) {
    setMailPreviewState(mailStateId, null);
    previewState = null;
  }

  status.textContent = `${attachedThreads.length} attached thread(s) · ${escapeHtml(String((mailStatus && mailStatus.message) || ''))}`;
  const navItems = buildMailNavItems(activeMailboxes);
  const threadListMarkup = visibleResults.length
    ? visibleResults.map((item) => {
      const threadId = String((item && item.id) || '').trim();
      const isActive = previewState && String((previewState && previewState.thread_id) || '') === threadId;
      const isSelected = selectedSources.has(threadId);
      const isUnread = Number((item && item.unread_count) || 0) > 0;
      const sender = resolveMailThreadCounterparty(item);
      return `
        <div class="mail-list-row ${isActive ? 'active' : ''} ${isUnread ? 'unread' : ''}">
          <button type="button" class="mail-list-row-main" data-mail-thread-preview="${escapeHtml(threadId)}">
            <div class="mail-row-sub mail-row-sender">
              <span class="mail-row-sender-name">${escapeHtml(sender)}</span>
            </div>
            <div class="mail-row-title ${isUnread ? 'unread' : ''}">${escapeHtml(String((item && item.subject) || 'Untitled thread'))}</div>
          </button>
          <div class="mail-row-side">
            ${isUnread ? `<span class="mail-row-count">${escapeHtml(String(item.unread_count || 0))}</span>` : ''}
            <button type="button" class="mail-row-select-btn ${isSelected ? 'active' : ''}" data-mail-thread-select="${escapeHtml(threadId)}">
              ${selectionMode ? 'Remove' : (isSelected ? 'Selected' : 'Select')}
            </button>
          </div>
        </div>
      `;
    }).join('')
    : (selectionMode
      ? '<div class="muted small">No selected threads left. Use Back to return to the full list.</div>'
      : '<div class="muted small">No synced threads yet. Add a mailbox in Settings and run sync.</div>');

  let contentMarkup = '<div class="muted small">Select a thread above to preview it.</div>';
  if (previewState && previewState.thread && Array.isArray(previewState.thread.messages)) {
    contentMarkup = orderMailThreadMessages(previewState.thread.messages).map((message) => renderMailPreviewMarkup(message)).join('');
  }

  const composeMarkup = composer && composer.open ? `
    <div class="mail-block mail-compose-block">
      <h4>${escapeHtml(composer.mode === 'reply' ? 'Reply' : (composer.mode === 'draft' ? 'Draft' : 'Compose'))}</h4>
      <div class="mail-compose-grid">
        <label class="settings-field">Account
          <select id="mail-compose-account">
            ${accountOptions.map((account) => `
              <option value="${escapeHtml(String((account && account.id) || ''))}" ${String((account && account.id) || '') === String(composer.account_id || '') ? 'selected' : ''}>
                ${escapeHtml(String((account && account.label) || (account && account.email) || 'Mailbox'))}
              </option>
            `).join('')}
          </select>
        </label>
        <label class="settings-field">To
          <input id="mail-compose-to" type="text" value="${escapeHtml(String(composer.to || ''))}" />
        </label>
        <label class="settings-field">Cc
          <input id="mail-compose-cc" type="text" value="${escapeHtml(String(composer.cc || ''))}" />
        </label>
        <label class="settings-field">Bcc
          <input id="mail-compose-bcc" type="text" value="${escapeHtml(String(composer.bcc || ''))}" />
        </label>
        <label class="settings-field mail-compose-subject">Subject
          <input id="mail-compose-subject" type="text" value="${escapeHtml(String(composer.subject || ''))}" />
        </label>
        <label class="settings-field mail-compose-body">Body
          <textarea id="mail-compose-body">${escapeHtml(String(composer.body_text || ''))}</textarea>
        </label>
      </div>
      <div class="mail-attachment-list mail-compose-attachments">
        ${(Array.isArray(composer.attachments) ? composer.attachments : []).map((attachment) => `
          <div class="mail-attachment">
            <span>${escapeHtml(String((attachment && attachment.file_name) || 'attachment'))}</span>
            <button type="button" class="mail-attachment-remove" data-mail-compose-remove-attachment="${escapeHtml(String((attachment && attachment.source_path) || '').trim())}">Remove</button>
          </div>
        `).join('') || '<div class="muted small">No attachments added.</div>'}
      </div>
      <div class="settings-inline-actions">
        <button id="mail-compose-attach-btn">Add Attachment</button>
        <button id="mail-compose-save-draft-btn">Save Draft</button>
        <button id="mail-compose-send-btn">Send</button>
        <button id="mail-compose-cancel-btn">Close</button>
      </div>
    </div>
  ` : '';

  const persistMailTabState = async (patch = {}) => {
    if (!activeMailTab || !state.activeSrId) return;
    const res = await api.srPatchTab(state.activeSrId, activeMailTab.id, {
      mail_view_state: {
        ...(activeMailTab.mail_view_state || {}),
        ...patch,
      },
    });
    if (res && res.ok) state.references = res.references || state.references;
  };

  body.innerHTML = `
    <div class="mail-layout">
    <div class="mail-sidebar mail-block">
      <div class="mail-sidebar-section">
        <label class="settings-field">Account
          <select id="mail-account-select">
            ${accountOptions.map((account) => `
              <option value="${escapeHtml(String((account && account.id) || ''))}" ${String((account && account.id) || '') === activeAccountId ? 'selected' : ''}>
                ${escapeHtml(String((account && account.label) || (account && account.email) || 'Mailbox'))}
              </option>
            `).join('')}
          </select>
        </label>
      </div>
      <div class="mail-sidebar-section">
        <div class="mail-sidebar-title">Folders</div>
        <div class="mail-folder-list">
          ${navItems.map((item) => {
            const isActive = item.type === 'mailbox'
              ? String(nav.mailbox_path || '') === String(item.key || '')
              : String(nav.smart_view || 'inbox') === String(item.key || '');
            return `
              <button type="button" class="mail-folder-item ${isActive ? 'active' : ''}" data-mail-folder-type="${escapeHtml(item.type)}" data-mail-folder-key="${escapeHtml(String(item.key || ''))}">
                ${escapeHtml(String(item.label || 'Folder'))}
              </button>
            `;
          }).join('')}
        </div>
      </div>
    </div>
    <div class="mail-main ${composer && composer.open ? 'mail-main-compose' : ''}">
    <div class="mail-toolbar">
      <input id="mail-search-input" type="text" value="${escapeHtml(query)}" placeholder="Search your Mail accounts" />
      <button id="mail-search-btn">Search</button>
      <button id="mail-refresh-btn">Refresh</button>
      ${selectionMode ? '<button id="mail-back-btn">Back</button>' : ''}
      ${!selectionMode && selectedThreadIds.length > 0 ? '<button id="mail-attach-btn">Add Selected</button>' : ''}
      <button id="mail-compose-open-btn">Compose</button>
      ${previewState && previewState.thread ? '<button id="mail-reply-btn">Reply</button>' : ''}
      ${previewState && previewState.thread && previewState.thread.capabilities && previewState.thread.capabilities.supports_archive ? '<button id="mail-archive-btn">Archive</button>' : ''}
      ${previewState && previewState.thread ? '<button id="mail-toggle-read-btn">Read/Unread</button>' : ''}
      ${(selectionMode || (previewState && previewState.thread)) ? '<button id="mail-delete-btn">Trash</button>' : ''}
    </div>
    ${composer && composer.open ? `
      <div class="mail-composer-view">
        ${composeMarkup.replace('mail-compose-block', 'mail-compose-block mail-compose-block-full')}
      </div>
    ` : `
      <div class="mail-block mail-list-block">
        <h4>${selectionMode ? 'Selected Threads' : 'Threads'}${(selectionMode ? committedThreadIds.length : selectedThreadIds.length) > 0 ? ` <span class="muted small">${selectionMode ? committedThreadIds.length : selectedThreadIds.length} selected</span>` : ''}</h4>
        <div class="mail-list-scroll">
          ${threadListMarkup}
        </div>
      </div>
      <div class="mail-block mail-content-block mail-thread-view">
        <h4>Content</h4>
        <div class="mail-content-scroll">
          ${contentMarkup}
        </div>
      </div>
    `}
    </div>
    </div>
  `;

  e('mail-search-btn')?.addEventListener('click', async () => {
    const nextQuery = (e('mail-search-input') && e('mail-search-input').value) || '';
    clearMailSelection(mailStateId);
    setMailPreviewState(mailStateId, null);
    await runMailSearch(mailStateId, nextQuery);
    await persistMailTabState({ query: String(nextQuery || '').trim() });
    await renderMailPanel();
  });
  e('mail-search-input')?.addEventListener('keydown', async (event) => {
    if (event.key !== 'Enter') return;
    event.preventDefault();
    const nextQuery = (e('mail-search-input') && e('mail-search-input').value) || '';
    clearMailSelection(mailStateId);
    setMailPreviewState(mailStateId, null);
    await runMailSearch(mailStateId, nextQuery);
    await persistMailTabState({ query: String(nextQuery || '').trim() });
    await renderMailPanel();
  });
  e('mail-account-select')?.addEventListener('change', async (event) => {
    setMailNavState(mailStateId, {
      account_id: String((event.target && event.target.value) || '').trim(),
      mailbox_path: '',
      smart_view: 'inbox',
    });
    clearMailSelection(mailStateId);
    state.mailSearchResultsByRef.delete(mailStateId);
    setMailPreviewState(mailStateId, null);
    await persistMailTabState({ account_id: String((event.target && event.target.value) || '').trim(), mailbox_path: '', smart_view: 'inbox' });
    await renderMailPanel();
  });
  body.querySelectorAll('button[data-mail-folder-key]').forEach((node) => {
    node.addEventListener('click', async () => {
      const type = String(node.getAttribute('data-mail-folder-type') || '').trim();
      const key = String(node.getAttribute('data-mail-folder-key') || '').trim();
      setMailNavState(mailStateId, {
        mailbox_path: type === 'mailbox' ? key : '',
        smart_view: type === 'smart' ? key : '',
      });
      clearMailSelection(mailStateId);
      state.mailSearchResultsByRef.delete(mailStateId);
      setMailPreviewState(mailStateId, null);
      await persistMailTabState({
        mailbox_path: type === 'mailbox' ? key : '',
        smart_view: type === 'smart' ? key : '',
      });
      await renderMailPanel();
    });
  });
  e('mail-back-btn')?.addEventListener('click', async () => {
    setMailSelectedView(mailStateId, false);
    setMailPreviewState(mailStateId, null);
    await renderMailPanel();
  });
  e('mail-attach-btn')?.addEventListener('click', async () => {
    const chosenThreadIds = getSelectedMailThreadIds(mailStateId, results);
    if (!chosenThreadIds.length) {
      showPassiveNotification('Select at least one mail thread to add.');
      return;
    }
    const res = await api.srAttachMailThreadsFromStore(state.activeSrId, chosenThreadIds, activeMailTab.id);
    if (!res || !res.ok) {
      window.alert((res && res.message) || 'Unable to attach mail threads.');
      return;
    }
    state.references = res.references || await api.srList();
    if (res.tab && res.tab.id) {
      state.activeSurface = makeActiveSurface('mail', { mailTabId: String(res.tab.id || '').trim() });
      rememberSurfaceForReference(state.activeSrId, state.activeSurface);
    }
    setCommittedMailThreadIds(mailStateId, chosenThreadIds);
    setMailSelectedView(mailStateId, true);
    setMailPreviewState(mailStateId, null);
    renderReferences();
    renderWorkspaceTabs();
    await renderMailPanel();
    showPassiveNotification(`${chosenThreadIds.length} mail thread(s) added to this reference.`);
  });
  e('mail-refresh-btn')?.addEventListener('click', async () => {
    if (!activeAccountId) return;
    const res = await api.mailSyncAccount(activeAccountId);
    if (!res || !res.ok) {
      window.alert((res && res.message) || 'Unable to sync mailbox.');
      return;
    }
    if (Array.isArray(res.accounts)) state.mailAccounts = res.accounts;
    if (Array.isArray(res.mailboxes)) state.mailboxesByAccount.set(activeAccountId, res.mailboxes);
    clearMailSelection(mailStateId);
    setMailPreviewState(mailStateId, null);
    await runMailSearch(mailStateId, query);
    await renderMailPanel();
  });
  e('mail-compose-open-btn')?.addEventListener('click', async () => {
    const next = getMailComposerState(mailStateId);
    setMailComposerState(mailStateId, {
      ...next,
      open: true,
      mode: 'compose',
      account_id: next.account_id || String(((accountOptions[0] || {}).id) || '').trim(),
      attachments: [],
      draft_key: '',
    });
    await renderMailPanel();
  });
  e('mail-reply-btn')?.addEventListener('click', async () => {
    setMailComposerState(mailStateId, buildReplyDraftFromPreview(previewState));
    await renderMailPanel();
  });
  e('mail-compose-cancel-btn')?.addEventListener('click', async () => {
    closeMailComposer(mailStateId);
    await renderMailPanel();
  });
  e('mail-compose-attach-btn')?.addEventListener('click', async () => {
    const pick = await api.mailPickAttachments();
    if (!pick || !pick.ok || !Array.isArray(pick.attachments)) return;
    const current = getMailComposerState(mailStateId);
    setMailComposerState(mailStateId, {
      attachments: current.attachments.concat(pick.attachments),
    });
    await renderMailPanel();
  });
  e('mail-compose-save-draft-btn')?.addEventListener('click', async () => {
    const accountId = String((e('mail-compose-account') && e('mail-compose-account').value) || '').trim();
    const current = getMailComposerState(mailStateId);
    const payload = {
      to: String((e('mail-compose-to') && e('mail-compose-to').value) || '').trim().split(',').map((item) => item.trim()).filter(Boolean),
      cc: String((e('mail-compose-cc') && e('mail-compose-cc').value) || '').trim().split(',').map((item) => item.trim()).filter(Boolean),
      bcc: String((e('mail-compose-bcc') && e('mail-compose-bcc').value) || '').trim().split(',').map((item) => item.trim()).filter(Boolean),
      subject: String((e('mail-compose-subject') && e('mail-compose-subject').value) || '').trim(),
      body_text: String((e('mail-compose-body') && e('mail-compose-body').value) || ''),
      in_reply_to: String(current.in_reply_to || '').trim(),
      references: Array.isArray(current.references) ? current.references : [],
      attachments: Array.isArray(current.attachments) ? current.attachments : [],
      draft_key: String(current.draft_key || '').trim(),
    };
    const res = await api.mailSaveDraft(accountId, payload);
    if (!res || !res.ok) {
      window.alert((res && res.message) || 'Unable to save draft.');
      return;
    }
    setMailComposerState(mailStateId, { draft_key: String((res && res.draft_key) || current.draft_key || '').trim() });
    showPassiveNotification('Draft saved.');
  });
  e('mail-compose-send-btn')?.addEventListener('click', async () => {
    const accountId = String((e('mail-compose-account') && e('mail-compose-account').value) || '').trim();
    const current = getMailComposerState(mailStateId);
    const payload = {
      to: String((e('mail-compose-to') && e('mail-compose-to').value) || '').trim().split(',').map((item) => item.trim()).filter(Boolean),
      cc: String((e('mail-compose-cc') && e('mail-compose-cc').value) || '').trim().split(',').map((item) => item.trim()).filter(Boolean),
      bcc: String((e('mail-compose-bcc') && e('mail-compose-bcc').value) || '').trim().split(',').map((item) => item.trim()).filter(Boolean),
      subject: String((e('mail-compose-subject') && e('mail-compose-subject').value) || '').trim(),
      body_text: String((e('mail-compose-body') && e('mail-compose-body').value) || ''),
      in_reply_to: String(current.in_reply_to || '').trim(),
      references: Array.isArray(current.references) ? current.references : [],
      attachments: Array.isArray(current.attachments) ? current.attachments : [],
    };
    const res = await api.mailSendMessage(accountId, payload);
    if (!res || !res.ok) {
      window.alert((res && res.message) || 'Unable to send mail.');
      return;
    }
    state.mailAccounts = Array.isArray(res.accounts) ? res.accounts : state.mailAccounts;
    state.mailStatus = await api.mailStatus();
    closeMailComposer(mailStateId);
    await runMailSearch(mailStateId, query);
    await renderMailPanel();
  });
  body.querySelectorAll('button[data-mail-compose-remove-attachment]').forEach((node) => {
    node.addEventListener('click', async () => {
      const target = String(node.getAttribute('data-mail-compose-remove-attachment') || '').trim();
      const current = getMailComposerState(mailStateId);
      setMailComposerState(mailStateId, {
        attachments: (Array.isArray(current.attachments) ? current.attachments : []).filter((item) => String((item && item.source_path) || '').trim() !== target),
      });
      await renderMailPanel();
    });
  });
  e('mail-toggle-read-btn')?.addEventListener('click', async () => {
    const thread = previewState && previewState.thread ? previewState.thread : null;
    if (!thread) return;
    const unread = Array.isArray(thread.messages) ? thread.messages.some((message) => !!message.is_unread) : false;
    const res = await api.mailUpdateThreadState(String(thread.id || '').trim(), { is_unread: !unread });
    if (!res || !res.ok) {
      window.alert((res && res.message) || 'Unable to update thread state.');
      return;
    }
    await runMailSearch(mailStateId, query);
    const previewRes = await api.mailPreviewSource(String(thread.id || '').trim());
    if (previewRes && previewRes.ok && previewRes.thread) setMailPreviewState(mailStateId, { thread_id: String(thread.id || '').trim(), thread: previewRes.thread });
    await renderMailPanel();
  });
  e('mail-archive-btn')?.addEventListener('click', async () => {
    const thread = previewState && previewState.thread ? previewState.thread : null;
    if (!thread) return;
    const res = await api.mailMoveThread(String(thread.id || '').trim(), 'archive');
    if (!res || !res.ok) {
      window.alert((res && res.message) || 'Unable to archive thread.');
      return;
    }
    setMailPreviewState(mailStateId, null);
    await runMailSearch(mailStateId, query);
    await renderMailPanel();
  });
  e('mail-delete-btn')?.addEventListener('click', async () => {
    const thread = previewState && previewState.thread ? previewState.thread : null;
    const selectedThreadIds = getSelectedMailThreadIds(mailStateId, results);
    const threadIds = selectedThreadIds.length
      ? selectedThreadIds
      : (thread && thread.id ? [String(thread.id || '').trim()] : []);
    if (!threadIds.length) {
      showPassiveNotification('Select or open a mail thread first.');
      return;
    }
    const confirmed = window.confirm(
      threadIds.length === 1
        ? 'Move this thread to trash?'
        : `Move ${threadIds.length} selected threads to trash?`
    );
    if (!confirmed) return;
    showPassiveNotification(threadIds.length === 1 ? 'Moving thread to trash...' : `Moving ${threadIds.length} threads to trash...`, 1800);
    const deleteRes = await deleteMailThreads(threadIds);
    if (!deleteRes.ok) {
      clearMailSelection(mailStateId);
      setMailPreviewState(mailStateId, null);
      await runMailSearch(mailStateId, query);
      await renderMailPanel();
      const summary = deleteRes.deletedCount > 0
        ? `${deleteRes.deletedCount} thread(s) moved to trash, ${deleteRes.failed.length} failed.`
        : ((deleteRes.failed[0] && deleteRes.failed[0].message) || 'Unable to move thread to trash.');
      window.alert(summary);
      return;
    }
    removeMailThreadsFromLocalState(threadIds);
    clearMailSelection(mailStateId);
    setMailPreviewState(mailStateId, null);
    await renderMailPanel();
    await runMailSearch(mailStateId, query);
    await renderMailPanel();
    await refreshTopbarBadges();
    showPassiveNotification(
      threadIds.length === 1
        ? 'Mail thread moved to trash.'
        : `${threadIds.length} mail thread(s) moved to trash.`
    );
  });
  body.querySelectorAll('button[data-mail-thread-preview]').forEach((node) => {
    node.addEventListener('click', async () => {
      const threadId = String(node.getAttribute('data-mail-thread-preview') || '').trim();
      if (!threadId) return;
      const thread = await openMailThreadPreview(mailStateId, threadId);
      if (!thread) return;
      await renderMailPanel();
    });
  });
  body.querySelectorAll('button[data-mail-thread-select]').forEach((node) => {
    node.addEventListener('click', async (event) => {
      event.stopPropagation();
      const threadId = String(node.getAttribute('data-mail-thread-select') || '').trim();
      if (!threadId) return;
      const nextChecked = !selectedSources.has(threadId);
      setMailSelectedSource(mailStateId, threadId, nextChecked);
      if (selectionMode && !nextChecked) {
        const nextCommitted = committedThreadIds.filter((id) => id !== threadId);
        setCommittedMailThreadIds(mailStateId, nextCommitted);
        if (!nextCommitted.length) {
          setMailSelectedView(mailStateId, false);
        }
        setMailPreviewState(mailStateId, null);
      }
      await renderMailPanel();
    });
  });
}

async function renderGlobalMailPage() {
  const body = e('global-mail-body');
  const status = e('global-mail-status');
  const statusLine = e('global-mail-status-line');
  if (!body || !status || !statusLine) return;
  const viewId = GLOBAL_MAIL_VIEW_ID;
  const stateId = resolveMailStateId(viewId);
  const searchState = getMailSearchState(viewId);
  const nav = getMailNavState(viewId);
  const query = searchState.query;
  let results = searchState.results;
  const previewState = getMailPreviewState(viewId);
  const composer = getMailComposerState(viewId);
  const mailStatus = state.mailStatus || await api.mailStatus();
  state.mailStatus = mailStatus || null;
  const accountOptions = Array.isArray(state.mailAccounts) ? state.mailAccounts : [];
  const activeAccountId = nav.account_id || String(((accountOptions[0] || {}).id) || '').trim();
  const activeAccount = getMailAccountById(activeAccountId);
  if (!nav.account_id && activeAccountId) setMailNavState(viewId, { account_id: activeAccountId });
  const activeMailboxes = state.mailboxesByAccount.get(activeAccountId) || [];
  if (!Array.isArray(results) || results.length === 0) {
    const res = await api.mailSearchLocalThreads(query, 80, true, '', activeAccountId, nav.mailbox_path || '', nav.smart_view || '');
    results = (res && Array.isArray(res.items)) ? res.items : [];
    state.mailSearchResultsByRef.set(stateId, results);
  }
  status.textContent = String((mailStatus && mailStatus.message) || 'Mail');
  statusLine.textContent = activeAccount
    ? describeMailAccountStatus(activeAccount)
    : 'No mailbox account configured.';
  const navItems = buildMailNavItems(activeMailboxes);
  const threadListMarkup = results.length
    ? results.map((item) => {
      const threadId = String((item && item.id) || '').trim();
      const isActive = previewState && String((previewState && previewState.thread_id) || '') === threadId;
      const isUnread = Number((item && item.unread_count) || 0) > 0;
      const sender = resolveMailThreadCounterparty(item);
      return `
        <div class="mail-list-row ${isActive ? 'active' : ''} ${isUnread ? 'unread' : ''}">
          <button type="button" class="mail-list-row-main" data-global-mail-thread-preview="${escapeHtml(threadId)}">
            <div class="mail-row-sub mail-row-sender">
              <span class="mail-row-sender-name">${escapeHtml(sender)}</span>
            </div>
            <div class="mail-row-title ${isUnread ? 'unread' : ''}">${escapeHtml(String((item && item.subject) || 'Untitled thread'))}</div>
          </button>
          <div class="mail-row-side">
            ${isUnread ? `<span class="mail-row-count">${escapeHtml(String(item.unread_count || 0))}</span>` : ''}
          </div>
        </div>
      `;
    }).join('')
    : '<div class="muted small">No synced threads yet. Add an account in Settings and run sync.</div>';

  let contentMarkup = '<div class="muted small">Select a thread to preview it.</div>';
  if (previewState && previewState.thread && Array.isArray(previewState.thread.messages)) {
    contentMarkup = orderMailThreadMessages(previewState.thread.messages).map((message) => renderMailPreviewMarkup(message)).join('');
  }

  const composeMarkup = composer && composer.open ? `
    <div class="mail-block mail-compose-block">
      <h4>${escapeHtml(composer.mode === 'reply' ? 'Reply' : 'Compose')}</h4>
      <div class="mail-compose-grid">
        <label class="settings-field">Account
          <select id="global-mail-compose-account">
            ${accountOptions.map((account) => `
              <option value="${escapeHtml(String((account && account.id) || ''))}" ${String((account && account.id) || '') === String(composer.account_id || '') ? 'selected' : ''}>
                ${escapeHtml(String((account && account.label) || (account && account.email) || 'Mailbox'))}
              </option>
            `).join('')}
          </select>
        </label>
        <label class="settings-field">To
          <input id="global-mail-compose-to" type="text" value="${escapeHtml(String(composer.to || ''))}" />
        </label>
        <label class="settings-field">Cc
          <input id="global-mail-compose-cc" type="text" value="${escapeHtml(String(composer.cc || ''))}" />
        </label>
        <label class="settings-field">Bcc
          <input id="global-mail-compose-bcc" type="text" value="${escapeHtml(String(composer.bcc || ''))}" />
        </label>
        <label class="settings-field mail-compose-subject">Subject
          <input id="global-mail-compose-subject" type="text" value="${escapeHtml(String(composer.subject || ''))}" />
        </label>
        <label class="settings-field mail-compose-body">Body
          <textarea id="global-mail-compose-body">${escapeHtml(String(composer.body_text || ''))}</textarea>
        </label>
      </div>
      <div class="mail-attachment-list mail-compose-attachments">
        ${(Array.isArray(composer.attachments) ? composer.attachments : []).map((attachment) => `
          <div class="mail-attachment">
            <span>${escapeHtml(String((attachment && attachment.file_name) || 'attachment'))}</span>
            <button type="button" class="mail-attachment-remove" data-global-mail-compose-remove-attachment="${escapeHtml(String((attachment && attachment.source_path) || '').trim())}">Remove</button>
          </div>
        `).join('') || '<div class="muted small">No attachments added.</div>'}
      </div>
      <div class="settings-inline-actions">
        <button id="global-mail-compose-attach-btn">Add Attachment</button>
        <button id="global-mail-compose-save-draft-btn">Save Draft</button>
        <button id="global-mail-compose-send-btn">Send</button>
        <button id="global-mail-compose-cancel-btn">Close</button>
      </div>
    </div>
  ` : '';

  body.innerHTML = `
    <div class="mail-layout">
      <div class="mail-sidebar mail-block">
        <div class="mail-sidebar-section">
          <label class="settings-field">Account
            <select id="global-mail-account-select">
              ${accountOptions.map((account) => `
                <option value="${escapeHtml(String((account && account.id) || ''))}" ${String((account && account.id) || '') === activeAccountId ? 'selected' : ''}>
                  ${escapeHtml(String((account && account.label) || (account && account.email) || 'Mailbox'))}
                </option>
              `).join('')}
            </select>
          </label>
        </div>
        <div class="mail-sidebar-section">
          <div class="mail-sidebar-title">Folders</div>
          <div class="mail-folder-list">
            ${navItems.map((item) => {
              const isActive = item.type === 'mailbox'
                ? String(nav.mailbox_path || '') === String(item.key || '')
                : String(nav.smart_view || 'inbox') === String(item.key || '');
              return `<button type="button" class="mail-folder-item ${isActive ? 'active' : ''}" data-global-mail-folder-type="${escapeHtml(item.type)}" data-global-mail-folder-key="${escapeHtml(String(item.key || ''))}">${escapeHtml(String(item.label || 'Folder'))}</button>`;
            }).join('')}
          </div>
        </div>
      </div>
      <div class="mail-main ${composer && composer.open ? 'mail-main-compose' : ''}">
        <div class="mail-toolbar">
          <input id="global-mail-search-input" type="text" value="${escapeHtml(query)}" placeholder="Search your Mail accounts" />
          <button id="global-mail-search-btn">Search</button>
          <button id="global-mail-refresh-btn">Refresh</button>
          <button id="global-mail-compose-open-btn">Compose</button>
          ${previewState && previewState.thread ? '<button id="global-mail-reply-btn">Reply</button>' : ''}
          ${previewState && previewState.thread && previewState.thread.capabilities && previewState.thread.capabilities.supports_archive ? '<button id="global-mail-archive-btn">Archive</button>' : ''}
          ${previewState && previewState.thread ? '<button id="global-mail-toggle-read-btn">Read/Unread</button>' : ''}
          ${previewState && previewState.thread ? '<button id="global-mail-delete-btn">Trash</button>' : ''}
        </div>
        ${composer && composer.open ? `
          <div class="mail-composer-view">
            ${composeMarkup.replace('mail-compose-block', 'mail-compose-block mail-compose-block-full')}
          </div>
        ` : `
          <div class="mail-block mail-list-block">
            <h4>Threads</h4>
            <div class="mail-list-scroll">${threadListMarkup}</div>
          </div>
          <div class="mail-block mail-content-block mail-thread-view">
            <h4>Content</h4>
            <div class="mail-content-scroll">${contentMarkup}</div>
          </div>
        `}
      </div>
    </div>
  `;

  e('global-mail-search-btn')?.addEventListener('click', async () => {
    const nextQuery = (e('global-mail-search-input') && e('global-mail-search-input').value) || '';
    await runMailSearch(viewId, nextQuery);
    await renderGlobalMailPage();
  });
  e('global-mail-search-input')?.addEventListener('keydown', async (event) => {
    if (event.key !== 'Enter') return;
    event.preventDefault();
    const nextQuery = (e('global-mail-search-input') && e('global-mail-search-input').value) || '';
    await runMailSearch(viewId, nextQuery);
    await renderGlobalMailPage();
  });
  e('global-mail-account-select')?.addEventListener('change', async (event) => {
    setMailNavState(viewId, { account_id: String((event.target && event.target.value) || '').trim(), mailbox_path: '', smart_view: 'inbox' });
    state.mailSearchResultsByRef.delete(stateId);
    setMailPreviewState(viewId, null);
    await renderGlobalMailPage();
  });
  body.querySelectorAll('button[data-global-mail-folder-key]').forEach((node) => {
    node.addEventListener('click', async () => {
      const type = String(node.getAttribute('data-global-mail-folder-type') || '').trim();
      const key = String(node.getAttribute('data-global-mail-folder-key') || '').trim();
      setMailNavState(viewId, {
        mailbox_path: type === 'mailbox' ? key : '',
        smart_view: type === 'smart' ? key : '',
      });
      state.mailSearchResultsByRef.delete(stateId);
      setMailPreviewState(viewId, null);
      await renderGlobalMailPage();
    });
  });
  e('global-mail-refresh-btn')?.addEventListener('click', async () => {
    if (!activeAccountId) return;
    const res = await api.mailSyncAccount(activeAccountId);
    if (!res || !res.ok) {
      window.alert((res && res.message) || 'Unable to sync mailbox.');
      return;
    }
    if (Array.isArray(res.accounts)) state.mailAccounts = res.accounts;
    if (Array.isArray(res.mailboxes)) state.mailboxesByAccount.set(activeAccountId, res.mailboxes);
    await runMailSearch(viewId, query);
    await renderGlobalMailPage();
  });
  e('global-mail-compose-open-btn')?.addEventListener('click', async () => {
    setMailComposerState(viewId, { ...getMailComposerState(viewId), open: true, mode: 'compose', account_id: activeAccountId, attachments: [], draft_key: '' });
    await renderGlobalMailPage();
  });
  e('global-mail-reply-btn')?.addEventListener('click', async () => {
    setMailComposerState(viewId, buildReplyDraftFromPreview(previewState));
    await renderGlobalMailPage();
  });
  e('global-mail-compose-cancel-btn')?.addEventListener('click', async () => {
    closeMailComposer(viewId);
    await renderGlobalMailPage();
  });
  e('global-mail-compose-attach-btn')?.addEventListener('click', async () => {
    const pick = await api.mailPickAttachments();
    if (!pick || !pick.ok || !Array.isArray(pick.attachments)) return;
    const current = getMailComposerState(viewId);
    setMailComposerState(viewId, { attachments: current.attachments.concat(pick.attachments) });
    await renderGlobalMailPage();
  });
  e('global-mail-compose-save-draft-btn')?.addEventListener('click', async () => {
    const accountId = String((e('global-mail-compose-account') && e('global-mail-compose-account').value) || '').trim();
    const current = getMailComposerState(viewId);
    const res = await api.mailSaveDraft(accountId, {
      to: String((e('global-mail-compose-to') && e('global-mail-compose-to').value) || '').trim().split(',').map((item) => item.trim()).filter(Boolean),
      cc: String((e('global-mail-compose-cc') && e('global-mail-compose-cc').value) || '').trim().split(',').map((item) => item.trim()).filter(Boolean),
      bcc: String((e('global-mail-compose-bcc') && e('global-mail-compose-bcc').value) || '').trim().split(',').map((item) => item.trim()).filter(Boolean),
      subject: String((e('global-mail-compose-subject') && e('global-mail-compose-subject').value) || '').trim(),
      body_text: String((e('global-mail-compose-body') && e('global-mail-compose-body').value) || ''),
      in_reply_to: String(current.in_reply_to || '').trim(),
      references: Array.isArray(current.references) ? current.references : [],
      attachments: Array.isArray(current.attachments) ? current.attachments : [],
      draft_key: String(current.draft_key || '').trim(),
    });
    if (!res || !res.ok) {
      window.alert((res && res.message) || 'Unable to save draft.');
      return;
    }
    setMailComposerState(viewId, { draft_key: String((res && res.draft_key) || current.draft_key || '').trim() });
    showPassiveNotification('Draft saved.');
  });
  e('global-mail-compose-send-btn')?.addEventListener('click', async () => {
    const accountId = String((e('global-mail-compose-account') && e('global-mail-compose-account').value) || '').trim();
    const current = getMailComposerState(viewId);
    const res = await api.mailSendMessage(accountId, {
      to: String((e('global-mail-compose-to') && e('global-mail-compose-to').value) || '').trim().split(',').map((item) => item.trim()).filter(Boolean),
      cc: String((e('global-mail-compose-cc') && e('global-mail-compose-cc').value) || '').trim().split(',').map((item) => item.trim()).filter(Boolean),
      bcc: String((e('global-mail-compose-bcc') && e('global-mail-compose-bcc').value) || '').trim().split(',').map((item) => item.trim()).filter(Boolean),
      subject: String((e('global-mail-compose-subject') && e('global-mail-compose-subject').value) || '').trim(),
      body_text: String((e('global-mail-compose-body') && e('global-mail-compose-body').value) || ''),
      in_reply_to: String(current.in_reply_to || '').trim(),
      references: Array.isArray(current.references) ? current.references : [],
      attachments: Array.isArray(current.attachments) ? current.attachments : [],
    });
    if (!res || !res.ok) {
      window.alert((res && res.message) || 'Unable to send mail.');
      return;
    }
    state.mailAccounts = Array.isArray(res.accounts) ? res.accounts : state.mailAccounts;
    closeMailComposer(viewId);
    await runMailSearch(viewId, query);
    await renderGlobalMailPage();
  });
  body.querySelectorAll('button[data-global-mail-compose-remove-attachment]').forEach((node) => {
    node.addEventListener('click', async () => {
      const target = String(node.getAttribute('data-global-mail-compose-remove-attachment') || '').trim();
      const current = getMailComposerState(viewId);
      setMailComposerState(viewId, {
        attachments: (Array.isArray(current.attachments) ? current.attachments : []).filter((item) => String((item && item.source_path) || '').trim() !== target),
      });
      await renderGlobalMailPage();
    });
  });
  e('global-mail-toggle-read-btn')?.addEventListener('click', async () => {
    const thread = previewState && previewState.thread ? previewState.thread : null;
    if (!thread) return;
    const unread = Array.isArray(thread.messages) ? thread.messages.some((message) => !!message.is_unread) : false;
    const res = await api.mailUpdateThreadState(String(thread.id || '').trim(), { is_unread: !unread });
    if (!res || !res.ok) {
      window.alert((res && res.message) || 'Unable to update thread state.');
      return;
    }
    await runMailSearch(viewId, query);
    const previewRes = await api.mailPreviewSource(String(thread.id || '').trim());
    if (previewRes && previewRes.ok && previewRes.thread) setMailPreviewState(viewId, { thread_id: String(thread.id || '').trim(), thread: previewRes.thread });
    await renderGlobalMailPage();
  });
  e('global-mail-archive-btn')?.addEventListener('click', async () => {
    const thread = previewState && previewState.thread ? previewState.thread : null;
    if (!thread) return;
    const res = await api.mailMoveThread(String(thread.id || '').trim(), 'archive');
    if (!res || !res.ok) {
      window.alert((res && res.message) || 'Unable to archive thread.');
      return;
    }
    setMailPreviewState(viewId, null);
    await runMailSearch(viewId, query);
    await renderGlobalMailPage();
  });
  e('global-mail-delete-btn')?.addEventListener('click', async () => {
    const thread = previewState && previewState.thread ? previewState.thread : null;
    if (!thread) return;
    const confirmed = window.confirm('Move this thread to trash?');
    if (!confirmed) return;
    showPassiveNotification('Moving thread to trash...', 1800);
    const res = await api.mailDeleteThread(String(thread.id || '').trim());
    if (!res || !res.ok) {
      window.alert((res && res.message) || 'Unable to move thread to trash.');
      return;
    }
    removeMailThreadsFromLocalState([String(thread.id || '').trim()]);
    setMailPreviewState(viewId, null);
    await renderGlobalMailPage();
    await runMailSearch(viewId, query);
    await renderGlobalMailPage();
    await refreshTopbarBadges();
    showPassiveNotification('Mail thread moved to trash.');
  });
  body.querySelectorAll('button[data-global-mail-thread-preview]').forEach((node) => {
    node.addEventListener('click', async () => {
      const threadId = String(node.getAttribute('data-global-mail-thread-preview') || '').trim();
      if (!threadId) return;
      const thread = await openMailThreadPreview(viewId, threadId);
      if (!thread) return;
      await renderGlobalMailPage();
    });
  });
}

async function openMailPage() {
  await setAppView('mail');
  await refreshMailStatus();
  await refreshMailAccounts();
  await renderGlobalMailPage();
  await refreshTopbarBadges();
}

async function handleMailEventPayload(payload = {}) {
  const type = String((payload && payload.type) || '').trim().toLowerCase();
  if (!type) return;
  if (type === 'sync_status') {
    const phase = String((payload && payload.phase) || '').trim().toLowerCase();
    const accountId = String((payload && payload.account_id) || '').trim();
    await refreshMailAccounts();
    state.mailStatus = await api.mailStatus();
    renderMailSettingsStatus();
    if (phase === 'finished') {
      if (state.appView === 'mail') {
        await refreshVisibleMailStoreState(GLOBAL_MAIL_VIEW_ID, accountId);
      }
      if (state.appView === 'workspace' && state.activeSurface.kind === 'mail') {
        await refreshVisibleMailStoreState(getActiveWorkspaceMailStateId(), accountId);
      }
    }
    if (state.appView === 'mail') await renderGlobalMailPage();
    if (state.appView === 'workspace' && state.activeSurface.kind === 'mail') await renderMailPanel();
    return;
  }
  if (type !== 'open_thread') return;
  const viewId = GLOBAL_MAIL_VIEW_ID;
  const accountId = String((payload && payload.account_id) || '').trim();
  const mailboxPath = String((payload && payload.mailbox_path) || '').trim();
  const smartView = String((payload && payload.smart_view) || '').trim();
  const threadId = String((payload && payload.thread_id) || '').trim();
  await openMailPage();
  setMailNavState(viewId, {
    account_id: accountId,
    mailbox_path: mailboxPath,
    smart_view: smartView,
  });
  state.mailSearchResultsByRef.delete(resolveMailStateId(viewId));
  if (threadId) {
    const previewRes = await api.mailPreviewSource(threadId);
    if (previewRes && previewRes.ok && previewRes.thread) {
      setMailPreviewState(viewId, { thread_id: threadId, thread: previewRes.thread });
    }
  }
  await runMailSearch(viewId, getMailSearchState(viewId).query || '');
  await renderGlobalMailPage();
}

async function showMailSurface(tabId) {
  const ref = getActiveReference();
  if (!ref) return;
  resetHtmlArtifactCodeViews(ref);
  stopHtmlArtifactRuntime();
  const tab = (Array.isArray(ref.tabs) ? ref.tabs : []).find((item) => String((item && item.id) || '') === String(tabId || ''));
  if (!tab || String((tab && tab.tab_kind) || '').trim().toLowerCase() !== 'mail') return;

  await api.hide();
  e('browser-placeholder')?.classList.add('hidden');
  e('artifact-editor')?.classList.add('hidden');
  e('files-panel')?.classList.add('hidden');
  e('skills-panel')?.classList.add('hidden');
  const panel = e('mail-panel');
  if (panel) panel.classList.remove('hidden');
  await refreshMailAccounts();
  await refreshMailStatus();
  await renderMailPanel();
  await syncUrlBarForActiveSurface();
  await api.markerSetContext({ srId: state.activeSrId, artifactId: null });
}

async function syncActiveSurface() {
  if (
    state.appView === 'hyperweb'
    || state.appView === 'private-shares'
    || state.appView === 'settings'
    || state.appView === 'history'
  ) {
    resetHtmlArtifactCodeViews();
    stopHtmlArtifactRuntime();
    await api.hide();
    return;
  }
  if (hasBlockingOverlay()) {
    resetHtmlArtifactCodeViews();
    stopHtmlArtifactRuntime();
    await api.hide();
    return;
  }

  const ref = getActiveReference();
  if (!ref) {
    resetHtmlArtifactCodeViews();
    stopHtmlArtifactRuntime();
    await api.hide();
    await syncUrlBarForActiveSurface();
    return;
  }

  const tabs = Array.isArray(ref.tabs) ? ref.tabs : [];
  const artifacts = Array.isArray(ref.artifacts) ? ref.artifacts : [];

  const rememberedArtifactId = String(state.activeArtifactByRef.get(String(ref.id || '')) || '').trim();
  const rememberedFilesTabId = String(state.activeFilesByRef.get(String(ref.id || '')) || '').trim();
  const rememberedSkillsTabId = String(state.activeSkillsByRef.get(String(ref.id || '')) || '').trim();
  const rememberedMailTabId = String(state.activeMailByRef.get(String(ref.id || '')) || '').trim();
  if (state.activeSurface.kind === 'web') {
    if (rememberedArtifactId) {
      state.activeSurface = makeActiveSurface('artifact', { artifactId: rememberedArtifactId });
    } else if (rememberedFilesTabId) {
      state.activeSurface = makeActiveSurface('files', { filesTabId: rememberedFilesTabId });
    } else if (rememberedSkillsTabId) {
      state.activeSurface = makeActiveSurface('skills', { skillsTabId: rememberedSkillsTabId });
    } else if (rememberedMailTabId) {
      state.activeSurface = makeActiveSurface('mail', { mailTabId: rememberedMailTabId });
    }
  }

  if (state.activeSurface.kind === 'artifact') {
    const artifactExists = artifacts.some((artifact) => String((artifact && artifact.id) || '') === String(state.activeSurface.artifactId || ''));
    if (artifactExists) {
      rememberSurfaceForReference(state.activeSrId, state.activeSurface);
      await showArtifactSurface(state.activeSurface.artifactId);
      return;
    }
    state.activeSurface = makeActiveSurface('web');
    rememberSurfaceForReference(state.activeSrId, state.activeSurface);
  }

  if (state.activeSurface.kind === 'files') {
    const filesExists = tabs.some((tab) => (
      String((tab && tab.id) || '') === String(state.activeSurface.filesTabId || '')
      && String((tab && tab.tab_kind) || '').trim().toLowerCase() === 'files'
    ));
    if (filesExists) {
      rememberSurfaceForReference(state.activeSrId, state.activeSurface);
      await showFilesSurface(state.activeSurface.filesTabId);
      return;
    }
    state.activeSurface = makeActiveSurface('web');
    rememberSurfaceForReference(state.activeSrId, state.activeSurface);
  }

  if (state.activeSurface.kind === 'skills') {
    const skillsExists = tabs.some((tab) => (
      String((tab && tab.id) || '') === String(state.activeSurface.skillsTabId || '')
      && String((tab && tab.tab_kind) || '').trim().toLowerCase() === 'skills'
    ));
    if (skillsExists) {
      rememberSurfaceForReference(state.activeSrId, state.activeSurface);
      await showSkillsSurface(state.activeSurface.skillsTabId);
      return;
    }
    state.activeSurface = makeActiveSurface('web');
    rememberSurfaceForReference(state.activeSrId, state.activeSurface);
  }

  if (state.activeSurface.kind === 'mail') {
    const mailExists = tabs.some((tab) => (
      String((tab && tab.id) || '') === String(state.activeSurface.mailTabId || '')
      && String((tab && tab.tab_kind) || '').trim().toLowerCase() === 'mail'
    ));
    if (mailExists) {
      rememberSurfaceForReference(state.activeSrId, state.activeSurface);
      await showMailSurface(state.activeSurface.mailTabId);
      return;
    }
    state.activeSurface = makeActiveSurface('web');
    rememberSurfaceForReference(state.activeSrId, state.activeSurface);
  }

  const activeWeb = getActiveWebTab(ref);
  if (activeWeb) {
    state.activeSurface = makeActiveSurface('web', { tabId: activeWeb.id });
    rememberSurfaceForReference(state.activeSrId, state.activeSurface);
    await showWebSurface(activeWeb);
    return;
  }

  const activeFiles = getActiveFilesTab(ref);
  if (activeFiles) {
    state.activeSurface = makeActiveSurface('files', { filesTabId: activeFiles.id });
    rememberSurfaceForReference(state.activeSrId, state.activeSurface);
    await showFilesSurface(activeFiles.id);
    return;
  }

  const activeSkills = getActiveSkillsTab(ref);
  if (activeSkills) {
    state.activeSurface = makeActiveSurface('skills', { skillsTabId: activeSkills.id });
    rememberSurfaceForReference(state.activeSrId, state.activeSurface);
    await showSkillsSurface(activeSkills.id);
    return;
  }

  const activeMail = getActiveMailTab(ref);
  if (activeMail) {
    state.activeSurface = makeActiveSurface('mail', { mailTabId: activeMail.id });
    rememberSurfaceForReference(state.activeSrId, state.activeSurface);
    await showMailSurface(activeMail.id);
    return;
  }

  await api.hide();
  hideNonWebSurfaces();
  e('browser-placeholder')?.classList.remove('hidden');
  await syncUrlBarForActiveSurface();
}

function renderContextFiles() {
  const ref = getActiveReference();
  if (!ref) return;
  if (state.activeSurface.kind === 'files') renderFilesPanel();
}

function getDiffQueue(srId) {
  const key = String(srId || '').trim();
  if (!state.diffQueueByRef.has(key)) state.diffQueueByRef.set(key, []);
  return state.diffQueueByRef.get(key);
}

function renderDiffPanel() {
  const panel = e('diff-review');
  if (!panel) return;

  const queue = getDiffQueue(state.activeSrId);
  if (!queue.length) {
    panel.classList.add('hidden');
    panel.innerHTML = '';
    return;
  }

  panel.classList.remove('hidden');
  panel.innerHTML = queue.map((op, index) => `
    <div class="diff-op" data-diff-index="${index}">
      <div><strong>${escapeHtml(op.target_kind || 'diff')}</strong> · ${escapeHtml(op.target_id || '')}</div>
      <div class="muted small">${escapeHtml(op.summary || '')}</div>
      <div class="diff-actions">
        <button data-apply-diff="${index}">Apply</button>
        <button data-reject-diff="${index}">Reject</button>
      </div>
    </div>
  `).join('');

  panel.querySelectorAll('button[data-apply-diff]').forEach((button) => {
    button.addEventListener('click', async () => {
      if (blockIfMemoryReplay()) return;
      const index = Number(button.getAttribute('data-apply-diff'));
      if (!Number.isFinite(index)) return;
      const queueRef = getDiffQueue(state.activeSrId);
      const op = queueRef[index];
      if (!op) return;

      const res = await api.srApplyDiffOp(op);
      if (!res || !res.ok) {
        window.alert((res && res.message) || 'Unable to apply diff operation.');
        return;
      }
      queueRef.splice(index, 1);
      state.references = await api.srList();
      renderReferences();
      renderWorkspaceTabs();
      renderContextFiles();
      renderDiffPanel();
      syncActiveSurface();
    });
  });

  panel.querySelectorAll('button[data-reject-diff]').forEach((button) => {
    button.addEventListener('click', () => {
      if (isMemoryReplayActive()) return;
      const index = Number(button.getAttribute('data-reject-diff'));
      if (!Number.isFinite(index)) return;
      const queueRef = getDiffQueue(state.activeSrId);
      queueRef.splice(index, 1);
      renderDiffPanel();
    });
  });
}

function addChatMessage(role, text) {
  const body = e('chat-body');
  if (!body) return null;
  const msg = document.createElement('div');
  msg.className = `chat-msg ${role}`;
  setChatMessageContent(msg, role, text);
  body.appendChild(msg);
  body.scrollTop = body.scrollHeight;
  return msg;
}

function splitUrlTrailingPunctuation(rawUrl) {
  let url = String(rawUrl || '').trim();
  let trailing = '';
  if (!url) return { url: '', trailing: '' };
  while (url.length > 0) {
    const tail = url.slice(-1);
    if (/[.,!?;:]/.test(tail)) {
      trailing = `${tail}${trailing}`;
      url = url.slice(0, -1);
      continue;
    }
    if (tail === ')') {
      const opens = (url.match(/\(/g) || []).length;
      const closes = (url.match(/\)/g) || []).length;
      if (closes > opens) {
        trailing = `${tail}${trailing}`;
        url = url.slice(0, -1);
        continue;
      }
    }
    break;
  }
  return { url, trailing };
}

function createChatExternalLink(url) {
  const target = String(url || '').trim();
  const link = document.createElement('a');
  link.className = 'chat-link';
  link.href = target;
  link.textContent = target;
  link.addEventListener('click', async (event) => {
    event.preventDefault();
    const wantsExternal = !!(event.metaKey || event.ctrlKey || event.shiftKey || event.altKey);
    if (wantsExternal) {
      if (!api || typeof api.openExternal !== 'function') return;
      try {
        await api.openExternal(target);
      } catch (_) {
        // noop
      }
      return;
    }
    const handledInApp = await handleBrowserUrlInputSubmit(target);
    if (handledInApp) return;
    if (api && typeof api.openExternal === 'function') {
      try {
        await api.openExternal(target);
      } catch (_) {
        // noop
      }
    }
  });
  return link;
}

function setChatMessageContent(node, role, text) {
  if (!node) return;
  const content = String(text || '');
  const normalizedRole = String(role || '').trim().toLowerCase();
  if (normalizedRole !== 'assistant') {
    node.textContent = content;
    return;
  }
  node.textContent = '';
  const re = /https?:\/\/[^\s<>"]+/g;
  let cursor = 0;
  let match;
  while ((match = re.exec(content)) !== null) {
    const start = Number(match.index || 0);
    const rawUrl = String(match[0] || '').trim();
    if (start > cursor) {
      node.appendChild(document.createTextNode(content.slice(cursor, start)));
    }
    const split = splitUrlTrailingPunctuation(rawUrl);
    if (split.url) {
      node.appendChild(createChatExternalLink(split.url));
    } else {
      node.appendChild(document.createTextNode(rawUrl));
    }
    if (split.trailing) {
      node.appendChild(document.createTextNode(split.trailing));
    }
    cursor = start + rawUrl.length;
  }
  if (cursor < content.length) {
    node.appendChild(document.createTextNode(content.slice(cursor)));
  }
}

function ensureStatusTimelineForRequest(requestId) {
  const id = String(requestId || '').trim();
  if (!id) return null;
  const existing = state.chatStatusByRequest.get(id);
  if (existing && existing.node && existing.node.isConnected) return existing;
  const body = e('chat-body');
  const userNode = state.chatUserNodeByRequest.get(id);
  if (!body || !userNode || !userNode.isConnected) return null;

  const details = document.createElement('details');
  details.className = 'chat-activity running';
  details.open = true;
  details.dataset.requestId = id;

  const summary = document.createElement('summary');
  summary.className = 'chat-activity-summary';
  summary.textContent = 'Agent activity (0 steps, running)';
  details.appendChild(summary);

  const lines = document.createElement('div');
  lines.className = 'chat-activity-lines';
  details.appendChild(lines);

  body.insertBefore(details, userNode.nextSibling);
  const timeline = {
    node: details,
    summary,
    lines,
    count: 0,
    errorCount: 0,
  };
  state.chatStatusByRequest.set(id, timeline);
  return timeline;
}

function appendStatusTimelineLine(requestId, payload = {}) {
  const timeline = ensureStatusTimelineForRequest(requestId);
  if (!timeline) return;
  const stateValue = String(payload.state || 'info').trim().toLowerCase();
  const source = String(payload.source || 'agent').trim().toLowerCase();
  const toolName = String(payload.tool_name || '').trim();
  const text = String(payload.text || '').trim();
  if (!text) return;

  const row = document.createElement('div');
  row.className = `chat-activity-line ${stateValue || 'info'}`;

  const dot = document.createElement('span');
  dot.className = 'chat-activity-dot';
  row.appendChild(dot);

  const label = document.createElement('span');
  label.className = 'chat-activity-text';
  const prefix = toolName ? `${toolName}: ` : (source === 'python_bridge' ? 'python: ' : '');
  label.textContent = `${prefix}${text}`;
  row.appendChild(label);

  timeline.lines.appendChild(row);
  timeline.count += 1;
  if (stateValue === 'error') timeline.errorCount += 1;
  timeline.summary.textContent = `Agent activity (${timeline.count} step${timeline.count === 1 ? '' : 's'}, running)`;
  timeline.node.classList.add('running');
  timeline.node.open = true;

  const body = e('chat-body');
  if (body) body.scrollTop = body.scrollHeight;
}

function finalizeStatusTimeline(requestId, failed = false) {
  const id = String(requestId || '').trim();
  if (!id) return;
  const timeline = state.chatStatusByRequest.get(id);
  if (!timeline || !timeline.node) return;
  timeline.node.classList.remove('running');
  if (failed || timeline.errorCount > 0) timeline.node.classList.add('has-error');
  const failureSuffix = timeline.errorCount > 0 ? `, ${timeline.errorCount} failed` : '';
  timeline.summary.textContent = `Agent activity (${timeline.count} step${timeline.count === 1 ? '' : 's'}${failureSuffix})`;
  timeline.node.open = false;
  state.chatUserNodeByRequest.delete(id);
  state.chatStatusByRequest.delete(id);
}

function renderChatMessages(messages) {
  const body = e('chat-body');
  if (!body) return;
  body.innerHTML = '';
  state.chatUserNodeByRequest.clear();
  state.chatStatusByRequest.clear();
  const list = Array.isArray(messages) ? messages : [];
  list.forEach((message) => {
    addChatMessage(String(message.role || 'assistant'), String(message.text || ''));
  });
}

function setChatBusy(busy) {
  const isBusy = !!busy || isMemoryReplayActive();
  const input = e('chat-input');
  const sendBtn = e('chat-send-btn');
  if (input) input.disabled = isBusy;
  if (sendBtn) sendBtn.disabled = isBusy;
}

function clearStreamingAssistantState() {
  state.streamingAssistant = null;
}

function refreshAgentModeAvailability() {
  // Agent mode is always-on by policy for supported providers.
}

async function loadProgramEditorForActiveReference() {
  const input = e('program-editor-input');
  const status = e('program-editor-status');
  if (!input || !status) return;
  const srId = String(state.activeSrId || '').trim();
  if (!srId) {
    input.value = '';
    status.textContent = 'No active reference.';
    return;
  }
  const res = await api.srGetProgram(srId);
  if (!res || !res.ok) {
    input.value = '';
    status.textContent = (res && res.message) ? res.message : 'Unable to load program.';
    return;
  }
  input.value = String(res.program || '');
  status.textContent = '';
}

function ensureStreamingAssistantNode(requestId) {
  const chatBody = e('chat-body');
  if (!chatBody) return null;
  if (
    state.streamingAssistant
    && state.streamingAssistant.requestId === requestId
    && state.streamingAssistant.node
  ) {
    return state.streamingAssistant.node;
  }
  const node = addChatMessage('assistant', '');
  state.streamingAssistant = {
    requestId,
    node,
    text: '',
  };
  return node;
}

async function handleChatStreamPayload(payload) {
  const data = (payload && typeof payload === 'object') ? payload : {};
  const requestId = String(data.request_id || '').trim();
  const phase = String(data.phase || '').trim().toLowerCase();
  if (!requestId || !phase) return;
  if (requestId !== String(state.activeChatRequestId || '').trim()) return;

  if (phase === 'status') {
    appendStatusTimelineLine(requestId, data);
    const meta = (data.meta && typeof data.meta === 'object') ? data.meta : {};
    const pendingTabs = Array.isArray(meta.pending_workspace_tabs) ? meta.pending_workspace_tabs : [];
    if (pendingTabs.length > 0) {
      const targetSrId = String(data.sr_id || state.activeChatRequestSrId || state.activeSrId || '').trim();
      await applyPendingUpdates({ pending_workspace_tabs: pendingTabs }, targetSrId);
    }
    return;
  }

  if (phase === 'delta') {
    const delta = String(data.delta_text || '');
    if (!delta) return;
    const node = ensureStreamingAssistantNode(requestId);
    if (!node) return;
    const nextText = `${String((state.streamingAssistant && state.streamingAssistant.text) || '')}${delta}`;
    state.streamingAssistant.text = nextText;
    setChatMessageContent(node, 'assistant', nextText);
    const chatBody = e('chat-body');
    if (chatBody) chatBody.scrollTop = chatBody.scrollHeight;
    return;
  }

  if (phase === 'final') {
    const finalText = String(data.message || '').trim() || String((state.streamingAssistant && state.streamingAssistant.text) || '').trim() || 'No response.';
    const targetSrId = String(data.sr_id || state.activeChatRequestSrId || state.activeSrId || '').trim();
    let node = null;
    if (state.streamingAssistant && state.streamingAssistant.requestId === requestId) {
      node = state.streamingAssistant.node || null;
    }
    if (!node) node = addChatMessage('assistant', finalText);
    if (node) setChatMessageContent(node, 'assistant', finalText);

    if (targetSrId) {
      await api.srAppendChatMessage(targetSrId, 'assistant', finalText);
    }
    await applyPendingUpdates(data, targetSrId);
    finalizeStatusTimeline(requestId, false);

    clearStreamingAssistantState();
    state.activeChatRequestId = null;
    state.activeChatRequestSrId = null;
    setChatBusy(false);
    if (!shouldPreferRuntimeFocusAfterChat() || !focusActiveHtmlRuntime('chat-final')) {
      const input = e('chat-input');
      if (input) input.focus();
    }
    return;
  }

  if (phase === 'error') {
    const message = String(data.message || 'Request failed.');
    if (state.streamingAssistant && state.streamingAssistant.requestId === requestId && state.streamingAssistant.node) {
      setChatMessageContent(state.streamingAssistant.node, 'assistant', message);
    } else {
      addChatMessage('assistant', message);
    }
    finalizeStatusTimeline(requestId, true);
    clearStreamingAssistantState();
    state.activeChatRequestId = null;
    state.activeChatRequestSrId = null;
    setChatBusy(false);
    if (!shouldPreferRuntimeFocusAfterChat() || !focusActiveHtmlRuntime('chat-error')) {
      const input = e('chat-input');
      if (input) input.focus();
    }
  }
}

async function handleCrawlerStreamPayload(payload) {
  const data = (payload && typeof payload === 'object') ? payload : {};
  const phase = String(data.phase || '').trim().toLowerCase();
  const job = (data.job && typeof data.job === 'object') ? data.job : {};
  const jobId = String((job && job.id) || '').trim();
  const srId = String((job && job.sr_id) || '').trim();

  if (phase === 'started') {
    setStatusText('public-topic-status', `Crawler started${jobId ? ` (${jobId})` : ''}.`);
    return;
  }
  if (phase === 'stopping') {
    setStatusText('public-topic-status', `Crawler stopping${jobId ? ` (${jobId})` : ''}...`);
    return;
  }
  if (phase === 'failed') {
    const errorMessage = String((job && job.error) || data.message || 'Crawler failed.');
    setStatusText('public-topic-status', errorMessage);
    return;
  }
  if (phase === 'completed' || phase === 'stopped') {
    const resultCount = Number((job && job.result_count) || 0);
    setStatusText('public-topic-status', `Crawler ${phase}${jobId ? ` (${jobId})` : ''}. Results: ${resultCount}.`);
    return;
  }
  if (phase === 'ingested') {
    const ingest = (data.ingest && typeof data.ingest === 'object') ? data.ingest : {};
    const importedCount = Number((ingest && ingest.imported_count) || 0);
    setStatusText('public-topic-status', `Crawler context imported: ${importedCount} file(s).`);
    if (srId && srId === String(state.activeSrId || '').trim()) {
      state.references = await api.srList();
      renderReferences();
      renderWorkspaceTabs();
      renderContextFiles();
      renderFilesPanel();
      renderDiffPanel();
      await syncActiveSurface();
    }
    return;
  }
  if (phase === 'ingest_error') {
    setStatusText('public-topic-status', String(data.message || 'Crawler ingest failed.'));
  }
}

async function loadChatThread() {
  if (!state.activeSrId) return;
  if (isMemoryReplayActive()) {
    const ref = getActiveReference();
    const snapshotMessages = Array.isArray(ref && ref.chat_thread && ref.chat_thread.messages)
      ? ref.chat_thread.messages
      : [];
    renderChatMessages(snapshotMessages);
    return;
  }
  const thread = await api.srGetChatThread(state.activeSrId);
  if (!thread || !thread.ok) {
    renderChatMessages([]);
    return;
  }
  renderChatMessages(thread.messages || []);
}

async function applyPendingUpdates(response, targetSrId = null) {
  if (!response || typeof response !== 'object') return;
  const scopeSrId = String(targetSrId || state.activeSrId || '').trim();

  const pendingWeight = Array.isArray(response.pending_weight_updates) ? response.pending_weight_updates : [];
  for (const item of pendingWeight) {
    const srId = String((item && item.sr_id) || scopeSrId || '').trim();
    const weights = item && item.weights;
    if (!srId || !weights || typeof weights !== 'object') continue;
    await api.srUpdateAgentWeights(srId, weights);
  }

  const pendingTrace = Array.isArray(response.pending_decision_traces) ? response.pending_decision_traces : [];
  for (const item of pendingTrace) {
    const srId = String((item && item.sr_id) || scopeSrId || '').trim();
    const step = item && item.step;
    if (!srId || !step) continue;
    await api.srAppendDecisionTrace(srId, step);
  }

  const pendingArtifacts = Array.isArray(response.pending_artifacts) ? response.pending_artifacts : [];
  for (const artifact of pendingArtifacts) {
    if (!artifact || typeof artifact !== 'object') continue;
    const srId = String((artifact.reference_id) || scopeSrId || '').trim();
    if (!srId) continue;
    await api.srUpsertArtifact(srId, artifact);
  }

  const pendingTabs = Array.isArray(response.pending_workspace_tabs) ? response.pending_workspace_tabs : [];
  for (const tab of pendingTabs) {
    if (!tab || typeof tab !== 'object') continue;
    const type = String(tab.type || '').trim().toLowerCase();
    const srId = String((tab.reference_id) || scopeSrId || '').trim();
    if (!srId) continue;

    if (type === 'web') {
      const url = String((tab && tab.url) || '').trim();
      if (!url || srId !== state.activeSrId) continue;
      const ref = getReferenceById(srId);
      const existingWebTab = (Array.isArray(ref && ref.tabs) ? ref.tabs : []).find(
        (t) => String((t && t.url) || '') === url && String((t && t.tab_kind) || 'web') === 'web',
      );
      let activeTabId = '';
      if (existingWebTab) {
        const setRes = await api.srSetActiveTab(srId, existingWebTab.id);
        if (setRes && setRes.ok) {
          state.references = setRes.references || state.references;
          activeTabId = String(
            ((setRes.reference && setRes.reference.active_tab_id) || existingWebTab.id || '')
          ).trim();
        }
      } else {
        const addRes = await api.srAddTab(srId, { url, title: String((tab && tab.title) || url) });
        if (addRes && addRes.ok) {
          state.references = addRes.references || state.references;
          activeTabId = String(
            ((addRes.tab && addRes.tab.id) || (addRes.reference && addRes.reference.active_tab_id) || '')
          ).trim();
        }
      }
      if (!activeTabId) {
        const activeRef = getReferenceById(srId);
        activeTabId = String((activeRef && activeRef.active_tab_id) || '').trim();
      }
      await navigateActiveRuntimeTab(url);
      state.activeSurface = makeActiveSurface('web', activeTabId ? { tabId: activeTabId } : {});
      rememberSurfaceForReference(srId, state.activeSurface);
      continue;
    }

    if (type === 'viz' || type === 'viz_png') {
      const vizRequest = (tab.viz_request && typeof tab.viz_request === 'object') ? tab.viz_request : {};
      const pythonCode = String((vizRequest && vizRequest.python_code) || '').trim();
      const pngPath = String((tab && tab.viz_png_path) || '').trim();
      const pngBase64 = String((tab && tab.viz_png_base64) || '').trim();
      const imageLine = pngPath
        ? `![Legacy visualization frame](${String(pngPath.startsWith('file://') ? pngPath : `file://${pngPath}`)})`
        : (pngBase64 ? `![Legacy visualization frame](data:image/png;base64,${pngBase64})` : '');
      const contentParts = [
        '# Legacy Visualization',
        '',
        'This output was mapped from a deprecated visualization payload into an artifact.',
        '',
        `- Renderer: ${String(tab.renderer || 'canvas')}`,
        `- Source: ${String(tab.viz_source || 'unknown')}`,
      ];
      if (imageLine) {
        contentParts.push('', '## Snapshot', '', imageLine);
      }
      if (pythonCode) {
        contentParts.push('', '## Python Code', '', '```python', pythonCode, '```');
      }
      const artifactPayload = {
        id: String(tab.artifact_id || '').trim() || `artifact_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`,
        type: 'markdown',
        title: String(tab.title || 'Visualization').trim().slice(0, 180) || 'Visualization',
        content: contentParts.join('\n'),
        updated_at: Date.now(),
      };
      const upsertRes = await api.srUpsertArtifact(srId, artifactPayload);
      if (upsertRes && upsertRes.ok && upsertRes.artifact && srId === state.activeSrId) {
        state.activeSurface = makeActiveSurface('artifact', { artifactId: String(upsertRes.artifact.id || '') });
        rememberSurfaceForReference(state.activeSrId, state.activeSurface);
      }
      continue;
    }

    if (type === 'viz_existing') {
      showPassiveNotification('Visualization tabs are deprecated. Opened output is now routed through artifacts.');
      continue;
    }

    if (type === 'artifact') {
      const artifactId = String((tab && tab.artifact_id) || '').trim();
      if (artifactId && srId === state.activeSrId) {
        state.activeSurface = makeActiveSurface('artifact', { artifactId });
        rememberSurfaceForReference(state.activeSrId, state.activeSurface);
      }
    }
  }

  const pendingDiffOps = Array.isArray(response.pending_diff_ops) ? response.pending_diff_ops : [];
  if (scopeSrId && pendingDiffOps.length > 0) {
    const queue = getDiffQueue(scopeSrId);
    pendingDiffOps.forEach((op) => queue.push(op));
  }

  const pendingHyperwebQueries = Array.isArray(response.pending_hyperweb_queries) ? response.pending_hyperweb_queries : [];
  for (const queryPayload of pendingHyperwebQueries) {
    const query = String((queryPayload && queryPayload.query) || '').trim();
    if (!query) continue;
    const result = await api.hyperwebReferenceSearch(query, 30, state.hyperwebFilterFingerprint || '');
    const resultCount = Number((result && result.results && result.results.length) || 0);
    if (result && result.ok && resultCount > 0) {
      addChatMessage('assistant', `Hyperweb has ${resultCount} relevant reference(s) for "${query}". Open the Hyperweb page to import.`);
      if (scopeSrId) {
        await api.srAppendChatMessage(scopeSrId, 'assistant', `Hyperweb found ${resultCount} suggestion(s) for "${query}".`);
      }
    } else if (result && result.ok && resultCount === 0) {
      addChatMessage('assistant', `No relevant Hyperweb references were found for "${query}".`);
      if (scopeSrId) {
        await api.srAppendChatMessage(scopeSrId, 'assistant', `No relevant Hyperweb references found for "${query}".`);
      }
    } else {
      addChatMessage('assistant', `Hyperweb search could not be completed for "${query}".`);
      if (scopeSrId) {
        await api.srAppendChatMessage(scopeSrId, 'assistant', `Hyperweb search could not be completed for "${query}".`);
      }
    }
  }

  state.references = await api.srList();
  renderReferences();
  renderWorkspaceTabs();
  renderContextFiles();
  renderFilesPanel();
  renderDiffPanel();
  await syncActiveSurface();
}

async function sendChatMessage() {
  if (blockIfMemoryReplay('Memory replay is read-only. Exit memory mode to chat with Lumino.')) return;
  if (state.activeChatRequestId) return;
  if (!state.activeSrId || !getReferenceById(state.activeSrId)) {
    await refreshAndRepairActiveReferenceSelection();
  }
  if (!state.activeSrId || !getReferenceById(state.activeSrId)) {
    window.alert('Select a reference first.');
    return;
  }
  const input = e('chat-input');
  if (!input) return;

  const message = String(input.value || '').trim();
  if (!message) return;

  input.value = '';
  setChatBusy(true);

  const ref = getActiveReference();
  const scopedRefs = buildLuminoScopeRefs(state.activeSrId);
  const provider = getSelectedProvider();
  const model = getSelectedModel();
  const requestId = `req_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`;
  const userNode = addChatMessage('user', message);
  state.chatUserNodeByRequest.set(requestId, userNode);
  state.chatStatusByRequest.delete(requestId);
  await api.srAppendChatMessage(state.activeSrId, 'user', message);
  void noteReferenceRankingInteraction('chat_send', { srId: state.activeSrId, chatPrompt: message });
  state.activeChatRequestId = requestId;
  state.activeChatRequestSrId = state.activeSrId;
  clearStreamingAssistantState();

  try {
    const startRes = await api.chatStart({
      message,
      sr_id: state.activeSrId,
      request_id: requestId,
      provider,
      model,
      agent_mode: true,
      active_surface: {
        kind: String((state.activeSurface && state.activeSurface.kind) || 'web'),
        artifact_id: state.activeSurface && state.activeSurface.kind === 'artifact'
          ? String(state.activeSurface.artifactId || '').trim()
          : '',
      },
      sr_artifacts: Array.isArray(ref && ref.artifacts) ? ref.artifacts : [],
      sr_context_files: Array.isArray(ref && ref.context_files) ? ref.context_files : [],
      sr_all_refs: scopedRefs,
    });
    if (!startRes || !startRes.ok) {
      const msg = String((startRes && startRes.message) || 'Unable to start Lumino request.');
      addChatMessage('assistant', msg);
      state.chatUserNodeByRequest.delete(requestId);
      state.chatStatusByRequest.delete(requestId);
      state.activeChatRequestId = null;
      state.activeChatRequestSrId = null;
      setChatBusy(false);
      input.focus();
    }
  } catch (err) {
    addChatMessage('assistant', `Request failed: ${err && err.message ? err.message : 'unknown error'}`);
    state.chatUserNodeByRequest.delete(requestId);
    state.chatStatusByRequest.delete(requestId);
    state.activeChatRequestId = null;
    state.activeChatRequestSrId = null;
    setChatBusy(false);
    input.focus();
  }
}

function setupBrowserEvents() {
  api.onNavigate(async (data) => {
    const url = String((data && data.url) || '').trim();
    if (url) state.browserCurrentUrl = url;

    const input = e('browser-url-input');
    if (input && state.activeSurface.kind === 'web') {
      input.value = state.browserCurrentUrl;
    }
    await refreshUncommittedActionCue();
  });

  api.onTitleUpdate(async (data) => {
    const title = String((data && data.title) || '').trim();
    if (title) state.browserCurrentTitle = title;
  });

  api.onLoadingChange((_data) => {
    // Reserved for future loader UI.
  });

  if (typeof api.onChatStream === 'function') {
    api.onChatStream((payload) => {
      handleChatStreamPayload(payload).catch(() => {});
    });
  }
  if (typeof api.onCrawlerStream === 'function') {
    api.onCrawlerStream((payload) => {
      handleCrawlerStreamPayload(payload).catch(() => {});
    });
  }
  if (typeof api.onHyperwebChat === 'function') {
    api.onHyperwebChat((payload = {}) => {
      const eventName = String((payload && payload.event) || '').trim().toLowerCase();
      const inboxKind = String((payload && payload.kind) || '').trim().toLowerCase();
      const touchesWorkspaceReferences = (
        eventName === 'share_invite'
        || eventName === 'share_status'
        || eventName === 'share_revoke'
        || eventName === 'share_delete'
        || (eventName === 'inbox_entry' && inboxKind === 'share_notice')
      );
      refreshTopbarBadges().catch(() => {});
      if (touchesWorkspaceReferences) {
        api.srList().then((refs) => {
          state.references = Array.isArray(refs) ? refs : state.references;
          renderReferences();
          renderWorkspaceTabs();
          renderContextFiles();
          renderDiffPanel();
          syncActiveSurface().catch(() => {});
        }).catch(() => {});
      }
      if (state.appView === 'hyperweb' && state.hyperwebActiveTab === 'chat') {
        const chatRefreshMode = resolveHyperwebChatRefreshMode(payload);
        if (chatRefreshMode) {
          scheduleHyperwebChatRefresh(chatRefreshMode).catch(() => {});
        }
        return;
      }
      if (state.appView === 'hyperweb') {
        refreshHyperwebFeedAndReferences().catch(() => {});
      }
      if (state.appView === 'private') {
        refreshPrivateSharesData().catch(() => {});
      }
    });
  }
  if (typeof api.onHyperwebOpenThread === 'function') {
    api.onHyperwebOpenThread((payload) => {
      const mode = String((payload && payload.mode) || 'dm').trim().toLowerCase() === 'room' ? 'room' : 'dm';
      const peerId = String((payload && payload.peer_id) || '').trim();
      const roomId = String((payload && payload.room_id) || '').trim();
      setAppView('hyperweb').then(async () => {
        state.hyperwebActiveTab = 'chat';
        state.hyperwebChatMode = mode;
        if (mode === 'room') {
          state.hyperwebChatRoomId = roomId;
        } else {
          state.hyperwebChatPeerId = peerId;
        }
        renderAppView();
        await refreshHyperwebChatData();
      }).catch(() => {});
    });
  }
  if (typeof api.onAudible === 'function') {
    api.onAudible((payload) => {
      const audible = !!(payload && payload.audible);
      const srId = String(state.activeSrId || '').trim();
      state.audibleByRef.clear();
      if (srId && audible) state.audibleByRef.set(srId, true);
      renderReferences();
    });
  }
  if (typeof api.onMarkerUpdate === 'function') {
    api.onMarkerUpdate((payload) => {
      const message = String((payload && payload.message) || '').trim();
      if (message) showPassiveNotification(message);
    });
  }
  if (typeof api.onMailEvent === 'function') {
    api.onMailEvent((payload) => {
      refreshTopbarBadges().catch(() => {});
      handleMailEventPayload(payload).catch(() => {});
    });
  }
}

async function createRootFromCurrentPage() {
  const homeUrl = getDefaultSearchHomeUrl();
  const homeTitle = getDefaultSearchHomeTitle();
  const seedUrl = homeUrl;
  const seedTitle = homeTitle;
  const res = await api.srCreateRoot({
    title: seedTitle || 'New Root Reference',
    current_tab: {
      url: seedUrl,
      title: seedTitle || seedUrl || homeTitle,
    },
  });
  if (res && res.ok && res.reference) {
    state.references = res.references || state.references;
    const rootId = String((res.reference && res.reference.id) || '').trim();
    const rootTabId = String((res.reference && res.reference.active_tab_id) || '').trim();
    await activateReferenceSurface(rootId, makeActiveSurface('web', rootTabId ? { tabId: rootTabId } : {}));
    if (!isBlankNavigationUrl(seedUrl)) {
      void maybeQueueYouTubeTranscriptIngestion(rootId, seedUrl, seedTitle, { silentFailure: true });
    }
  }
}

async function createEmptyReferenceWorkspace() {
  const res = await api.srCreateEmptyWorkspace({
    title: 'Untitled',
    intent: '',
  });
  if (!res || !res.ok || !res.reference) {
    window.alert((res && res.message) || 'Unable to create empty reference workspace.');
    return;
  }
  state.references = res.references || state.references;
  const nextId = String((res.reference && res.reference.id) || '').trim();
  if (nextId) {
    setActiveReference(nextId);
    const artifactId = (res.reference.artifacts && res.reference.artifacts[0] && res.reference.artifacts[0].id) || null;
    if (artifactId) {
      state.activeSurface = makeActiveSurface('artifact', { artifactId });
      rememberSurfaceForReference(nextId, state.activeSurface);
      renderWorkspaceTabs();
      await syncActiveSurface();
    }
  }
}

async function clearChatAndAutoForkCurrentReference() {
  if (!state.activeSrId) {
    window.alert('Select a reference first.');
    return;
  }
  if (state.activeChatRequestId) {
    try {
      await api.chatCancel(state.activeChatRequestId);
    } catch (_) {
      // noop
    }
    state.activeChatRequestId = null;
    state.activeChatRequestSrId = null;
    clearStreamingAssistantState();
    setChatBusy(false);
  }
  const res = await api.srClearChatAndAutoFork(state.activeSrId);
  if (!res || !res.ok || !res.activeReference) {
    window.alert((res && res.message) || 'Unable to clear chat and fork reference.');
    return;
  }
  state.references = res.references || await api.srList();
  setActiveReference(res.activeReference.id);
  renderChatMessages([]);
}

async function importExternalContextFile(filePath) {
  if (!state.activeSrId) {
    window.alert('Select a reference first.');
    return;
  }
  const res = await api.srAddContextFile(state.activeSrId, filePath);
  if (!res || !res.ok) {
    window.alert((res && res.message) || 'Unable to import context file.');
    return;
  }
  state.references = res.references || await api.srList();
  const filesTabId = String((res && res.files_tab && res.files_tab.id) || '').trim();
  if (filesTabId) {
    state.activeSurface = makeActiveSurface('files', { filesTabId });
    rememberSurfaceForReference(state.activeSrId, state.activeSurface);
  }
  renderReferences();
  renderWorkspaceTabs();
  renderContextFiles();
  renderFilesPanel();
  await syncActiveSurface();
  void noteReferenceRankingInteraction('context_file_add', { srId: state.activeSrId, browserTitle: filePath });
}

async function commitCurrentPageToActiveReference() {
  const currentTab = await buildCurrentBrowserTabPayload();
  if (!currentTab || !currentTab.url) {
    showPassiveNotification('No page loaded to commit.');
    return;
  }
  const activeRef = getActiveReference();
  if (activeRef && !isUncommittedCurrentUrl(activeRef, currentTab.url)) {
    showPassiveNotification('Current page is already in this reference.');
    return;
  }
  const sourceTabId = String(((activeRef && getActiveWebTab(activeRef)) || {}).id || '').trim();

  const res = await api.srSaveInActive({
    active_sr_id: state.activeSrId,
    current_tab: {
      url: currentTab.url || getDefaultSearchHomeUrl(),
      title: currentTab.title || currentTab.url || 'Untitled',
    },
    insert_after_tab_id: sourceTabId,
    no_change_policy: 'append',
  });

  if (res && res.ok) {
    state.references = res.references || state.references;
    const committedSrId = String((res.reference && res.reference.id) || state.activeSrId || '').trim();
    if (committedSrId) state.activeSrId = committedSrId;
    const committedTabId = String((res.reference && res.reference.active_tab_id) || '').trim();
    state.activeSurface = makeActiveSurface('web', committedTabId ? { tabId: committedTabId } : {});
    rememberSurfaceForReference(state.activeSrId, state.activeSurface);
    renderReferences();
    renderWorkspaceTabs();
    await syncActiveSurface();
    await refreshUncommittedActionCue();
    if (state.activeSrId && currentTab.url) {
      void maybeQueueYouTubeTranscriptIngestion(state.activeSrId, currentTab.url, currentTab.title, { silentFailure: true });
    }
    void noteReferenceRankingInteraction('commit_page', {
      srId: state.activeSrId,
      browserTitle: currentTab.title,
      browserUrl: currentTab.url,
    });
  }
}

async function mountFolderToActiveReference() {
  if (!state.activeSrId) {
    showPassiveNotification('Select a reference before mounting a folder.');
    return;
  }
  const res = await api.srMountFolder(state.activeSrId, '');
  if (!res || !res.ok) {
    if (res && res.canceled) return;
    window.alert((res && res.message) || 'Unable to mount folder.');
    return;
  }
  state.references = res.references || state.references;
  const filesTabId = String((res && res.files_tab && res.files_tab.id) || '').trim();
  if (filesTabId) {
    state.activeSurface = makeActiveSurface('files', { filesTabId });
    rememberSurfaceForReference(state.activeSrId, state.activeSurface);
  }
  renderReferences();
  renderWorkspaceTabs();
  renderContextFiles();
  renderFilesPanel();
  await syncActiveSurface();
}

async function setZenMode(enabled, options = {}) {
  const next = !!enabled;
  if (state.zenMode === next && !options.force) return;
  state.zenMode = next;
  applyZenModeUi();
  if (!options.skipPersist) persistZenModePreference(next);
  if (state.appView === 'workspace') {
    await syncActiveSurface();
  } else if (state.appView === 'history') {
    await syncHistoryPreviewBounds();
  }
}

async function toggleZenMode() {
  await setZenMode(!state.zenMode);
}

function updateTopbarViewButtons() {
  const active = String(state.appView || 'workspace').trim().toLowerCase();
  e('workspace-open-btn')?.classList.toggle('active', active === 'workspace');
  e('mail-open-btn')?.classList.toggle('active', active === 'mail');
  e('hyperweb-open-btn')?.classList.toggle('active', active === 'hyperweb');
  e('private-shares-open-btn')?.classList.toggle('active', active === 'private-shares');
  e('settings-open-btn')?.classList.toggle('active', active === 'settings');
  e('history-open-btn')?.classList.toggle('active', active === 'history');
  renderTopbarBadges();
}

async function setAppView(viewName) {
  const target = String(viewName || 'workspace').trim().toLowerCase();
  state.appView = (target === 'mail' || target === 'hyperweb' || target === 'private-shares' || target === 'settings' || target === 'history')
    ? target
    : 'workspace';
  const root = e('app-root');
  const mail = e('global-mail-page');
  const hyperweb = e('hyperweb-page');
  const privateShares = e('private-shares-page');
  const settings = e('settings-page');
  const history = e('history-page');
  if (root) root.classList.toggle('hidden', state.appView !== 'workspace');
  if (mail) mail.classList.toggle('hidden', state.appView !== 'mail');
  if (hyperweb) hyperweb.classList.toggle('hidden', state.appView !== 'hyperweb');
  if (privateShares) privateShares.classList.toggle('hidden', state.appView !== 'private-shares');
  if (settings) settings.classList.toggle('hidden', state.appView !== 'settings');
  if (history) history.classList.toggle('hidden', state.appView !== 'history');
  document.body.classList.remove('mobile-left-open', 'mobile-right-open');
  closeTopbarMenu();
  updateTopbarViewButtons();
  if (state.appView === 'hyperweb') {
    saveHyperwebLastSeenAt(Date.now());
    state.hyperwebUnreadCount = Math.max(0, Number(state.hyperwebUnreadCount || 0));
    renderTopbarBadges();
  }
  if (state.appView === 'hyperweb' || state.appView === 'private-shares' || state.appView === 'settings' || state.appView === 'mail') {
    await api.historyPreviewHide();
    await api.hide();
    return;
  }
  if (state.appView === 'history') {
    await api.hide();
    await api.historyPreviewHide();
    return;
  }
  await api.historyPreviewHide();
  await syncActiveSurface();
}

async function setHyperwebSurfaceTab(tab, options = {}) {
  const normalized = String(tab || 'feed').trim().toLowerCase();
  const next = (normalized === 'refs' || normalized === 'chat') ? normalized : 'feed';
  state.hyperwebActiveTab = next;
  const grid = e('hyperweb-grid');
  if (grid) {
    grid.classList.toggle('view-feed', next === 'feed');
    grid.classList.toggle('view-refs', next === 'refs');
    grid.classList.toggle('view-chat', next === 'chat');
  }
  e('hyperweb-tab-feed-btn')?.classList.toggle('active', next === 'feed');
  e('hyperweb-tab-refs-btn')?.classList.toggle('active', next === 'refs');
  e('hyperweb-tab-chat-btn')?.classList.toggle('active', next === 'chat');
  if (options.skipRefresh) return;
  if (next === 'chat') {
    await refreshHyperwebChatData();
    return;
  }
  if (next === 'refs') {
    await refreshHyperwebReferences();
    return;
  }
  await refreshHyperwebFeedAndReferences();
}

function closePublishSnapshotModal(options = {}) {
  const overlay = e('publish-snapshot-overlay');
  if (overlay) overlay.classList.add('hidden');
  state.publishSnapshotTargetId = '';
  if (!options.skipSurfaceSync) {
    void syncActiveSurface();
  }
}

async function openPublishSnapshotModal(srId) {
  const targetId = String(srId || '').trim();
  if (!targetId) return;
  const ref = getReferenceById(targetId);
  state.publishSnapshotTargetId = targetId;
  await api.hide();
  const overlay = e('publish-snapshot-overlay');
  const meta = e('publish-snapshot-meta');
  if (meta) {
    const title = ref ? String(ref.title || 'Untitled') : 'Untitled';
    meta.textContent = `Reference: ${title}`;
  }
  if (overlay) overlay.classList.remove('hidden');
  void syncActiveSurface();
}

function closeShareReferenceModal(options = {}) {
  const overlay = e('share-reference-overlay');
  if (overlay) overlay.classList.add('hidden');
  state.shareReferenceTargetId = '';
  state.shareMemberSearchQuery = '';
  state.shareRecipientSelection = new Set();
  const input = e('share-member-search-input');
  if (input) input.value = '';
  if (!options.skipSurfaceSync) {
    void syncActiveSurface();
  }
}

function shareStatusLabel(status) {
  const normalized = String(status || '').trim().toLowerCase();
  if (normalized === 'read_granted') return 'Read Granted';
  if (normalized === 'write_pending') return 'Write Pending';
  if (normalized === 'write_accepted') return 'Write Accepted';
  if (normalized === 'declined') return 'Declined';
  if (normalized === 'revoked') return 'Revoked';
  return 'Pending';
}

function shareStatusClass(status) {
  const normalized = String(status || '').trim().toLowerCase();
  if (normalized === 'write_accepted') return 'accepted';
  if (normalized === 'declined' || normalized === 'revoked') return normalized;
  return 'pending';
}

async function refreshWorkspaceReferenceState() {
  if (typeof api.srList !== 'function') return state.references;
  const refs = await api.srList();
  if (!Array.isArray(refs)) return state.references;
  state.references = refs;
  renderReferences();
  renderWorkspaceTabs();
  renderContextFiles();
  renderDiffPanel();
  return refs;
}

function findWorkspaceReferenceForPrivateShare(options = {}) {
  const shareId = String((options && options.share_id) || '').trim();
  const roomId = String((options && options.room_id) || '').trim();
  return (Array.isArray(state.references) ? state.references : []).find((ref) => {
    const meta = (ref && ref.source_metadata && typeof ref.source_metadata === 'object') ? ref.source_metadata : {};
    if (shareId && String(meta.private_share_id || meta.share_id || '').trim() === shareId) return true;
    if (roomId && String(meta.private_share_room_id || meta.room_id || '').trim() === roomId) return true;
    return false;
  }) || null;
}

async function openWorkspaceReferenceFromPrivateShare(options = {}) {
  let refs = await refreshWorkspaceReferenceState();
  if (!Array.isArray(refs)) return false;
  let match = findWorkspaceReferenceForPrivateShare(options);
  const roomId = String((options && options.room_id) || '').trim();
  if (!match && roomId && typeof api.hyperwebOpenSharedRoom === 'function') {
    const openRes = await api.hyperwebOpenSharedRoom(roomId);
    if (openRes && openRes.ok && openRes.room) {
      state.privateActiveRoomId = roomId;
      applySharedRoomState(openRes.room);
    }
    refs = await refreshWorkspaceReferenceState();
    match = findWorkspaceReferenceForPrivateShare(options);
  }
  if (!match || !match.id) {
    setStatusText('shares-status-line', 'Shared reference is available, but its workspace snapshot was not found.');
    return false;
  }
  await setAppView('workspace');
  await activateReferenceSurface(String(match.id || '').trim());
  return true;
}

function renderShareMemberList() {
  const holder = e('share-member-list');
  if (!holder) return;
  const query = String(state.shareMemberSearchQuery || '').trim().toLowerCase();
  const members = (Array.isArray(state.shareMemberDirectory) ? state.shareMemberDirectory : [])
    .filter((member) => !(member && member.is_self))
    .filter((member) => {
      if (!query) return true;
      const text = `${String(member.display_name || '')} ${String(member.member_id || '')} ${String(member.search_blob || '')}`.toLowerCase();
      return text.includes(query);
    });
  if (members.length === 0) {
    holder.innerHTML = '<div class="muted small">No members found.</div>';
    return;
  }
  holder.innerHTML = members.map((member) => {
    const memberId = String((member && member.member_id) || '').trim();
    const selected = state.shareRecipientSelection.has(memberId);
    const alias = escapeHtml(String((member && member.display_name) || memberId));
    const presence = String((member && member.presence_status) || '').trim().toLowerCase();
    const desc = `${String((member && member.member_id) || '')}${presence ? ` · ${presence.replace(/_/g, ' ')}` : ''}`.trim();
    return `
      <label class="share-member-row">
        <input type="checkbox" data-share-member="${escapeHtml(memberId)}" ${selected ? 'checked' : ''} />
        <div>
          <div>${alias}</div>
          <div class="muted small">${escapeHtml(desc)}</div>
        </div>
      </label>
    `;
  }).join('');
  holder.querySelectorAll('input[data-share-member]').forEach((node) => {
    node.addEventListener('change', () => {
      const memberId = String(node.getAttribute('data-share-member') || '').trim();
      if (!memberId) return;
      if (node.checked) state.shareRecipientSelection.add(memberId);
      else state.shareRecipientSelection.delete(memberId);
    });
  });
}

async function refreshShareMemberDirectory() {
  if (!api.hyperwebMembersList) {
    state.shareMemberDirectory = [];
    renderShareMemberList();
    setStatusText('shares-status-line', 'Member directory unavailable.');
    return;
  }
  const res = await api.hyperwebMembersList();
  if (!res || !res.ok) {
    state.shareMemberDirectory = [];
    renderShareMemberList();
    setStatusText('shares-status-line', (res && res.message) ? res.message : 'Unable to load member directory.');
    return;
  }
  state.shareMemberDirectory = Array.isArray(res.members) ? res.members : [];
  renderShareMemberList();
}

async function openShareReferenceModal(srId) {
  const targetId = String(srId || '').trim();
  if (!targetId) return;
  state.shareReferenceTargetId = targetId;
  state.shareMemberSearchQuery = '';
  state.shareRecipientSelection = new Set();
  const input = e('share-member-search-input');
  if (input) input.value = '';
  await api.hide();
  await refreshShareMemberDirectory();
  const overlay = e('share-reference-overlay');
  if (overlay) overlay.classList.remove('hidden');
  void syncActiveSurface();
}

function setSharesTab(tab) {
  const next = String(tab || 'incoming').trim().toLowerCase();
  state.sharesActiveTab = (next === 'outgoing' || next === 'rooms') ? next : 'incoming';
  e('shares-tab-incoming-btn')?.classList.toggle('active', state.sharesActiveTab === 'incoming');
  e('shares-tab-outgoing-btn')?.classList.toggle('active', state.sharesActiveTab === 'outgoing');
  e('shares-tab-rooms-btn')?.classList.toggle('active', state.sharesActiveTab === 'rooms');
  e('shares-incoming-panel')?.classList.toggle('hidden', state.sharesActiveTab !== 'incoming');
  e('shares-outgoing-panel')?.classList.toggle('hidden', state.sharesActiveTab !== 'outgoing');
  e('shares-rooms-panel')?.classList.toggle('hidden', state.sharesActiveTab !== 'rooms');
}

function renderIncomingShares() {
  const holder = e('shares-incoming-panel');
  if (!holder) return;
  const rows = Array.isArray(state.privateIncomingShares) ? state.privateIncomingShares : [];
  if (rows.length === 0) {
    holder.innerHTML = '<div class="muted small">No incoming shares.</div>';
    return;
  }
  holder.innerHTML = rows.map((item) => {
    const shareId = escapeHtml(String((item && item.share_id) || ''));
    const roomId = escapeHtml(String((item && item.room_id) || ''));
    const status = String((item && item.write_status) || 'write_pending');
    const title = escapeHtml(String((item && item.reference_title) || 'Shared reference'));
    const owner = escapeHtml(String((item && item.owner_alias) || (item && item.owner_id) || 'owner'));
    return `
      <div class="share-row">
        <div class="share-row-header">
          <strong>${title}</strong>
          <span class="share-chip ${escapeHtml(shareStatusClass(status))}">${escapeHtml(shareStatusLabel(status))}</span>
        </div>
        <div class="muted small">From ${owner} · ${escapeHtml(formatAgo(item && item.created_at))}</div>
        <div class="share-row-actions">
          <button data-share-open="${shareId}" data-share-room="${roomId}">Open (Read)</button>
          <button data-share-open-workspace="${shareId}" data-share-room="${roomId}">Open in Workspace</button>
          <button data-share-accept="${shareId}" ${status === 'write_accepted' ? 'disabled' : ''}>Accept Write</button>
          <button data-share-decline="${shareId}" ${(status === 'declined' || status === 'revoked') ? 'disabled' : ''}>Decline Write</button>
        </div>
      </div>
    `;
  }).join('');

  holder.querySelectorAll('button[data-share-open]').forEach((button) => {
    button.addEventListener('click', async () => {
      const roomId = String(button.getAttribute('data-share-room') || '').trim();
      if (!roomId) return;
      setSharesTab('rooms');
      await openSharedRoom(roomId);
    });
  });
  holder.querySelectorAll('button[data-share-open-workspace]').forEach((button) => {
    button.addEventListener('click', async () => {
      const shareId = String(button.getAttribute('data-share-open-workspace') || '').trim();
      const roomId = String(button.getAttribute('data-share-room') || '').trim();
      if (!shareId && !roomId) return;
      await openWorkspaceReferenceFromPrivateShare({
        share_id: shareId,
        room_id: roomId,
      });
    });
  });
  holder.querySelectorAll('button[data-share-accept]').forEach((button) => {
    button.addEventListener('click', async () => {
      const shareId = String(button.getAttribute('data-share-accept') || '').trim();
      const roomId = String(button.getAttribute('data-share-room') || '').trim();
      if (!shareId || !api.hyperwebAcceptShareWrite) return;
      const res = await api.hyperwebAcceptShareWrite(shareId);
      if (!res || !res.ok) {
        setStatusText('shares-status-line', (res && res.message) ? res.message : 'Unable to accept write access.');
        return;
      }
      await refreshPrivateSharesData();
      const referenceId = String((res && res.reference && res.reference.id) || '').trim();
      if (referenceId) {
        await refreshWorkspaceReferenceState();
        await setAppView('workspace');
        await activateReferenceSurface(referenceId);
        return;
      }
      await openWorkspaceReferenceFromPrivateShare({
        share_id: shareId,
        room_id: roomId,
      });
    });
  });
  holder.querySelectorAll('button[data-share-decline]').forEach((button) => {
    button.addEventListener('click', async () => {
      const shareId = String(button.getAttribute('data-share-decline') || '').trim();
      if (!shareId || !api.hyperwebDeclineShareWrite) return;
      const res = await api.hyperwebDeclineShareWrite(shareId);
      if (!res || !res.ok) {
        setStatusText('shares-status-line', (res && res.message) ? res.message : 'Unable to decline write access.');
        return;
      }
      await refreshPrivateSharesData();
    });
  });
}

function renderOutgoingShares() {
  const holder = e('shares-outgoing-panel');
  if (!holder) return;
  const rows = Array.isArray(state.privateOutgoingShares) ? state.privateOutgoingShares : [];
  if (rows.length === 0) {
    holder.innerHTML = '<div class="muted small">No outgoing shares.</div>';
    return;
  }
  holder.innerHTML = rows.map((item) => {
    const shareId = escapeHtml(String((item && item.share_id) || ''));
    const title = escapeHtml(String((item && item.reference_title) || 'Shared reference'));
    const recipients = Array.isArray(item && item.recipients) ? item.recipients : [];
    const chips = recipients.map((recipient) => {
      const status = String((recipient && (recipient.write_status || recipient.status)) || 'write_pending');
      const readGranted = !!(recipient && recipient.read_access !== false);
      const who = escapeHtml(String((recipient && recipient.display_name) || (recipient && recipient.member_id) || 'member'));
      const readClass = readGranted ? 'accepted' : 'revoked';
      const readText = readGranted ? 'Read Granted' : 'Read Revoked';
      return `
        <span class="share-chip ${escapeHtml(readClass)}">${who}: ${escapeHtml(readText)}</span>
        <span class="share-chip ${escapeHtml(shareStatusClass(status))}">${who}: ${escapeHtml(shareStatusLabel(status))}</span>
      `;
    }).join('');
    return `
      <div class="share-row">
        <div class="share-row-header">
          <strong>${title}</strong>
          <span class="muted small">${escapeHtml(formatAgo(item && item.created_at))}</span>
        </div>
        <div class="share-chip-row">${chips || '<span class="muted small">No recipients.</span>'}</div>
        <div class="share-row-actions">
          <button data-share-revoke="${shareId}">Revoke Access</button>
          <button class="danger" data-share-delete="${shareId}">Delete Share</button>
        </div>
      </div>
    `;
  }).join('');

  holder.querySelectorAll('button[data-share-revoke]').forEach((button) => {
    button.addEventListener('click', async () => {
      const shareId = String(button.getAttribute('data-share-revoke') || '').trim();
      if (!shareId || !api.hyperwebRevokeShare) return;
      const res = await api.hyperwebRevokeShare(shareId);
      if (!res || !res.ok) {
        setStatusText('shares-status-line', (res && res.message) ? res.message : 'Unable to revoke access.');
        return;
      }
      await refreshPrivateSharesData();
    });
  });
  holder.querySelectorAll('button[data-share-delete]').forEach((button) => {
    button.addEventListener('click', async () => {
      const shareId = String(button.getAttribute('data-share-delete') || '').trim();
      if (!shareId || !api.hyperwebDeleteShare) return;
      const confirmed = window.confirm('Delete this shared reference and its shared room for all participants? This cannot be undone.');
      if (!confirmed) return;
      const res = await api.hyperwebDeleteShare(shareId);
      if (!res || !res.ok) {
        setStatusText('shares-status-line', (res && res.message) ? res.message : 'Unable to delete share.');
        return;
      }
      await refreshPrivateSharesData();
    });
  });
}

function applySharedRoomState(room) {
  state.privateActiveRoom = room || null;
  const header = e('shares-room-header');
  const workspaceBtn = e('shares-room-open-workspace-btn');
  const note = e('shares-room-readonly-note');
  const editor = e('shares-room-editor');
  if (!room) {
    if (header) header.textContent = 'Select a room to start collaborating.';
    if (workspaceBtn) workspaceBtn.classList.add('hidden');
    if (note) note.classList.add('hidden');
    if (editor) {
      editor.value = '';
      editor.readOnly = true;
    }
    return;
  }
  const owner = String(room.owner_alias || room.owner_id || 'owner');
  const participants = Array.isArray(room.participants) ? room.participants.length : 0;
  if (header) header.textContent = `${String(room.reference_title || 'Room')} · owner ${owner} · participants ${participants}`;
  if (workspaceBtn) {
    workspaceBtn.classList.toggle('hidden', !String((room && room.workspace_reference_id) || '').trim());
    workspaceBtn.dataset.roomId = String((room && room.room_id) || '');
    workspaceBtn.dataset.shareId = String((room && room.share_id) || '');
  }
  const canWrite = !!room.can_write;
  if (note) note.classList.toggle('hidden', canWrite);
  if (editor) {
    editor.readOnly = !canWrite;
    editor.value = String(room.content || '');
  }
}

function renderSharedRooms() {
  const panel = e('shares-rooms-panel');
  if (!panel) return;
  const rooms = Array.isArray(state.privateSharedRooms) ? state.privateSharedRooms : [];
  let list = e('shares-room-list');
  if (!list) {
    list = document.createElement('div');
    list.id = 'shares-room-list';
    list.className = 'share-room-list';
    panel.insertBefore(list, panel.firstChild);
  }
  if (rooms.length === 0) {
    list.innerHTML = '<div class="muted small">No shared rooms yet.</div>';
    applySharedRoomState(null);
    return;
  }
  list.innerHTML = rooms.map((room) => {
    const roomId = escapeHtml(String((room && room.room_id) || ''));
    const active = String(state.privateActiveRoomId || '') === String((room && room.room_id) || '');
    return `
      <button class="${active ? 'active' : ''}" data-share-room-open="${roomId}">
        ${escapeHtml(String((room && room.reference_title) || 'Room'))}
      </button>
    `;
  }).join('');
  list.querySelectorAll('button[data-share-room-open]').forEach((button) => {
    button.addEventListener('click', async () => {
      const roomId = String(button.getAttribute('data-share-room-open') || '').trim();
      if (!roomId) return;
      await openSharedRoom(roomId);
    });
  });
}

async function openSharedRoom(roomId) {
  const id = String(roomId || '').trim();
  if (!id || !api.hyperwebOpenSharedRoom) return;
  const res = await api.hyperwebOpenSharedRoom(id);
  if (!res || !res.ok) {
    setStatusText('shares-status-line', (res && res.message) ? res.message : 'Unable to open shared room.');
    return;
  }
  state.privateActiveRoomId = id;
  if (res && res.room) {
    state.privateSharedRooms = (Array.isArray(state.privateSharedRooms) ? state.privateSharedRooms : []).map((room) => {
      if (String((room && room.room_id) || '') !== id) return room;
      return {
        ...room,
        workspace_reference_id: String((res.room && res.room.workspace_reference_id) || (room && room.workspace_reference_id) || ''),
        workspace_reference_title: String((res.room && res.room.workspace_reference_title) || (room && room.workspace_reference_title) || ''),
      };
    });
  }
  applySharedRoomState(res.room || null);
  renderSharedRooms();
}

async function refreshPrivateSharesData() {
  const listReq = api.hyperwebListShares ? api.hyperwebListShares() : Promise.resolve({ ok: true, incoming: [], outgoing: [] });
  const roomReq = api.hyperwebListSharedRooms ? api.hyperwebListSharedRooms() : Promise.resolve({ ok: true, rooms: [] });
  const [sharesRes, roomsRes] = await Promise.all([listReq, roomReq]);
  if (!sharesRes || !sharesRes.ok) {
    setStatusText('shares-status-line', (sharesRes && sharesRes.message) ? sharesRes.message : 'Unable to load private shares.');
  }
  state.privateIncomingShares = (sharesRes && sharesRes.ok && Array.isArray(sharesRes.incoming)) ? sharesRes.incoming : [];
  state.privateOutgoingShares = (sharesRes && sharesRes.ok && Array.isArray(sharesRes.outgoing)) ? sharesRes.outgoing : [];
  state.privateSharedRooms = (roomsRes && roomsRes.ok && Array.isArray(roomsRes.rooms)) ? roomsRes.rooms : [];
  if (sharesRes && sharesRes.ok) {
    setStatusText(
      'shares-status-line',
      `Incoming ${state.privateIncomingShares.length} · Outgoing ${state.privateOutgoingShares.length} · Rooms ${state.privateSharedRooms.length}`
    );
  }
  renderIncomingShares();
  renderOutgoingShares();
  renderSharedRooms();
  if (state.privateActiveRoomId) {
    const exists = state.privateSharedRooms.some((room) => String((room && room.room_id) || '') === String(state.privateActiveRoomId || ''));
    if (!exists) {
      state.privateActiveRoomId = '';
      applySharedRoomState(null);
    } else {
      await openSharedRoom(state.privateActiveRoomId);
    }
  } else {
    applySharedRoomState(null);
  }
}

async function openPrivateSharesPage() {
  await setAppView('private-shares');
  setSharesTab(state.sharesActiveTab || 'incoming');
  await refreshPrivateSharesData();
}

function renderHyperwebIdentityLine() {
  const line = e('hyperweb-identity-line');
  if (!line) return;
  const identity = state.hyperwebIdentity || {};
  const alias = String(identity.display_alias || 'node');
  const fp = String(identity.fingerprint || '').slice(0, 12);
  const filter = String(state.hyperwebFilterFingerprint || '').trim();
  line.textContent = filter
    ? `You: ${alias} (${fp}) · filter active: ${filter}`
    : `You: ${alias} (${fp})`;
}

function hyperwebChatMemberLabel(member) {
  if (!member || typeof member !== 'object') return 'member';
  const name = String(member.display_name || member.member_id || 'member').trim();
  const presence = String(member.presence_status || (member.is_online ? 'online' : 'offline')).trim().toLowerCase();
  if (presence === 'online') return `${name} (online)`;
  if (presence === 'seen_recently') return `${name} (seen recently)`;
  return `${name} (offline)`;
}

function getHyperwebSelectableDmPeers() {
  const peersById = new Map();
  const members = Array.isArray(state.hyperwebChatMembers) ? state.hyperwebChatMembers : [];
  members.forEach((member) => {
    const id = String((member && member.member_id) || '').trim();
    if (!id || member.is_self) return;
    peersById.set(id, member);
  });
  const conversations = Array.isArray(state.hyperwebChatConversations) ? state.hyperwebChatConversations : [];
  conversations.forEach((conversation) => {
    const id = String((conversation && conversation.peer_id) || '').trim();
    if (!id || peersById.has(id)) return;
    peersById.set(id, {
      member_id: id,
      display_name: String((conversation && conversation.display_name) || id),
      is_self: false,
      is_online: false,
    });
  });
  return Array.from(peersById.values()).sort((a, b) => {
    return String((a && a.display_name) || '').localeCompare(String((b && b.display_name) || ''));
  });
}

function ensureHyperwebDmSelection() {
  if (state.hyperwebChatMode !== 'dm') return;
  if (String(state.hyperwebChatPeerId || '').trim()) return;
  const conversations = Array.isArray(state.hyperwebChatConversations) ? state.hyperwebChatConversations : [];
  const mostRecent = conversations[0] || null;
  if (mostRecent && mostRecent.peer_id) {
    state.hyperwebChatPeerId = String(mostRecent.peer_id || '').trim();
  }
}

function resolveHyperwebPeerDisplayName(peerId = '') {
  const target = String(peerId || '').trim();
  if (!target) return 'peer';
  const member = (Array.isArray(state.hyperwebChatMembers) ? state.hyperwebChatMembers : []).find(
    (item) => String((item && item.member_id) || '').trim() === target,
  );
  if (member && member.display_name) return String(member.display_name || '').trim();
  const conversation = (Array.isArray(state.hyperwebChatConversations) ? state.hyperwebChatConversations : []).find(
    (item) => String((item && item.peer_id) || '').trim() === target,
  );
  if (conversation && conversation.display_name) return String(conversation.display_name || '').trim();
  return target;
}

function truncateHyperwebConversationPreview(text = '', maxLength = 80) {
  const normalized = String(text || '').replace(/\s+/g, ' ').trim();
  if (!normalized) return '';
  if (normalized.length <= maxLength) return normalized;
  return `${normalized.slice(0, Math.max(0, maxLength - 3)).trimEnd()}...`;
}

function renderHyperwebChatLayout() {
  const layout = e('hyperweb-chat-layout');
  if (!layout) return;
  const isDm = state.hyperwebChatMode === 'dm';
  layout.classList.toggle('is-dm', isDm);
  layout.classList.toggle('is-room', !isDm);
}

function renderHyperwebChatThreadHeader() {
  const node = e('hyperweb-chat-thread-meta');
  const retentionNode = e('hyperweb-chat-thread-retention');
  if (!node) return;
  const mode = state.hyperwebChatMode === 'room' ? 'room' : 'dm';
  if (mode === 'dm') {
    const peerName = resolveHyperwebPeerDisplayName(state.hyperwebChatPeerId || '');
    const presence = state.hyperwebChatActivePresence && state.hyperwebChatActivePresence.presence_status
      ? String(state.hyperwebChatActivePresence.presence_status)
      : 'offline';
    const presenceLabel = presence === 'online'
      ? 'online'
      : (presence === 'seen_recently' ? 'seen recently' : 'offline');
    node.textContent = peerName ? `${peerName} · ${presenceLabel}` : 'Choose a peer.';
  } else {
    const room = (Array.isArray(state.hyperwebChatRooms) ? state.hyperwebChatRooms : []).find((item) => String((item && item.room_id) || '') === String(state.hyperwebChatRoomId || ''));
    const roomName = String((room && room.room_name) || state.hyperwebChatRoomId || 'Room');
    node.textContent = `${roomName} · live peers ${Number(state.hyperwebChatLivePeerCount || 0)}`;
  }
  if (retentionNode) retentionNode.value = String(((state.hyperwebChatThreadPolicy || {}).retention) || 'off');
}

function renderHyperwebChatSelectors() {
  const peerSelect = e('hyperweb-chat-peer-select');
  const roomSelect = e('hyperweb-chat-room-select');
  const modeSelect = e('hyperweb-chat-mode-select');
  renderHyperwebChatLayout();
  if (modeSelect) modeSelect.value = state.hyperwebChatMode === 'room' ? 'room' : 'dm';
  const peers = getHyperwebSelectableDmPeers();
  if (peerSelect) {
    const options = ['<option value="">Choose a peer</option>'];
    options.push(...peers.map((peer) => {
      return `<option value="${escapeHtml(String(peer.member_id || ''))}">${escapeHtml(hyperwebChatMemberLabel(peer))}</option>`;
    }));
    peerSelect.innerHTML = options.join('');
    if (state.hyperwebChatPeerId && peers.some((peer) => String(peer.member_id || '') === state.hyperwebChatPeerId)) {
      peerSelect.value = state.hyperwebChatPeerId;
    } else {
      peerSelect.value = '';
    }
  }
  const rooms = Array.isArray(state.hyperwebChatRooms) ? state.hyperwebChatRooms : [];
  if (roomSelect) {
    roomSelect.innerHTML = rooms.length === 0
      ? '<option value="">No rooms</option>'
      : rooms.map((room) => `<option value="${escapeHtml(String(room.room_id || ''))}">${escapeHtml(String(room.room_name || room.room_id || 'Room'))}</option>`).join('');
    if (state.hyperwebChatRoomId && rooms.some((room) => String(room.room_id || '') === state.hyperwebChatRoomId)) {
      roomSelect.value = state.hyperwebChatRoomId;
    } else if (rooms[0]) {
      state.hyperwebChatRoomId = String(rooms[0].room_id || '');
      roomSelect.value = state.hyperwebChatRoomId;
    }
  }
  const isRoom = state.hyperwebChatMode === 'room';
  peerSelect?.classList.toggle('hidden', isRoom);
  roomSelect?.classList.toggle('hidden', !isRoom);
}

function renderHyperwebDmConversationList() {
  const holder = e('hyperweb-chat-conversations');
  if (!holder) return;
  if (state.hyperwebChatMode !== 'dm') {
    holder.innerHTML = '';
    return;
  }

  const conversations = Array.isArray(state.hyperwebChatConversations) ? state.hyperwebChatConversations : [];
  if (conversations.length === 0) {
    holder.innerHTML = '<div class="hyperweb-chat-conversation-empty muted small">No previous conversations yet.</div>';
    return;
  }

  holder.innerHTML = conversations.map((conversation) => {
    const peerId = String((conversation && conversation.peer_id) || '').trim();
    const displayName = escapeHtml(String((conversation && conversation.display_name) || peerId || 'Peer'));
    const isActive = peerId === String(state.hyperwebChatPeerId || '').trim();
    const preview = truncateHyperwebConversationPreview(String((conversation && conversation.last_message_text) || ''));
    const unreadCount = Math.max(0, Number((conversation && conversation.unread_count) || 0));
    const timeLabel = Number((conversation && conversation.last_message_ts) || 0) > 0
      ? escapeHtml(formatAgo(conversation.last_message_ts))
      : '';
    return `
      <button class="hyperweb-chat-conversation-btn${isActive ? ' active' : ''}" type="button" data-hyperweb-peer-id="${escapeHtml(peerId)}">
        <div class="hyperweb-chat-conversation-head">
          <span class="hyperweb-chat-conversation-name">${displayName}</span>
          <span class="hyperweb-chat-conversation-time">${timeLabel}</span>
        </div>
        <div class="hyperweb-chat-conversation-preview">${escapeHtml(preview || 'No messages yet.')}</div>
        <div class="hyperweb-chat-conversation-foot">
          <span></span>
          ${unreadCount > 0 ? `<span class="hyperweb-chat-conversation-unread">${escapeHtml(String(unreadCount))}</span>` : ''}
        </div>
      </button>
    `;
  }).join('');

  holder.querySelectorAll('button[data-hyperweb-peer-id]').forEach((button) => {
    button.addEventListener('click', async () => {
      const peerId = String(button.getAttribute('data-hyperweb-peer-id') || '').trim();
      if (!peerId || peerId === String(state.hyperwebChatPeerId || '').trim()) return;
      state.hyperwebChatPeerId = peerId;
      renderHyperwebChatSelectors();
      renderHyperwebDmConversationList();
      await refreshHyperwebChatHistory();
    });
  });
}

function base64ToBytes(value = '') {
  const raw = window.atob(String(value || ''));
  const bytes = new Uint8Array(raw.length);
  for (let index = 0; index < raw.length; index += 1) {
    bytes[index] = raw.charCodeAt(index);
  }
  return bytes;
}

async function downloadHyperwebChatAttachment(messageId = '') {
  if (!api.hyperwebChatAttachment) return;
  const targetId = String(messageId || '').trim();
  if (!targetId) return;
  const res = await api.hyperwebChatAttachment(targetId);
  if (!res || !res.ok || !res.file) {
    setStatusText('hyperweb-chat-status', (res && res.message) ? res.message : 'Unable to load attachment.');
    return;
  }
  const file = res.file || {};
  const bytes = base64ToBytes(String(file.data_base64 || ''));
  const blob = new Blob([bytes], { type: String(file.mime || 'application/octet-stream') });
  const url = window.URL.createObjectURL(blob);
  const anchor = document.createElement('a');
  anchor.href = url;
  anchor.download = String(file.name || 'file.bin');
  document.body.appendChild(anchor);
  anchor.click();
  anchor.remove();
  window.setTimeout(() => {
    window.URL.revokeObjectURL(url);
  }, 1000);
}

function renderHyperwebChatThread() {
  const holder = e('hyperweb-chat-thread');
  renderHyperwebChatThreadHeader();
  if (!holder) return;
  if (state.hyperwebChatMode === 'dm' && !String(state.hyperwebChatPeerId || '').trim()) {
    holder.innerHTML = '<div class="muted small">Choose a peer to continue a chat.</div>';
    return;
  }
  const messages = Array.isArray(state.hyperwebChatMessages) ? state.hyperwebChatMessages : [];
  if (messages.length === 0) {
    holder.innerHTML = '<div class="muted small">No messages yet.</div>';
    return;
  }
  holder.innerHTML = messages.map((item) => {
    const outgoing = String((item && item.direction) || '').trim().toLowerCase() === 'out';
    const from = escapeHtml(resolveHyperwebPeerDisplayName(String((item && item.from_peer_id) || '')));
    const to = escapeHtml(resolveHyperwebPeerDisplayName(String((item && item.to_peer_id) || '')));
    const text = escapeHtml(String((item && item.text) || ''));
    const file = item && item.file ? item.file : null;
    const receipt = item && item.read_by && Object.keys(item.read_by || {}).length > 0
      ? 'read'
      : ((item && item.delivered_by && Object.keys(item.delivered_by || {}).length > 0) ? 'delivered' : String((item && item.status) || 'queued').replace(/_/g, ' '));
    const canUndo = outgoing && (receipt === 'sent to mesh' || receipt === 'queued');
    const messageId = escapeHtml(String((item && item.message_id) || ''));
    return `
      <div class="hyperweb-chat-item ${outgoing ? 'out' : 'in'}">
        <div class="hyperweb-chat-meta">
          <span>${outgoing ? `You -> ${to || 'peer'}` : from}</span>
          <span>${escapeHtml(formatAgo(item && item.ts))}${outgoing ? ` · ${escapeHtml(receipt)}` : ''}</span>
        </div>
        ${text ? `<div class="hyperweb-chat-text">${text}</div>` : ''}
        ${file ? `<div class="hyperweb-chat-file"><button type="button" class="hyperweb-chat-download-btn" data-message-id="${messageId}">Download ${escapeHtml(String(file.name || 'file.bin'))}</button> (${escapeHtml(String(file.size || 0))} bytes)</div>` : ''}
        <div class="hyperweb-chat-item-actions">
          <button class="hyperweb-chat-delete-btn" data-message-id="${messageId}">Delete</button>
          ${canUndo ? `<button class="hyperweb-chat-undo-btn" data-message-id="${messageId}">Undo send</button>` : ''}
        </div>
      </div>
    `;
  }).join('');
  holder.scrollTop = holder.scrollHeight;
  holder.querySelectorAll('.hyperweb-chat-download-btn').forEach((button) => {
    button.addEventListener('click', async () => {
      const messageId = String(button.getAttribute('data-message-id') || '').trim();
      if (!messageId) return;
      button.disabled = true;
      try {
        await downloadHyperwebChatAttachment(messageId);
      } finally {
        button.disabled = false;
      }
    });
  });
  holder.querySelectorAll('.hyperweb-chat-delete-btn').forEach((button) => {
    button.addEventListener('click', async () => {
      if (!api.hyperwebChatDeleteMessage) return;
      const messageId = String(button.getAttribute('data-message-id') || '').trim();
      if (!messageId) return;
      const res = await api.hyperwebChatDeleteMessage(messageId);
      if (!res || !res.ok) {
        setStatusText('hyperweb-chat-status', (res && res.message) ? res.message : 'Unable to delete message.');
        return;
      }
      await scheduleHyperwebChatRefresh('incremental');
    });
  });
  holder.querySelectorAll('.hyperweb-chat-undo-btn').forEach((button) => {
    button.addEventListener('click', async () => {
      if (!api.hyperwebChatDeleteMessage) return;
      const messageId = String(button.getAttribute('data-message-id') || '').trim();
      if (!messageId) return;
      const res = await api.hyperwebChatDeleteMessage(messageId);
      if (!res || !res.ok) {
        setStatusText('hyperweb-chat-status', (res && res.message) ? res.message : 'Unable to undo message.');
        return;
      }
      await scheduleHyperwebChatRefresh('incremental');
    });
  });
}

async function refreshHyperwebChatConversations() {
  if (!api.hyperwebChatConversations) return;
  const res = await api.hyperwebChatConversations();
  if (!res || !res.ok) {
    state.hyperwebChatConversations = [];
    renderHyperwebChatSelectors();
    renderHyperwebDmConversationList();
    return;
  }
  state.hyperwebChatConversations = Array.isArray(res.conversations) ? res.conversations : [];
  ensureHyperwebDmSelection();
  renderHyperwebChatSelectors();
  renderHyperwebDmConversationList();
}

async function refreshHyperwebChatHistory() {
  if (!api.hyperwebChatHistory) return;
  const payload = {
    mode: state.hyperwebChatMode,
    peer_id: state.hyperwebChatMode === 'dm' ? state.hyperwebChatPeerId : '',
    room_id: state.hyperwebChatMode === 'room' ? state.hyperwebChatRoomId : '',
    limit: 240,
  };
  const res = await api.hyperwebChatHistory(payload);
  if (!res || !res.ok) {
    setStatusText('hyperweb-chat-status', (res && res.message) ? res.message : 'Unable to load chat history.');
    state.hyperwebChatMessages = [];
    renderHyperwebChatThread();
    return;
  }
  state.hyperwebChatMessages = Array.isArray(res.messages) ? res.messages : [];
  state.hyperwebChatRooms = Array.isArray(res.rooms) ? res.rooms : state.hyperwebChatRooms;
  state.hyperwebChatMembers = Array.isArray(res.members) ? res.members : state.hyperwebChatMembers;
  state.hyperwebChatThreadId = String(res.thread_id || '').trim();
  state.hyperwebChatThreadPolicy = res.thread_policy && typeof res.thread_policy === 'object'
    ? res.thread_policy
    : { retention: 'off' };
  state.hyperwebChatActivePresence = res.active_presence && typeof res.active_presence === 'object'
    ? res.active_presence
    : null;
  state.hyperwebChatLivePeerCount = Number(res.live_peer_count || 0);
  renderHyperwebChatSelectors();
  renderHyperwebDmConversationList();
  renderHyperwebChatThread();
  setStatusText('hyperweb-chat-status', `Messages: ${state.hyperwebChatMessages.length} · live peers ${state.hyperwebChatLivePeerCount}`);
  if (api.hyperwebChatMarkRead && state.hyperwebChatMessages.length > 0) {
    const unread = state.hyperwebChatMessages
      .filter((item) => String((item && item.direction) || '') === 'in')
      .map((item) => String((item && item.message_id) || '').trim())
      .filter(Boolean);
    if (unread.length > 0) {
      const markReadRes = await api.hyperwebChatMarkRead({
        peer_id: state.hyperwebChatMode === 'dm' ? state.hyperwebChatPeerId : '',
        room_id: state.hyperwebChatMode === 'room' ? state.hyperwebChatRoomId : '',
        message_ids: unread,
      });
      if (markReadRes && markReadRes.ok && state.hyperwebChatMode === 'dm') {
        await refreshHyperwebChatConversations();
      }
    }
  }
}

async function refreshHyperwebChatData() {
  if (!api.hyperwebMembersList) return;
  const [membersRes, roomsRes, conversationsRes] = await Promise.all([
    api.hyperwebMembersList(),
    api.hyperwebChatRoomsList ? api.hyperwebChatRoomsList() : Promise.resolve({ ok: true, rooms: [] }),
    api.hyperwebChatConversations ? api.hyperwebChatConversations() : Promise.resolve({ ok: true, conversations: [] }),
  ]);
  state.hyperwebChatMembers = (membersRes && membersRes.ok && Array.isArray(membersRes.members)) ? membersRes.members : [];
  state.hyperwebChatRooms = (roomsRes && roomsRes.ok && Array.isArray(roomsRes.rooms)) ? roomsRes.rooms : [];
  state.hyperwebChatConversations = (conversationsRes && conversationsRes.ok && Array.isArray(conversationsRes.conversations))
    ? conversationsRes.conversations
    : [];
  ensureHyperwebDmSelection();
  renderHyperwebChatSelectors();
  renderHyperwebDmConversationList();
  await refreshHyperwebChatHistory();
}

function renderHyperwebFeedItems(posts) {
  const holder = e('hyperweb-feed');
  if (!holder) return;
  const list = Array.isArray(posts) ? posts : [];
  if (list.length === 0) {
    holder.innerHTML = '<div class="muted small">No public posts yet.</div>';
    return;
  }

  holder.innerHTML = list.map((post) => {
    const postId = escapeHtml(String((post && post.post_id) || ''));
    const author = escapeHtml(String((post && post.author_alias) || (post && post.author_fingerprint) || 'node'));
    const authorFp = escapeHtml(String((post && post.author_fingerprint) || ''));
    const postStatus = String((post && post.status) || 'visible').trim().toLowerCase();
    const postRemoved = postStatus === 'hidden' || postStatus === 'deleted' || !!(post && post.removed_by_threshold);
    const body = postRemoved
      ? 'Removed by community threshold'
      : escapeHtml(String((post && post.body) || ''));
    const votes = (post && post.votes) || {};
    const replies = Array.isArray(post && post.replies) ? post.replies : [];
    const replyMarkup = replies.map((reply) => {
      const replyId = escapeHtml(String((reply && reply.reply_id) || ''));
      const replyAuthor = escapeHtml(String((reply && reply.author_alias) || (reply && reply.author_fingerprint) || 'node'));
      const replyAuthorFp = escapeHtml(String((reply && reply.author_fingerprint) || ''));
      const replyStatus = String((reply && reply.status) || 'visible').trim().toLowerCase();
      const replyRemoved = replyStatus === 'hidden' || replyStatus === 'deleted' || !!(reply && reply.removed_by_threshold);
      const replyBody = replyRemoved
        ? 'Removed by community threshold'
        : escapeHtml(String((reply && reply.body) || ''));
      const replyVotes = (reply && reply.votes) || {};
      return `
        <div class="hyperweb-reply ${replyRemoved ? 'hyperweb-item-tombstone' : ''}">
          <div class="hyperweb-item-meta">
            <button data-hw-filter="${replyAuthorFp}">${replyAuthor}</button>
            <span>${formatAgo(reply.created_at)}</span>
          </div>
          <div class="hyperweb-item-body">${replyBody}</div>
          <div class="hyperweb-item-actions">
            <button data-hw-vote="${replyId}" data-hw-vote-value="1">▲</button>
            <button data-hw-vote="${replyId}" data-hw-vote-value="-1">▼</button>
            <button data-hw-report="${replyId}" data-hw-report-kind="reply">Report</button>
            <span class="muted small">score ${Number(replyVotes.net || 0)} (${Number(replyVotes.up || 0)}/${Number(replyVotes.down || 0)})</span>
          </div>
        </div>
      `;
    }).join('');
    return `
      <article class="hyperweb-item ${postRemoved ? 'hyperweb-item-tombstone' : ''}">
        <div class="hyperweb-item-meta">
          <button data-hw-filter="${authorFp}">${author}</button>
          <span>${formatAgo(post.created_at)}</span>
        </div>
        <div class="hyperweb-item-body">${body}</div>
        <div class="hyperweb-item-actions">
          <button data-hw-vote="${postId}" data-hw-vote-value="1">▲</button>
          <button data-hw-vote="${postId}" data-hw-vote-value="-1">▼</button>
          <button data-hw-reply="${postId}">Reply</button>
          <button data-hw-report="${postId}" data-hw-report-kind="post">Report</button>
          <span class="muted small">score ${Number(votes.net || 0)} (${Number(votes.up || 0)}/${Number(votes.down || 0)})</span>
        </div>
        <div class="hyperweb-replies">${replyMarkup}</div>
      </article>
    `;
  }).join('');

  holder.querySelectorAll('button[data-hw-filter]').forEach((button) => {
    button.addEventListener('click', async () => {
      const fp = String(button.getAttribute('data-hw-filter') || '').trim();
      if (!fp) return;
      state.hyperwebFilterFingerprint = fp;
      renderHyperwebIdentityLine();
      await refreshHyperwebFeedAndReferences();
    });
  });

  holder.querySelectorAll('button[data-hw-vote]').forEach((button) => {
    button.addEventListener('click', async () => {
      const targetId = String(button.getAttribute('data-hw-vote') || '').trim();
      const value = Number(button.getAttribute('data-hw-vote-value') || 0);
      if (!targetId || (value !== 1 && value !== -1)) return;
      const res = await api.hyperwebVoteSet(targetId, value);
      if (!res || !res.ok) {
        setStatusText('hyperweb-feed-status', (res && res.message) ? res.message : 'Unable to submit vote.');
        return;
      }
      await refreshHyperwebFeedAndReferences();
    });
  });

  holder.querySelectorAll('button[data-hw-reply]').forEach((button) => {
    button.addEventListener('click', async () => {
      const postId = String(button.getAttribute('data-hw-reply') || '').trim();
      if (!postId) return;
      const body = window.prompt('Reply:', '');
      if (body === null) return;
      const res = await api.hyperwebPostReply(postId, body);
      if (!res || !res.ok) {
        setStatusText('hyperweb-feed-status', (res && res.message) ? res.message : 'Unable to post reply.');
        return;
      }
      await refreshHyperwebFeedAndReferences();
    });
  });

  holder.querySelectorAll('button[data-hw-report]').forEach((button) => {
    button.addEventListener('click', async () => {
      const targetId = String(button.getAttribute('data-hw-report') || '').trim();
      const kind = String(button.getAttribute('data-hw-report-kind') || 'post').trim().toLowerCase();
      if (!targetId || !api.hyperwebReportTarget) return;
      const reason = window.prompt('Report reason:', '');
      if (reason === null) return;
      const res = await api.hyperwebReportTarget(targetId, kind, reason);
      if (!res || !res.ok) {
        setStatusText('hyperweb-feed-status', (res && res.message) ? res.message : 'Unable to submit report.');
        return;
      }
      setStatusText('hyperweb-feed-status', 'Report submitted.');
    });
  });
}

function renderHyperwebReferenceResults(list) {
  const holder = e('hyperweb-ref-results');
  if (!holder) return;
  const rows = Array.isArray(list) ? list : [];
  if (rows.length === 0) {
    const hasQuery = !!String((e('hyperweb-ref-query-input') && e('hyperweb-ref-query-input').value) || '').trim();
    holder.innerHTML = hasQuery
      ? '<div class="muted small">Nothing matched your search.</div>'
      : '<div class="muted small">No public references yet.</div>';
    return;
  }
  holder.innerHTML = rows.map((item) => {
    const key = referenceResultKey(item);
    const expanded = state.hyperwebReferenceExpandedKeys.has(key);
    const title = escapeHtml(normalizeInlineText((item && item.title) || 'Untitled'));
    const peer = escapeHtml(normalizeInlineText((item && (item.peer_name || item.source_peer_name || item.source_peer_id)) || 'peer'));
    const peerIdRaw = String((item && (item.peer_id || item.source_peer_id)) || '').trim();
    const peerId = escapeHtml(peerIdRaw);
    const intent = escapeHtml(normalizeInlineText((item && item.intent) || ''));
    const summary = escapeHtml(normalizeInlineText((item && item.summary_text) || 'No summary available.'));
    const excerpt = escapeHtml(normalizeInlineText((item && item.content_excerpt) || ''));
    const score = Number((item && item.score) || 0);
    const votes = (item && item.votes) || {};
    const status = String((item && item.status) || 'visible').trim().toLowerCase();
    const statusLabel = status === 'pending' ? 'Pending' : (status === 'hidden' ? 'Hidden' : 'Visible');
    const tagText = escapeHtml((Array.isArray(item && item.tags) ? item.tags.slice(0, 6) : []).join(', '));
    const updatedAt = formatAgo(item && (item.published_at || item.updated_at));
    const snapshotIdRaw = String((item && item.snapshot_id) || '').trim();
    const targetId = escapeHtml(snapshotIdRaw || String((item && item.reference_id) || ''));
    const removed = status === 'hidden';
    const identityFingerprint = String((state.hyperwebIdentity && state.hyperwebIdentity.fingerprint) || '').trim().toUpperCase();
    const isLocalSnapshot = String((item && item.reference_key) || '').startsWith('snapshot:');
    const canDelete = !!snapshotIdRaw
      && isLocalSnapshot
      && !!identityFingerprint
      && String(peerIdRaw || '').toUpperCase() === identityFingerprint;
    return `
      <div class="hyperweb-ref-item ${removed ? 'hyperweb-item-tombstone' : ''}">
        <div class="hyperweb-ref-main">
          <div class="hyperweb-ref-title">${title}</div>
          <div class="hyperweb-ref-meta-line muted small">
            <button data-hw-filter="${peerId}">${peer}</button>
            · rank ${score.toFixed(2)}
            · votes ${Number(votes.net || 0)}
            ${updatedAt ? ` · ${escapeHtml(updatedAt)}` : ''}
            <span class="hyperweb-status-badge ${escapeHtml(status)}">${escapeHtml(statusLabel)}</span>
          </div>
          ${intent ? `<div class="hyperweb-ref-intent-line muted small">${intent}</div>` : ''}
          <div class="hyperweb-ref-summary">${removed ? 'Removed by community threshold' : summary}</div>
          ${expanded ? `
            <div class="hyperweb-ref-expanded">
              ${tagText ? `<div class="hyperweb-ref-tags-line muted small">tags: ${tagText}</div>` : ''}
              ${excerpt ? `<div class="hyperweb-ref-excerpt-line muted small">${excerpt}</div>` : ''}
            </div>
          ` : ''}
        </div>
        <div class="hyperweb-ref-actions">
          <button data-hw-ref-expand="${escapeHtml(key)}">${expanded ? 'Collapse' : 'Expand'}</button>
          <button data-hw-ref-import="${escapeHtml(key)}">Import</button>
          <button data-hw-vote="${targetId}" data-hw-vote-value="1">Upvote</button>
          <button data-hw-vote="${targetId}" data-hw-vote-value="-1">Downvote</button>
          <button data-hw-report="${targetId}" data-hw-report-kind="reference">Report</button>
          ${canDelete ? `<button class="danger" data-hw-ref-delete="${escapeHtml(snapshotIdRaw)}">Delete</button>` : ''}
        </div>
      </div>
    `;
  }).join('');

  holder.querySelectorAll('button[data-hw-ref-expand]').forEach((button) => {
    button.addEventListener('click', () => {
      const key = String(button.getAttribute('data-hw-ref-expand') || '').trim();
      if (!key) return;
      if (state.hyperwebReferenceExpandedKeys.has(key)) {
        state.hyperwebReferenceExpandedKeys.delete(key);
      } else {
        state.hyperwebReferenceExpandedKeys.add(key);
      }
      renderHyperwebReferenceResults(state.hyperwebReferenceResults);
    });
  });

  holder.querySelectorAll('button[data-hw-ref-import]').forEach((button) => {
    button.addEventListener('click', async () => {
      const key = String(button.getAttribute('data-hw-ref-import') || '').trim();
      const item = state.hyperwebReferenceResults.find((row) => referenceResultKey(row) === key);
      if (!item) return;
      const res = await api.hyperwebImportReference(item.import_payload || item);
      if (!res || !res.ok) {
        setStatusText('hyperweb-ref-status', (res && res.message) ? res.message : 'Unable to import reference.');
        return;
      }
      state.references = res.references || await api.srList();
      renderReferences();
      renderWorkspaceTabs();
      renderContextFiles();
      renderDiffPanel();
      setStatusText('hyperweb-ref-status', `Imported "${(res.imported && res.imported.title) || 'reference'}".`);
      const importedId = String((res && res.imported && res.imported.id) || '').trim();
      if (importedId) {
        await setAppView('workspace');
        await activateReferenceSurface(importedId);
      }
    });
  });

  holder.querySelectorAll('button[data-hw-vote]').forEach((button) => {
    button.addEventListener('click', async () => {
      const targetId = String(button.getAttribute('data-hw-vote') || '').trim();
      const value = Number(button.getAttribute('data-hw-vote-value') || 0);
      if (!targetId || (value !== 1 && value !== -1)) return;
      const res = await api.hyperwebVoteSet(targetId, value);
      if (!res || !res.ok) {
        setStatusText('hyperweb-ref-status', (res && res.message) ? res.message : 'Unable to submit vote.');
        return;
      }
      await refreshHyperwebReferences();
    });
  });

  holder.querySelectorAll('button[data-hw-report]').forEach((button) => {
    button.addEventListener('click', async () => {
      const targetId = String(button.getAttribute('data-hw-report') || '').trim();
      if (!targetId || !api.hyperwebReportTarget) return;
      const reason = window.prompt('Report reason:', '');
      if (reason === null) return;
      const res = await api.hyperwebReportTarget(targetId, 'reference', reason);
      if (!res || !res.ok) {
        setStatusText('hyperweb-ref-status', (res && res.message) ? res.message : 'Unable to submit report.');
        return;
      }
      setStatusText('hyperweb-ref-status', 'Report submitted.');
    });
  });

  holder.querySelectorAll('button[data-hw-ref-delete]').forEach((button) => {
    button.addEventListener('click', async () => {
      const snapshotId = String(button.getAttribute('data-hw-ref-delete') || '').trim();
      if (!snapshotId || !api.hyperwebDeleteSnapshot) return;
      const confirmed = window.confirm('Delete this public reference snapshot? This cannot be undone.');
      if (!confirmed) return;
      const res = await api.hyperwebDeleteSnapshot(snapshotId);
      if (!res || !res.ok) {
        setStatusText('hyperweb-ref-status', (res && res.message) ? res.message : 'Unable to delete public reference.');
        return;
      }
      await refreshHyperwebReferences();
    });
  });

  holder.querySelectorAll('button[data-hw-filter]').forEach((button) => {
    button.addEventListener('click', async () => {
      const fp = String(button.getAttribute('data-hw-filter') || '').trim();
      if (!fp) return;
      state.hyperwebFilterFingerprint = fp;
      renderHyperwebIdentityLine();
      await refreshHyperwebFeedAndReferences();
    });
  });
}

function renderHyperwebPostSearchResults(results) {
  const holder = e('hyperweb-feed');
  if (!holder) return;
  const list = Array.isArray(results) ? results : [];
  if (list.length === 0) {
    holder.innerHTML = '<div class="muted small">No matching posts.</div>';
    return;
  }
  holder.innerHTML = list.map((item) => {
    const cls = 'hyperweb-item hyperweb-search-hit';
    const author = escapeHtml(String((item && item.author_alias) || (item && item.author_fingerprint) || 'node'));
    const fp = escapeHtml(String((item && item.author_fingerprint) || ''));
    const snippet = escapeHtml(String((item && item.snippet) || ''));
    const kind = escapeHtml(String((item && item.target_type) || 'post'));
    const score = Number((item && item.score) || 0);
    return `
      <article class="${cls}">
        <div class="hyperweb-item-meta">
          <button data-hw-filter="${fp}">${author}</button>
          <span>${kind} · score ${score.toFixed(2)}${item && item.created_at ? ` · ${escapeHtml(formatAgo(item.created_at))}` : ''}</span>
        </div>
        <div class="hyperweb-item-body">${snippet}</div>
      </article>
    `;
  }).join('');

  holder.querySelectorAll('button[data-hw-filter]').forEach((button) => {
    button.addEventListener('click', async () => {
      const fp = String(button.getAttribute('data-hw-filter') || '').trim();
      if (!fp) return;
      state.hyperwebFilterFingerprint = fp;
      renderHyperwebIdentityLine();
      await refreshHyperwebFeedAndReferences();
    });
  });
}

async function refreshHyperwebReferences() {
  const query = String((e('hyperweb-ref-query-input') && e('hyperweb-ref-query-input').value) || '').trim();
  const res = await api.hyperwebReferenceSearch(query, 40, state.hyperwebFilterFingerprint || '');
  if (!res || !res.ok) {
    setStatusText('hyperweb-ref-status', (res && res.message) ? res.message : 'Reference search unavailable.');
    state.hyperwebReferenceResults = [];
    renderHyperwebReferenceResults([]);
    return;
  }
  state.hyperwebReferenceResults = Array.isArray(res.results) ? res.results : [];
  renderHyperwebReferenceResults(state.hyperwebReferenceResults);
  setStatusText(
    'hyperweb-ref-status',
    `References: ${state.hyperwebReferenceResults.length} (local ${Number(res.local_count || 0)} · remote ${Number(res.remote_count || 0)})`
  );
}

async function refreshHyperwebFeedAndReferences() {
  const social = await api.hyperwebSocialStatus();
  state.hyperwebIdentity = social && social.identity ? social.identity : null;
  renderHyperwebIdentityLine();

  const postQuery = String(state.hyperwebPostSearchQuery || '').trim();
  if (postQuery) {
    const searchRes = await api.hyperwebPostSearch(postQuery, 40, state.hyperwebFilterFingerprint || '');
    if (!searchRes || !searchRes.ok) {
      setStatusText('hyperweb-feed-status', (searchRes && searchRes.message) ? searchRes.message : 'Post search unavailable.');
      renderHyperwebPostSearchResults([]);
    } else {
      renderHyperwebPostSearchResults(searchRes.results || []);
      setStatusText('hyperweb-feed-status', `Search "${postQuery}" · ${Number((searchRes.results || []).length)} hit(s)`);
    }
  } else {
    const feed = await api.hyperwebFeedQuery(state.hyperwebFilterFingerprint || '');
    if (!feed || !feed.ok) {
      setStatusText('hyperweb-feed-status', (feed && feed.message) ? feed.message : 'Feed unavailable.');
      renderHyperwebFeedItems([]);
    } else {
      state.hyperwebFeed = Array.isArray(feed.posts) ? feed.posts : [];
      renderHyperwebFeedItems(state.hyperwebFeed);
      setStatusText('hyperweb-feed-status', `Posts: ${state.hyperwebFeed.length} · live peers ${Number(feed.peer_count || 0)}`);
    }
  }
  await refreshHyperwebReferences();
  if (state.appView === 'hyperweb') {
    saveHyperwebLastSeenAt(Date.now());
  }
}

async function openHyperwebPage() {
  await setAppView('hyperweb');
  await setHyperwebSurfaceTab(state.hyperwebActiveTab || 'feed', { skipRefresh: true });
  applyHyperwebSplitRatio(state.hyperwebSplitRatio, { skipPersist: true });
  if (state.hyperwebActiveTab === 'chat') {
    await refreshHyperwebChatData();
    await refreshTopbarBadges();
    return;
  }
  if (state.hyperwebActiveTab === 'refs') {
    await refreshHyperwebReferences();
    await refreshTopbarBadges();
    return;
  }
  await refreshHyperwebFeedAndReferences();
  await refreshTopbarBadges();
}

function setHistoryStatus(text) {
  const node = e('history-status-line');
  if (!node) return;
  node.textContent = String(text || '');
}

function historyClusterColor(clusterId) {
  const palette = ['#2878c9', '#20a36f', '#de8f29', '#b658c8', '#d24646', '#7d8a99', '#0f9fb9', '#8a6e4d', '#d572aa', '#6aa13f'];
  const idx = Math.abs(Number(clusterId || 0)) % palette.length;
  return palette[idx];
}

function historyPointById(pointId) {
  return (Array.isArray(state.historyMapPoints) ? state.historyMapPoints : []).find((point) => String((point && point.id) || '') === String(pointId || '')) || null;
}

function renderHistoryMapLegend() {
  const holder = e('history-map-legend');
  if (!holder) return;
  const clusters = Array.isArray(state.historyMapClusters) ? state.historyMapClusters : [];
  if (!clusters.length) {
    holder.innerHTML = '';
    holder.classList.add('hidden');
    return;
  }
  const note = '<div class="history-map-legend-note">Colors show semantic groups of similar pages. They are cluster labels, not fixed content categories.</div>';
  const items = clusters
    .slice()
    .sort((a, b) => Number(b.count || 0) - Number(a.count || 0))
    .map((cluster) => {
      const clusterId = Number(cluster.cluster_id || 0);
      const count = Number(cluster.count || 0);
      return `
        <div class="history-map-legend-item" title="${escapeHtml(`Semantic cluster ${clusterId}`)}">
          <span class="history-map-legend-swatch" style="background:${escapeHtml(historyClusterColor(clusterId))};"></span>
          <span>Group ${escapeHtml(String(clusterId + 1))} (${escapeHtml(String(count))})</span>
        </div>
      `;
    });
  holder.innerHTML = `${note}${items.join('')}`;
  holder.classList.remove('hidden');
}

function updateHistoryPreviewMeta(entry) {
  const node = e('history-preview-meta');
  if (!node) return;
  if (!entry) {
    node.textContent = 'Select an entry';
    return;
  }
  const host = String(entry.url_host || '');
  const ts = Number(entry.committed_at || 0);
  const dateText = Number.isFinite(ts) && ts > 0 ? new Date(ts).toLocaleString() : '';
  node.textContent = [host, dateText].filter(Boolean).join(' • ');
}

function renderHistoryCachedPreview(entry) {
  const placeholder = e('history-preview-placeholder');
  const content = e('history-preview-content');
  if (!placeholder || !content) return;
  if (!entry) {
    content.innerHTML = '';
    content.classList.add('hidden');
    placeholder.classList.remove('hidden');
    return;
  }
  const title = escapeHtml(String(entry.title || entry.url || 'Untitled'));
  const url = escapeHtml(String(entry.url || ''));
  const excerpt = String(entry.content_excerpt || '').trim();
  const excerptHtml = excerpt
    ? escapeHtml(excerpt.slice(0, 2000))
    : 'No cached page excerpt available for this entry.';
  const tokenText = Array.isArray(entry.semantic_tokens) ? entry.semantic_tokens.slice(0, 16).join(', ') : '';
  content.innerHTML = `
    <div class="history-preview-cached">
      <h4>${title}</h4>
      <div class="history-preview-cached-url">${url}</div>
      <div class="history-preview-cached-excerpt">${excerptHtml}</div>
      ${tokenText ? `<div class="muted small">${escapeHtml(tokenText)}</div>` : ''}
    </div>
  `;
  content.classList.remove('hidden');
  placeholder.classList.add('hidden');
}

async function showHistoryPreviewForEntry(entry) {
  await api.historyPreviewHide();
  renderHistoryCachedPreview(entry || null);
}

function renderHistoryList() {
  const holder = e('history-list');
  if (!holder) return;
  const list = Array.isArray(state.historyEntries) ? state.historyEntries : [];
  const count = e('history-count');
  if (count) count.textContent = String(list.length);
  if (list.length === 0) {
    holder.innerHTML = '<div class="muted small">No committed URLs in private history yet.</div>';
    return;
  }
  holder.innerHTML = list.map((entry) => {
    const id = escapeHtml(String((entry && entry.id) || ''));
    const title = escapeHtml(String((entry && entry.title) || (entry && entry.url) || 'Untitled'));
    const url = escapeHtml(String((entry && entry.url) || ''));
    const host = escapeHtml(String((entry && entry.url_host) || ''));
    const active = String(state.historySelectedId || '') === String((entry && entry.id) || '') ? 'active' : '';
    return `
      <div class="history-item ${active}" data-history-open="${id}">
        <div class="history-item-main">
          <div class="history-item-title">${title}</div>
          <div class="history-item-url">${url}</div>
          <div class="history-item-meta">${host} · ${formatAgo(entry.committed_at)}</div>
        </div>
        <div class="history-item-actions">
          <button data-history-copy="${id}">Copy</button>
          <button data-history-delete="${id}">Delete</button>
        </div>
      </div>
    `;
  }).join('');

  holder.querySelectorAll('[data-history-open]').forEach((node) => {
    node.addEventListener('click', async (event) => {
      const copyBtn = event.target && event.target.closest ? event.target.closest('button[data-history-copy]') : null;
      const deleteBtn = event.target && event.target.closest ? event.target.closest('button[data-history-delete]') : null;
      if (copyBtn || deleteBtn) return;
      const historyId = String(node.getAttribute('data-history-open') || '').trim();
      if (!historyId) return;
      const entry = state.historyEntries.find((row) => String((row && row.id) || '') === historyId);
      if (!entry) return;
      state.historySelectedId = historyId;
      renderHistoryList();
      updateHistoryPreviewMeta(entry);
      await showHistoryPreviewForEntry(entry);
    });
  });

  holder.querySelectorAll('button[data-history-copy]').forEach((button) => {
    button.addEventListener('click', async () => {
      const historyId = String(button.getAttribute('data-history-copy') || '').trim();
      const entry = state.historyEntries.find((row) => String((row && row.id) || '') === historyId);
      if (!entry) return;
      try {
        if (navigator && navigator.clipboard && navigator.clipboard.writeText) {
          await navigator.clipboard.writeText(String(entry.url || ''));
        } else {
          window.prompt('Copy URL:', String(entry.url || ''));
        }
        setHistoryStatus('URL copied to clipboard.');
      } catch (_) {
        window.prompt('Copy URL:', String(entry.url || ''));
      }
    });
  });

  holder.querySelectorAll('button[data-history-delete]').forEach((button) => {
    button.addEventListener('click', async () => {
      const historyId = String(button.getAttribute('data-history-delete') || '').trim();
      if (!historyId) return;
      const res = await api.historyDelete(historyId);
      if (!res || !res.ok) {
        setHistoryStatus((res && res.message) ? res.message : 'Unable to delete history item.');
        return;
      }
      if (String(state.historySelectedId || '') === historyId) {
        state.historySelectedId = '';
        await api.historyPreviewHide();
        renderHistoryCachedPreview(null);
        updateHistoryPreviewMeta(null);
      }
      await loadHistoryPageData();
      setHistoryStatus('History entry deleted.');
    });
  });
}

function drawHistorySemanticMap() {
  const canvas = e('history-map-canvas');
  const tooltip = e('history-map-tooltip');
  const wrap = e('history-map-canvas-wrap');
  renderHistoryMapLegend();
  if (!canvas) return;
  const ctx = canvas.getContext('2d');
  if (!ctx) return;

  const points = Array.isArray(state.historyMapPoints) ? state.historyMapPoints : [];
  const bounds = state.historyMapBounds || { min_x: -1, max_x: 1, min_y: -1, max_y: 1 };
  const dpr = Math.max(1, Number(window.devicePixelRatio || 1));
  const cssWidth = Math.max(240, Math.floor(canvas.clientWidth || 900));
  const cssHeight = Math.max(160, Math.floor(canvas.clientHeight || 420));
  const width = Math.max(1, Math.floor(cssWidth * dpr));
  const height = Math.max(1, Math.floor(cssHeight * dpr));
  if (canvas.width !== width) canvas.width = width;
  if (canvas.height !== height) canvas.height = height;
  const pad = 18;
  ctx.setTransform(1, 0, 0, 1, 0, 0);
  ctx.scale(dpr, dpr);
  ctx.imageSmoothingEnabled = false;
  ctx.clearRect(0, 0, cssWidth, cssHeight);
  ctx.fillStyle = '#f5f6f8';
  ctx.fillRect(0, 0, cssWidth, cssHeight);

  const spanX = Math.max(0.0001, Number(bounds.max_x || 1) - Number(bounds.min_x || -1));
  const spanY = Math.max(0.0001, Number(bounds.max_y || 1) - Number(bounds.min_y || -1));
  const projected = points.map((point) => {
    const x = pad + ((Number(point.x || 0) - Number(bounds.min_x || 0)) / spanX) * (cssWidth - (pad * 2));
    const y = pad + ((Number(point.y || 0) - Number(bounds.min_y || 0)) / spanY) * (cssHeight - (pad * 2));
    return { ...point, px: x, py: y };
  });

  projected.forEach((point) => {
    ctx.beginPath();
    ctx.fillStyle = historyClusterColor(point.cluster_id);
    const radius = String(state.historySelectedId || '') === String(point.id || '') ? 4 : 2.9;
    ctx.arc(point.px, point.py, radius, 0, Math.PI * 2);
    ctx.fill();
    ctx.lineWidth = 0.9;
    ctx.strokeStyle = 'rgba(255,255,255,0.72)';
    ctx.stroke();
  });

  const status = e('history-map-status');
  if (status) status.textContent = `${points.length} point(s)`;

  canvas.onmousemove = (event) => {
    if (!tooltip) return;
    const rect = canvas.getBoundingClientRect();
    const x = ((event.clientX - rect.left) / rect.width) * cssWidth;
    const y = ((event.clientY - rect.top) / rect.height) * cssHeight;
    let found = null;
    let minDist = 9;
    projected.forEach((point) => {
      const dx = point.px - x;
      const dy = point.py - y;
      const dist = Math.sqrt((dx * dx) + (dy * dy));
      if (dist < minDist) {
        minDist = dist;
        found = point;
      }
    });
    if (!found) {
      tooltip.classList.add('hidden');
      state.historyHoveredPointId = '';
      return;
    }
    state.historyHoveredPointId = String(found.id || '');
    const tokenText = Array.isArray(found.semantic_tokens) ? found.semantic_tokens.join(', ') : '';
    tooltip.innerHTML = `
      <div><strong>${escapeHtml(String(found.title || 'Untitled'))}</strong></div>
      <div>${escapeHtml(String(found.url_host || ''))}</div>
      <div class="muted small">${escapeHtml(String(found.url || ''))}</div>
      <div class="muted small">${escapeHtml(tokenText.slice(0, 240))}</div>
    `;
    const hostWidth = Math.max(20, Number((wrap && wrap.clientWidth) || canvas.clientWidth || cssWidth));
    const hostHeight = Math.max(20, Number((wrap && wrap.clientHeight) || canvas.clientHeight || cssHeight));
    tooltip.classList.remove('hidden');
    const tooltipWidth = Math.max(20, Number(tooltip.offsetWidth || 220));
    const tooltipHeight = Math.max(20, Number(tooltip.offsetHeight || 80));
    const left = Math.min(Math.max(8, Number(event.offsetX || 0) + 14), Math.max(8, hostWidth - tooltipWidth - 8));
    const top = Math.min(Math.max(8, Number(event.offsetY || 0) + 14), Math.max(8, hostHeight - tooltipHeight - 8));
    tooltip.style.left = `${left}px`;
    tooltip.style.top = `${top}px`;
  };
  canvas.onmouseleave = () => {
    if (tooltip) tooltip.classList.add('hidden');
    state.historyHoveredPointId = '';
  };
  canvas.onclick = async (event) => {
    const rect = canvas.getBoundingClientRect();
    const x = ((event.clientX - rect.left) / rect.width) * cssWidth;
    const y = ((event.clientY - rect.top) / rect.height) * cssHeight;
    let found = null;
    let minDist = 10;
    projected.forEach((point) => {
      const dx = point.px - x;
      const dy = point.py - y;
      const dist = Math.sqrt((dx * dx) + (dy * dy));
      if (dist < minDist) {
        minDist = dist;
        found = point;
      }
    });
    if (!found) return;
    const id = String(found.id || '');
    let entry = state.historyEntries.find((row) => String((row && row.id) || '') === id);
    if (!entry) {
      const lookup = await api.historyGet(id);
      if (!lookup || !lookup.ok || !lookup.entry) return;
      entry = lookup.entry;
    }
    state.historySelectedId = id;
    renderHistoryList();
    updateHistoryPreviewMeta(entry);
    await showHistoryPreviewForEntry(entry);
  };
}

async function refreshHistorySemanticMap() {
  const res = await api.historySemanticMap({
    query: state.historySearchQuery,
    max_points: 2000,
  });
  if (!res || !res.ok) {
    state.historyMapPoints = [];
    state.historyMapBounds = null;
    state.historyMapClusters = [];
    drawHistorySemanticMap();
    return;
  }
  state.historyMapPoints = Array.isArray(res.points) ? res.points : [];
  state.historyMapBounds = res.bounds || null;
  state.historyMapClusters = Array.isArray(res.clusters) ? res.clusters : [];
  drawHistorySemanticMap();
}

async function loadHistoryPageData() {
  const res = await api.historyList({
    query: state.historySearchQuery,
    limit: 2000,
    offset: 0,
  });
  if (!res || !res.ok) {
    state.historyEntries = [];
    renderHistoryList();
    await refreshHistorySemanticMap();
    setHistoryStatus((res && res.message) ? res.message : 'Unable to load history.');
    return;
  }
  state.historyEntries = Array.isArray(res.entries) ? res.entries : [];
  renderHistoryList();
  await refreshHistorySemanticMap();
  setHistoryStatus(`Loaded ${state.historyEntries.length} item(s).`);
  if (state.historySelectedId) {
    const selected = state.historyEntries.find((item) => String((item && item.id) || '') === String(state.historySelectedId || ''));
    if (!selected) {
      state.historySelectedId = '';
      await api.historyPreviewHide();
      renderHistoryCachedPreview(null);
      updateHistoryPreviewMeta(null);
      renderHistoryList();
    }
  }
  if (!state.historySelectedId && state.historyEntries.length > 0) {
    state.historySelectedId = String((state.historyEntries[0] && state.historyEntries[0].id) || '');
    const first = state.historyEntries[0];
    updateHistoryPreviewMeta(first);
    await showHistoryPreviewForEntry(first);
    renderHistoryList();
  } else if (!state.historyEntries.length) {
    await api.historyPreviewHide();
    renderHistoryCachedPreview(null);
    updateHistoryPreviewMeta(null);
  }
}

async function openHistoryPage() {
  await setAppView('history');
  await loadHistoryPageData();
}

function getSettingsFormElements() {
  return Array.from(document.querySelectorAll('[data-setting]'));
}

function parseCommaSeparatedList(value) {
  if (Array.isArray(value)) {
    return value.map((item) => String(item || '').trim()).filter(Boolean);
  }
  return String(value || '')
    .split(',')
    .map((item) => String(item || '').trim())
    .filter(Boolean);
}

function renderSettingsLmstudioModelSelect(selectId, selectedValue = '') {
  const select = e(selectId);
  if (!select) return;
  const models = Array.isArray(state.settingsLmstudioModels) ? state.settingsLmstudioModels.filter(Boolean) : [];
  const uniqueModels = Array.from(new Set(models));
  const preferred = String(selectedValue || '').trim();
  if (!uniqueModels.length) {
    if (preferred) {
      select.innerHTML = `<option value="${escapeHtml(preferred)}">${escapeHtml(preferred)} (saved)</option>`;
      select.value = preferred;
    } else {
      select.innerHTML = '<option value="">No models loaded</option>';
      select.value = '';
    }
    return;
  }
  let options = uniqueModels.map((model) => `<option value="${escapeHtml(model)}">${escapeHtml(model)}</option>`).join('');
  if (preferred && !uniqueModels.includes(preferred)) {
    options = `<option value="${escapeHtml(preferred)}">${escapeHtml(preferred)} (saved)</option>${options}`;
  }
  select.innerHTML = options;
  if (preferred) {
    select.value = preferred;
  } else {
    select.value = uniqueModels[0];
  }
}

function renderSettingsAbstractionStatus() {
  const node = e('settings-abstraction-status');
  if (!node) return;
  const status = state.settingsAbstractionStatus || null;
  const draft = normalizeSettingsDraft(state.settingsDraft || {});
  const draftEnabled = !!draft.abstraction_enabled;
  const persistedEnabled = status && status.ok !== false ? !!status.enabled : null;
  if (state.settingsDirty && persistedEnabled != null && draftEnabled !== persistedEnabled) {
    node.textContent = draftEnabled
      ? 'Abstraction will be enabled after save.'
      : 'Abstraction will be disabled after save.';
    return;
  }
  if (!status || status.ok === false) {
    node.textContent = status && status.message ? status.message : 'Unavailable';
    return;
  }
  if (status.enabled === false) {
    node.textContent = String(status.message || 'Abstraction is disabled.');
    return;
  }
  const counts = status.counts && typeof status.counts === 'object' ? status.counts : {};
  const refs = Array.isArray(status.references) ? status.references : [];
  const totalLocalFiles = refs.reduce((acc, item) => acc + Number((item && item.local_file_count) || 0), 0);
  const updated = Array.isArray(status.references)
    ? status.references.map((item) => Number((item && item.updated_at) || 0)).filter((value) => Number.isFinite(value) && value > 0)
    : [];
  const latest = updated.length ? Math.max(...updated) : 0;
  const latestText = latest > 0 ? ` · updated ${formatAgo(latest)}` : '';
  node.textContent = `refs: ready=${Number(counts.ready || 0)}, building=${Number(counts.building || 0)}, stale=${Number(counts.stale || 0)}, error=${Number(counts.error || 0)} · local files=${Number(totalLocalFiles || 0)}${latestText}`;
}

function renderSettingsRagStatus() {
  const node = e('settings-rag-status');
  if (!node) return;
  const status = state.settingsRagStatus || null;
  const draft = normalizeSettingsDraft(state.settingsDraft || {});
  if (!draft.rag_enabled) {
    node.textContent = 'RAG is disabled.';
    return;
  }
  if (!status || status.ok === false) {
    node.textContent = status && status.message ? status.message : 'Unavailable';
    return;
  }
  const counts = status.counts && typeof status.counts === 'object' ? status.counts : {};
  const refs = Array.isArray(status.references) ? status.references : [];
  const active = refs[0] && typeof refs[0] === 'object' ? refs[0] : null;
  const source = String(status.source || draft.rag_embedding_source || RAG_EMBEDDING_SOURCE_DEFAULT).trim() || RAG_EMBEDDING_SOURCE_DEFAULT;
  const updated = refs.map((item) => Number((item && item.updated_at) || 0)).filter((v) => Number.isFinite(v) && v > 0);
  const latest = updated.length ? Math.max(...updated) : 0;
  const latestText = latest > 0 ? ` · updated ${formatAgo(latest)}` : '';
  const runtimeText = active
    ? ` · runtime=${String(active.embedding_runtime || 'none') || 'none'} · model=${String(active.model_id || '-').trim() || '-'}`
    : '';
  node.textContent = `source=${source} · ready=${Number(counts.ready || 0)}, missing=${Number(counts.missing || 0)}, empty=${Number(counts.empty || 0)}, error=${Number(counts.error || 0)}${runtimeText}${latestText}`;
}

function isConfiguredSettingsValue(key = '', value, defaults = {}) {
  if (!key) return false;
  const baseline = defaults[key];
  if (Array.isArray(value) || Array.isArray(baseline)) {
    return JSON.stringify(Array.isArray(value) ? value : []) !== JSON.stringify(Array.isArray(baseline) ? baseline : []);
  }
  return value !== baseline;
}

function renderSettingsConfiguredState() {
  const defaults = normalizeSettingsDraft({});
  const configuredSections = new Set();
  getSettingsFormElements().forEach((node) => {
    const key = String(node.getAttribute('data-setting') || '').trim();
    const field = node.closest('.settings-field');
    const configured = isConfiguredSettingsValue(key, (state.settingsDraft || {})[key], defaults);
    if (field) field.classList.toggle('settings-configured', configured);
    node.classList.toggle('settings-configured-control', configured);
    const section = node.closest('.settings-accordion');
    if (configured && section) configuredSections.add(section);
  });

  const statusConfiguredSelectors = [
    ['#settings-lmstudio-token-status', !!state.lmstudioTokenConfigured],
    ['#settings-orchestrator-web-key-status', !!state.orchestratorWebKeyConfigured],
    ['#settings-telegram-token-status', !!(state.telegramRuntimeStatus && state.telegramRuntimeStatus.token_configured)],
  ];
  statusConfiguredSelectors.forEach(([selector, configured]) => {
    const node = document.querySelector(selector);
    const field = node ? node.closest('.settings-field') : null;
    if (field) field.classList.toggle('settings-configured', configured);
    if (configured && field) {
      const section = field.closest('.settings-accordion');
      if (section) configuredSections.add(section);
    }
  });

  const providerKeysSection = e('settings-provider-keys');
  const providerEntries = Array.isArray(state.providerKeysState && state.providerKeysState.providers)
    ? state.providerKeysState.providers
    : [];
  const hasConfiguredProviderKeys = providerEntries.some((entry) => entry && (entry.configured || (Array.isArray(entry.keys) && entry.keys.length > 0)));
  if (providerKeysSection) {
    providerKeysSection.classList.toggle('settings-section-configured', hasConfiguredProviderKeys);
    if (hasConfiguredProviderKeys) configuredSections.add(providerKeysSection);
  }

  document.querySelectorAll('#settings-page .settings-accordion').forEach((section) => {
    section.classList.toggle('settings-section-configured', configuredSections.has(section));
  });
}

function normalizeSettingsDraft(raw = {}) {
  const src = (raw && typeof raw === 'object') ? raw : {};
  const telegramAllowedChatIds = parseCommaSeparatedList(src.telegram_allowed_chat_ids);
  const telegramAllowedUsernames = parseCommaSeparatedList(src.telegram_allowed_usernames)
    .map((item) => item.toLowerCase().replace(/^@/, ''));
  const historyMaxRaw = Number(src.history_max_entries);
  const historyMaxEntries = Number.isFinite(historyMaxRaw)
    ? Math.max(500, Math.min(10000, Math.round(historyMaxRaw)))
    : HISTORY_DEFAULT_MAX_ENTRIES;
  const mailPollIntervalRaw = Number(src.mail_poll_interval_sec);
  const mailPollIntervalSec = Number.isFinite(mailPollIntervalRaw)
    ? Math.max(120, Math.min(900, Math.round(mailPollIntervalRaw)))
    : 300;
  const ragEmbeddingSource = String(src.rag_embedding_source || RAG_EMBEDDING_SOURCE_DEFAULT).trim().toLowerCase() === 'inbuilt'
    ? 'inbuilt'
    : 'lmstudio';
  return {
    default_search_engine: String(src.default_search_engine || 'ddg').trim().toLowerCase(),
    reference_ranking_enabled: !!src.reference_ranking_enabled,
    lumino_last_provider: String(src.lumino_last_provider || 'openai').trim().toLowerCase(),
    lumino_last_model: String(src.lumino_last_model || '').trim(),
    lmstudio_base_url: String(src.lmstudio_base_url || 'http://127.0.0.1:1234').trim(),
    lmstudio_default_model: String(src.lmstudio_default_model || '').trim(),
    orchestrator_web_provider: String(src.orchestrator_web_provider || 'ddg').trim().toLowerCase(),
    abstraction_enabled: !!src.abstraction_enabled,
    abstraction_model: String(src.abstraction_model || '').trim(),
    abstraction_strict_redaction: Object.prototype.hasOwnProperty.call(src, 'abstraction_strict_redaction')
      ? !!src.abstraction_strict_redaction
      : true,
    image_analysis_model: String(src.image_analysis_model || '').trim(),
    image_analysis_prompt: String(src.image_analysis_prompt || IMAGE_ANALYSIS_PROMPT_DEFAULT).trim() || IMAGE_ANALYSIS_PROMPT_DEFAULT,
    rag_enabled: Object.prototype.hasOwnProperty.call(src, 'rag_enabled') ? !!src.rag_enabled : true,
    rag_embedding_source: ragEmbeddingSource,
    rag_embedding_model: String(src.rag_embedding_model || RAG_EMBEDDING_MODEL_DEFAULT).trim() || RAG_EMBEDDING_MODEL_DEFAULT,
    rag_top_k: Number.isFinite(Number(src.rag_top_k))
      ? Math.max(1, Math.min(24, Math.round(Number(src.rag_top_k))))
      : RAG_TOP_K_DEFAULT,
    telegram_enabled: !!src.telegram_enabled,
    telegram_allowed_chat_ids: telegramAllowedChatIds,
    telegram_allowed_usernames: telegramAllowedUsernames,
    telegram_poll_interval_sec: Number(src.telegram_poll_interval_sec || 2),
    hyperweb_enabled: !!src.hyperweb_enabled,
    hyperweb_display_name: String(src.hyperweb_display_name || '').trim(),
    crawler_mode: String(src.crawler_mode || 'broad').trim().toLowerCase(),
    crawler_markdown_first: !!src.crawler_markdown_first,
    crawler_robots_default: String(src.crawler_robots_default || 'respect').trim().toLowerCase(),
    crawler_depth_default: Number(src.crawler_depth_default || 3),
    crawler_page_cap_default: Number(src.crawler_page_cap_default || 80),
    agent_mode_v1_enabled: !!src.agent_mode_v1_enabled,
    mail_sync_enabled: !!src.mail_sync_enabled,
    mail_poll_interval_sec: mailPollIntervalSec,
    history_enabled: Object.prototype.hasOwnProperty.call(src, 'history_enabled') ? !!src.history_enabled : true,
    history_max_entries: historyMaxEntries,
  };
}

function validateSettingsDraft(draft = {}) {
  const d = normalizeSettingsDraft(draft);
  const errors = {};
  if (!['ddg', 'google', 'bing'].includes(d.default_search_engine)) errors.default_search_engine = 'Invalid search engine.';
  if (!PROVIDERS.includes(d.lumino_last_provider)) errors.lumino_last_provider = 'Unsupported provider.';
  if (!/^https?:\/\//i.test(d.lmstudio_base_url || '')) errors.lmstudio_base_url = 'LM Studio URL must start with http:// or https://';
  if (!['ddg', 'serpapi'].includes(d.orchestrator_web_provider)) errors.orchestrator_web_provider = 'Invalid web provider.';
  if (d.abstraction_enabled && !String(d.abstraction_model || d.image_analysis_model || '').trim()) {
    errors.abstraction_model = 'Abstraction model is required when abstraction is enabled (or set Image Analysis Model as fallback).';
  }
  if (!Number.isFinite(d.telegram_poll_interval_sec) || d.telegram_poll_interval_sec < 1 || d.telegram_poll_interval_sec > 30) {
    errors.telegram_poll_interval_sec = 'Polling interval must be 1..30 sec.';
  }
  if (!Number.isFinite(d.mail_poll_interval_sec) || d.mail_poll_interval_sec < 120 || d.mail_poll_interval_sec > 900) {
    errors.mail_poll_interval_sec = 'Mail polling interval must be 120..900 sec.';
  }
  if (!['safe', 'broad'].includes(d.crawler_mode)) errors.crawler_mode = 'Invalid crawler mode.';
  if (!['respect', 'ignore'].includes(d.crawler_robots_default)) errors.crawler_robots_default = 'Invalid robots setting.';
  if (!Number.isFinite(d.crawler_depth_default) || d.crawler_depth_default < 1 || d.crawler_depth_default > 6) {
    errors.crawler_depth_default = 'Depth must be 1..6.';
  }
  if (!Number.isFinite(d.crawler_page_cap_default) || d.crawler_page_cap_default < 5 || d.crawler_page_cap_default > 300) {
    errors.crawler_page_cap_default = 'Page cap must be 5..300.';
  }
  if (!Number.isFinite(d.history_max_entries) || d.history_max_entries < 500 || d.history_max_entries > 10000) {
    errors.history_max_entries = 'History max entries must be 500..10000.';
  }
  if (!['lmstudio', 'inbuilt'].includes(String(d.rag_embedding_source || '').trim().toLowerCase())) {
    errors.rag_embedding_source = 'RAG embedding source must be lmstudio or inbuilt.';
  }
  if (!Number.isFinite(d.rag_top_k) || d.rag_top_k < 1 || d.rag_top_k > 24) {
    errors.rag_top_k = 'RAG Top-K must be 1..24.';
  }
  return errors;
}

function isSettingsDirty() {
  if (!state.settingsDraft || !state.settingsPersisted) return false;
  return JSON.stringify(state.settingsDraft) !== JSON.stringify(state.settingsPersisted);
}

function readSettingsDraftFromForm() {
  const draft = state.settingsDraft ? { ...state.settingsDraft } : {};
  getSettingsFormElements().forEach((node) => {
    const key = String(node.getAttribute('data-setting') || '').trim();
    if (!key) return;
    if (node.type === 'checkbox') {
      draft[key] = !!node.checked;
      return;
    }
    if (node.type === 'number') {
      draft[key] = Number(node.value || 0);
      return;
    }
    draft[key] = String(node.value || '');
  });
  state.settingsDraft = normalizeSettingsDraft(draft);
  state.settingsValidationErrors = validateSettingsDraft(state.settingsDraft);
  state.settingsDirty = isSettingsDirty();
}

function renderSettingsStatusLine() {
  const node = e('settings-status-line');
  const saveBtn = e('settings-save-btn');
  const cancelBtn = e('settings-cancel-btn');
  const errorCount = Object.keys(state.settingsValidationErrors || {}).length;
  const dirty = !!state.settingsDirty;
  if (node) {
    if (errorCount > 0) {
      node.textContent = `Validation errors: ${errorCount}`;
    } else if (state.settingsSaveState) {
      node.textContent = state.settingsSaveState;
    } else if (dirty) {
      node.textContent = 'Unsaved changes';
    } else {
      node.textContent = 'All changes saved.';
    }
  }
  if (saveBtn) saveBtn.disabled = !dirty || errorCount > 0;
  if (cancelBtn) cancelBtn.disabled = !dirty;
}

function renderAppDataProtectionStatus() {
  const node = e('settings-appdata-lock-status');
  const touchBtn = e('settings-appdata-unlock-touchid-btn');
  const pwdBtn = e('settings-appdata-unlock-password-btn');
  const lockBtn = e('settings-appdata-lock-btn');
  const status = state.appDataProtectionStatus || null;
  if (!node) return;
  if (!status || status.ok === false) {
    node.textContent = 'Unavailable';
    if (touchBtn) touchBtn.disabled = true;
    if (pwdBtn) pwdBtn.disabled = true;
    if (lockBtn) lockBtn.disabled = true;
    return;
  }
  const locked = !!status.locked;
  node.textContent = locked
    ? 'Locked'
    : `Unlocked${status.unlocked_at ? ` · ${formatAgo(status.unlocked_at)}` : ''}`;
  if (touchBtn) touchBtn.disabled = !status.touchid_available || !locked;
  if (pwdBtn) pwdBtn.disabled = !locked;
  if (lockBtn) lockBtn.disabled = locked;
}

function renderSettingsForm() {
  const draft = normalizeSettingsDraft(state.settingsDraft || {});
  renderSettingsLmstudioModelSelect('settings-abstraction-model', draft.abstraction_model);
  renderSettingsLmstudioModelSelect('settings-image-analysis-model', draft.image_analysis_model);
  renderSettingsLmstudioModelSelect('settings-rag-embedding-model', draft.rag_embedding_model);
  getSettingsFormElements().forEach((node) => {
    const key = String(node.getAttribute('data-setting') || '').trim();
    if (!key) return;
    const value = draft[key];
    if (node.type === 'checkbox') {
      node.checked = !!value;
    } else {
      if (Array.isArray(value)) {
        node.value = value.join(', ');
      } else {
        node.value = value === null || typeof value === 'undefined' ? '' : String(value);
      }
    }
    node.classList.toggle('invalid', !!(state.settingsValidationErrors && state.settingsValidationErrors[key]));
    node.title = (state.settingsValidationErrors && state.settingsValidationErrors[key]) || '';
  });
  const ragUsesLmstudio = draft.rag_embedding_source !== 'inbuilt';
  const ragModelNode = e('settings-rag-embedding-model');
  const ragFetchBtn = e('settings-rag-fetch-models-btn');
  if (ragModelNode) ragModelNode.disabled = !ragUsesLmstudio;
  if (ragFetchBtn) ragFetchBtn.disabled = !ragUsesLmstudio;
  renderMailSettingsStatus();
  renderMailAccountsList();
  renderAppDataProtectionStatus();
  renderSettingsAbstractionStatus();
  renderSettingsRagStatus();
  renderSettingsStatusLine();
  renderSettingsHyperwebControls();
  renderSettingsConfiguredState();
}

function renderSettingsDiagnostics() {
  const diag = state.settingsDiagnostics || {};
  const hyper = e('settings-diagnostics-hyperweb');
  const identity = e('settings-diagnostics-identity');
  const python = e('settings-diagnostics-python');
  const pythonDownloadBtn = e('settings-python-download-btn');
  if (hyper) hyper.textContent = JSON.stringify((diag && diag.hyperweb) || {}, null, 2);
  if (identity) identity.textContent = JSON.stringify((diag && diag.hyperweb_identity) || {}, null, 2);
  if (python) {
    const tool = (diag && diag.python && diag.python.tool && typeof diag.python.tool === 'object')
      ? diag.python.tool
      : {};
    const lines = [];
    lines.push(`available: ${tool.ok ? 'yes' : 'no'}`);
    if (tool.version) lines.push(`version: ${tool.version}`);
    if (tool.python_bin) lines.push(`binary: ${tool.python_bin}`);
    if (tool.source) lines.push(`source: ${tool.source}`);
    if (tool.message) lines.push(`note: ${tool.message}`);
    if (!tool.ok && electronApi && electronApi.platform === 'win32') {
      lines.push('recommended: install Python 3.11 x64 and make sure python or python3 is available in PATH');
    }
    python.textContent = lines.join('\n');
  }
  if (pythonDownloadBtn) {
    pythonDownloadBtn.hidden = !(electronApi && electronApi.platform === 'win32');
  }
  renderSettingsHyperwebControls();
}

function renderSettingsHyperwebControls() {
  const statusNode = e('settings-hyperweb-network-status');
  const connectBtn = e('settings-hyperweb-connect-btn');
  const disconnectBtn = e('settings-hyperweb-disconnect-btn');
  const draft = normalizeSettingsDraft(state.settingsDraft || {});
  const diag = state.settingsDiagnostics || {};
  const hyper = (state.hyperwebStatus && typeof state.hyperwebStatus === 'object')
    ? state.hyperwebStatus
    : ((diag && diag.hyperweb && typeof diag.hyperweb === 'object') ? diag.hyperweb : {});
  const allowed = !!draft.hyperweb_enabled;
  const connected = !!hyper.connected;
  const degraded = !!hyper.degraded;
  const peerCount = Math.max(0, Number(hyper.peer_count || 0));
  const pendingEntries = Math.max(0, Number(hyper.pending_private_entries || 0));

  if (statusNode) {
    if (!allowed) {
      statusNode.textContent = 'Disabled. Turn on "Allow Hyperweb" before going online.';
    } else if (connected && degraded) {
      statusNode.textContent = `Online with issues. ${peerCount} peer${peerCount === 1 ? '' : 's'} connected${pendingEntries > 0 ? ` · ${pendingEntries} pending private entr${pendingEntries === 1 ? 'y' : 'ies'}` : ''}.`;
    } else if (connected) {
      statusNode.textContent = `Online. ${peerCount} peer${peerCount === 1 ? '' : 's'} connected${pendingEntries > 0 ? ` · ${pendingEntries} pending private entr${pendingEntries === 1 ? 'y' : 'ies'}` : ''}.`;
    } else {
      statusNode.textContent = 'Offline. Trusted peers stay saved, but live sync and direct delivery are paused.';
    }
  }

  if (connectBtn) connectBtn.disabled = !allowed || connected;
  if (disconnectBtn) disconnectBtn.disabled = !connected;
}

function formatTrustedPeerFingerprint(peerId = '') {
  const clean = String(peerId || '').trim().toUpperCase();
  if (!clean) return '';
  if (clean.length <= 16) return clean;
  return `${clean.slice(0, 8)}...${clean.slice(-6)}`;
}

function trustedPeerPresenceLabel(peer = {}) {
  const presence = String((peer && peer.presence_status) || '').trim().toLowerCase();
  if (presence === 'online') return 'online';
  if (presence === 'seen_recently') return 'seen recently';
  return 'offline';
}

function renderSettingsTrustedPeers() {
  const holder = e('settings-hyperweb-trusted-peers');
  if (!holder) return;
  const peers = Array.isArray(state.settingsTrustedPeers) ? state.settingsTrustedPeers : [];
  if (peers.length === 0) {
    holder.innerHTML = '<div class="settings-hyperweb-trusted-peer-empty">No trusted peers yet.</div>';
    return;
  }
  holder.innerHTML = peers.map((peer) => {
    const peerId = String((peer && peer.peer_id) || '').trim().toUpperCase();
    const alias = escapeHtml(String((peer && peer.alias) || peerId || 'Peer'));
    const detailParts = [
      formatTrustedPeerFingerprint(peerId),
      trustedPeerPresenceLabel(peer),
    ];
    if (Number((peer && peer.last_seen_at) || 0) > 0 && String((peer && peer.presence_status) || '').trim().toLowerCase() !== 'online') {
      detailParts.push(formatAgo(Number(peer.last_seen_at || 0)));
    }
    if (String((peer && peer.relay_peer_id) || '').trim()) {
      detailParts.push(`relay ${formatTrustedPeerFingerprint(String(peer.relay_peer_id || ''))}`);
    }
    return `
      <div class="settings-hyperweb-trusted-peer-row">
        <div class="settings-hyperweb-trusted-peer-meta">
          <div class="settings-hyperweb-trusted-peer-name">${alias}</div>
          <div class="settings-hyperweb-trusted-peer-detail" title="${escapeHtml(peerId)}">${escapeHtml(detailParts.filter(Boolean).join(' · '))}</div>
        </div>
        <button type="button" data-settings-remove-peer="${escapeHtml(peerId)}">Remove</button>
      </div>
    `;
  }).join('');
  holder.querySelectorAll('button[data-settings-remove-peer]').forEach((button) => {
    button.addEventListener('click', async () => {
      const peerId = String(button.getAttribute('data-settings-remove-peer') || '').trim().toUpperCase();
      if (!peerId || !api || typeof api.hyperwebForgetPeer !== 'function') return;
      const confirmed = window.confirm(
        'Remove this trusted peer locally?\n\nTrust will be removed, DM history deleted, private-share communication and history deleted, and shared access revoked locally.'
      );
      if (!confirmed) return;
      button.disabled = true;
      try {
        const res = await api.hyperwebForgetPeer(peerId);
        if (!res || !res.ok) {
          state.settingsSaveState = (res && res.message) ? res.message : 'Unable to remove trusted peer.';
          renderSettingsStatusLine();
          return;
        }
        state.settingsSaveState = 'Trusted peer removed locally.';
        renderSettingsStatusLine();
        await handleTrustedPeerRemoved(res, peerId);
      } finally {
        button.disabled = false;
      }
    });
  });
}

async function refreshSettingsTrustedPeers() {
  if (!api || typeof api.hyperwebTrustedPeersList !== 'function') {
    state.settingsTrustedPeers = [];
    renderSettingsTrustedPeers();
    return;
  }
  const res = await api.hyperwebTrustedPeersList();
  state.settingsTrustedPeers = (res && res.ok && Array.isArray(res.peers)) ? res.peers : [];
  renderSettingsTrustedPeers();
}

async function handleTrustedPeerRemoved(res = {}, peerId = '') {
  const targetId = String(peerId || '').trim().toUpperCase();
  const removedRoomIds = new Set(Array.isArray(res && res.removed_room_ids) ? res.removed_room_ids.map((id) => String(id || '').trim()) : []);
  const removedActiveDm = String(state.hyperwebChatPeerId || '').trim().toUpperCase() === targetId;
  const removedActiveRoom = removedRoomIds.has(String(state.privateActiveRoomId || '').trim());

  await refreshSettingsTrustedPeers();
  try {
    const diagnostics = await api.settingsDiagnostics();
    if (diagnostics && diagnostics.ok) {
      state.settingsDiagnostics = diagnostics;
      renderSettingsDiagnostics();
    }
  } catch (_) {}
  try { await refreshShareMemberDirectory(); } catch (_) {}
  try { await refreshHyperwebStatus(); } catch (_) {}
  try { await refreshPrivateSharesData(); } catch (_) {}
  if (state.appView === 'hyperweb' && state.hyperwebActiveTab === 'chat') {
    try { await refreshHyperwebChatData(); } catch (_) {}
  }

  if (removedActiveDm) {
    state.hyperwebChatPeerId = '';
    state.hyperwebChatMessages = [];
    state.hyperwebChatThreadId = '';
    state.hyperwebChatThreadPolicy = { retention: 'off' };
    state.hyperwebChatActivePresence = null;
    state.hyperwebChatLivePeerCount = 0;
    renderHyperwebChatSelectors();
    renderHyperwebDmConversationList();
    renderHyperwebChatThread();
    setStatusText('hyperweb-chat-status', 'Trusted peer removed locally. Conversation cleared.');
  }

  if (removedActiveRoom) {
    state.privateActiveRoomId = '';
    applySharedRoomState(null);
    renderSharedRooms();
  }
}

function normalizeHyperwebInviteTokenInput(value = '') {
  const raw = String(value || '').trim();
  if (!raw) return '';
  try {
    const parsed = new URL(raw);
    const token = String(parsed.searchParams.get('token') || '').trim();
    if (token) return token;
  } catch (_) {
    // treat as raw token
  }
  return raw;
}

function readDangerConfirmPhrase() {
  const input = e('settings-danger-confirm-input');
  return String((input && input.value) || '').trim();
}

function clearDangerConfirmPhrase() {
  const input = e('settings-danger-confirm-input');
  if (input) input.value = '';
}

function renderTelegramSettingsStatus() {
  const node = e('settings-telegram-token-status');
  if (!node) return;
  const status = state.telegramRuntimeStatus || {};
  if (!status || status.ok === false) {
    node.textContent = `Error: ${status && status.message ? status.message : 'Unavailable'}`;
    return;
  }
  const runtime = status.runtime && typeof status.runtime === 'object' ? status.runtime : {};
  const enabled = !!status.enabled;
  const configured = !!status.token_configured;
  const running = !!runtime.running;
  const activeUsers = Math.max(0, Number(status.active_user_count || 0));
  node.textContent = `enabled=${enabled ? 'yes' : 'no'}, token=${configured ? 'configured' : 'missing'}, running=${running ? 'yes' : 'no'}, users=${activeUsers}`;
  renderSettingsConfiguredState();
}

function renderOrchestratorUsersList() {
  const node = e('settings-telegram-users-list');
  if (!node) return;
  const users = Array.isArray(state.orchestratorUsers) ? state.orchestratorUsers : [];
  if (state.orchestratorUsersLoading) {
    node.innerHTML = '<div class="muted small">Loading users...</div>';
    return;
  }
  if (!users.length) {
    node.innerHTML = '<div class="muted small">No active users.</div>';
    return;
  }
  node.innerHTML = users.map((user) => {
    const chatId = String((user && user.chat_id) || '').trim();
    const userId = escapeHtml(String((user && user.user_id) || '').trim() || 'unknown');
    const usernameRaw = String((user && user.telegram_username) || '').trim();
    const username = usernameRaw ? `@${escapeHtml(usernameRaw.replace(/^@/, ''))}` : 'no username';
    const secondary = [
      `chat: ${escapeHtml(chatId || '-')}`,
      username,
      `prompts: ${Math.max(0, Number((user && user.prompts_total) || 0))}`,
    ].join(' · ');
    return `
      <div class="settings-user-row">
        <div class="settings-user-meta">
          <div class="settings-user-primary">${userId}</div>
          <div class="settings-user-secondary">${secondary}</div>
        </div>
        <button data-telegram-revoke-chat-id="${escapeHtml(chatId)}">Revoke</button>
      </div>
    `;
  }).join('');
}

function renderLmstudioTokenStatus() {
  const node = e('settings-lmstudio-token-status');
  if (!node) return;
  if (state.lmstudioTokenConfigured == null) {
    node.textContent = 'Unknown';
    return;
  }
  node.textContent = state.lmstudioTokenConfigured ? 'Configured' : 'Missing';
  renderSettingsConfiguredState();
}

function renderOrchestratorWebKeyStatus() {
  const node = e('settings-orchestrator-web-key-status');
  if (!node) return;
  if (state.orchestratorWebKeyConfigured == null) {
    node.textContent = 'Unknown';
    return;
  }
  node.textContent = state.orchestratorWebKeyConfigured ? 'Configured' : 'Missing';
  renderSettingsConfiguredState();
}

function renderMailSettingsStatus() {
  const node = e('settings-mail-status');
  if (!node) return;
  const status = state.mailStatus || null;
  const draft = normalizeSettingsDraft(state.settingsDraft || {});
  if (!draft.mail_sync_enabled) {
    node.textContent = 'Mail sync is disabled.';
    return;
  }
  if (!status || status.ok === false) {
    node.textContent = (status && status.message) || 'Unavailable';
    return;
  }
  const count = Array.isArray(state.mailAccounts) ? state.mailAccounts.length : 0;
  const running = status.scheduler_running ? 'scheduler on' : 'scheduler off';
  const interval = Number(status.poll_interval_sec || draft.mail_poll_interval_sec || 300);
  node.textContent = `${count} mailbox account(s) configured · ${running} · every ${Math.round(interval / 60)} min`;
}

function describeMailAccountStatus(account = {}) {
  const syncState = String((account && account.sync_state) || 'idle').trim().toLowerCase();
  const stateLabel = syncState === 'syncing'
    ? 'Syncing now'
    : (syncState === 'error' ? 'Sync error' : 'Idle');
  const checked = formatAgo((account && account.last_sync_at) || 0) || 'never';
  const success = formatAgo((account && account.last_success_at) || 0) || 'never';
  const newThreads = Math.max(0, Number((account && account.new_threads_count) || 0));
  const newMessages = Math.max(0, Number((account && account.new_messages_count) || 0));
  const base = `${stateLabel} · Last checked: ${checked} · Last success: ${success}`;
  if (account && account.last_error) return `${base} · ${String(account.last_error || '').trim()}`;
  if (newThreads > 0 || newMessages > 0) return `${base} · New: ${newThreads} thread(s), ${newMessages} message(s)`;
  return base;
}

function getMailAccountById(accountId = '') {
  const id = String(accountId || '').trim();
  const accounts = Array.isArray(state.mailAccounts) ? state.mailAccounts : [];
  return accounts.find((item) => String((item && item.id) || '').trim() === id) || null;
}

function renderMailAccountsList() {
  const node = e('settings-mail-accounts-list');
  if (!node) return;
  const accounts = Array.isArray(state.mailAccounts) ? state.mailAccounts : [];
  if (!accounts.length) {
    node.innerHTML = '<div class="muted small">No mailbox accounts configured.</div>';
    return;
  }
  node.innerHTML = accounts.map((account) => `
    <div class="settings-mail-account-row">
      <div class="settings-mail-account-meta">
        <div class="settings-user-primary">${escapeHtml(String((account && account.label) || 'Mailbox'))}</div>
        <div class="settings-user-secondary">
          ${escapeHtml(String((account && account.email) || ''))} · ${escapeHtml(String((account && account.account_type) || 'manual_imap_smtp').replace(/_/g, ' '))} · ${escapeHtml(String((account && account.provider) || 'generic').replace(/_/g, ' '))}
        </div>
        <div class="settings-user-secondary">
          ${escapeHtml(describeMailAccountStatus(account))}
        </div>
        <div class="settings-user-secondary">
          ${escapeHtml(String((account && account.host) || ''))}:${escapeHtml(String((account && account.port) || ''))} · ${escapeHtml(String((account && account.mailbox) || 'INBOX'))}
        </div>
      </div>
      <div class="settings-inline-actions">
        <label class="settings-field settings-boolean-field">
          <span class="settings-toggle-card">
            <input type="checkbox" data-mail-toggle-notifications="${escapeHtml(String((account && account.id) || ''))}" ${account && account.notifications_enabled ? 'checked' : ''} />
            <span>Notifications</span>
          </span>
        </label>
        <button data-mail-sync-account="${escapeHtml(String((account && account.id) || ''))}">Sync</button>
        <button data-mail-delete-account="${escapeHtml(String((account && account.id) || ''))}">Delete</button>
      </div>
    </div>
  `).join('');
}

async function refreshTelegramSettingsStatus() {
  if (!api.telegramStatus) return;
  const res = await api.telegramStatus();
  state.telegramRuntimeStatus = res || null;
  renderTelegramSettingsStatus();
}

async function refreshMailAccounts() {
  if (!api.mailListAccounts) return;
  const res = await api.mailListAccounts();
  state.mailAccounts = (res && res.ok && Array.isArray(res.accounts)) ? res.accounts : [];
  if (api.mailListMailboxes) {
    const next = new Map();
    for (const account of state.mailAccounts) {
      const mailboxRes = await api.mailListMailboxes(String((account && account.id) || ''));
      next.set(String((account && account.id) || '').trim(), (mailboxRes && mailboxRes.ok && Array.isArray(mailboxRes.mailboxes)) ? mailboxRes.mailboxes : []);
    }
    state.mailboxesByAccount = next;
  }
  renderMailAccountsList();
  renderMailSettingsStatus();
}

function resetMailAccountForm() {
  ['settings-mail-account-label', 'settings-mail-account-email', 'settings-mail-account-host', 'settings-mail-account-username', 'settings-mail-account-password', 'settings-mail-account-smtp-host', 'settings-mail-account-smtp-username', 'settings-mail-account-smtp-password'].forEach((id) => {
    if (e(id)) e(id).value = '';
  });
  if (e('settings-mail-account-provider')) e('settings-mail-account-provider').value = 'generic';
  if (e('settings-mail-account-port')) e('settings-mail-account-port').value = '993';
  if (e('settings-mail-account-smtp-port')) e('settings-mail-account-smtp-port').value = '465';
  if (e('settings-mail-account-mailbox')) e('settings-mail-account-mailbox').value = 'INBOX';
  if (e('settings-mail-account-tls')) e('settings-mail-account-tls').checked = true;
  if (e('settings-mail-account-smtp-tls')) e('settings-mail-account-smtp-tls').checked = true;
  if (e('settings-mail-account-smtp-starttls')) e('settings-mail-account-smtp-starttls').checked = false;
}

async function refreshAppDataProtectionStatus() {
  if (!api.appDataProtectionStatus) return;
  const res = await api.appDataProtectionStatus();
  state.appDataProtectionStatus = res || null;
  renderAppDataProtectionStatus();
}

async function refreshOrchestratorUsersList() {
  const node = e('settings-telegram-users-list');
  if (!node || !api.orchestratorUsersList) return;
  state.orchestratorUsersLoading = true;
  renderOrchestratorUsersList();
  const res = await api.orchestratorUsersList();
  state.orchestratorUsers = (res && res.ok && Array.isArray(res.users)) ? res.users : [];
  state.orchestratorUsersLoading = false;
  renderOrchestratorUsersList();
}

async function refreshLmstudioTokenStatus() {
  if (!api.lmstudioTokenStatus) return;
  const res = await api.lmstudioTokenStatus();
  state.lmstudioTokenConfigured = !!(res && res.ok && res.token_configured);
  renderLmstudioTokenStatus();
}

async function refreshOrchestratorWebKeyStatus() {
  if (!api.orchestratorWebKeyStatus) return;
  const res = await api.orchestratorWebKeyStatus();
  state.orchestratorWebKeyConfigured = !!(res && res.ok && res.key_configured);
  renderOrchestratorWebKeyStatus();
}

async function refreshMailStatus() {
  if (!api.mailStatus) return;
  const res = await api.mailStatus();
  state.mailStatus = res || null;
  renderMailSettingsStatus();
  await refreshMailAccounts();
  await refreshTopbarBadges();
  return res;
}

async function refreshSettingsLmstudioModelOptions() {
  if (!api.providerListModels) return;
  let res = null;
  try {
    res = await api.providerListModels('lmstudio', '');
  } catch (_) {
    res = null;
  }
  if (!res || !res.ok) {
    state.settingsLmstudioModels = [];
    return;
  }
  state.settingsLmstudioModels = Array.isArray(res.models) ? res.models : [];
}

async function refreshAbstractionStatus() {
  if (!api.abstractionStatus) return;
  const res = await api.abstractionStatus({});
  state.settingsAbstractionStatus = res || null;
  renderSettingsAbstractionStatus();
}

async function refreshRagStatus() {
  if (!api.ragStatus) return;
  const srId = String(state.activeSrId || '').trim();
  const res = await api.ragStatus(srId ? { sr_id: srId } : {});
  state.settingsRagStatus = res || null;
  renderSettingsRagStatus();
}

async function loadSettingsData() {
  let prefRes = null;
  let diagnostics = null;
  try {
    prefRes = await api.getPreferences();
  } catch (_) {
    prefRes = null;
  }
  try {
    diagnostics = await api.settingsDiagnostics();
  } catch (_) {
    diagnostics = null;
  }
  try { await refreshSettingsLmstudioModelOptions(); } catch (_) {}
  try { await refreshProviderKeysState({ renderSettings: true }); } catch (_) {}
  if (prefRes && prefRes.ok) {
    state.settingsPersisted = normalizeSettingsDraft(prefRes);
    state.settingsDraft = normalizeSettingsDraft(prefRes);
    state.settingsValidationErrors = {};
    state.settingsDirty = false;
    state.settingsSaveState = '';
    renderSettingsForm();
  }
  try { await refreshTelegramSettingsStatus(); } catch (_) {}
  try { await refreshOrchestratorUsersList(); } catch (_) {}
  try { await refreshLmstudioTokenStatus(); } catch (_) {}
  try { await refreshOrchestratorWebKeyStatus(); } catch (_) {}
  try { await refreshMailStatus(); } catch (_) {}
  try { await refreshAbstractionStatus(); } catch (_) {}
  try { await refreshRagStatus(); } catch (_) {}
  try { await refreshAppDataProtectionStatus(); } catch (_) {}
  try { await refreshHyperwebStatus(); } catch (_) {}
  try { await refreshSettingsTrustedPeers(); } catch (_) {}
  if (diagnostics && diagnostics.ok) {
    state.settingsDiagnostics = diagnostics;
    renderSettingsDiagnostics();
  }
}

function resetSettingsAccordion(openSection = '') {
  const sections = Array.from(document.querySelectorAll('#settings-page .settings-accordion'));
  sections.forEach((section) => {
    const key = String(section.getAttribute('data-settings-section') || '').trim();
    section.open = !!(openSection && key === openSection);
  });
}

function setupSettingsAccordion() {
  const sections = Array.from(document.querySelectorAll('#settings-page .settings-accordion'));
  sections.forEach((section) => {
    section.addEventListener('toggle', () => {
      if (!section.open) return;
      sections.forEach((other) => {
        if (other !== section) other.open = false;
      });
    });
  });
}

async function openSettingsPage() {
  await setAppView('settings');
  await loadSettingsData();
  resetSettingsAccordion('');
}

function applySettingsToTopbar(settings) {
  const next = normalizeSettingsDraft(settings || {});
  const engineSelect = e('default-search-engine-select');
  if (engineSelect) engineSelect.value = next.default_search_engine;
  const onboardingSelect = e('onboarding-search-engine-select');
  if (onboardingSelect) onboardingSelect.value = next.default_search_engine;
  const providerValue = PROVIDERS.includes(next.lumino_last_provider) ? next.lumino_last_provider : 'openai';
  const providerSelect = e('provider-select');
  if (providerSelect) providerSelect.value = providerValue;
  state.selectedProvider = providerValue;
  state.selectedModel = next.lumino_last_model;
}

async function saveSettingsDraft() {
  readSettingsDraftFromForm();
  renderSettingsStatusLine();
  if (Object.keys(state.settingsValidationErrors || {}).length > 0) return;
  const res = await api.updatePreferences(state.settingsDraft || {});
  if (!res || !res.ok) {
    state.settingsSaveState = (res && res.message) ? res.message : 'Unable to save settings.';
    renderSettingsStatusLine();
    return;
  }
  state.settingsPersisted = normalizeSettingsDraft(res.settings || state.settingsDraft || {});
  state.settingsDraft = normalizeSettingsDraft(res.settings || state.settingsDraft || {});
  state.settingsDirty = false;
  state.settingsSaveState = 'Saved.';
  applySettingsToTopbar(state.settingsPersisted);
  await refreshReferenceRankingState({ ensureFresh: true });
  await fetchModelsForProvider(state.settingsPersisted.lumino_last_provider, {
    statusId: 'provider-status',
    forceModel: state.settingsPersisted.lumino_last_model,
    persistSelection: true,
  });
  await refreshTelegramSettingsStatus();
  await refreshLmstudioTokenStatus();
  await refreshOrchestratorWebKeyStatus();
  await refreshMailStatus();
  await refreshAbstractionStatus();
  await refreshRagStatus();
  await refreshHyperwebStatus();
  renderSettingsForm();
  renderReferences();
  const diagnostics = await api.settingsDiagnostics();
  if (diagnostics && diagnostics.ok) {
    state.settingsDiagnostics = diagnostics;
    renderSettingsDiagnostics();
  }
}

async function refreshProviderKeysState(options = {}) {
  if (!api || typeof api.providerKeysList !== 'function') {
    state.providerKeysState = { providers: [] };
    if (options.renderSettings !== false) renderSettingsProviderKeys();
    return { ok: false, message: 'Provider key API unavailable.', providers: [] };
  }
  const res = await api.providerKeysList();
  if (!res || !res.ok) {
    if (options.clearOnError) state.providerKeysState = { providers: [] };
    if (options.renderSettings !== false) renderSettingsProviderKeys();
    return res || { ok: false, message: 'Unable to load provider keys.', providers: [] };
  }
  state.providerKeysState = {
    providers: Array.isArray(res.providers) ? res.providers : [],
  };
  if (options.renderSettings !== false) renderSettingsProviderKeys();
  return { ok: true, providers: state.providerKeysState.providers };
}

function renderSettingsProviderKeys() {
  const holder = e('settings-provider-keys-list');
  if (!holder) return;
  const entries = Array.isArray(state.providerKeysState && state.providerKeysState.providers)
    ? state.providerKeysState.providers
    : [];
  holder.innerHTML = PROVIDERS.map((provider) => {
    const entry = entries.find((item) => String((item && item.provider) || '') === provider) || {};
    const keys = Array.isArray(entry.keys) ? entry.keys : [];
    const primaryKeyId = normalizeProviderKeyId(entry.primary_key_id);
    const hasKeys = keys.length > 0;
    const keyRows = keys.length
      ? keys.map((key) => {
        const keyId = normalizeProviderKeyId(key && key.key_id);
        const label = escapeHtml(String((key && key.label) || keyId || 'Key'));
        const isPrimary = keyId && keyId === primaryKeyId;
        const configured = !!(key && key.configured);
        return `
          <div class="settings-provider-key-row">
            <div class="settings-provider-key-meta">
              <strong>${label}</strong>
              ${isPrimary ? '<span class="settings-provider-key-badge">Primary</span>' : ''}
              ${configured ? '<span class="settings-provider-key-badge configured">Configured</span>' : '<span class="settings-provider-key-badge missing">Missing</span>'}
            </div>
            <div class="settings-provider-key-actions">
              <button data-provider-key-fetch="${escapeHtml(provider)}" data-provider-key-id="${escapeHtml(keyId)}">Fetch Models</button>
              <button data-provider-key-primary="${escapeHtml(provider)}" data-provider-key-id="${escapeHtml(keyId)}" ${isPrimary ? 'disabled' : ''}>Set Primary</button>
              <button data-provider-key-delete="${escapeHtml(provider)}" data-provider-key-id="${escapeHtml(keyId)}">Delete</button>
            </div>
          </div>
        `;
      }).join('')
      : '<div class="muted small">No keys configured.</div>';
    const statusText = escapeHtml(getProviderKeyStatusText(provider));
    const primaryLabel = hasKeys && primaryKeyId
      ? escapeHtml(getProviderKeyLabel(provider, primaryKeyId) || primaryKeyId)
      : '';
    return `
      <article class="settings-provider-key-card">
        <div class="settings-provider-key-head">
          <div>
            <strong>${escapeHtml(provider)}</strong>
            <div class="muted small">${primaryLabel ? `Primary: ${primaryLabel}` : 'Primary: none'}</div>
          </div>
          <button data-provider-key-add="${escapeHtml(provider)}">Add Key</button>
        </div>
        <div class="settings-provider-key-list">${keyRows}</div>
        <div id="settings-provider-key-status-${escapeHtml(provider)}" class="muted small">${statusText}</div>
      </article>
    `;
  }).join('');
  renderSettingsConfiguredState();

  holder.querySelectorAll('button[data-provider-key-add]').forEach((button) => {
    button.addEventListener('click', () => {
      const provider = String(button.getAttribute('data-provider-key-add') || '').trim().toLowerCase();
      if (!PROVIDERS.includes(provider)) return;
      setProviderKeyStatusText(provider, '');
      openProviderKeyModal({
        provider,
        fromSettings: true,
        setPrimary: !getProviderPrimaryKeyId(provider),
        lockProvider: true,
      });
    });
  });

  holder.querySelectorAll('button[data-provider-key-primary]').forEach((button) => {
    button.addEventListener('click', async () => {
      const provider = String(button.getAttribute('data-provider-key-primary') || '').trim().toLowerCase();
      const keyId = normalizeProviderKeyId(button.getAttribute('data-provider-key-id') || '');
      if (!PROVIDERS.includes(provider) || !keyId) return;
      const res = await api.providerKeySetPrimary({ provider, keyId });
      if (!res || !res.ok) {
        setProviderKeyStatusText(provider, (res && res.message) ? res.message : 'Unable to set primary key.');
        return;
      }
      setProviderKeyStatusText(provider, `Primary key set to ${getProviderKeyLabel(provider, keyId) || keyId}.`);
      await refreshProviderKeysState();
      if (provider === getSelectedProvider()) {
        await fetchModelsForProvider(provider, { statusId: 'provider-status', persistSelection: true });
      }
      await refreshProviderStatus({ reload: false });
    });
  });

  holder.querySelectorAll('button[data-provider-key-delete]').forEach((button) => {
    button.addEventListener('click', async () => {
      const provider = String(button.getAttribute('data-provider-key-delete') || '').trim().toLowerCase();
      const keyId = normalizeProviderKeyId(button.getAttribute('data-provider-key-id') || '');
      if (!PROVIDERS.includes(provider) || !keyId) return;
      const ok = window.confirm(`Delete key "${getProviderKeyLabel(provider, keyId) || keyId}" from ${provider}?`);
      if (!ok) return;
      const res = await api.providerKeyDelete({ provider, keyId });
      if (!res || !res.ok) {
        setProviderKeyStatusText(provider, (res && res.message) ? res.message : 'Unable to delete key.');
        return;
      }
      setProviderKeyStatusText(provider, `Deleted key ${keyId}.`);
      await refreshProviderKeysState();
      if (provider === getSelectedProvider()) {
        const selectedEntry = getProviderStateEntry(provider);
        if (selectedEntry && selectedEntry.configured) {
          await fetchModelsForProvider(provider, { statusId: 'provider-status', persistSelection: true });
        } else {
          renderModelDropdown([]);
        }
      }
      await refreshProviderStatus({ reload: false });
    });
  });

  holder.querySelectorAll('button[data-provider-key-fetch]').forEach((button) => {
    button.addEventListener('click', async () => {
      const provider = String(button.getAttribute('data-provider-key-fetch') || '').trim().toLowerCase();
      const keyId = normalizeProviderKeyId(button.getAttribute('data-provider-key-id') || '');
      if (!PROVIDERS.includes(provider) || !keyId) return;
      await fetchModelsForProvider(provider, {
        statusId: `settings-provider-key-status-${provider}`,
        keyId,
        applyToMain: false,
        persistSelection: false,
      });
      const node = e(`settings-provider-key-status-${provider}`);
      if (node) setProviderKeyStatusText(provider, node.textContent || '');
    });
  });
}

function openProviderKeyModal(options = {}) {
  const provider = String(options.provider || getSelectedProvider()).trim().toLowerCase();
  if (!PROVIDERS.includes(provider)) return;
  state.providerKeyModal = {
    provider,
    keyId: normalizeProviderKeyId(options.keyId || options.key_id || ''),
    label: String(options.label || '').trim(),
    setPrimary: options.setPrimary !== false,
    fromSettings: !!options.fromSettings,
    lockProvider: !!options.lockProvider,
  };
  const overlay = e('provider-key-overlay');
  if (!overlay) return;
  const title = e('provider-key-modal-title');
  const providerSelect = e('provider-key-modal-provider');
  const labelInput = e('provider-key-modal-label');
  const keyInput = e('provider-key-modal-api-key');
  const primaryCheckbox = e('provider-key-modal-set-primary');
  const status = e('provider-key-modal-status');
  if (title) {
    title.textContent = state.providerKeyModal.keyId ? 'Update Provider Key' : 'Add Provider Key';
  }
  if (providerSelect) {
    providerSelect.value = provider;
    providerSelect.disabled = !!state.providerKeyModal.lockProvider;
  }
  if (labelInput) {
    labelInput.value = state.providerKeyModal.label || '';
  }
  if (keyInput) {
    keyInput.value = '';
  }
  if (primaryCheckbox) {
    primaryCheckbox.checked = !!state.providerKeyModal.setPrimary;
  }
  if (status) status.textContent = '';
  overlay.classList.remove('hidden');
  void syncActiveSurface();
}

function closeProviderKeyModal(options = {}) {
  const overlay = e('provider-key-overlay');
  if (overlay) overlay.classList.add('hidden');
  const keyInput = e('provider-key-modal-api-key');
  const status = e('provider-key-modal-status');
  if (keyInput) keyInput.value = '';
  if (status) status.textContent = '';
  if (!options.skipSurfaceSync) {
    void syncActiveSurface();
  }
}

async function submitProviderKeyModal() {
  const providerSelect = e('provider-key-modal-provider');
  const labelInput = e('provider-key-modal-label');
  const keyInput = e('provider-key-modal-api-key');
  const primaryCheckbox = e('provider-key-modal-set-primary');
  const status = e('provider-key-modal-status');
  const provider = String((providerSelect && providerSelect.value) || state.providerKeyModal.provider || '').trim().toLowerCase();
  const label = String((labelInput && labelInput.value) || state.providerKeyModal.label || '').trim();
  const apiKey = String((keyInput && keyInput.value) || '').trim();
  const keyId = normalizeProviderKeyId(state.providerKeyModal.keyId || '');
  const setPrimary = !!(primaryCheckbox && primaryCheckbox.checked);
  if (!PROVIDERS.includes(provider)) {
    if (status) status.textContent = 'Unsupported provider.';
    return;
  }
  if (!apiKey) {
    if (status) status.textContent = 'API key is required.';
    return;
  }
  const currentPrimary = getProviderPrimaryKeyId(provider);
  const res = await api.providerKeyUpsert({
    provider,
    keyId,
    label,
    apiKey,
    setPrimary,
  });
  if (!res || !res.ok) {
    if (status) status.textContent = (res && res.message) ? res.message : 'Unable to save key.';
    return;
  }
  await refreshProviderKeysState();
  const selectedProvider = getSelectedProvider();
  const shouldRefreshMain = provider === selectedProvider
    && (setPrimary || (keyId && keyId === currentPrimary));
  if (shouldRefreshMain) {
    await fetchModelsForProvider(provider, { statusId: 'provider-status', persistSelection: true });
  }
  await refreshProviderStatus({ reload: false });
  setProviderKeyStatusText(provider, `Saved key ${label || keyId || ''}.`);
  closeProviderKeyModal();
}

async function refreshProviderStatus(options = {}) {
  const status = e('provider-status');
  if (!status) return;
  if (options.reload !== false) {
    await refreshProviderKeysState({ renderSettings: state.appView === 'settings' });
  }
  await refreshLmstudioTokenStatus();
  const providers = Array.isArray(state.providerKeysState && state.providerKeysState.providers)
    ? state.providerKeysState.providers
    : [];
  const configuredSet = new Set(providers.filter((item) => item && item.configured).map((item) => item.provider));
  if (state.lmstudioTokenConfigured) configuredSet.add('lmstudio');
  const configured = Array.from(configuredSet);
  const selectedProvider = getSelectedProvider();
  const selectedModel = getSelectedModel();
  status.textContent = selectedModel
    ? `${selectedProvider} · ${selectedModel}`
    : (configured.length ? `${configured.length} provider${configured.length === 1 ? '' : 's'} configured` : 'No provider keys configured.');
  refreshAgentModeAvailability();
}

function setupChatPanelResize() {
  const splitter = e('chat-panel-resizer');
  const root = e('app-root');
  if (!splitter || !root) return;
  let dragging = false;

  const onMove = (event) => {
    if (!dragging) return;
    const rect = root.getBoundingClientRect();
    if (!rect || rect.width <= 0) return;
    const width = rect.right - event.clientX;
    state.chatPanelWidth = clamp(Math.round(width), 252, 420);
    document.body.style.setProperty('--left-panel-width', `${state.chatPanelWidth}px`);
    document.body.style.setProperty('--right-panel-width', `${state.chatPanelWidth}px`);
    if (state.chatPanelCollapsed) setChatPanelCollapsed(false, { skipPersist: true });
  };

  const onUp = () => {
    if (!dragging) return;
    dragging = false;
    persistChatPanelWidthPreference(state.chatPanelWidth);
    persistChatPanelCollapsedPreference(state.chatPanelCollapsed);
    void syncActiveSurface();
    window.removeEventListener('mousemove', onMove);
    window.removeEventListener('mouseup', onUp);
  };

  splitter.addEventListener('mousedown', (event) => {
    if (window.innerWidth <= 980) return;
    event.preventDefault();
    dragging = true;
    window.addEventListener('mousemove', onMove);
    window.addEventListener('mouseup', onUp);
  });

  splitter.addEventListener('keydown', (event) => {
    if (event.key !== 'ArrowLeft' && event.key !== 'ArrowRight') return;
    event.preventDefault();
    const step = event.key === 'ArrowLeft' ? 20 : -20;
    state.chatPanelWidth = clamp(Number(state.chatPanelWidth || 252) + step, 252, 420);
    applyChatPanelState();
    persistChatPanelWidthPreference(state.chatPanelWidth);
    if (state.chatPanelCollapsed) setChatPanelCollapsed(false);
  });
}

function setupWorkspaceChromeToggles() {
  e('workspace-left-rail-toggle-btn')?.addEventListener('click', () => {
    document.body.classList.toggle('mobile-left-open');
    document.body.classList.remove('mobile-right-open');
    const button = e('workspace-left-rail-toggle-btn');
    if (button) button.setAttribute('aria-expanded', document.body.classList.contains('mobile-left-open') ? 'true' : 'false');
  });

  const toggleRight = () => {
    if (window.innerWidth <= 980) {
      document.body.classList.toggle('mobile-right-open');
      document.body.classList.remove('mobile-left-open');
      const button = e('workspace-right-rail-toggle-btn');
      if (button) button.setAttribute('aria-expanded', document.body.classList.contains('mobile-right-open') ? 'true' : 'false');
    }
  };

  e('workspace-right-rail-toggle-btn')?.addEventListener('click', toggleRight);
}

function setupTopbarMenu() {
  e('app-tools-btn')?.addEventListener('click', (event) => {
    event.preventDefault();
    event.stopPropagation();
    toggleTopbarMenu();
  });

  e('app-tools-menu')?.addEventListener('click', (event) => {
    event.stopPropagation();
  });

  document.addEventListener('click', () => {
    closeTopbarMenu();
  });
}

function setupHyperwebSplitter() {
  const splitter = e('hyperweb-splitter');
  const grid = e('hyperweb-grid');
  if (!splitter || !grid) return;
  let dragging = false;
  const onMove = (event) => {
    if (!dragging) return;
    const rect = grid.getBoundingClientRect();
    if (!rect || rect.width <= 0) return;
    const nextRatio = (event.clientX - rect.left) / rect.width;
    applyHyperwebSplitRatio(nextRatio);
  };
  const onUp = () => {
    if (!dragging) return;
    dragging = false;
    window.removeEventListener('mousemove', onMove);
    window.removeEventListener('mouseup', onUp);
  };

  splitter.addEventListener('mousedown', (event) => {
    event.preventDefault();
    dragging = true;
    window.addEventListener('mousemove', onMove);
    window.addEventListener('mouseup', onUp);
  });

  splitter.addEventListener('keydown', (event) => {
    if (event.key !== 'ArrowLeft' && event.key !== 'ArrowRight') return;
    event.preventDefault();
    const step = event.key === 'ArrowLeft' ? -0.05 : 0.05;
    applyHyperwebSplitRatio(state.hyperwebSplitRatio + step);
  });
}

function setupArtifactHorizontalSplitter() {
  const splitter = e('artifact-splitter-horizontal');
  const shell = e('artifact-viewer-shell');
  if (!splitter || !shell) return;

  let dragging = false;
  const onMove = (event) => {
    if (!dragging) return;
    if (!shell.classList.contains('artifact-mode-image-text')) return;
    const rect = shell.getBoundingClientRect();
    if (!rect || rect.height <= 0) return;
    const nextRatio = (event.clientY - rect.top) / rect.height;
    const clamped = clamp(nextRatio, ARTIFACT_MIN_SPLIT_RATIO, ARTIFACT_MAX_SPLIT_RATIO);
    setArtifactSplitRatioForReference(state.activeSrId, clamped);
    applyArtifactSplitLayout(clamped);
  };

  const onUp = () => {
    if (!dragging) return;
    dragging = false;
    window.removeEventListener('mousemove', onMove);
    window.removeEventListener('mouseup', onUp);
  };

  splitter.addEventListener('mousedown', (event) => {
    if (!shell.classList.contains('artifact-mode-image-text')) return;
    event.preventDefault();
    dragging = true;
    window.addEventListener('mousemove', onMove);
    window.addEventListener('mouseup', onUp);
  });

  splitter.addEventListener('keydown', (event) => {
    if (event.key !== 'ArrowUp' && event.key !== 'ArrowDown') return;
    event.preventDefault();
    const current = getArtifactSplitRatioForReference(state.activeSrId);
    const step = event.key === 'ArrowUp' ? -0.04 : 0.04;
    const next = clamp(current + step, ARTIFACT_MIN_SPLIT_RATIO, ARTIFACT_MAX_SPLIT_RATIO);
    setArtifactSplitRatioForReference(state.activeSrId, next);
    applyArtifactSplitLayout(next);
  });
}

async function refreshHyperwebStatus() {
  const statusNode = e('hyperweb-status');
  if (!statusNode) return;
  const res = await api.hyperwebStatus();
  if (!res || !res.ok) {
    statusNode.textContent = 'Hyperweb status unavailable.';
    state.hyperwebStatus = null;
    return;
  }
  state.hyperwebStatus = res;
  const peerCount = Number(res.peer_count || 0);
  const mode = res.connected ? 'connected' : 'disconnected';
  const topicCount = Array.isArray(res.topic_ids) ? res.topic_ids.length : 0;
  const pendingPrivate = Number(res.pending_private_entries || 0);
  statusNode.textContent = `Hyperweb ${mode} · live peers ${peerCount} · joined topics ${topicCount} · pending private ${pendingPrivate}`;
  await refreshTopbarBadges();
}

function showAboutModal(show) {
  const overlay = e('about-overlay');
  if (!overlay) return;
  overlay.classList.toggle('hidden', !show);
  syncActiveSurface();
}

function showOnboarding(show) {
  const overlay = e('onboarding-overlay');
  if (!overlay) return;
  overlay.classList.toggle('hidden', !show);
  syncActiveSurface();
}

function markOnboardingComplete() {
  state.onboardingComplete = true;
  try {
    localStorage.setItem('subgrapher_onboarding_complete', '1');
  } catch (_) {
    // noop
  }
  showOnboarding(false);
}

async function runBrowserImport(source, statusId = 'onboarding-status') {
  const res = await api.importFromBrowser(source);
  if (!res || !res.ok) {
    setStatusText(statusId, (res && res.message) ? res.message : `Import from ${source} failed.`);
    return;
  }
  const historyCount = Number((res && res.imported_breakdown && res.imported_breakdown.history_count) || 0);
  const bookmarkCount = Number((res && res.imported_breakdown && res.imported_breakdown.bookmark_count) || 0);
  if (historyCount > 0 || bookmarkCount > 0) {
    setStatusText(
      statusId,
      `Imported ${res.imported_count || 0} URL(s) from ${source} (history: ${historyCount}, bookmarks: ${bookmarkCount}). Cookies/passwords are not imported.`,
    );
  } else {
    setStatusText(statusId, (res && res.message) ? res.message : `Imported ${res.imported_count || 0} URL(s) from ${source}.`);
  }
  state.references = await api.srList();
  if (res.reference && res.reference.id) {
    setActiveReference(String(res.reference.id));
    return;
  }
  renderReferences();
  renderWorkspaceTabs();
  renderContextFiles();
  renderDiffPanel();
  await syncActiveSurface();
}

async function setupOnboardingBindings() {
  e('onboarding-apply-search-engine-btn')?.addEventListener('click', async () => {
    const engine = String((e('onboarding-search-engine-select') && e('onboarding-search-engine-select').value) || 'ddg').trim().toLowerCase();
    const res = await api.setDefaultSearchEngine(engine);
    if (!res || !res.ok) {
      setStatusText('onboarding-status', (res && res.message) ? res.message : 'Unable to set search engine.');
      return;
    }
    const topSelect = e('default-search-engine-select');
    if (topSelect) topSelect.value = engine;
    setStatusText('onboarding-status', `Default search engine set to ${engine}.`);
  });

  e('onboarding-import-chrome-btn')?.addEventListener('click', () => {
    runBrowserImport('chrome', 'onboarding-status');
  });
  e('onboarding-import-safari-btn')?.addEventListener('click', () => {
    runBrowserImport('safari', 'onboarding-status');
  });

  e('onboarding-default-browser-btn')?.addEventListener('click', async () => {
    const res = await api.requestDefaultBrowser();
    setStatusText('onboarding-status', (res && res.message) ? res.message : 'Opened default browser settings.');
  });

  const onboardingProvider = e('onboarding-provider-select');
  if (onboardingProvider) {
    onboardingProvider.addEventListener('change', async () => {
      const provider = String(onboardingProvider.value || 'openai').trim().toLowerCase();
      await fetchModelsForProvider(provider, { statusId: 'onboarding-models-status', persistSelection: false });
    });
  }

  e('onboarding-save-key-btn')?.addEventListener('click', async () => {
    const provider = String((e('onboarding-provider-select') && e('onboarding-provider-select').value) || 'openai').trim().toLowerCase();
    const keyInput = e('onboarding-api-key-input');
    const apiKey = String((keyInput && keyInput.value) || '').trim();
    if (!PROVIDERS.includes(provider)) {
      setStatusText('onboarding-models-status', 'Unsupported provider.');
      return;
    }
    if (!apiKey) {
      setStatusText('onboarding-models-status', 'API key is required.');
      return;
    }
    const saveRes = await api.providerSetKey(provider, apiKey);
    if (!saveRes || !saveRes.ok) {
      setStatusText('onboarding-models-status', (saveRes && saveRes.message) ? saveRes.message : 'Unable to save API key.');
      return;
    }
    state.selectedProvider = provider;
    const providerSelect = e('provider-select');
    if (providerSelect) providerSelect.value = provider;
    if (keyInput) keyInput.value = '';
    await persistLuminoSelection(provider, state.selectedModel || '');
    await fetchModelsForProvider(provider, { statusId: 'onboarding-models-status', persistSelection: true });
    await refreshProviderStatus();
  });

  e('onboarding-complete-btn')?.addEventListener('click', async () => {
    markOnboardingComplete();
    await refreshProviderStatus();
    await refreshHyperwebStatus();
  });
}

function bindControls() {
  bindBrowserZoomShortcuts();

  e('reference-search')?.addEventListener('input', (event) => {
    state.searchQuery = normalizeReferenceSearchQuery(event.target && event.target.value ? event.target.value : '');
    renderReferences();
  });
  e('reference-search')?.addEventListener('keydown', (event) => {
    if (event.key === 'Escape' && state.searchQuery) {
      event.preventDefault();
      const input = e('reference-search');
      if (input) input.value = '';
      state.searchQuery = '';
      renderReferences();
    }
  });
  e('reference-search-clear-btn')?.addEventListener('click', (event) => {
    event.preventDefault();
    const input = e('reference-search');
    if (input) input.value = '';
    state.searchQuery = '';
    renderReferences();
  });
  e('reference-color-filter-row')?.querySelectorAll('button[data-color-filter]').forEach((button) => {
    button.addEventListener('click', (event) => {
      event.preventDefault();
      const filter = String(button.getAttribute('data-color-filter') || '').trim().toLowerCase();
      if (filter === 'all') {
        resetReferenceColorFilter();
        renderReferences();
        return;
      }
      if (!REFERENCE_COLOR_TAGS.includes(filter)) return;
      const current = state.referenceColorFilter && state.referenceColorFilter.selected instanceof Set
        ? new Set(state.referenceColorFilter.selected)
        : new Set();
      if (current.has(filter)) {
        current.delete(filter);
      } else {
        current.add(filter);
      }
      if (current.size === 0) {
        resetReferenceColorFilter();
      } else {
        state.referenceColorFilter = { mode: 'colors', selected: current };
      }
      renderReferences();
    });
  });
  e('reference-auto-only-toggle')?.addEventListener('change', (event) => {
    state.referenceAutoOnly = !!(event.target && event.target.checked);
    renderReferences();
  });
  if (!document.__referenceColorPickerCloseBound) {
    document.addEventListener('click', (event) => {
      const target = event.target;
      if (
        target
        && typeof target.closest === 'function'
        && (target.closest('[data-action="toggle-color-picker"]') || target.closest('[data-color-picker]'))
      ) {
        return;
      }
      if (state.referenceColorPicker && state.referenceColorPicker.openSrId) {
        state.referenceColorPicker.openSrId = null;
        renderReferences();
      }
    });
    document.__referenceColorPickerCloseBound = true;
  }

  e('ref-new-empty-btn')?.addEventListener('click', () => {
    createEmptyReferenceWorkspace();
  });

  e('browser-reload-btn')?.addEventListener('click', () => {
    api.reload();
  });

  e('browser-memory-btn')?.addEventListener('click', async () => {
    if (isMemoryReplayActive()) {
      await exitMemoryReplay();
      return;
    }
    await enterMemoryReplay();
  });

  const markerModeBtn = e('browser-marker-mode-btn');
  if (markerModeBtn) {
    let markerHoldTimer = null;
    let markerLongPressTriggered = false;

    const clearMarkerHoldTimer = () => {
      if (!markerHoldTimer) return;
      clearTimeout(markerHoldTimer);
      markerHoldTimer = null;
    };

    markerModeBtn.addEventListener('pointerdown', (event) => {
      if (event && Number.isFinite(event.button) && event.button !== 0) return;
      markerLongPressTriggered = false;
      clearMarkerHoldTimer();
      markerHoldTimer = setTimeout(async () => {
        markerHoldTimer = null;
        markerLongPressTriggered = true;
        try {
          const res = await api.markerClearActive();
          state.references = (res && res.references) || state.references;
          renderArtifactHighlightLayer();
          const message = String((res && res.message) || 'Marker clear completed.').trim();
          showPassiveNotification(message || 'Marker clear completed.');
        } catch (_) {
          showPassiveNotification('Unable to clear markers.');
        }
      }, BROWSER_MARKER_HOLD_MS);
    });
    markerModeBtn.addEventListener('pointerup', clearMarkerHoldTimer);
    markerModeBtn.addEventListener('pointercancel', clearMarkerHoldTimer);
    markerModeBtn.addEventListener('pointerleave', clearMarkerHoldTimer);
    markerModeBtn.addEventListener('click', async (event) => {
      if (markerLongPressTriggered) {
        markerLongPressTriggered = false;
        event.preventDefault();
        event.stopPropagation();
        return;
      }
      await setBrowserMarkerMode(!state.markerMode);
    });
  }

  e('browser-commit-tab-btn')?.addEventListener('click', () => {
    if (blockIfMemoryReplay()) return;
    commitCurrentPageToActiveReference();
  });

  e('browser-new-root-btn')?.addEventListener('click', () => {
    if (blockIfMemoryReplay()) return;
    createRootFromCurrentPage();
  });

  e('browser-fork-child-btn')?.addEventListener('click', async () => {
    if (blockIfMemoryReplay()) return;
    if (!state.activeSrId) return;
    const parentSrId = String(state.activeSrId || '').trim();
    const currentTab = await buildCurrentBrowserTabPayload();
    const res = await api.srFork(parentSrId);
    if (res && res.ok && res.reference) {
      state.references = res.references || state.references;
      const childSrId = String((res.reference && res.reference.id) || '').trim();
      let childTabId = String((res.reference && res.reference.active_tab_id) || '').trim();
      const childRef = getReferenceById(childSrId) || res.reference;
      if (currentTab && currentTab.url && childRef && isUncommittedCurrentUrl(childRef, currentTab.url)) {
        const addRes = await api.srAddTab(childSrId, currentTab);
        if (addRes && addRes.ok) {
          state.references = addRes.references || state.references;
          childTabId = String((addRes.tab && addRes.tab.id) || '').trim()
            || String((addRes.reference && addRes.reference.active_tab_id) || '').trim()
            || childTabId;
          void maybeQueueYouTubeTranscriptIngestion(childSrId, currentTab.url, currentTab.title, { silentFailure: true });
        }
      }
      await activateReferenceSurface(childSrId, makeActiveSurface('web', childTabId ? { tabId: childTabId } : {}));
    }
  });

  e('browser-url-input')?.addEventListener('keydown', async (event) => {
    if (event.key !== 'Enter') return;
    if (blockIfMemoryReplay('Memory replay is read-only. Use checkpoint controls to navigate.')) return;
    event.preventDefault();
    const value = String(event.target && event.target.value ? event.target.value : '').trim();
    if (!value) return;
    await handleBrowserUrlInputSubmit(value);
  });

  const artifactInput = e('artifact-input');
  artifactInput?.addEventListener('input', () => {
    if (isMemoryReplayActive()) return;
    if (state.suppressArtifactInput) return;
    if (state.activeSurface.kind !== 'artifact') return;
    renderArtifactHighlightLayer();

    const status = e('artifact-status');
    if (status) status.textContent = 'Saving...';

    if (state.artifactSaveTimer) clearTimeout(state.artifactSaveTimer);
    state.artifactSaveTimer = setTimeout(async () => {
      const ref = getActiveReference();
      if (!ref) return;
      const artifactId = String(state.activeSurface.artifactId || '').trim();
      const artifact = (Array.isArray(ref.artifacts) ? ref.artifacts : []).find((item) => String((item && item.id) || '') === artifactId);
      if (!artifact) return;

      const input = e('artifact-input');
      const nextContent = String(input && input.value ? input.value : '');
      const artifactType = normalizeArtifactType(artifact.type);
      if (artifactType === 'markdown') {
        void refreshArtifactVisualState(nextContent, artifact.id);
      }
      const res = await api.srUpsertArtifact(state.activeSrId, {
        ...artifact,
        type: artifactType,
        content: nextContent,
        updated_at: Date.now(),
      });
      if (res && res.ok) {
        state.references = res.references || state.references;
        if (status) status.textContent = 'Saved';
        renderWorkspaceTabs();
        const updatedRef = getActiveReference();
        const updatedArtifact = (Array.isArray(updatedRef && updatedRef.artifacts) ? updatedRef.artifacts : [])
          .find((item) => String((item && item.id) || '') === String(artifact.id || ''));
        if (updatedArtifact) {
          if (artifactType === 'html') {
            ensureHtmlArtifactRuntime(updatedArtifact, nextContent, { focus: getArtifactViewMode(updatedArtifact.id, updatedArtifact.type) !== ARTIFACT_VIEW_MODE_CODE, focusReason: 'artifact-save' });
          } else {
            updateArtifactRuntimeControls(updatedArtifact);
          }
        }
        void noteReferenceRankingInteraction('artifact_edit', { srId: state.activeSrId });
      } else if (status) {
        status.textContent = 'Save failed';
      }
    }, 350);
  });
  artifactInput?.addEventListener('select', () => {
    renderArtifactHighlightLayer();
  });
  artifactInput?.addEventListener('focus', () => {
    renderArtifactHighlightLayer();
  });
  artifactInput?.addEventListener('blur', () => {
    renderArtifactHighlightLayer();
  });
  artifactInput?.addEventListener('mouseup', () => {
    renderArtifactHighlightLayer();
    scheduleToggleActiveArtifactMarkerSelection(0);
  });
  artifactInput?.addEventListener('keyup', (event) => {
    const key = String((event && event.key) || '').toLowerCase();
    if (key === 'shift' || key === 'control' || key === 'meta' || key === 'alt') return;
    renderArtifactHighlightLayer();
    scheduleToggleActiveArtifactMarkerSelection(0);
  });
  artifactInput?.addEventListener('scroll', () => {
    renderArtifactHighlightLayer();
  });

  e('artifact-zoom-in-btn')?.addEventListener('click', () => {
    adjustArtifactImageZoom(ARTIFACT_ZOOM_STEP);
  });

  e('artifact-zoom-out-btn')?.addEventListener('click', () => {
    adjustArtifactImageZoom(-ARTIFACT_ZOOM_STEP);
  });

  e('artifact-carousel-prev-btn')?.addEventListener('click', () => {
    cycleArtifactImage(-1);
  });

  e('artifact-carousel-next-btn')?.addEventListener('click', () => {
    cycleArtifactImage(1);
  });

  e('artifact-save-image-btn')?.addEventListener('click', async () => {
    await saveActiveArtifactImage();
  });

  e('artifact-mode-code-btn')?.addEventListener('click', () => {
    const ref = getActiveReference();
    if (!ref || state.activeSurface.kind !== 'artifact') return;
    const artifactId = String(state.activeSurface.artifactId || '').trim();
    if (!artifactId) return;
    const currentMode = getArtifactViewMode(artifactId, 'html');
    setArtifactViewMode(
      artifactId,
      currentMode === ARTIFACT_VIEW_MODE_CODE ? ARTIFACT_VIEW_MODE_PREVIEW : ARTIFACT_VIEW_MODE_CODE,
    );
    const artifact = (Array.isArray(ref.artifacts) ? ref.artifacts : []).find((item) => String((item && item.id) || '') === artifactId);
    if (artifact) {
      updateArtifactRuntimeControls(artifact);
      if (getArtifactViewMode(artifactId, artifact.type) !== ARTIFACT_VIEW_MODE_CODE) {
        focusActiveHtmlRuntime('code-toggle-preview');
      }
    }
  });

  e('artifact-run-refresh-btn')?.addEventListener('click', () => {
    refreshActiveHtmlArtifactRuntime();
  });

  e('chat-send-btn')?.addEventListener('click', () => {
    if (blockIfMemoryReplay('Memory replay is read-only. Exit memory mode to chat.')) return;
    sendChatMessage();
  });

  e('chat-input')?.addEventListener('keydown', (event) => {
    if (event.key === 'Enter') {
      if (blockIfMemoryReplay('Memory replay is read-only. Exit memory mode to chat.')) return;
      event.preventDefault();
      sendChatMessage();
    }
  });

  e('memory-prev-btn')?.addEventListener('click', async () => {
    if (!isMemoryReplayActive()) return;
    await stepMemoryReplay(-1);
  });
  e('memory-next-btn')?.addEventListener('click', async () => {
    if (!isMemoryReplayActive()) return;
    await stepMemoryReplay(1);
  });
  e('memory-play-btn')?.addEventListener('click', async () => {
    if (!isMemoryReplayActive()) return;
    if (state.memoryReplay.playing) {
      state.memoryReplay.playing = false;
      clearMemoryReplayTimer();
      renderMemoryRail();
      return;
    }
    state.memoryReplay.playing = true;
    clearMemoryReplayTimer();
    state.memoryReplay.timer = setInterval(() => {
      stepMemoryReplay(1).catch(() => {});
    }, 1800);
    renderMemoryRail();
  });
  e('memory-lane-all-btn')?.addEventListener('click', async () => {
    if (!isMemoryReplayActive()) return;
    await setMemoryReplayLane('all');
  });
  e('memory-lane-periodic-btn')?.addEventListener('click', async () => {
    if (!isMemoryReplayActive()) return;
    await setMemoryReplayLane('periodic');
  });
  e('memory-lane-semantic-btn')?.addEventListener('click', async () => {
    if (!isMemoryReplayActive()) return;
    await setMemoryReplayLane('semantic');
  });
  e('memory-exit-btn')?.addEventListener('click', async () => {
    await exitMemoryReplay();
  });
  e('memory-diff-btn')?.addEventListener('click', async () => {
    if (!isMemoryReplayActive()) return;
    const srId = String(state.activeSrId || '').trim();
    const checkpointId = String(state.memoryReplay.activeCheckpointId || '').trim();
    if (!srId || !checkpointId) return;
    const res = await api.memoryPreviewDiff(srId, checkpointId, 'current');
    if (!res || !res.ok) {
      showPassiveNotification((res && res.message) ? res.message : 'Unable to preview memory diff.');
      return;
    }
    window.alert(JSON.stringify(res.diff || {}, null, 2));
  });
  e('memory-attach-btn')?.addEventListener('click', async () => {
    if (!isMemoryReplayActive()) return;
    const srId = String(state.activeSrId || '').trim();
    const checkpointId = String(state.memoryReplay.activeCheckpointId || '').trim();
    if (!srId || !checkpointId) return;
    const res = await api.memoryAttachDiffContext(srId, checkpointId, ['summary', 'diff']);
    if (!res || !res.ok || !res.attached_context) {
      showPassiveNotification((res && res.message) ? res.message : 'Unable to attach memory context.');
      return;
    }
    const input = e('chat-input');
    if (input) {
      input.value = String((res.attached_context && res.attached_context.text) || '');
      input.focus();
    }
    showPassiveNotification('Memory diff attached to chat input.');
  });
  e('memory-commit-btn')?.addEventListener('click', async () => {
    if (!isMemoryReplayActive()) return;
    const srId = String(state.activeSrId || '').trim();
    const checkpointId = String(state.memoryReplay.activeCheckpointId || '').trim();
    if (!srId || !checkpointId) return;
    const res = await api.memoryForkFromCheckpoint(srId, checkpointId, '');
    if (!res || !res.ok || !res.reference) {
      showPassiveNotification((res && res.message) ? res.message : 'Unable to commit memory fork.');
      return;
    }
    state.references = res.references || await api.srList();
    await exitMemoryReplay();
    setActiveReference(res.reference.id);
    showPassiveNotification('Memory fork created and opened.');
  });

  e('about-open-btn')?.addEventListener('click', () => {
    closeTopbarMenu();
    showAboutModal(true);
  });
  e('about-close-btn')?.addEventListener('click', () => {
    showAboutModal(false);
  });
  document.querySelectorAll('#about-overlay a[data-external-url]').forEach((link) => {
    link.addEventListener('click', async (event) => {
      event.preventDefault();
      event.stopPropagation();
      const href = String(link.getAttribute('data-external-url') || link.getAttribute('href') || '').trim();
      if (!href || !api.openExternal) return;
      await api.openExternal(href);
    });
  });

  e('default-search-engine-select')?.addEventListener('change', async (event) => {
    const engine = String(event.target && event.target.value ? event.target.value : 'ddg').trim().toLowerCase();
    const res = await api.setDefaultSearchEngine(engine);
    if (!res || !res.ok) {
      window.alert((res && res.message) || 'Unable to set default search engine.');
      return;
    }
    const onboardingSelect = e('onboarding-search-engine-select');
    if (onboardingSelect) onboardingSelect.value = engine;
  });

  e('workspace-open-btn')?.addEventListener('click', async () => {
    await setAppView('workspace');
  });

  e('mail-open-btn')?.addEventListener('click', async () => {
    await openMailPage();
  });

  e('hyperweb-open-btn')?.addEventListener('click', async () => {
    await openHyperwebPage();
  });

  e('private-shares-open-btn')?.addEventListener('click', async () => {
    await openPrivateSharesPage();
  });

  e('settings-open-btn')?.addEventListener('click', async () => {
    await openSettingsPage();
  });

  e('history-open-btn')?.addEventListener('click', async () => {
    await openHistoryPage();
  });

  e('hyperweb-close-btn')?.addEventListener('click', async () => {
    await setAppView('workspace');
  });

  e('shares-close-btn')?.addEventListener('click', async () => {
    await setAppView('workspace');
  });

  e('settings-close-btn')?.addEventListener('click', async () => {
    await setAppView('workspace');
  });

  e('history-close-btn')?.addEventListener('click', async () => {
    await setAppView('workspace');
  });

  e('global-mail-back-btn')?.addEventListener('click', async () => {
    await setAppView('workspace');
  });

  e('history-view-preview-btn')?.addEventListener('click', () => {
    setHistoryDetailView('preview');
  });

  e('history-view-map-btn')?.addEventListener('click', () => {
    setHistoryDetailView('map');
  });

  e('shares-refresh-btn')?.addEventListener('click', async () => {
    await refreshPrivateSharesData();
  });

  e('shares-tab-incoming-btn')?.addEventListener('click', () => setSharesTab('incoming'));
  e('shares-tab-outgoing-btn')?.addEventListener('click', () => setSharesTab('outgoing'));
  e('shares-tab-rooms-btn')?.addEventListener('click', () => setSharesTab('rooms'));
  e('shares-room-open-workspace-btn')?.addEventListener('click', async (event) => {
    const button = event && event.currentTarget ? event.currentTarget : null;
    const shareId = String((button && button.dataset && button.dataset.shareId) || '').trim();
    const roomId = String((button && button.dataset && button.dataset.roomId) || '').trim();
    if (!shareId && !roomId) return;
    await openWorkspaceReferenceFromPrivateShare({
      share_id: shareId,
      room_id: roomId,
    });
  });

  e('hyperweb-tab-feed-btn')?.addEventListener('click', async () => {
    await setHyperwebSurfaceTab('feed');
  });
  e('hyperweb-tab-refs-btn')?.addEventListener('click', async () => {
    await setHyperwebSurfaceTab('refs');
  });
  e('hyperweb-tab-chat-btn')?.addEventListener('click', async () => {
    await setHyperwebSurfaceTab('chat');
  });

  e('settings-save-btn')?.addEventListener('click', async () => {
    await saveSettingsDraft();
  });

  e('settings-import-chrome-btn')?.addEventListener('click', () => {
    state.settingsSaveState = '';
    renderSettingsStatusLine();
    runBrowserImport('chrome', 'settings-status-line');
  });

  e('settings-import-safari-btn')?.addEventListener('click', () => {
    state.settingsSaveState = '';
    renderSettingsStatusLine();
    runBrowserImport('safari', 'settings-status-line');
  });

  e('settings-mail-account-save-btn')?.addEventListener('click', async (event) => {
    event.preventDefault();
    const saveBtn = e('settings-mail-account-save-btn');
    const label = String((e('settings-mail-account-label') && e('settings-mail-account-label').value) || '').trim();
    const email = String((e('settings-mail-account-email') && e('settings-mail-account-email').value) || '').trim();
    const host = String((e('settings-mail-account-host') && e('settings-mail-account-host').value) || '').trim();
    const username = String((e('settings-mail-account-username') && e('settings-mail-account-username').value) || '').trim();
    const password = String((e('settings-mail-account-password') && e('settings-mail-account-password').value) || '');
    if (!label || !email || !host || !username || !password) {
      state.settingsSaveState = 'Label, email, IMAP host, username, and password are required.';
      renderSettingsStatusLine();
      return;
    }
    const priorLabel = saveBtn ? saveBtn.textContent : '';
    if (saveBtn) {
      saveBtn.disabled = true;
      saveBtn.textContent = 'Saving...';
    }
    const payload = {
      account_type: 'manual_imap_smtp',
      provider: (e('settings-mail-account-provider') && e('settings-mail-account-provider').value) || 'generic',
      label,
      email,
      host,
      port: Number((e('settings-mail-account-port') && e('settings-mail-account-port').value) || 993),
      username,
      mailbox: (e('settings-mail-account-mailbox') && e('settings-mail-account-mailbox').value) || 'INBOX',
      password,
      smtp_host: (e('settings-mail-account-smtp-host') && e('settings-mail-account-smtp-host').value) || '',
      smtp_port: Number((e('settings-mail-account-smtp-port') && e('settings-mail-account-smtp-port').value) || 465),
      smtp_username: (e('settings-mail-account-smtp-username') && e('settings-mail-account-smtp-username').value) || '',
      smtp_password: (e('settings-mail-account-smtp-password') && e('settings-mail-account-smtp-password').value) || '',
      use_tls: !!(e('settings-mail-account-tls') && e('settings-mail-account-tls').checked),
      smtp_use_tls: !!(e('settings-mail-account-smtp-tls') && e('settings-mail-account-smtp-tls').checked),
      smtp_starttls: !!(e('settings-mail-account-smtp-starttls') && e('settings-mail-account-smtp-starttls').checked),
    };
    try {
      const res = await api.mailSaveAccount(payload);
      if (!res || !res.ok) {
        state.settingsSaveState = (res && res.message) || 'Unable to save mailbox account.';
        renderSettingsStatusLine();
        return;
      }
      resetMailAccountForm();
      state.mailAccounts = Array.isArray(res.accounts) ? res.accounts : state.mailAccounts;
      state.settingsSaveState = 'Mailbox saved.';
      renderSettingsStatusLine();
      await refreshMailAccounts();
      await refreshMailStatus();
    } catch (err) {
      state.settingsSaveState = String((err && err.message) || 'Unable to save mailbox account.');
      renderSettingsStatusLine();
    } finally {
      if (saveBtn) {
        saveBtn.disabled = false;
        saveBtn.textContent = priorLabel || 'Add Mail Account';
      }
    }
  });

  e('settings-mail-google-connect-btn')?.addEventListener('click', async () => {
    const clientId = String((e('settings-mail-google-client-id') && e('settings-mail-google-client-id').value) || '').trim();
    const clientSecret = String((e('settings-mail-google-client-secret') && e('settings-mail-google-client-secret').value) || '').trim();
    const res = await api.mailStartGoogleOAuth({ client_id: clientId, client_secret: clientSecret });
    if (!res || !res.ok) {
      state.settingsSaveState = (res && res.message) || 'Unable to connect Gmail.';
      renderSettingsStatusLine();
      return;
    }
    if (e('settings-mail-google-client-id')) e('settings-mail-google-client-id').value = '';
    if (e('settings-mail-google-client-secret')) e('settings-mail-google-client-secret').value = '';
    state.mailAccounts = Array.isArray(res.accounts) ? res.accounts : state.mailAccounts;
    state.settingsSaveState = 'Google mailbox connected.';
    renderSettingsStatusLine();
    await refreshMailAccounts();
    await refreshMailStatus();
  });

  e('settings-mail-sync-all-btn')?.addEventListener('click', async () => {
    const accounts = Array.isArray(state.mailAccounts) ? state.mailAccounts : [];
    if (!accounts.length) {
      state.settingsSaveState = 'Add a mailbox first.';
      renderSettingsStatusLine();
      return;
    }
    for (const account of accounts) {
      const res = await api.mailSyncAccount(String((account && account.id) || ''));
      if (!res || !res.ok) {
        state.settingsSaveState = (res && res.message) || `Unable to sync ${String((account && account.label) || 'mailbox')}.`;
        renderSettingsStatusLine();
        await refreshMailAccounts();
        await refreshMailStatus();
        return;
      }
      if (Array.isArray(res.accounts)) state.mailAccounts = res.accounts;
      if (Array.isArray(res.mailboxes)) state.mailboxesByAccount.set(String((account && account.id) || '').trim(), res.mailboxes);
    }
    state.settingsSaveState = 'Mailbox sync completed.';
    renderSettingsStatusLine();
    await refreshMailAccounts();
    await refreshMailStatus();
    if (state.appView === 'workspace') await renderMailPanel();
    if (state.appView === 'mail') await renderGlobalMailPage();
  });

  e('settings-mail-accounts-list')?.addEventListener('click', async (event) => {
    const syncBtn = event.target.closest('[data-mail-sync-account]');
    if (syncBtn) {
      const accountId = String(syncBtn.getAttribute('data-mail-sync-account') || '').trim();
      const res = await api.mailSyncAccount(accountId);
      state.settingsSaveState = (res && res.ok)
        ? 'Mailbox sync completed.'
        : ((res && res.message) || 'Unable to sync mailbox.');
      if (res && Array.isArray(res.accounts)) state.mailAccounts = res.accounts;
      if (res && res.ok && Array.isArray(res.mailboxes)) state.mailboxesByAccount.set(accountId, res.mailboxes);
      renderSettingsStatusLine();
      await refreshMailAccounts();
      await refreshMailStatus();
      if (state.appView === 'workspace') await renderMailPanel();
      if (state.appView === 'mail') await renderGlobalMailPage();
      return;
    }
    const deleteBtn = event.target.closest('[data-mail-delete-account]');
    if (deleteBtn) {
      const accountId = String(deleteBtn.getAttribute('data-mail-delete-account') || '').trim();
      const res = await api.mailDeleteAccount(accountId);
      if (!res || !res.ok) {
        state.settingsSaveState = (res && res.message) || 'Unable to delete mailbox.';
        renderSettingsStatusLine();
        return;
      }
      state.mailAccounts = Array.isArray(res.accounts) ? res.accounts : [];
      state.mailboxesByAccount.delete(accountId);
      resetMailAccountForm();
      state.settingsSaveState = 'Mailbox deleted.';
      renderSettingsStatusLine();
      await refreshMailStatus();
    }
  });
  e('settings-mail-accounts-list')?.addEventListener('change', async (event) => {
    const toggle = event.target.closest('[data-mail-toggle-notifications]');
    if (!toggle) return;
    const accountId = String(toggle.getAttribute('data-mail-toggle-notifications') || '').trim();
    const existing = getMailAccountById(accountId);
    if (!accountId || !existing) return;
    const res = await api.mailSaveAccount({
      id: accountId,
      notifications_enabled: !!toggle.checked,
    });
    if (!res || !res.ok) {
      state.settingsSaveState = (res && res.message) || 'Unable to update mailbox notifications.';
      renderSettingsStatusLine();
      await refreshMailAccounts();
      return;
    }
    state.mailAccounts = Array.isArray(res.accounts) ? res.accounts : state.mailAccounts;
    state.settingsSaveState = 'Mailbox notifications updated.';
    renderSettingsStatusLine();
    await refreshMailAccounts();
    if (state.appView === 'mail') await renderGlobalMailPage();
  });

  e('settings-cancel-btn')?.addEventListener('click', () => {
    state.settingsDraft = normalizeSettingsDraft(state.settingsPersisted || {});
    state.settingsValidationErrors = {};
    state.settingsDirty = false;
    state.settingsSaveState = 'Changes reverted.';
    renderSettingsForm();
  });

  e('settings-telegram-set-token-btn')?.addEventListener('click', async () => {
    const input = e('settings-telegram-token-input');
    const token = String((input && input.value) || '').trim();
    if (!token || !api.telegramSetToken) {
      state.settingsSaveState = 'Telegram token is required.';
      renderSettingsStatusLine();
      return;
    }
    const res = await api.telegramSetToken(token);
    if (!res || !res.ok) {
      state.settingsSaveState = (res && res.message) ? res.message : 'Unable to set Telegram token.';
      renderSettingsStatusLine();
      return;
    }
    if (input) input.value = '';
    state.settingsSaveState = 'Telegram token saved.';
    if (res.settings && res.settings.ok !== false) {
      state.settingsPersisted = normalizeSettingsDraft(res.settings);
      state.settingsDraft = normalizeSettingsDraft(res.settings);
      state.settingsDirty = false;
      renderSettingsForm();
    } else {
      renderSettingsStatusLine();
    }
    await refreshTelegramSettingsStatus();
  });

  e('settings-telegram-clear-token-btn')?.addEventListener('click', async () => {
    if (!api.telegramClearToken) return;
    const res = await api.telegramClearToken();
    if (!res || !res.ok) {
      state.settingsSaveState = (res && res.message) ? res.message : 'Unable to clear Telegram token.';
      renderSettingsStatusLine();
      return;
    }
    const input = e('settings-telegram-token-input');
    if (input) input.value = '';
    state.settingsSaveState = 'Telegram token cleared.';
    if (res.settings && res.settings.ok !== false) {
      state.settingsPersisted = normalizeSettingsDraft(res.settings);
      state.settingsDraft = normalizeSettingsDraft(res.settings);
      state.settingsDirty = false;
      renderSettingsForm();
    } else {
      renderSettingsStatusLine();
    }
    await refreshTelegramSettingsStatus();
  });

  e('settings-telegram-test-btn')?.addEventListener('click', async () => {
    if (!api.telegramTestMessage) return;
    const chatInput = e('settings-telegram-test-chat-id');
    const chatId = String((chatInput && chatInput.value) || '').trim();
    const res = await api.telegramTestMessage({
      chat_id: chatId,
      text: 'Subgrapher Telegram bot test ping.',
    });
    state.settingsSaveState = (res && res.ok)
      ? `Telegram test message sent${res.chat_id ? ` to ${res.chat_id}` : ''}.`
      : ((res && res.message) ? res.message : 'Unable to send Telegram test message.');
    renderSettingsStatusLine();
    await refreshTelegramSettingsStatus();
  });

  e('settings-telegram-users-list')?.addEventListener('click', async (event) => {
    const trigger = event && event.target && typeof event.target.closest === 'function'
      ? event.target.closest('button[data-telegram-revoke-chat-id]')
      : null;
    if (!trigger || !api.orchestratorUserRevoke) return;
    const chatId = String(trigger.getAttribute('data-telegram-revoke-chat-id') || '').trim();
    if (!chatId) return;
    trigger.disabled = true;
    const res = await api.orchestratorUserRevoke({ chat_id: chatId });
    state.settingsSaveState = (res && res.ok)
      ? `Revoked Telegram user ${chatId}.`
      : ((res && res.message) ? res.message : 'Unable to revoke Telegram user.');
    renderSettingsStatusLine();
    await refreshOrchestratorUsersList();
    await refreshTelegramSettingsStatus();
  });

  e('settings-lmstudio-token-set-btn')?.addEventListener('click', async () => {
    const input = e('settings-lmstudio-token-input');
    const token = String((input && input.value) || '').trim();
    if (!token || !api.lmstudioSetToken) {
      state.settingsSaveState = 'LM Studio token is required.';
      renderSettingsStatusLine();
      return;
    }
    const res = await api.lmstudioSetToken(token);
    if (!res || !res.ok) {
      state.settingsSaveState = (res && res.message) ? res.message : 'Unable to set LM Studio token.';
      renderSettingsStatusLine();
      return;
    }
    if (input) input.value = '';
    state.settingsSaveState = 'LM Studio token saved.';
    if (res.settings && res.settings.ok !== false) {
      state.settingsPersisted = normalizeSettingsDraft(res.settings);
      state.settingsDraft = normalizeSettingsDraft(res.settings);
      state.settingsDirty = false;
      renderSettingsForm();
    } else {
      renderSettingsStatusLine();
    }
    await refreshLmstudioTokenStatus();
  });

  e('settings-lmstudio-token-clear-btn')?.addEventListener('click', async () => {
    if (!api.lmstudioClearToken) return;
    const res = await api.lmstudioClearToken();
    if (!res || !res.ok) {
      state.settingsSaveState = (res && res.message) ? res.message : 'Unable to clear LM Studio token.';
      renderSettingsStatusLine();
      return;
    }
    const input = e('settings-lmstudio-token-input');
    if (input) input.value = '';
    state.settingsSaveState = 'LM Studio token cleared.';
    if (res.settings && res.settings.ok !== false) {
      state.settingsPersisted = normalizeSettingsDraft(res.settings);
      state.settingsDraft = normalizeSettingsDraft(res.settings);
      state.settingsDirty = false;
      renderSettingsForm();
    } else {
      renderSettingsStatusLine();
    }
    await refreshLmstudioTokenStatus();
  });

  e('settings-abstraction-fetch-models-btn')?.addEventListener('click', async () => {
    await refreshSettingsLmstudioModelOptions();
    renderSettingsForm();
    state.settingsSaveState = (Array.isArray(state.settingsLmstudioModels) && state.settingsLmstudioModels.length > 0)
      ? `Loaded ${state.settingsLmstudioModels.length} LM Studio model(s).`
      : 'No LM Studio models found.';
    renderSettingsStatusLine();
  });

  e('settings-image-analysis-fetch-models-btn')?.addEventListener('click', async () => {
    await refreshSettingsLmstudioModelOptions();
    renderSettingsForm();
    state.settingsSaveState = (Array.isArray(state.settingsLmstudioModels) && state.settingsLmstudioModels.length > 0)
      ? `Loaded ${state.settingsLmstudioModels.length} LM Studio model(s).`
      : 'No LM Studio models found.';
    renderSettingsStatusLine();
  });

  e('settings-rag-fetch-models-btn')?.addEventListener('click', async () => {
    await refreshSettingsLmstudioModelOptions();
    renderSettingsForm();
    state.settingsSaveState = (Array.isArray(state.settingsLmstudioModels) && state.settingsLmstudioModels.length > 0)
      ? `Loaded ${state.settingsLmstudioModels.length} LM Studio model(s).`
      : 'No LM Studio models found.';
    renderSettingsStatusLine();
  });

  e('settings-abstraction-rebuild-btn')?.addEventListener('click', async () => {
    if (!api.abstractionRebuild) return;
    state.settingsSaveState = 'Rebuilding abstraction copies...';
    renderSettingsStatusLine();
    const res = await api.abstractionRebuild({});
    state.settingsSaveState = (res && res.ok)
      ? 'Abstraction rebuild completed.'
      : ((res && res.message) ? res.message : 'Abstraction rebuild failed.');
    if (res && res.status) {
      state.settingsAbstractionStatus = res.status;
    } else {
      await refreshAbstractionStatus();
    }
    renderSettingsAbstractionStatus();
    renderSettingsStatusLine();
  });

  e('settings-rag-reindex-btn')?.addEventListener('click', async () => {
    if (!api.ragReindex) return;
    const srId = String(state.activeSrId || '').trim();
    if (!srId) {
      state.settingsSaveState = 'Select an active workspace before reindexing RAG.';
      renderSettingsStatusLine();
      return;
    }
    state.settingsSaveState = 'Reindexing local RAG...';
    renderSettingsStatusLine();
    const res = await api.ragReindex({ sr_id: srId });
    state.settingsSaveState = (res && res.ok)
      ? 'RAG reindex completed.'
      : ((res && res.result && res.result.message) || (res && res.message) || 'RAG reindex failed.');
    if (res && res.status) {
      state.settingsRagStatus = res.status;
      renderSettingsRagStatus();
    } else {
      await refreshRagStatus();
    }
    renderSettingsStatusLine();
  });

  e('settings-orchestrator-web-key-set-btn')?.addEventListener('click', async () => {
    const input = e('settings-orchestrator-web-key-input');
    const key = String((input && input.value) || '').trim();
    if (!key || !api.orchestratorWebSetKey) {
      state.settingsSaveState = 'Web provider key is required.';
      renderSettingsStatusLine();
      return;
    }
    const res = await api.orchestratorWebSetKey(key);
    if (!res || !res.ok) {
      state.settingsSaveState = (res && res.message) ? res.message : 'Unable to set web provider key.';
      renderSettingsStatusLine();
      return;
    }
    if (input) input.value = '';
    state.settingsSaveState = 'Web provider key saved.';
    if (res.settings && res.settings.ok !== false) {
      state.settingsPersisted = normalizeSettingsDraft(res.settings);
      state.settingsDraft = normalizeSettingsDraft(res.settings);
      state.settingsDirty = false;
      renderSettingsForm();
    } else {
      renderSettingsStatusLine();
    }
    await refreshOrchestratorWebKeyStatus();
  });

  e('settings-orchestrator-web-key-clear-btn')?.addEventListener('click', async () => {
    if (!api.orchestratorWebClearKey) return;
    const res = await api.orchestratorWebClearKey();
    if (!res || !res.ok) {
      state.settingsSaveState = (res && res.message) ? res.message : 'Unable to clear web provider key.';
      renderSettingsStatusLine();
      return;
    }
    const input = e('settings-orchestrator-web-key-input');
    if (input) input.value = '';
    state.settingsSaveState = 'Web provider key cleared.';
    if (res.settings && res.settings.ok !== false) {
      state.settingsPersisted = normalizeSettingsDraft(res.settings);
      state.settingsDraft = normalizeSettingsDraft(res.settings);
      state.settingsDirty = false;
      renderSettingsForm();
    } else {
      renderSettingsStatusLine();
    }
    await refreshOrchestratorWebKeyStatus();
  });

  getSettingsFormElements().forEach((node) => {
    node.addEventListener('input', () => {
      readSettingsDraftFromForm();
      state.settingsSaveState = '';
      renderSettingsStatusLine();
    });
    node.addEventListener('change', () => {
      const key = String(node.getAttribute('data-setting') || '').trim();
      readSettingsDraftFromForm();
      state.settingsSaveState = '';
      renderSettingsStatusLine();
      if (key === 'rag_embedding_source') {
        renderSettingsForm();
      }
    });
  });

  e('settings-refresh-diagnostics-btn')?.addEventListener('click', async () => {
    const diagnostics = await api.settingsDiagnostics();
    if (diagnostics && diagnostics.ok) {
      state.settingsDiagnostics = diagnostics;
      renderSettingsDiagnostics();
    }
  });

  e('settings-python-download-btn')?.addEventListener('click', async () => {
    if (!api || typeof api.openExternal !== 'function') return;
    await api.openExternal(PYTHON_WINDOWS_DOWNLOAD_URL);
  });

  e('settings-hyperweb-invite-generate-btn')?.addEventListener('click', async () => {
    if (!api || typeof api.hyperwebCreateInvite !== 'function') return;
    const res = await api.hyperwebCreateInvite();
    if (!res || !res.ok) {
      state.settingsSaveState = (res && res.message) ? res.message : 'Unable to generate invite key.';
      renderSettingsStatusLine();
      return;
    }
    const output = e('settings-hyperweb-invite-output');
    if (output) output.value = String((res && res.token) || '').trim();
    state.settingsSaveState = 'Invite key generated.';
    renderSettingsStatusLine();
  });

  e('settings-hyperweb-invite-copy-btn')?.addEventListener('click', async () => {
    const output = e('settings-hyperweb-invite-output');
    const token = String((output && output.value) || '').trim();
    if (!token) {
      state.settingsSaveState = 'Generate an invite key first.';
      renderSettingsStatusLine();
      return;
    }
    try {
      if (navigator && navigator.clipboard && navigator.clipboard.writeText) {
        await navigator.clipboard.writeText(token);
      } else {
        window.prompt('Copy invite key:', token);
      }
      state.settingsSaveState = 'Invite key copied.';
    } catch (_) {
      window.prompt('Copy invite key:', token);
      state.settingsSaveState = 'Invite key ready to copy.';
    }
    renderSettingsStatusLine();
  });

  e('settings-hyperweb-invite-accept-btn')?.addEventListener('click', async () => {
    if (!api || typeof api.hyperwebAcceptInvite !== 'function') return;
    const input = e('settings-hyperweb-invite-input');
    const token = normalizeHyperwebInviteTokenInput((input && input.value) || '');
    if (!token) {
      state.settingsSaveState = 'Paste an invite key first.';
      renderSettingsStatusLine();
      return;
    }
    const res = await api.hyperwebAcceptInvite(token);
    if (!res || !res.ok) {
      state.settingsSaveState = (res && res.message) ? res.message : 'Unable to accept invite key.';
      renderSettingsStatusLine();
      return;
    }
    if (input) input.value = '';
    state.settingsSaveState = 'Invite key accepted. Peer added to Hyperweb.';
    renderSettingsStatusLine();
    await refreshHyperwebChatData();
    await refreshHyperwebStatus();
    await refreshSettingsTrustedPeers();
    const diagnostics = await api.settingsDiagnostics();
    if (diagnostics && diagnostics.ok) {
      state.settingsDiagnostics = diagnostics;
      renderSettingsDiagnostics();
    }
  });

  e('settings-hyperweb-connect-btn')?.addEventListener('click', async () => {
    await api.hyperwebConnect();
    await refreshHyperwebStatus();
    await refreshSettingsTrustedPeers();
    const diagnostics = await api.settingsDiagnostics();
    if (diagnostics && diagnostics.ok) {
      state.settingsDiagnostics = diagnostics;
      renderSettingsDiagnostics();
    }
  });

  e('settings-hyperweb-disconnect-btn')?.addEventListener('click', async () => {
    await api.hyperwebDisconnect();
    await refreshHyperwebStatus();
    await refreshSettingsTrustedPeers();
    const diagnostics = await api.settingsDiagnostics();
    if (diagnostics && diagnostics.ok) {
      state.settingsDiagnostics = diagnostics;
      renderSettingsDiagnostics();
    }
  });

  e('settings-danger-reset-hyperweb-identity-btn')?.addEventListener('click', async () => {
    const phrase = readDangerConfirmPhrase();
    if (!phrase) {
      state.settingsSaveState = 'Type RESET in the confirmation field first.';
      renderSettingsStatusLine();
      return;
    }
    const res = await api.settingsDangerResetHyperwebIdentity({ phrase });
    if (!res || !res.ok) {
      state.settingsSaveState = (res && res.message) ? res.message : 'Unable to reset Hyperweb identity.';
      renderSettingsStatusLine();
      return;
    }
    clearDangerConfirmPhrase();
    state.settingsSaveState = 'Hyperweb identity reset.';
    renderSettingsStatusLine();
    state.hyperwebChatPeerId = '';
    state.hyperwebChatMessages = [];
    state.hyperwebChatConversations = [];
    state.hyperwebChatMembers = [];
    state.hyperwebReferenceResults = [];
    state.hyperwebFeed = [];
    const diagnostics = await api.settingsDiagnostics();
    if (diagnostics && diagnostics.ok) {
      state.settingsDiagnostics = diagnostics;
      renderSettingsDiagnostics();
    }
    await refreshHyperwebStatus();
    await refreshHyperwebChatData();
    await refreshHyperwebFeedAndReferences();
  });

  e('settings-danger-clear-social-cache-btn')?.addEventListener('click', async () => {
    const phrase = readDangerConfirmPhrase();
    if (!phrase) {
      state.settingsSaveState = 'Type RESET in the confirmation field first.';
      renderSettingsStatusLine();
      return;
    }
    const res = await api.settingsDangerClearHyperwebSocialCache({ phrase });
    if (!res || !res.ok) {
      state.settingsSaveState = (res && res.message) ? res.message : 'Unable to clear social cache.';
      renderSettingsStatusLine();
      return;
    }
    clearDangerConfirmPhrase();
    state.settingsSaveState = 'Hyperweb social cache cleared.';
    renderSettingsStatusLine();
    state.hyperwebChatPeerId = '';
    state.hyperwebChatMessages = [];
    state.hyperwebChatConversations = [];
    state.hyperwebChatMembers = [];
    state.hyperwebReferenceResults = [];
    state.hyperwebFeed = [];
    const diagnostics = await api.settingsDiagnostics();
    if (diagnostics && diagnostics.ok) {
      state.settingsDiagnostics = diagnostics;
      renderSettingsDiagnostics();
    }
    await refreshHyperwebStatus();
    await refreshHyperwebChatData();
    await refreshHyperwebFeedAndReferences();
  });

  e('settings-appdata-unlock-touchid-btn')?.addEventListener('click', async () => {
    if (!api.appDataProtectionUnlock) return;
    const res = await api.appDataProtectionUnlock({ method: 'touchid' });
    if (!res || !res.ok) {
      state.settingsSaveState = (res && res.message) ? res.message : 'Unable to unlock app data.';
      renderSettingsStatusLine();
    } else {
      state.settingsSaveState = 'App data unlocked.';
      renderSettingsStatusLine();
    }
    await refreshAppDataProtectionStatus();
  });

  e('settings-appdata-unlock-password-btn')?.addEventListener('click', async () => {
    if (!api.appDataProtectionUnlock) return;
    const input = e('settings-appdata-password');
    const password = String((input && input.value) || '');
    if (!password) {
      state.settingsSaveState = 'Enter system password.';
      renderSettingsStatusLine();
      return;
    }
    const res = await api.appDataProtectionUnlock({ method: 'password', password });
    if (!res || !res.ok) {
      state.settingsSaveState = (res && res.message) ? res.message : 'Unable to unlock app data.';
      renderSettingsStatusLine();
    } else {
      state.settingsSaveState = 'App data unlocked.';
      renderSettingsStatusLine();
      if (input) input.value = '';
    }
    await refreshAppDataProtectionStatus();
  });

  e('settings-appdata-lock-btn')?.addEventListener('click', async () => {
    if (!api.appDataProtectionLock) return;
    const res = await api.appDataProtectionLock();
    state.settingsSaveState = (res && res.ok) ? 'App data locked.' : ((res && res.message) ? res.message : 'Unable to lock app data.');
    renderSettingsStatusLine();
    await refreshAppDataProtectionStatus();
  });

  e('publish-snapshot-cancel-btn')?.addEventListener('click', () => {
    closePublishSnapshotModal();
  });
  e('publish-snapshot-confirm-btn')?.addEventListener('click', async () => {
    await publishSnapshotFromModal();
  });
  e('publish-snapshot-overlay')?.addEventListener('click', (event) => {
    if (event.target && event.target.id === 'publish-snapshot-overlay') closePublishSnapshotModal();
  });

  e('share-reference-cancel-btn')?.addEventListener('click', () => {
    closeShareReferenceModal();
  });
  e('share-reference-send-btn')?.addEventListener('click', async () => {
    await sendPrivateShareFromModal();
  });
  e('share-reference-overlay')?.addEventListener('click', (event) => {
    if (event.target && event.target.id === 'share-reference-overlay') closeShareReferenceModal();
  });
  e('share-member-search-input')?.addEventListener('input', (event) => {
    state.shareMemberSearchQuery = String(event.target && event.target.value ? event.target.value : '').trim();
    renderShareMemberList();
  });

  e('provider-key-modal-cancel-btn')?.addEventListener('click', () => {
    closeProviderKeyModal();
  });
  e('provider-key-modal-save-btn')?.addEventListener('click', async () => {
    await submitProviderKeyModal();
  });
  e('provider-key-overlay')?.addEventListener('click', (event) => {
    if (event.target && event.target.id === 'provider-key-overlay') closeProviderKeyModal();
  });
  e('provider-key-modal-provider')?.addEventListener('change', (event) => {
    const provider = String(event.target && event.target.value ? event.target.value : '').trim().toLowerCase();
    if (!PROVIDERS.includes(provider)) return;
    state.providerKeyModal.provider = provider;
  });
  e('context-preview-close-btn')?.addEventListener('click', () => {
    closeContextPreviewModal();
  });
  e('context-preview-overlay')?.addEventListener('click', (event) => {
    if (event.target && event.target.id === 'context-preview-overlay') closeContextPreviewModal();
  });
  e('context-preview-open-path-btn')?.addEventListener('click', async () => {
    const button = e('context-preview-open-path-btn');
    const targetPath = String((button && button.dataset && button.dataset.filePath) || '').trim();
    if (!targetPath || !api.openPath) return;
    const res = await api.openPath(targetPath);
    if (!res || !res.ok) {
      showPassiveNotification((res && res.message) ? res.message : 'Unable to open file path.');
      return;
    }
    showPassiveNotification('Opened in default app.', 1200);
  });

  if (!document.__contextPreviewEscBound) {
    document.addEventListener('keydown', (event) => {
      if (!event || event.defaultPrevented || event.key !== 'Escape') return;
      if (!isModalOpen('context-preview-overlay')) return;
      event.preventDefault();
      event.stopPropagation();
      closeContextPreviewModal();
    }, true);
    document.__contextPreviewEscBound = true;
  }

  e('shares-room-editor')?.addEventListener('input', (event) => {
    const room = state.privateActiveRoom;
    if (!room || !room.can_write || !api.hyperwebCollabApplyUpdate) return;
    const roomId = String(room.room_id || '').trim();
    if (!roomId) return;
    const content = String(event.target && event.target.value ? event.target.value : '');
    if (state.privateRoomEditorSaveTimer) clearTimeout(state.privateRoomEditorSaveTimer);
    state.privateRoomEditorSaveTimer = setTimeout(async () => {
      state.privateRoomEditorSaveTimer = null;
      const res = await api.hyperwebCollabApplyUpdate(roomId, { content });
      if (!res || !res.ok) {
        setStatusText('shares-status-line', (res && res.message) ? res.message : 'Unable to sync room update.');
      }
    }, 220);
  });

  e('hyperweb-reset-filter-btn')?.addEventListener('click', async () => {
    state.hyperwebFilterFingerprint = '';
    if (e('hyperweb-ref-query-input')) e('hyperweb-ref-query-input').value = '';
    await api.hyperwebResetFilter();
    renderHyperwebIdentityLine();
    if (state.hyperwebActiveTab === 'chat') {
      await scheduleHyperwebChatRefresh('full');
      return;
    }
    if (state.hyperwebActiveTab === 'refs') {
      await refreshHyperwebReferences();
      return;
    }
    await refreshHyperwebFeedAndReferences();
  });

  e('hyperweb-post-send-btn')?.addEventListener('click', async () => {
    const input = e('hyperweb-post-input');
    const body = String((input && input.value) || '').trim();
    if (!body) {
      setStatusText('hyperweb-feed-status', 'Write something to post.');
      return;
    }
    const res = await api.hyperwebPostCreate(body);
    if (!res || !res.ok) {
      setStatusText('hyperweb-feed-status', (res && res.message) ? res.message : 'Unable to create post.');
      return;
    }
    if (input) input.value = '';
    await refreshHyperwebFeedAndReferences();
  });

  e('hyperweb-post-search-btn')?.addEventListener('click', async () => {
    state.hyperwebPostSearchQuery = String((e('hyperweb-post-search-input') && e('hyperweb-post-search-input').value) || '').trim();
    await refreshHyperwebFeedAndReferences();
  });

  e('hyperweb-post-search-clear-btn')?.addEventListener('click', async () => {
    state.hyperwebPostSearchQuery = '';
    const input = e('hyperweb-post-search-input');
    if (input) input.value = '';
    await refreshHyperwebFeedAndReferences();
  });

  e('hyperweb-post-search-input')?.addEventListener('keydown', async (event) => {
    if (event.key !== 'Enter') return;
    event.preventDefault();
    state.hyperwebPostSearchQuery = String(event.target && event.target.value ? event.target.value : '').trim();
    await refreshHyperwebFeedAndReferences();
  });

  e('hyperweb-chat-mode-select')?.addEventListener('change', async (event) => {
    const mode = String(event.target && event.target.value ? event.target.value : 'dm').trim().toLowerCase();
    state.hyperwebChatMode = mode === 'room' ? 'room' : 'dm';
    await refreshHyperwebChatData();
  });
  e('hyperweb-chat-peer-select')?.addEventListener('change', async (event) => {
    state.hyperwebChatPeerId = String(event.target && event.target.value ? event.target.value : '').trim();
    renderHyperwebDmConversationList();
    await refreshHyperwebChatHistory();
  });
  e('hyperweb-chat-room-select')?.addEventListener('change', async (event) => {
    state.hyperwebChatRoomId = String(event.target && event.target.value ? event.target.value : '').trim();
    await refreshHyperwebChatHistory();
  });
  e('hyperweb-chat-room-create-btn')?.addEventListener('click', async () => {
    if (!api.hyperwebChatRoomCreate) return;
    const roomName = window.prompt('Room name:', '');
    if (roomName === null) return;
    const members = (Array.isArray(state.hyperwebChatMembers) ? state.hyperwebChatMembers : [])
      .filter((member) => !member.is_self && member.is_online)
      .map((member) => String(member.member_id || '').trim())
      .filter(Boolean);
    const res = await api.hyperwebChatRoomCreate({
      room_name: roomName,
      member_ids: members,
    });
    if (!res || !res.ok || !res.room) {
      setStatusText('hyperweb-chat-status', (res && res.message) ? res.message : 'Unable to create room.');
      return;
    }
    state.hyperwebChatMode = 'room';
    state.hyperwebChatRoomId = String(res.room.room_id || '');
    await refreshHyperwebChatData();
  });
  e('hyperweb-chat-thread-delete-btn')?.addEventListener('click', async () => {
    if (!api.hyperwebChatDeleteThread) return;
    const threadId = String(state.hyperwebChatThreadId || '').trim();
    if (!threadId) return;
    const res = await api.hyperwebChatDeleteThread(threadId);
    if (!res || !res.ok) {
      setStatusText('hyperweb-chat-status', (res && res.message) ? res.message : 'Unable to delete thread.');
      return;
    }
    state.hyperwebChatMessages = [];
    await refreshHyperwebChatData();
  });
  e('hyperweb-chat-thread-retention')?.addEventListener('change', async (event) => {
    if (!api.hyperwebChatThreadPolicySet) return;
    const threadId = String(state.hyperwebChatThreadId || '').trim();
    const retention = String(event.target && event.target.value ? event.target.value : 'off').trim();
    if (!threadId) return;
    const res = await api.hyperwebChatThreadPolicySet(threadId, retention);
    if (!res || !res.ok) {
      setStatusText('hyperweb-chat-status', (res && res.message) ? res.message : 'Unable to update thread auto-delete.');
      renderHyperwebChatThreadHeader();
      return;
    }
    await refreshHyperwebChatData();
  });
  e('hyperweb-chat-file-btn')?.addEventListener('click', () => {
    e('hyperweb-chat-file-input')?.click();
  });
  e('hyperweb-chat-file-input')?.addEventListener('change', (event) => {
    const node = event && event.target ? event.target : null;
    const file = node && node.files && node.files[0] ? node.files[0] : null;
    state.hyperwebChatPendingFile = file || null;
    if (file) {
      setStatusText('hyperweb-chat-status', `Attached: ${String(file.name || 'file')}`);
    }
  });
  e('hyperweb-chat-send-btn')?.addEventListener('click', async () => {
    if (!api.hyperwebChatSend) return;
    const input = e('hyperweb-chat-input');
    const text = String((input && input.value) || '').trim();
    const fileInput = e('hyperweb-chat-file-input');
    const pendingFile = state.hyperwebChatPendingFile;
    const payload = {
      mode: state.hyperwebChatMode,
      peer_id: state.hyperwebChatMode === 'dm' ? state.hyperwebChatPeerId : '',
      room_id: state.hyperwebChatMode === 'room' ? state.hyperwebChatRoomId : '',
    };
    if (!text && !pendingFile) {
      setStatusText('hyperweb-chat-status', 'Write a message or attach a file.');
      return;
    }
    if (state.hyperwebChatMode === 'room') {
      const room = (Array.isArray(state.hyperwebChatRooms) ? state.hyperwebChatRooms : []).find((row) => String((row && row.room_id) || '') === state.hyperwebChatRoomId);
      payload.recipient_ids = Array.isArray(room && room.members) ? room.members : [];
    }
    if (pendingFile) {
      const dataBase64 = await new Promise((resolve, reject) => {
        const reader = new FileReader();
        reader.onload = () => {
          const raw = String(reader.result || '');
          const idx = raw.indexOf(',');
          resolve(idx >= 0 ? raw.slice(idx + 1) : raw);
        };
        reader.onerror = () => reject(new Error('Unable to read file.'));
        reader.readAsDataURL(pendingFile);
      }).catch(() => '');
      if (!dataBase64) {
        setStatusText('hyperweb-chat-status', 'Unable to read attached file.');
        return;
      }
      payload.file = {
        name: String(pendingFile.name || 'file.bin'),
        mime: String(pendingFile.type || 'application/octet-stream'),
        data_base64: dataBase64,
      };
    }
    payload.text = text;
    const res = await api.hyperwebChatSend(payload);
    if (!res || !res.ok) {
      setStatusText('hyperweb-chat-status', (res && res.message) ? res.message : 'Unable to send chat message.');
      return;
    }
    if (input) input.value = '';
    state.hyperwebChatPendingFile = null;
    if (fileInput) fileInput.value = '';
    await scheduleHyperwebChatRefresh('incremental');
  });
  e('hyperweb-chat-input')?.addEventListener('keydown', async (event) => {
    if (event.key !== 'Enter' || event.shiftKey) return;
    event.preventDefault();
    const sendBtn = e('hyperweb-chat-send-btn');
    if (sendBtn) sendBtn.click();
  });

  e('hyperweb-ref-query-btn')?.addEventListener('click', async () => {
    await refreshHyperwebReferences();
  });

  e('hyperweb-ref-query-input')?.addEventListener('keydown', async (event) => {
    if (event.key !== 'Enter') return;
    event.preventDefault();
    await refreshHyperwebReferences();
  });

  e('history-search-btn')?.addEventListener('click', async () => {
    state.historySearchQuery = String((e('history-search-input') && e('history-search-input').value) || '').trim();
    await loadHistoryPageData();
  });

  e('history-search-input')?.addEventListener('keydown', async (event) => {
    if (event.key !== 'Enter') return;
    event.preventDefault();
    state.historySearchQuery = String(event.target && event.target.value ? event.target.value : '').trim();
    await loadHistoryPageData();
  });

  e('history-clear-btn')?.addEventListener('click', async () => {
    const text = window.prompt('Type DELETE to clear private history:', '');
    if (text === null) return;
    const res = await api.historyClear(String(text || ''));
    if (!res || !res.ok) {
      setHistoryStatus((res && res.message) ? res.message : 'Unable to clear history.');
      return;
    }
    state.historySelectedId = '';
    await api.historyPreviewHide();
    renderHistoryCachedPreview(null);
    updateHistoryPreviewMeta(null);
    await loadHistoryPageData();
    setHistoryStatus(`Deleted ${Number(res.cleared_count || 0)} history item(s).`);
  });

  e('provider-set-key-btn')?.addEventListener('click', async () => {
    const select = e('provider-select');
    const provider = String(select && select.value ? select.value : '').trim().toLowerCase();
    if (!PROVIDERS.includes(provider)) return;
    openProviderKeyModal({
      provider,
      fromSettings: false,
      setPrimary: true,
      lockProvider: true,
    });
  });

  e('provider-clear-key-btn')?.addEventListener('click', async () => {
    const select = e('provider-select');
    const provider = String(select && select.value ? select.value : '').trim().toLowerCase();
    if (!PROVIDERS.includes(provider)) return;

    const ok = window.confirm(`Delete the primary ${provider} API key from secure storage?`);
    if (!ok) return;

    const res = await api.providerDeleteKey(provider);
    if (!res || !res.ok) {
      window.alert((res && res.message) || 'Unable to remove key.');
      return;
    }
    await refreshProviderKeysState();
    const entry = getProviderStateEntry(provider);
    if (entry && entry.configured) {
      await fetchModelsForProvider(provider, { statusId: 'provider-status', persistSelection: true });
    } else {
      renderModelDropdown([]);
    }
    await refreshProviderStatus({ reload: false });
    refreshAgentModeAvailability();
  });

  e('provider-select')?.addEventListener('change', async (event) => {
    const provider = String(event.target && event.target.value ? event.target.value : 'openai').trim().toLowerCase();
    state.selectedProvider = provider;
    state.selectedModel = '';
    renderModelDropdown([], { forceModel: '', preserveOnEmpty: false });
    await persistLuminoSelection(provider, '');
    await fetchModelsForProvider(provider, { statusId: 'provider-status', persistSelection: true });
    await refreshProviderStatus();
    refreshAgentModeAvailability();
  });

  e('provider-model-select')?.addEventListener('change', async () => {
    state.selectedModel = getSelectedModel();
    await persistLuminoSelection(getSelectedProvider(), state.selectedModel);
    await refreshProviderStatus({ reload: false });
  });

  e('provider-refresh-models-btn')?.addEventListener('click', async () => {
    const provider = getSelectedProvider();
    await fetchModelsForProvider(provider, { statusId: 'provider-status', persistSelection: true });
    await refreshProviderStatus({ reload: false });
    refreshAgentModeAvailability();
  });

  e('program-editor-save-btn')?.addEventListener('click', async () => {
    const srId = String(state.activeSrId || '').trim();
    const input = e('program-editor-input');
    const status = e('program-editor-status');
    if (!srId || !input) {
      if (status) status.textContent = 'No active reference.';
      return;
    }
    const program = String(input.value || '');
    const res = await api.srSetProgram(srId, program);
    if (!res || !res.ok) {
      if (status) status.textContent = (res && res.message) ? res.message : 'Unable to save program.';
      return;
    }
    state.references = res.references || await api.srList();
    if (status) status.textContent = 'Program saved.';
    renderReferences();
    renderWorkspaceTabs();
  });

  e('hyperweb-connect-menu-btn')?.addEventListener('click', async () => {
    closeTopbarMenu();
    const res = await api.hyperwebConnect();
    if (!res || !res.ok) {
      window.alert((res && res.message) || 'Unable to connect Hyperweb.');
    } else if (res.message) {
      showPassiveNotification(String(res.message || 'Hyperweb connected.'));
    }
    await refreshHyperwebStatus();
  });

  e('lumino-context-add-btn')?.addEventListener('click', (event) => {
    event.preventDefault();
    e('lumino-context-file-input')?.click();
  });

  e('lumino-context-file-input')?.addEventListener('change', async (event) => {
    const file = event.target && event.target.files && event.target.files[0];
    if (!file) return;
    const absolutePath = String(file.path || file.name || '').trim();
    if (!absolutePath) {
      window.alert('Unable to resolve selected file path.');
      event.target.value = '';
      return;
    }
    await importExternalContextFile(absolutePath);
    event.target.value = '';
  });

  e('lumino-refresh-btn')?.addEventListener('click', async () => {
    await clearChatAndAutoForkCurrentReference();
  });

  const updateBounds = async () => {
    if (state.activeSurface.kind !== 'web') return;
    const bounds = await computeBrowserBounds();
    if (!bounds) return;
    await api.updateBounds(bounds);
  };
  const updateHistoryBounds = async () => {
    if (state.appView !== 'history') return;
    drawHistorySemanticMap();
  };
  window.addEventListener('resize', () => {
    if (window.innerWidth > 980) {
      document.body.classList.remove('mobile-left-open', 'mobile-right-open');
    }
    updateBounds();
    updateHistoryBounds();
    if (state.activeSurface.kind === 'artifact') {
      applyArtifactSplitLayout(getArtifactSplitRatioForReference(state.activeSrId));
    }
  });

  if ('ResizeObserver' in window) {
    const observer = new ResizeObserver(() => {
      updateBounds();
      if (state.activeSurface.kind === 'artifact') {
        applyArtifactSplitLayout(getArtifactSplitRatioForReference(state.activeSrId));
      }
    });
    const container = e('browser-view-container');
    if (container) observer.observe(container);
    const historyContainer = e('history-preview-container');
    if (historyContainer) observer.observe(historyContainer);
  }
}

async function initialize() {
  if (!api) {
    window.alert('Electron API is unavailable.');
    return;
  }

  try {
    state.onboardingComplete = localStorage.getItem('subgrapher_onboarding_complete') === '1';
  } catch (_) {
    state.onboardingComplete = false;
  }
  loadReferenceCollapsedState();
  loadArtifactViewPreferences();
  state.markerMode = loadBrowserMarkerModePreference();
  state.hyperwebSplitRatio = loadHyperwebSplitRatio();
  state.zenMode = loadZenModePreference();
  state.chatPanelWidth = loadChatPanelWidthPreference();
  state.chatPanelCollapsed = loadChatPanelCollapsedPreference();
  applyZenModeUi();
  applyChatPanelState();
  const savedUiZoom = loadUiZoomPreference();
  if (
    savedUiZoom
    && electronApi
    && typeof electronApi.setZoomFactor === 'function'
  ) {
    electronApi.setZoomFactor(savedUiZoom);
  }

  bindControls();
  setupSettingsAccordion();
  setupTopbarMenu();
  setupWorkspaceChromeToggles();
  setupChatPanelResize();
  setupHyperwebSplitter();
  setupArtifactHorizontalSplitter();
  await setHyperwebSurfaceTab(state.hyperwebActiveTab || 'feed', { skipRefresh: true });
  setSharesTab(state.sharesActiveTab || 'incoming');
  applyHyperwebSplitRatio(state.hyperwebSplitRatio, { skipPersist: true });
  setHistoryDetailView(state.historyDetailView);
  updateBrowserMarkerModeUi();
  await setBrowserMarkerMode(state.markerMode, { persist: false });
  await setupOnboardingBindings();
  setupBrowserEvents();

  const prefRes = await api.getPreferences();
  if (prefRes && prefRes.ok) {
    const engine = String(prefRes.default_search_engine || 'ddg').trim().toLowerCase();
    const topSelect = e('default-search-engine-select');
    if (topSelect) topSelect.value = engine;
    const onboardingSelect = e('onboarding-search-engine-select');
    if (onboardingSelect) onboardingSelect.value = engine;
    const savedProvider = String(prefRes.lumino_last_provider || 'openai').trim().toLowerCase();
    const savedModel = String(prefRes.lumino_last_model || '').trim();
    state.selectedProvider = PROVIDERS.includes(savedProvider) ? savedProvider : 'openai';
    state.selectedModel = savedModel;
  }
  await refreshReferenceRankingState({ ensureFresh: true });

  state.references = await api.srList();
  if (!Array.isArray(state.references) || state.references.length === 0) {
    const homeUrl = getDefaultSearchHomeUrl();
    const homeTitle = getDefaultSearchHomeTitle();
    const rootRes = await api.srCreateRoot({
      title: 'Subgrapher Root',
      current_tab: { url: homeUrl, title: homeTitle },
    });
    state.references = rootRes && rootRes.references ? rootRes.references : [];
  }

  if (state.references.length > 0) {
    state.activeSrId = String((state.references[0] && state.references[0].id) || '').trim();
  }

  renderReferences();
  renderWorkspaceTabs();
  renderContextFiles();
  renderDiffPanel();

  const providerSelect = e('provider-select');
  if (providerSelect) {
    providerSelect.value = state.selectedProvider;
  }

  await refreshProviderStatus();
  await fetchModelsForProvider(getSelectedProvider(), {
    statusId: 'provider-status',
    forceModel: state.selectedModel,
    persistSelection: true,
  });
  state.selectedModel = getSelectedModel();
  refreshAgentModeAvailability();
  await refreshHyperwebStatus();
  await refreshTopbarBadges();
  await loadChatThread();
  await loadProgramEditorForActiveReference();
  await syncActiveSurface();
  await refreshUncommittedActionCue();
  setChatBusy(false);

  showOnboarding(!state.onboardingComplete);
}

initialize();
