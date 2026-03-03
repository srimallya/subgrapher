/* global window */

const electronApi = window.electronAPI || null;
const api = electronApi && electronApi.browser;

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
  },
  suppressArtifactInput: false,
  artifactMarkerSelectionTimer: null,
  artifactMarkerSelectionSignature: '',
  artifactMarkerSelectionAt: 0,
  diffQueueByRef: new Map(),
  selectedProvider: 'openai',
  selectedModel: '',
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
  referenceInlineRename: { srId: null, draft: '' },
  referenceSearchMatchedIds: new Set(),
  referenceColorFilter: { mode: 'all', selected: new Set() },
  referenceAutoOnly: false,
  referenceColorPicker: { openSrId: null },
  activeArtifactByRef: new Map(),
  activeFilesByRef: new Map(),
  activeSkillsByRef: new Map(),
  workspaceBrowserTabMap: new Map(),
  trustCommonsStatus: null,
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
  settingsDirty: false,
  settingsValidationErrors: {},
  settingsDiagnostics: null,
  settingsSaveState: '',
  telegramRuntimeStatus: null,
  orchestratorUsers: [],
  orchestratorUsersLoading: false,
  lmstudioTokenConfigured: null,
  orchestratorWebKeyConfigured: null,
  settingsLmstudioModels: [],
  settingsAbstractionStatus: null,
  historyEntries: [],
  historySearchQuery: '',
  historySelectedId: '',
  historyMapPoints: [],
  historyMapBounds: null,
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
let passiveNoticeTimer = null;

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
    ...(patch && typeof patch === 'object' ? patch : {}),
  };
}

function getReferenceById(srId) {
  const id = String(srId || '').trim();
  return state.references.find((ref) => String((ref && ref.id) || '') === id) || null;
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
    return;
  }
  if (kind === 'files' && surface.filesTabId) {
    state.activeFilesByRef.set(refId, String(surface.filesTabId));
    state.activeArtifactByRef.delete(refId);
    state.activeSkillsByRef.delete(refId);
    return;
  }
  if (kind === 'skills' && surface.skillsTabId) {
    state.activeSkillsByRef.set(refId, String(surface.skillsTabId));
    state.activeArtifactByRef.delete(refId);
    state.activeFilesByRef.delete(refId);
    return;
  }
  state.activeArtifactByRef.delete(refId);
  state.activeFilesByRef.delete(refId);
  state.activeSkillsByRef.delete(refId);
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
  if (state.memoryReplay.index < 0 || state.memoryReplay.index >= filtered.length) {
    state.memoryReplay.index = 0;
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
    playBtn.disabled = !active || filtered.length <= 1;
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
  state.memoryReplay.index = 0;
  state.memoryReplay.activeCheckpointId = '';
  state.memoryReplay.virtualReference = null;
  await refreshMemoryReplayList();
  renderMemoryRail();
  setChatBusy(false);
  if (state.memoryReplay.filtered.length > 0) {
    await loadMemoryCheckpointByIndex(0);
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
  const previous = options.forceModel ? '' : getSelectedModel();
  const chosen = String(options.forceModel || previous || list[0] || '').trim();

  if (list.length === 0) {
    const fallbackModel = String(options.forceModel || previous || state.selectedModel || '').trim();
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
  const previousModel = String(state.selectedModel || '').trim();
  const keyLabel = targetKeyId ? getProviderKeyLabel(targetProvider, targetKeyId) : '';
  setStatusText(statusId, `Fetching models from ${targetProvider}${keyLabel ? ` (${keyLabel})` : ''}...`);
  const res = await api.providerListModels(targetProvider, targetKeyId);
  if (!res || !res.ok) {
    setStatusText(statusId, (res && res.message) ? res.message : `Unable to fetch models for ${targetProvider}.`);
    return null;
  }
  const models = Array.isArray(res.models) ? res.models : [];
  const applyToMain = options.applyToMain !== false;
  if (applyToMain) {
    renderModelDropdown(models, { forceModel: options.forceModel || '' });
  }
  if (applyToMain && options.persistSelection !== false) {
    await persistLuminoSelection(targetProvider, getSelectedModel() || previousModel);
  }
  const suffix = res.fallback ? ' (fallback list)' : '';
  const keySuffix = keyLabel ? ` via ${keyLabel}` : '';
  setStatusText(statusId, `Loaded ${models.length} model(s) for ${targetProvider}${keySuffix}${suffix}.`);
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
  if (!panel) return;
  panel.classList.toggle('browser-has-uncommitted', !!active);
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
  meta.textContent = `${tabCount} tab(s) · ${artifactCount} artifact(s) · ${contextCount} context file(s)`;
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

  const sortByPinnedThenUpdated = (a, b) => {
    const aPinned = !!(a && a.pinned_root);
    const bPinned = !!(b && b.pinned_root);
    if (aPinned !== bPinned) return bPinned ? 1 : -1;
    // Within the same zone, the active reference always sorts first
    const aActive = String(a && a.id) === String(state.activeSrId);
    const bActive = String(b && b.id) === String(state.activeSrId);
    if (aActive !== bActive) return bActive ? 1 : -1;
    return Number((b && b.updated_at) || 0) - Number((a && a.updated_at) || 0);
  };

  Object.keys(childrenMap).forEach((parentId) => {
    childrenMap[parentId].sort(sortByPinnedThenUpdated);
  });

  const roots = refs
    .filter((ref) => {
      const parentId = String((ref && ref.parent_id) || '').trim();
      return !parentId || !idMap[parentId];
    })
    .sort(sortByPinnedThenUpdated);

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
    const subtitle = `${ref.relation_type || 'root'} · ${tabCount} tab(s)`;
    const isActive = srId === String(state.activeSrId || '');
    // A reference is a root node if it has no parent OR its parent is not in the visible map
    // (handles orphaned roots whose parent was deleted)
    const refParentId = String((ref && ref.parent_id) || '').trim();
    const isRoot = !refParentId || !idMap[refParentId];
    const isPinnable = isRoot && !isCandidate;
    const isSearchHit = query && matchedIds.has(srId);
    const isInlineRename = state.referenceInlineRename.srId === srId;
    const isAudible = !!state.audibleByRef.get(srId);
    const colorTag = sanitizeReferenceColorTag(ref && ref.color_tag);
    const isColorPickerOpen = String((state.referenceColorPicker && state.referenceColorPicker.openSrId) || '') === srId;
    const agentMeta = (ref && ref.agent_meta && typeof ref.agent_meta === 'object') ? ref.agent_meta : {};
    const isAuto = String(agentMeta.created_by || '').trim().toLowerCase() === 'lumino_b';
    const autoStatusRaw = String(agentMeta.status || '').trim().toLowerCase();
    const autoStatus = ['pending', 'active', 'failed'].includes(autoStatusRaw) ? autoStatusRaw : 'active';
    const titleMarkup = isInlineRename
      ? `<input type="text" class="reference-inline-rename-input" data-action="rename-input" data-sr-id="${escapeHtml(srId)}" maxlength="120" value="${escapeHtml(state.referenceInlineRename.draft || ref.title || 'Untitled')}" />`
      : `
        <div class="reference-title">
          <button type="button" class="reference-color-dot" data-action="toggle-color-picker" data-sr-id="${escapeHtml(srId)}" data-color-tag="${escapeHtml(colorTag)}" title="Set reference color"></button>
          <span class="reference-title-text">${escapeHtml(ref.title || 'Untitled')}</span>
          ${isAuto ? `<span class="reference-auto-badge ${escapeHtml(autoStatus)}">AUTO</span>` : ''}
          ${isAudible ? '<span class="reference-audible-badge">AUDIO</span>' : ''}
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
          <div class="reference-item ${isActive ? 'active' : ''} ${isSearchHit ? 'search-hit' : ''} ${colorTag ? `color-${escapeHtml(colorTag)}` : ''}" data-ref-id="${escapeHtml(srId)}">
            ${titleMarkup}
            <div class="reference-sub">${escapeHtml(subtitle)}</div>
            <div class="reference-actions">
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
            </div>
            ${isCandidate ? '' : '<div class="reference-privacy-badge">Keep Private</div>'}
          </div>
        </div>
        ${hasChildren ? `<div class="reference-children ${collapsed ? 'collapsed' : ''}" data-parent-id="${escapeHtml(srId)}">${renderedChildren.join('')}</div>` : ''}
      </div>
    `;
  };

  const treeMarkup = roots.map((root) => renderNode(root, 0)).filter(Boolean).join('');
  list.innerHTML = treeMarkup || '<div class="references-empty">No references match this query.</div>';

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

  list.querySelectorAll('.reference-item').forEach((node) => {
    node.addEventListener('click', async (event) => {
      if (event.target && event.target.closest('button')) return;
      if (event.target && event.target.closest('input[data-action="rename-input"]')) return;
      const srId = String(node.getAttribute('data-ref-id') || '').trim();
      if (!srId) return;
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
    setStatusText('shares-status-line', 'Select at least one TTC member.');
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
  const dmSuffix = attemptedDm > 0 ? ` TTC DM delivered ${deliveredDm}/${attemptedDm}.` : '';
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
  const artifacts = Array.isArray(ref.artifacts) ? ref.artifacts : [];

  const activeWeb = getActiveWebTab(ref);
  const activeWebId = activeWeb ? String(activeWeb.id) : '';

  holder.innerHTML = `
    <button id="workspace-add-tab-btn" class="workspace-add-btn" type="button" title="Add tab">+</button>
    <div id="workspace-add-menu" class="workspace-add-menu hidden">
      <button type="button" data-workspace-add-action="web">+ web</button>
      <button type="button" data-workspace-add-action="md">+ md</button>
      <button type="button" data-workspace-add-action="folder">+ folder</button>
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
      </div>
    `).join('')}
    ${skillsTabs.map((tab) => `
      <div class="workspace-tab ${(state.activeSurface.kind === 'skills' && String(state.activeSurface.skillsTabId || '') === String(tab.id || '')) ? 'active' : ''}" data-kind="skills" data-tab-id="${escapeHtml(tab.id)}">
        <span class="workspace-tab-label-wrap"><span class="workspace-tab-label" title="${escapeHtml(tab.title || 'Skills')}">${escapeHtml(tab.title || 'Skills')}</span></span>
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
    const positionAddMenu = () => {
      const rect = addBtn.getBoundingClientRect();
      const menuHeight = Math.max(120, Number(addMenu.offsetHeight || 0));
      const menuWidth = Math.max(130, Number(addMenu.offsetWidth || 0));
      const top = Math.max(8, Math.round(rect.top - menuHeight - 6));
      const maxLeft = Math.max(8, Math.round(window.innerWidth - menuWidth - 8));
      const left = Math.max(8, Math.min(Math.round(rect.left), maxLeft));
      addMenu.style.left = `${left}px`;
      addMenu.style.top = `${top}px`;
    };
    const hideAddMenu = () => {
      addMenu.classList.add('hidden');
      addMenu.style.visibility = '';
      addMenu.style.pointerEvents = '';
    };
    addBtn.addEventListener('click', (event) => {
      if (replayMode) {
        event.preventDefault();
        showPassiveNotification('Memory replay is read-only.');
        return;
      }
      event.preventDefault();
      event.stopPropagation();
      const willOpen = addMenu.classList.contains('hidden');
      if (!willOpen) {
        hideAddMenu();
        return;
      }
      addMenu.classList.remove('hidden');
      addMenu.style.visibility = 'hidden';
      addMenu.style.pointerEvents = 'none';
      positionAddMenu();
      requestAnimationFrame(() => {
        positionAddMenu();
        addMenu.style.visibility = '';
        addMenu.style.pointerEvents = '';
      });
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
        hideAddMenu();
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
      }
    });
  });

  holder.querySelectorAll('.workspace-tab[data-kind="files"]').forEach((node) => {
    node.addEventListener('click', async () => {
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

  holder.querySelectorAll('.workspace-tab[data-kind="skills"]').forEach((node) => {
    node.addEventListener('click', async () => {
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
    document.addEventListener('click', (event) => {
      const menu = e('workspace-add-menu');
      const btn = e('workspace-add-tab-btn');
      const target = event.target;
      if (!menu) return;
      if (menu.contains(target)) return;
      if (btn && btn.contains(target)) return;
      menu.classList.add('hidden');
    });
    document.__workspaceAddMenuCloseBound = true;
  }
  if (!document.__workspaceAddMenuViewportBound) {
    const reposition = () => {
      const menu = e('workspace-add-menu');
      const btn = e('workspace-add-tab-btn');
      if (!menu || !btn || menu.classList.contains('hidden')) return;
      const rect = btn.getBoundingClientRect();
      const menuHeight = Math.max(120, Number(menu.offsetHeight || 0));
      const menuWidth = Math.max(130, Number(menu.offsetWidth || 0));
      const top = Math.max(8, Math.round(rect.top - menuHeight - 6));
      const maxLeft = Math.max(8, Math.round(window.innerWidth - menuWidth - 8));
      const left = Math.max(8, Math.min(Math.round(rect.left), maxLeft));
      menu.style.left = `${left}px`;
      menu.style.top = `${top}px`;
    };
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
}

function isModalOpen(id) {
  const overlay = e(id);
  if (!overlay) return false;
  return !overlay.classList.contains('hidden');
}

function hasBlockingOverlay() {
  return (
    isModalOpen('onboarding-overlay')
    || isModalOpen('about-overlay')
    || isModalOpen('publish-snapshot-overlay')
    || isModalOpen('share-reference-overlay')
    || isModalOpen('provider-key-overlay')
  );
}

function isEditableKeyboardTarget(target) {
  if (!target || typeof target !== 'object') return false;
  if (target.isContentEditable) return true;
  const tagName = String(target.tagName || '').toLowerCase();
  return tagName === 'input' || tagName === 'textarea' || tagName === 'select';
}

function parseShortcutCommandFromKeyboardEvent(event) {
  if (!event) return '';
  const key = String(event.key || '');
  const code = String(event.code || '');
  const lowerKey = key.toLowerCase();

  const hasCtrlOrMeta = !!(event.ctrlKey || event.metaKey);
  if (hasCtrlOrMeta && !event.altKey) {
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
      && state.zenMode
      && !event.ctrlKey
      && !event.metaKey
      && !event.altKey
      && !event.shiftKey
      && !hasBlockingOverlay()
      && !isEditableKeyboardTarget(event.target)
    ) {
      event.preventDefault();
      event.stopPropagation();
      await setZenMode(false);
      showPassiveNotification('Zen mode off.', 1000);
      return;
    }
    if (hasBlockingOverlay()) return;
    if (isEditableKeyboardTarget(event.target)) return;
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
  return raw === ARTIFACT_VIEW_MODE_PREVIEW ? ARTIFACT_VIEW_MODE_PREVIEW : ARTIFACT_VIEW_MODE_CODE;
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
  host.innerHTML = `<div class="artifact-html-placeholder">${escapeHtml(String(message || '').trim() || 'Press Start to run HTML artifact.')}</div>`;
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
      artifactId: '', running: false, stale: false, objectUrl: '', iframeEl: null,
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
      artifactId: '', running: false, stale: false, objectUrl: '', iframeEl: null,
    };
  const runtimeForArtifact = isHtml && runtime.running && String(runtime.artifactId || '') === artifactId;

  const chip = e('artifact-type-chip');
  const codeBtn = e('artifact-mode-code-btn');
  const previewBtn = e('artifact-mode-preview-btn');
  const startBtn = e('artifact-run-start-btn');
  const stopBtn = e('artifact-run-stop-btn');
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
  if (previewBtn) {
    previewBtn.classList.toggle('hidden', !isHtml);
    previewBtn.classList.toggle('active', isHtml && mode === ARTIFACT_VIEW_MODE_PREVIEW);
    previewBtn.disabled = !isHtml;
  }
  if (startBtn) {
    startBtn.classList.toggle('hidden', !isHtml);
    startBtn.disabled = !isHtml || (runtimeForArtifact && !runtime.stale);
  }
  if (stopBtn) {
    stopBtn.classList.toggle('hidden', !isHtml);
    stopBtn.disabled = !isHtml || !runtimeForArtifact;
  }
  if (runtimeStatus) {
    if (!isHtml) {
      runtimeStatus.textContent = 'Markdown artifact';
    } else if (!runtimeForArtifact) {
      runtimeStatus.textContent = 'HTML runtime stopped';
    } else if (runtime.stale) {
      runtimeStatus.textContent = 'HTML runtime running (stale, restart required; click preview if keys do not respond)';
    } else {
      runtimeStatus.textContent = 'HTML runtime running (click preview if keys do not respond)';
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
  };
  focusActiveHtmlRuntime('runtime-start');
  return true;
}

function startActiveHtmlArtifactRuntime() {
  const ref = getActiveReference();
  if (!ref || state.activeSurface.kind !== 'artifact') return;
  const artifactId = String(state.activeSurface.artifactId || '').trim();
  const artifact = (Array.isArray(ref.artifacts) ? ref.artifacts : []).find((item) => String((item && item.id) || '') === artifactId);
  if (!artifact || normalizeArtifactType(artifact.type) !== 'html') return;
  const input = e('artifact-input');
  const source = String((input && typeof input.value === 'string') ? input.value : (artifact.content || ''));
  if (!startHtmlArtifactRuntime(artifact, source)) {
    showPassiveNotification('Unable to start HTML runtime.');
    return;
  }
  updateArtifactRuntimeControls(artifact);
  focusActiveHtmlRuntime('runtime-start-button');
}

function stopActiveHtmlArtifactRuntime() {
  const ref = getActiveReference();
  const artifactId = String((state.activeSurface && state.activeSurface.artifactId) || '').trim();
  stopHtmlArtifactRuntime({ preserveArtifactId: true });
  if (!ref || !artifactId) return;
  const artifact = (Array.isArray(ref.artifacts) ? ref.artifacts : []).find((item) => String((item && item.id) || '') === artifactId);
  if (artifact) {
    if (getArtifactViewMode(artifactId, artifact.type) === ARTIFACT_VIEW_MODE_PREVIEW) {
      renderHtmlRuntimePlaceholder('Press Start to run HTML artifact.');
    }
    updateArtifactRuntimeControls(artifact);
  }
}

async function showWebSurface(tab) {
  if (!tab) return;
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
    const runningForSameArtifact = !!(
      state.htmlArtifactRuntime
      && state.htmlArtifactRuntime.running
      && String(state.htmlArtifactRuntime.artifactId || '') === String(artifact.id || '')
    );
    if (!runningForSameArtifact) {
      stopHtmlArtifactRuntime({ preserveArtifactId: true });
      if (getArtifactViewMode(artifact.id, artifactType) === ARTIFACT_VIEW_MODE_PREVIEW) {
        renderHtmlRuntimePlaceholder('Press Start to run HTML artifact.');
      }
    }
  }
  updateArtifactRuntimeControls(artifact);

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

  const mounts = Array.isArray(ref.folder_mounts) ? ref.folder_mounts : [];
  const files = Array.isArray(ref.context_files) ? ref.context_files : [];
  status.textContent = `${mounts.length} mount(s) · ${files.length} file(s)`;

  const mountMarkup = mounts.length
    ? mounts.map((mount) => {
      const pathText = escapeHtml(String((mount && mount.absolute_path) || ''));
      const count = Number((mount && mount.file_count) || 0);
      const skipped = Number((mount && mount.skipped_count) || 0);
      return `
        <div class="files-item" data-mount-id="${escapeHtml(String((mount && mount.id) || ''))}">
          <div class="files-item-title">${pathText || '(missing path)'}</div>
          <div class="files-item-sub">${count} indexed · ${skipped} skipped</div>
          <div class="files-item-actions">
            <button data-files-reindex="${escapeHtml(String((mount && mount.id) || ''))}" ${replayMode ? 'disabled' : ''}>Reindex</button>
            <button data-files-unmount="${escapeHtml(String((mount && mount.id) || ''))}" ${replayMode ? 'disabled' : ''}>Unmount</button>
          </div>
        </div>
      `;
    }).join('')
    : '<div class="muted small">No folder mounts yet. Use workspace + and choose + folder.</div>';

  const contextMarkup = files.length
    ? files.slice(0, 240).map((file) => {
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
    : '<div class="muted small">No indexed context files.</div>';

  body.innerHTML = `
    <div class="files-block">
      <h4>Mounted Folders</h4>
      ${mountMarkup}
    </div>
    <div class="files-block">
      <h4>Indexed Context Files</h4>
      ${contextMarkup}
    </div>
  `;

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
      renderReferences();
      renderWorkspaceTabs();
      renderContextFiles();
      renderFilesPanel();
      renderDiffPanel();
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
      if (!fileId || !state.activeSrId) return;
      const preview = await api.srGetContextFilePreview(state.activeSrId, fileId);
      if (!preview || !preview.ok) {
        window.alert((preview && preview.message) || 'Unable to preview file.');
        return;
      }
      window.alert(preview.preview || '(empty file)');
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
      renderReferences();
      renderWorkspaceTabs();
      renderContextFiles();
      renderFilesPanel();
      renderDiffPanel();
      await syncActiveSurface();
    });
  });
}

async function showFilesSurface(tabId) {
  const ref = getActiveReference();
  if (!ref) return;
  stopHtmlArtifactRuntime();
  const tab = (Array.isArray(ref.tabs) ? ref.tabs : []).find((item) => String((item && item.id) || '') === String(tabId || ''));
  if (!tab || String((tab && tab.tab_kind) || '').trim().toLowerCase() !== 'files') return;

  await api.hide();
  e('browser-placeholder')?.classList.add('hidden');
  e('artifact-editor')?.classList.add('hidden');
  e('skills-panel')?.classList.add('hidden');
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
  stopHtmlArtifactRuntime();
  const tab = (Array.isArray(ref.tabs) ? ref.tabs : []).find((item) => String((item && item.id) || '') === String(tabId || ''));
  if (!tab || String((tab && tab.tab_kind) || '').trim().toLowerCase() !== 'skills') return;

  await api.hide();
  e('browser-placeholder')?.classList.add('hidden');
  e('artifact-editor')?.classList.add('hidden');
  e('files-panel')?.classList.add('hidden');
  const panel = e('skills-panel');
  if (panel) panel.classList.remove('hidden');
  await renderSkillsPanel();
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
    stopHtmlArtifactRuntime();
    await api.hide();
    return;
  }
  if (hasBlockingOverlay()) {
    stopHtmlArtifactRuntime();
    await api.hide();
    return;
  }

  const ref = getActiveReference();
  if (!ref) {
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
  if (state.activeSurface.kind === 'web') {
    if (rememberedArtifactId) {
      state.activeSurface = makeActiveSurface('artifact', { artifactId: rememberedArtifactId });
    } else if (rememberedFilesTabId) {
      state.activeSurface = makeActiveSurface('files', { filesTabId: rememberedFilesTabId });
    } else if (rememberedSkillsTabId) {
      state.activeSurface = makeActiveSurface('skills', { skillsTabId: rememberedSkillsTabId });
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

  await api.hide();
  hideNonWebSurfaces();
  e('browser-placeholder')?.classList.remove('hidden');
  await syncUrlBarForActiveSurface();
}

function renderContextFiles() {
  const holder = e('context-files');
  const ref = getActiveReference();
  if (!holder) return;
  if (!ref) {
    holder.innerHTML = '';
    return;
  }

  const files = Array.isArray(ref.context_files) ? ref.context_files : [];
  if (!files.length) {
    holder.innerHTML = '<div class="muted">No mounted context files.</div>';
    return;
  }

  holder.innerHTML = files.slice(-20).map((file) => `
    <div class="context-file-item">
      <strong>${escapeHtml(file.original_name || file.relative_path || 'file')}</strong>
      <div class="muted small">${escapeHtml(file.summary || '')}</div>
      <button data-preview-context="${escapeHtml(file.id)}">Preview</button>
    </div>
  `).join('');

  holder.querySelectorAll('button[data-preview-context]').forEach((button) => {
    button.addEventListener('click', async () => {
      const fileId = String(button.getAttribute('data-preview-context') || '').trim();
      if (!fileId) return;
      const preview = await api.srGetContextFilePreview(state.activeSrId, fileId);
      if (!preview || !preview.ok) {
        window.alert((preview && preview.message) || 'Unable to preview file.');
        return;
      }
      window.alert(preview.preview || '(empty file)');
    });
  });

  if (state.activeSurface.kind === 'files') {
    renderFilesPanel();
  }
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
  if (!state.activeSrId) {
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
  if (typeof api.onAudible === 'function') {
    api.onAudible((payload) => {
      const audible = !!(payload && payload.audible);
      const srId = String(state.activeSrId || '').trim();
      state.audibleByRef.clear();
      if (srId && audible) state.audibleByRef.set(srId, true);
      renderReferences();
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
  renderReferences();
  renderWorkspaceTabs();
  renderContextFiles();
  renderFilesPanel();
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
  renderReferences();
  renderWorkspaceTabs();
  renderContextFiles();
  renderFilesPanel();
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
  e('hyperweb-open-btn')?.classList.toggle('active', active === 'hyperweb');
  e('private-shares-open-btn')?.classList.toggle('active', active === 'private-shares');
  e('settings-open-btn')?.classList.toggle('active', active === 'settings');
  e('history-open-btn')?.classList.toggle('active', active === 'history');
}

async function setAppView(viewName) {
  const target = String(viewName || 'workspace').trim().toLowerCase();
  state.appView = (target === 'hyperweb' || target === 'private-shares' || target === 'settings' || target === 'history')
    ? target
    : 'workspace';
  const root = e('app-root');
  const hyperweb = e('hyperweb-page');
  const privateShares = e('private-shares-page');
  const settings = e('settings-page');
  const history = e('history-page');
  if (root) root.classList.toggle('hidden', state.appView !== 'workspace');
  if (hyperweb) hyperweb.classList.toggle('hidden', state.appView !== 'hyperweb');
  if (privateShares) privateShares.classList.toggle('hidden', state.appView !== 'private-shares');
  if (settings) settings.classList.toggle('hidden', state.appView !== 'settings');
  if (history) history.classList.toggle('hidden', state.appView !== 'history');
  updateTopbarViewButtons();
  if (state.appView === 'hyperweb' || state.appView === 'private-shares' || state.appView === 'settings') {
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
  const next = String(tab || 'feed').trim().toLowerCase() === 'refs' ? 'refs' : 'feed';
  state.hyperwebActiveTab = next;
  const grid = e('hyperweb-grid');
  if (grid) {
    grid.classList.toggle('view-feed', next === 'feed');
    grid.classList.toggle('view-refs', next === 'refs');
    grid.style.gridTemplateColumns = '';
  }
  e('hyperweb-tab-feed-btn')?.classList.toggle('active', next === 'feed');
  e('hyperweb-tab-refs-btn')?.classList.toggle('active', next === 'refs');
  if (options.skipRefresh) return;
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

function renderShareMemberList() {
  const holder = e('share-member-list');
  if (!holder) return;
  const query = String(state.shareMemberSearchQuery || '').trim().toLowerCase();
  const members = (Array.isArray(state.shareMemberDirectory) ? state.shareMemberDirectory : [])
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
    const desc = String((member && member.is_self) ? 'You' : (member && member.member_id) || '');
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
  if (res.unauthenticated) {
    setStatusText('shares-status-line', (res && res.message) || 'TTC auth required to send a private share.');
  }
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
  holder.querySelectorAll('button[data-share-accept]').forEach((button) => {
    button.addEventListener('click', async () => {
      const shareId = String(button.getAttribute('data-share-accept') || '').trim();
      if (!shareId || !api.hyperwebAcceptShareWrite) return;
      const res = await api.hyperwebAcceptShareWrite(shareId);
      if (!res || !res.ok) {
        setStatusText('shares-status-line', (res && res.message) ? res.message : 'Unable to accept write access.');
        return;
      }
      await refreshPrivateSharesData();
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
  const note = e('shares-room-readonly-note');
  const editor = e('shares-room-editor');
  if (!room) {
    if (header) header.textContent = 'Select a room to start collaborating.';
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
      setStatusText('hyperweb-feed-status', `Posts: ${state.hyperwebFeed.length} · peers ${Number(feed.peer_count || 0)}`);
    }
  }
  await refreshHyperwebReferences();
}

async function openHyperwebPage() {
  await setAppView('hyperweb');
  await setHyperwebSurfaceTab(state.hyperwebActiveTab || 'feed', { skipRefresh: true });
  applyHyperwebSplitRatio(state.hyperwebSplitRatio, { skipPersist: true });
  if (state.hyperwebActiveTab === 'refs') {
    await refreshHyperwebReferences();
    return;
  }
  await refreshHyperwebFeedAndReferences();
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
  if (!placeholder) return;
  if (!entry) {
    placeholder.innerHTML = `
      <h3>History Preview</h3>
      <p>Select a URL on the left to view cached snapshot details.</p>
    `;
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
  placeholder.innerHTML = `
    <div class="history-preview-cached">
      <h4>${title}</h4>
      <div class="history-preview-cached-url">${url}</div>
      <div class="history-preview-cached-excerpt">${excerptHtml}</div>
      ${tokenText ? `<div class="muted small">${escapeHtml(tokenText)}</div>` : ''}
    </div>
  `;
  placeholder.classList.remove('hidden');
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
    const tooltipWidth = Math.max(20, Number(tooltip.offsetWidth || 220));
    const tooltipHeight = Math.max(20, Number(tooltip.offsetHeight || 80));
    const left = Math.min(Math.max(8, Number(event.offsetX || 0) + 14), Math.max(8, hostWidth - tooltipWidth - 8));
    const top = Math.min(Math.max(8, Number(event.offsetY || 0) + 14), Math.max(8, hostHeight - tooltipHeight - 8));
    tooltip.style.left = `${left}px`;
    tooltip.style.top = `${top}px`;
    tooltip.classList.remove('hidden');
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
    drawHistorySemanticMap();
    return;
  }
  state.historyMapPoints = Array.isArray(res.points) ? res.points : [];
  state.historyMapBounds = res.bounds || null;
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
  if (!status || status.ok === false) {
    node.textContent = status && status.message ? status.message : 'Unavailable';
    return;
  }
  if (status.enabled === false) {
    node.textContent = String(status.message || 'Abstraction is disabled.');
    return;
  }
  const counts = status.counts && typeof status.counts === 'object' ? status.counts : {};
  const updated = Array.isArray(status.references)
    ? status.references.map((item) => Number((item && item.updated_at) || 0)).filter((value) => Number.isFinite(value) && value > 0)
    : [];
  const latest = updated.length ? Math.max(...updated) : 0;
  const latestText = latest > 0 ? ` · updated ${formatAgo(latest)}` : '';
  node.textContent = `ready=${Number(counts.ready || 0)}, building=${Number(counts.building || 0)}, stale=${Number(counts.stale || 0)}, error=${Number(counts.error || 0)}${latestText}`;
}

function normalizeSettingsDraft(raw = {}) {
  const src = (raw && typeof raw === 'object') ? raw : {};
  const telegramAllowedChatIds = parseCommaSeparatedList(src.telegram_allowed_chat_ids);
  const telegramAllowedUsernames = parseCommaSeparatedList(src.telegram_allowed_usernames)
    .map((item) => item.toLowerCase().replace(/^@/, ''));
  return {
    default_search_engine: String(src.default_search_engine || 'ddg').trim().toLowerCase(),
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
    telegram_enabled: !!src.telegram_enabled,
    telegram_allowed_chat_ids: telegramAllowedChatIds,
    telegram_allowed_usernames: telegramAllowedUsernames,
    telegram_poll_interval_sec: Number(src.telegram_poll_interval_sec || 2),
    hyperweb_enabled: !!src.hyperweb_enabled,
    hyperweb_relay_url: String(src.hyperweb_relay_url || '').trim(),
    trustcommons_sync_enabled: !!src.trustcommons_sync_enabled,
    trustcommons_sync_port: Number(src.trustcommons_sync_port || 0),
    trustcommons_peer_sync_url: String(src.trustcommons_peer_sync_url || '').trim(),
    trustcommons_sync_interval_sec: Number(src.trustcommons_sync_interval_sec || 0),
    crawler_mode: String(src.crawler_mode || 'broad').trim().toLowerCase(),
    crawler_markdown_first: !!src.crawler_markdown_first,
    crawler_robots_default: String(src.crawler_robots_default || 'respect').trim().toLowerCase(),
    crawler_depth_default: Number(src.crawler_depth_default || 3),
    crawler_page_cap_default: Number(src.crawler_page_cap_default || 80),
    agent_mode_v1_enabled: !!src.agent_mode_v1_enabled,
    trustcommons_download_url: String(src.trustcommons_download_url || '').trim(),
    trustcommons_app_bundle_id: String(src.trustcommons_app_bundle_id || '').trim(),
    history_enabled: Object.prototype.hasOwnProperty.call(src, 'history_enabled') ? !!src.history_enabled : true,
    history_max_entries: Number(src.history_max_entries || 5000),
  };
}

function validateSettingsDraft(draft = {}) {
  const d = normalizeSettingsDraft(draft);
  const errors = {};
  if (!['ddg', 'google', 'bing'].includes(d.default_search_engine)) errors.default_search_engine = 'Invalid search engine.';
  if (!PROVIDERS.includes(d.lumino_last_provider)) errors.lumino_last_provider = 'Unsupported provider.';
  if (!/^https?:\/\//i.test(d.lmstudio_base_url || '')) errors.lmstudio_base_url = 'LM Studio URL must start with http:// or https://';
  if (!['ddg', 'serpapi'].includes(d.orchestrator_web_provider)) errors.orchestrator_web_provider = 'Invalid web provider.';
  if (d.abstraction_enabled && !String(d.abstraction_model || '').trim()) {
    errors.abstraction_model = 'Abstraction model is required when abstraction is enabled.';
  }
  if (!Number.isFinite(d.telegram_poll_interval_sec) || d.telegram_poll_interval_sec < 1 || d.telegram_poll_interval_sec > 30) {
    errors.telegram_poll_interval_sec = 'Polling interval must be 1..30 sec.';
  }
  if (!Number.isFinite(d.trustcommons_sync_port) || d.trustcommons_sync_port < 1024 || d.trustcommons_sync_port > 65535) {
    errors.trustcommons_sync_port = 'Port must be 1024..65535.';
  }
  if (!Number.isFinite(d.trustcommons_sync_interval_sec) || d.trustcommons_sync_interval_sec < 2 || d.trustcommons_sync_interval_sec > 60) {
    errors.trustcommons_sync_interval_sec = 'Interval must be 2..60 sec.';
  }
  if (d.trustcommons_peer_sync_url) {
    const isLoopback = /^http:\/\/(127\.0\.0\.1|localhost|\[::1\]|::1)(:\d+)?/i.test(d.trustcommons_peer_sync_url);
    if (!isLoopback) errors.trustcommons_peer_sync_url = 'Peer URL must be loopback HTTP.';
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

function renderSettingsForm() {
  const draft = normalizeSettingsDraft(state.settingsDraft || {});
  renderSettingsLmstudioModelSelect('settings-abstraction-model', draft.abstraction_model);
  renderSettingsLmstudioModelSelect('settings-image-analysis-model', draft.image_analysis_model);
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
  renderSettingsAbstractionStatus();
  renderSettingsStatusLine();
}

function renderSettingsDiagnostics() {
  const diag = state.settingsDiagnostics || {};
  const trust = e('settings-diagnostics-trustcommons');
  const hyper = e('settings-diagnostics-hyperweb');
  const identity = e('settings-diagnostics-identity');
  if (trust) trust.textContent = JSON.stringify((diag && diag.trustcommons) || {}, null, 2);
  if (hyper) hyper.textContent = JSON.stringify((diag && diag.hyperweb) || {}, null, 2);
  if (identity) identity.textContent = JSON.stringify((diag && diag.hyperweb_identity) || {}, null, 2);
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
}

function renderOrchestratorWebKeyStatus() {
  const node = e('settings-orchestrator-web-key-status');
  if (!node) return;
  if (state.orchestratorWebKeyConfigured == null) {
    node.textContent = 'Unknown';
    return;
  }
  node.textContent = state.orchestratorWebKeyConfigured ? 'Configured' : 'Missing';
}

async function refreshTelegramSettingsStatus() {
  if (!api.telegramStatus) return;
  const res = await api.telegramStatus();
  state.telegramRuntimeStatus = res || null;
  renderTelegramSettingsStatus();
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

async function refreshSettingsLmstudioModelOptions() {
  if (!api.providerListModels) return;
  const res = await api.providerListModels('lmstudio', '');
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

async function loadSettingsData() {
  const prefRes = await api.getPreferences();
  const diagnostics = await api.settingsDiagnostics();
  await refreshSettingsLmstudioModelOptions();
  await refreshProviderKeysState({ renderSettings: true });
  if (prefRes && prefRes.ok) {
    state.settingsPersisted = normalizeSettingsDraft(prefRes);
    state.settingsDraft = normalizeSettingsDraft(prefRes);
    state.settingsValidationErrors = {};
    state.settingsDirty = false;
    state.settingsSaveState = '';
    renderSettingsForm();
  }
  await refreshTelegramSettingsStatus();
  await refreshOrchestratorUsersList();
  await refreshLmstudioTokenStatus();
  await refreshOrchestratorWebKeyStatus();
  await refreshAbstractionStatus();
  if (diagnostics && diagnostics.ok) {
    state.settingsDiagnostics = diagnostics;
    renderSettingsDiagnostics();
  }
}

async function openSettingsPage() {
  await setAppView('settings');
  await loadSettingsData();
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
  await fetchModelsForProvider(state.settingsPersisted.lumino_last_provider, {
    statusId: 'provider-status',
    forceModel: state.settingsPersisted.lumino_last_model,
    persistSelection: true,
  });
  await refreshTelegramSettingsStatus();
  await refreshLmstudioTokenStatus();
  await refreshOrchestratorWebKeyStatus();
  await refreshAbstractionStatus();
  await refreshTrustCommonsStatus();
  await refreshHyperwebStatus();
  renderSettingsForm();
  const diagnostics = await api.settingsDiagnostics();
  if (diagnostics && diagnostics.ok) {
    state.settingsDiagnostics = diagnostics;
    renderSettingsDiagnostics();
  }
}

async function refreshTrustCommonsStatus() {
  const res = await api.trustCommonsStatus();
  if (!res || !res.ok) {
    state.trustCommonsStatus = null;
    const btn = e('trustcommons-connect-btn');
    if (btn) btn.textContent = 'TrustCommons Connect';
    return;
  }
  state.trustCommonsStatus = res;
  const btn = e('trustcommons-connect-btn');
  if (btn) {
    if (res.connected) {
      btn.textContent = `TrustCommons Connected (${res.identity_name || 'identity'})`;
    } else if (res.launched || (res.sync && res.sync.running)) {
      btn.textContent = `TrustCommons Ready (${res.identity_name || 'identity'})`;
    } else {
      btn.textContent = 'TrustCommons Connect';
    }
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
  status.textContent = configured.length
    ? `Configured: ${configured.join(', ')}${selectedModel ? ` · model ${selectedProvider}/${selectedModel}` : ''}`
    : 'No provider keys configured yet.';
  refreshAgentModeAvailability();
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
  const relay = String(res.relay_url || '');
  const mode = res.connected ? 'connected' : 'disconnected';
  const signalNote = res.signaling_available ? 'signaling ready' : 'signaling missing';
  statusNode.textContent = `Hyperweb ${mode} · peers ${peerCount} · ${signalNote}${relay ? ` · relay ${relay}` : ''}`;
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
    await refreshTrustCommonsStatus();
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
      } else if (
        state.htmlArtifactRuntime
        && state.htmlArtifactRuntime.running
        && String(state.htmlArtifactRuntime.artifactId || '') === String(artifact.id || '')
      ) {
        state.htmlArtifactRuntime.stale = true;
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
        if (updatedArtifact) updateArtifactRuntimeControls(updatedArtifact);
      } else if (status) {
        status.textContent = 'Save failed';
      }
    }, 350);
  });
  artifactInput?.addEventListener('select', () => {
    scheduleToggleActiveArtifactMarkerSelection(0);
  });
  artifactInput?.addEventListener('mouseup', () => {
    scheduleToggleActiveArtifactMarkerSelection(0);
  });
  artifactInput?.addEventListener('keyup', (event) => {
    const key = String((event && event.key) || '').toLowerCase();
    if (key === 'shift' || key === 'control' || key === 'meta' || key === 'alt') return;
    scheduleToggleActiveArtifactMarkerSelection(0);
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
    setArtifactViewMode(artifactId, ARTIFACT_VIEW_MODE_CODE);
    const artifact = (Array.isArray(ref.artifacts) ? ref.artifacts : []).find((item) => String((item && item.id) || '') === artifactId);
    if (artifact) updateArtifactRuntimeControls(artifact);
  });

  e('artifact-mode-preview-btn')?.addEventListener('click', () => {
    const ref = getActiveReference();
    if (!ref || state.activeSurface.kind !== 'artifact') return;
    const artifactId = String(state.activeSurface.artifactId || '').trim();
    if (!artifactId) return;
    setArtifactViewMode(artifactId, ARTIFACT_VIEW_MODE_PREVIEW);
    const artifact = (Array.isArray(ref.artifacts) ? ref.artifacts : []).find((item) => String((item && item.id) || '') === artifactId);
    if (!artifact) return;
    if (
      normalizeArtifactType(artifact.type) === 'html'
      && (!state.htmlArtifactRuntime || !state.htmlArtifactRuntime.running || String(state.htmlArtifactRuntime.artifactId || '') !== artifactId)
    ) {
      renderHtmlRuntimePlaceholder('Press Start to run HTML artifact.');
    }
    updateArtifactRuntimeControls(artifact);
    if (
      normalizeArtifactType(artifact.type) === 'html'
      && state.htmlArtifactRuntime
      && state.htmlArtifactRuntime.running
      && String(state.htmlArtifactRuntime.artifactId || '') === artifactId
    ) {
      focusActiveHtmlRuntime('preview-mode');
    }
  });

  e('artifact-run-start-btn')?.addEventListener('click', () => {
    startActiveHtmlArtifactRuntime();
  });

  e('artifact-run-stop-btn')?.addEventListener('click', () => {
    stopActiveHtmlArtifactRuntime();
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

  e('shares-refresh-btn')?.addEventListener('click', async () => {
    await refreshPrivateSharesData();
  });

  e('shares-tab-incoming-btn')?.addEventListener('click', () => setSharesTab('incoming'));
  e('shares-tab-outgoing-btn')?.addEventListener('click', () => setSharesTab('outgoing'));
  e('shares-tab-rooms-btn')?.addEventListener('click', () => setSharesTab('rooms'));

  e('hyperweb-tab-feed-btn')?.addEventListener('click', async () => {
    await setHyperwebSurfaceTab('feed');
  });
  e('hyperweb-tab-refs-btn')?.addEventListener('click', async () => {
    await setHyperwebSurfaceTab('refs');
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
      readSettingsDraftFromForm();
      state.settingsSaveState = '';
      renderSettingsStatusLine();
    });
  });

  e('settings-refresh-diagnostics-btn')?.addEventListener('click', async () => {
    const diagnostics = await api.settingsDiagnostics();
    if (diagnostics && diagnostics.ok) {
      state.settingsDiagnostics = diagnostics;
      renderSettingsDiagnostics();
    }
  });

  e('settings-trustcommons-connect-btn')?.addEventListener('click', async () => {
    await api.trustCommonsConnect();
    const diagnostics = await api.settingsDiagnostics();
    if (diagnostics && diagnostics.ok) {
      state.settingsDiagnostics = diagnostics;
      renderSettingsDiagnostics();
    }
  });

  e('settings-hyperweb-connect-btn')?.addEventListener('click', async () => {
    await api.hyperwebConnect();
    const diagnostics = await api.settingsDiagnostics();
    if (diagnostics && diagnostics.ok) {
      state.settingsDiagnostics = diagnostics;
      renderSettingsDiagnostics();
    }
  });

  e('settings-hyperweb-disconnect-btn')?.addEventListener('click', async () => {
    await api.hyperwebDisconnect();
    const diagnostics = await api.settingsDiagnostics();
    if (diagnostics && diagnostics.ok) {
      state.settingsDiagnostics = diagnostics;
      renderSettingsDiagnostics();
    }
  });

  e('settings-danger-reset-hyperweb-identity-btn')?.addEventListener('click', async () => {
    const text = window.prompt('Type RESET to confirm Hyperweb identity reset:', '');
    if (text === null) return;
    const res = await api.settingsDangerResetHyperwebIdentity({ phrase: String(text || '') });
    if (!res || !res.ok) {
      window.alert((res && res.message) || 'Unable to reset Hyperweb identity.');
      return;
    }
    const diagnostics = await api.settingsDiagnostics();
    if (diagnostics && diagnostics.ok) {
      state.settingsDiagnostics = diagnostics;
      renderSettingsDiagnostics();
    }
  });

  e('settings-danger-clear-social-cache-btn')?.addEventListener('click', async () => {
    const text = window.prompt('Type RESET to clear Hyperweb social cache:', '');
    if (text === null) return;
    const res = await api.settingsDangerClearHyperwebSocialCache({ phrase: String(text || '') });
    if (!res || !res.ok) {
      window.alert((res && res.message) || 'Unable to clear social cache.');
      return;
    }
    const diagnostics = await api.settingsDiagnostics();
    if (diagnostics && diagnostics.ok) {
      state.settingsDiagnostics = diagnostics;
      renderSettingsDiagnostics();
    }
  });

  e('settings-danger-reset-trustcommons-link-btn')?.addEventListener('click', async () => {
    const text = window.prompt('Type RESET to reset TrustCommons link:', '');
    if (text === null) return;
    const res = await api.settingsDangerResetTrustCommonsLink({ phrase: String(text || '') });
    if (!res || !res.ok) {
      window.alert((res && res.message) || 'Unable to reset TrustCommons link.');
      return;
    }
    const diagnostics = await api.settingsDiagnostics();
    if (diagnostics && diagnostics.ok) {
      state.settingsDiagnostics = diagnostics;
      renderSettingsDiagnostics();
    }
    await refreshTrustCommonsStatus();
    await refreshHyperwebStatus();
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

    const ok = window.confirm(`Delete the primary ${provider} API key from macOS Keychain?`);
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
    await persistLuminoSelection(provider, state.selectedModel || '');
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

  e('trustcommons-connect-btn')?.addEventListener('click', async () => {
    const res = await api.trustCommonsConnect();
    if (!res || !res.ok) {
      window.alert((res && res.message) || 'Unable to connect Trust Commons.');
    } else if (res.message) {
      showPassiveNotification(String(res.message || 'TrustCommons connected.'));
    }
    await refreshTrustCommonsStatus();
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
  applyZenModeUi();
  const savedUiZoom = loadUiZoomPreference();
  if (
    savedUiZoom
    && electronApi
    && typeof electronApi.setZoomFactor === 'function'
  ) {
    electronApi.setZoomFactor(savedUiZoom);
  }

  bindControls();
  setupHyperwebSplitter();
  setupArtifactHorizontalSplitter();
  await setHyperwebSurfaceTab(state.hyperwebActiveTab || 'feed', { skipRefresh: true });
  setSharesTab(state.sharesActiveTab || 'incoming');
  applyHyperwebSplitRatio(state.hyperwebSplitRatio, { skipPersist: true });
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
  await refreshTrustCommonsStatus();
  await refreshHyperwebStatus();
  await loadChatThread();
  await loadProgramEditorForActiveReference();
  await syncActiveSurface();
  await refreshUncommittedActionCue();
  setChatBusy(false);

  showOnboarding(!state.onboardingComplete);
}

initialize();
