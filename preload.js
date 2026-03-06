const { contextBridge, ipcRenderer, webFrame } = require('electron');

contextBridge.exposeInMainWorld('electronAPI', {
  platform: process.platform,
  isElectron: true,
  getZoomFactor: () => webFrame.getZoomFactor(),
  setZoomFactor: (factor) => {
    const next = Number(factor);
    if (!Number.isFinite(next) || next <= 0) {
      return { ok: false, zoom: webFrame.getZoomFactor(), message: 'Invalid zoom factor.' };
    }
    try {
      webFrame.setZoomFactor(next);
      return { ok: true, zoom: webFrame.getZoomFactor() };
    } catch (err) {
      return {
        ok: false,
        zoom: webFrame.getZoomFactor(),
        message: String((err && err.message) || 'Unable to set zoom factor.'),
      };
    }
  },
  browser: {
    show: (bounds) => ipcRenderer.invoke('browser:show', bounds),
    hide: () => ipcRenderer.invoke('browser:hide'),
    updateBounds: (bounds) => ipcRenderer.invoke('browser:updateBounds', bounds),
    historyPreviewShow: (bounds) => ipcRenderer.invoke('browser:historyPreviewShow', { bounds }),
    historyPreviewHide: () => ipcRenderer.invoke('browser:historyPreviewHide'),
    historyPreviewNavigate: (url) => ipcRenderer.invoke('browser:historyPreviewNavigate', { url }),
    historyPreviewUpdateBounds: (bounds) => ipcRenderer.invoke('browser:historyPreviewUpdateBounds', { bounds }),
    navigate: (url) => ipcRenderer.invoke('browser:navigate', url),
    reload: () => ipcRenderer.invoke('browser:reload'),
    back: () => ipcRenderer.invoke('browser:back'),
    forward: () => ipcRenderer.invoke('browser:forward'),
    getZoomFactor: () => ipcRenderer.invoke('browser:getZoomFactor'),
    setZoomFactor: (factor) => ipcRenderer.invoke('browser:setZoomFactor', factor),
    canGoBack: () => ipcRenderer.invoke('browser:canGoBack'),
    canGoForward: () => ipcRenderer.invoke('browser:canGoForward'),
    getCurrentUrl: () => ipcRenderer.invoke('browser:getCurrentUrl'),
    getPageContent: () => ipcRenderer.invoke('browser:getPageContent'),
    openExternal: (url) => ipcRenderer.invoke('browser:openExternal', { url }),
    openPath: (filePath) => ipcRenderer.invoke('browser:openPath', { file_path: filePath }),
    markerSetMode: (enabled) => ipcRenderer.invoke('browser:markerSetMode', enabled),
    markerSetContext: (payload) => ipcRenderer.invoke('browser:markerSetContext', payload),
    markerClearActive: () => ipcRenderer.invoke('browser:markerClearActive'),
    srToggleArtifactHighlight: (srId, artifactId, payload = {}) => ipcRenderer.invoke('browser:srToggleArtifactHighlight', {
      srId,
      artifactId,
      ...(payload || {}),
    }),

    srList: () => ipcRenderer.invoke('browser:srList'),
    memorySetEnabled: (srId, enabled) => ipcRenderer.invoke('browser:memorySetEnabled', { srId, enabled }),
    memoryList: (srId) => ipcRenderer.invoke('browser:memoryList', { srId }),
    memoryLoadCheckpoint: (srId, checkpointId) => ipcRenderer.invoke('browser:memoryLoadCheckpoint', { srId, checkpointId }),
    memoryPreviewDiff: (srId, checkpointId, against = 'current') => ipcRenderer.invoke('browser:memoryPreviewDiff', {
      srId,
      checkpointId,
      against,
    }),
    memoryForkFromCheckpoint: (srId, checkpointId, titleHint = '') => ipcRenderer.invoke('browser:memoryForkFromCheckpoint', {
      srId,
      checkpointId,
      titleHint,
    }),
    memoryAttachDiffContext: (srId, checkpointId, sections = ['summary', 'diff']) => ipcRenderer.invoke('browser:memoryAttachDiffContext', {
      srId,
      checkpointId,
      sections,
    }),
    srSetVisibility: (srId, visibility) => ipcRenderer.invoke('browser:srSetVisibility', { srId, visibility }),
    srPublishSnapshot: (srId) => ipcRenderer.invoke('browser:srPublishSnapshot', { srId }),
    srDiscoverPublicReferences: (query, limit) => ipcRenderer.invoke('browser:srDiscoverPublicReferences', { query, limit }),
    srCommitPublicCandidate: (srId, strategy, targetSrId) => ipcRenderer.invoke('browser:srCommitPublicCandidate', { srId, strategy, targetSrId }),
    srCreateRoot: (payload) => ipcRenderer.invoke('browser:srCreateRoot', payload),
    srFork: (srId) => ipcRenderer.invoke('browser:srFork', srId),
    srAddChild: (srId) => ipcRenderer.invoke('browser:srAddChild', srId),
    srRename: (srId, title) => ipcRenderer.invoke('browser:srRename', { srId, title }),
    srSetColorTag: (srId, colorTag) => ipcRenderer.invoke('browser:srSetColorTag', { srId, colorTag }),
    srSetPinnedRoot: (srId, pinned) => ipcRenderer.invoke('browser:srSetPinnedRoot', { srId, pinned }),
    srClearChatAndAutoFork: (srId) => ipcRenderer.invoke('browser:srClearChatAndAutoFork', srId),
    srDeleteWithSuccession: (srId) => ipcRenderer.invoke('browser:srDeleteWithSuccession', srId),
    srSearch: (query, top_k) => ipcRenderer.invoke('browser:srSearch', { query, top_k }),
    srSaveInActive: (payload) => ipcRenderer.invoke('browser:srSaveInActive', payload),
    srCreateEmptyWorkspace: (payload) => ipcRenderer.invoke('browser:srCreateEmptyWorkspace', payload || {}),
    srAddTab: (srId, tab, insertAfterTabId = '') => ipcRenderer.invoke('browser:srAddTab', {
      srId,
      tab,
      insert_after_tab_id: insertAfterTabId,
    }),
    srSetActiveTab: (srId, tabId) => ipcRenderer.invoke('browser:srSetActiveTab', { srId, tabId }),
    srPatchTab: (srId, tabId, patch) => ipcRenderer.invoke('browser:srPatchTab', { srId, tabId, patch }),
    srRemoveTab: (srId, tabId) => ipcRenderer.invoke('browser:srRemoveTab', { srId, tabId }),
    resolveArtifactAsset: (srId, uri) => ipcRenderer.invoke('browser:resolveArtifactAsset', { srId, uri }),
    saveArtifactImage: (srId, sourceUrl, suggestedName) => ipcRenderer.invoke('browser:saveArtifactImage', { srId, sourceUrl, suggestedName }),
    srGetProgram: (srId) => ipcRenderer.invoke('browser:srGetProgram', { srId }),
    srSetProgram: (srId, program) => ipcRenderer.invoke('browser:srSetProgram', { srId, program }),

    srUpsertArtifact: (srId, artifact) => ipcRenderer.invoke('browser:srUpsertArtifact', { srId, artifact }),
    srUpsertYouTubeTranscript: (srId, payload) => ipcRenderer.invoke('browser:srUpsertYouTubeTranscript', { srId, ...(payload || {}) }),
    srGetArtifact: (srId, artifactId) => ipcRenderer.invoke('browser:srGetArtifact', { srId, artifactId }),
    srDeleteArtifact: (srId, artifactId) => ipcRenderer.invoke('browser:srDeleteArtifact', { srId, artifactId }),

    srAppendChatMessage: (srId, role, text) => ipcRenderer.invoke('browser:srAppendChatMessage', { srId, role, text }),
    srGetChatThread: (srId) => ipcRenderer.invoke('browser:srGetChatThread', srId),
    srUpdateAgentWeights: (srId, weights) => ipcRenderer.invoke('browser:srUpdateAgentWeights', { srId, weights }),
    srAppendDecisionTrace: (srId, step) => ipcRenderer.invoke('browser:srAppendDecisionTrace', { srId, step }),

    srApplyDiffOp: (diffOp) => ipcRenderer.invoke('browser:srApplyDiffOp', diffOp || {}),
    srListPendingDiffOps: () => ipcRenderer.invoke('browser:srListPendingDiffOps'),

    srMountFolder: (srId, absolutePath) => ipcRenderer.invoke('browser:srMountFolder', { srId, absolutePath }),
    srReindexFolderMount: (srId, mountId) => ipcRenderer.invoke('browser:srReindexFolderMount', { srId, mountId }),
    srUnmountFolder: (srId, mountId) => ipcRenderer.invoke('browser:srUnmountFolder', { srId, mountId }),
    srAddContextFile: (srId, absolutePath) => ipcRenderer.invoke('browser:srAddContextFile', { srId, absolutePath }),
    srListContextFiles: (srId) => ipcRenderer.invoke('browser:srListContextFiles', srId),
    srGetContextFilePreview: (srId, fileId) => ipcRenderer.invoke('browser:srGetContextFilePreview', { srId, fileId }),
    srRemoveContextFile: (srId, fileId) => ipcRenderer.invoke('browser:srRemoveContextFile', { srId, fileId }),
    srListSkills: (srId) => ipcRenderer.invoke('browser:srListSkills', { srId }),
    srSaveSkill: (srId, skill, scope) => ipcRenderer.invoke('browser:srSaveSkill', { srId, skill, scope }),
    srDeleteSkill: (srId, skillId, scope) => ipcRenderer.invoke('browser:srDeleteSkill', { srId, skillId, scope }),
    srRunSkill: (srId, skillId, scope, args) => ipcRenderer.invoke('browser:srRunSkill', { srId, skillId, scope, args }),
    pythonCheck: () => ipcRenderer.invoke('browser:pythonCheck', {}),
    pythonExec: (srId, code) => ipcRenderer.invoke('browser:pythonExec', { srId, code }),
    pipInstall: (srId, packages) => ipcRenderer.invoke('browser:pipInstall', { srId, packages }),

    chatStart: (payload) => ipcRenderer.invoke('browser:chatStart', payload || {}),
    chatCancel: (requestId) => ipcRenderer.invoke('browser:chatCancel', { request_id: requestId }),
    chat: (payload) => ipcRenderer.invoke('browser:chat', payload || {}),
    crawlerStart: (payload) => ipcRenderer.invoke('browser:crawlerStart', payload || {}),
    crawlerStatus: (payload) => ipcRenderer.invoke('browser:crawlerStatus', payload || {}),
    crawlerStop: (payload) => ipcRenderer.invoke('browser:crawlerStop', payload || {}),

    providerSetKey: (provider, apiKey) => ipcRenderer.invoke('browser:providerSetKey', { provider, apiKey }),
    providerDeleteKey: (provider) => ipcRenderer.invoke('browser:providerDeleteKey', { provider }),
    providerListConfigured: () => ipcRenderer.invoke('browser:providerListConfigured'),
    providerKeysList: () => ipcRenderer.invoke('browser:providerKeysList'),
    providerKeyUpsert: (payload = {}) => ipcRenderer.invoke('browser:providerKeyUpsert', payload || {}),
    providerKeyDelete: (payload = {}) => ipcRenderer.invoke('browser:providerKeyDelete', payload || {}),
    providerKeySetPrimary: (payload = {}) => ipcRenderer.invoke('browser:providerKeySetPrimary', payload || {}),
    providerListModels: (provider, keyId = '') => ipcRenderer.invoke('browser:providerListModels', { provider, keyId }),

    importFromBrowser: (source) => ipcRenderer.invoke('browser:importFromBrowser', { source }),
    requestDefaultBrowser: () => ipcRenderer.invoke('browser:requestDefaultBrowser'),
    openDefaultBrowserSettings: () => ipcRenderer.invoke('browser:openDefaultBrowserSettings'),
    getPreferences: () => ipcRenderer.invoke('browser:getPreferences'),
    telegramStatus: () => ipcRenderer.invoke('browser:telegramStatus'),
    telegramSetToken: (token) => ipcRenderer.invoke('browser:telegramSetToken', { token }),
    telegramClearToken: () => ipcRenderer.invoke('browser:telegramClearToken'),
    telegramTestMessage: (payload = {}) => ipcRenderer.invoke('browser:telegramTestMessage', payload || {}),
    lmstudioTokenStatus: () => ipcRenderer.invoke('browser:lmstudioTokenStatus'),
    lmstudioSetToken: (token) => ipcRenderer.invoke('browser:lmstudioSetToken', { token }),
    lmstudioClearToken: () => ipcRenderer.invoke('browser:lmstudioClearToken'),
    orchestratorWebKeyStatus: () => ipcRenderer.invoke('browser:orchestratorWebKeyStatus'),
    orchestratorWebSetKey: (key) => ipcRenderer.invoke('browser:orchestratorWebSetKey', { key }),
    orchestratorWebClearKey: () => ipcRenderer.invoke('browser:orchestratorWebClearKey'),
    orchestratorJobList: (payload = {}) => ipcRenderer.invoke('browser:orchestratorJobList', payload || {}),
    orchestratorJobCreate: (payload = {}) => ipcRenderer.invoke('browser:orchestratorJobCreate', payload || {}),
    orchestratorJobEdit: (payload = {}) => ipcRenderer.invoke('browser:orchestratorJobEdit', payload || {}),
    orchestratorJobPause: (payload = {}) => ipcRenderer.invoke('browser:orchestratorJobPause', payload || {}),
    orchestratorJobResume: (payload = {}) => ipcRenderer.invoke('browser:orchestratorJobResume', payload || {}),
    orchestratorJobDelete: (payload = {}) => ipcRenderer.invoke('browser:orchestratorJobDelete', payload || {}),
    orchestratorUsersList: () => ipcRenderer.invoke('browser:orchestratorUsersList'),
    orchestratorUserRevoke: (payload = {}) => ipcRenderer.invoke('browser:orchestratorUserRevoke', payload || {}),
    historyList: (payload = {}) => ipcRenderer.invoke('browser:historyList', payload || {}),
    historyGet: (historyId) => ipcRenderer.invoke('browser:historyGet', { history_id: historyId }),
    historyDelete: (historyId) => ipcRenderer.invoke('browser:historyDelete', { history_id: historyId }),
    historyClear: (phrase = '') => ipcRenderer.invoke('browser:historyClear', { phrase }),
    historySemanticMap: (payload = {}) => ipcRenderer.invoke('browser:historySemanticMap', payload || {}),
    updatePreferences: (payload) => ipcRenderer.invoke('browser:updatePreferences', payload || {}),
    abstractionStatus: (payload = {}) => ipcRenderer.invoke('browser:abstractionStatus', payload || {}),
    abstractionRebuild: (payload = {}) => ipcRenderer.invoke('browser:abstractionRebuild', payload || {}),
    ragStatus: (payload = {}) => ipcRenderer.invoke('browser:ragStatus', payload || {}),
    ragReindex: (payload = {}) => ipcRenderer.invoke('browser:ragReindex', payload || {}),
    settingsDiagnostics: () => ipcRenderer.invoke('browser:settingsDiagnostics'),
    appDataProtectionStatus: () => ipcRenderer.invoke('browser:appDataProtectionStatus'),
    appDataProtectionLock: () => ipcRenderer.invoke('browser:appDataProtectionLock'),
    appDataProtectionUnlock: (payload = {}) => ipcRenderer.invoke('browser:appDataProtectionUnlock', payload || {}),
    settingsDangerResetHyperwebIdentity: (payload = {}) => ipcRenderer.invoke('browser:settingsDangerResetHyperwebIdentity', payload),
    settingsDangerClearHyperwebSocialCache: (payload = {}) => ipcRenderer.invoke('browser:settingsDangerClearHyperwebSocialCache', payload),
    settingsDangerResetTrustCommonsLink: (payload = {}) => ipcRenderer.invoke('browser:settingsDangerResetTrustCommonsLink', payload),
    setLuminoSelection: (provider, model) => ipcRenderer.invoke('browser:setLuminoSelection', { provider, model }),
    setDefaultSearchEngine: (engine) => ipcRenderer.invoke('browser:setDefaultSearchEngine', { engine }),

    hyperwebStatus: () => ipcRenderer.invoke('browser:hyperwebStatus'),
    hyperwebConnect: () => ipcRenderer.invoke('browser:hyperwebConnect'),
    hyperwebDisconnect: () => ipcRenderer.invoke('browser:hyperwebDisconnect'),
    hyperwebQuery: (query, limit) => ipcRenderer.invoke('browser:hyperwebQuery', { query, limit }),
    hyperwebImportSuggestion: (suggestion) => ipcRenderer.invoke('browser:hyperwebImportSuggestion', suggestion),
    hyperwebSocialStatus: () => ipcRenderer.invoke('browser:hyperwebSocialStatus'),
    hyperwebCreateInvite: () => ipcRenderer.invoke('browser:hyperwebCreateInvite'),
    hyperwebAcceptInvite: (token) => ipcRenderer.invoke('browser:hyperwebAcceptInvite', { token }),
    hyperwebListPeers: () => ipcRenderer.invoke('browser:hyperwebListPeers'),
    hyperwebPostCreate: (body) => ipcRenderer.invoke('browser:hyperwebPostCreate', { body }),
    hyperwebPostReply: (postId, body) => ipcRenderer.invoke('browser:hyperwebPostReply', { post_id: postId, body }),
    hyperwebVoteSet: (targetId, value) => ipcRenderer.invoke('browser:hyperwebVoteSet', { target_id: targetId, value }),
    hyperwebReportTarget: (targetId, targetKind = 'post', reason = '') => ipcRenderer.invoke('browser:hyperwebReportTarget', {
      target_id: targetId,
      target_kind: targetKind,
      reason,
    }),
    hyperwebDeleteSnapshot: (snapshotId) => ipcRenderer.invoke('browser:hyperwebDeleteSnapshot', {
      snapshot_id: snapshotId,
    }),
    hyperwebFeedQuery: (authorFingerprint = '') => ipcRenderer.invoke('browser:hyperwebFeedQuery', { author_fingerprint: authorFingerprint }),
    hyperwebProfileQuery: (authorFingerprint = '') => ipcRenderer.invoke('browser:hyperwebProfileQuery', { author_fingerprint: authorFingerprint }),
    hyperwebResetFilter: () => ipcRenderer.invoke('browser:hyperwebResetFilter'),
    hyperwebReferenceSearch: (query = '', limit = 40, authorFingerprint = '') => ipcRenderer.invoke('browser:hyperwebReferenceSearch', {
      query,
      limit,
      author_fingerprint: authorFingerprint,
    }),
    hyperwebImportReference: (item) => ipcRenderer.invoke('browser:hyperwebImportReference', { item }),
    hyperwebPostSearch: (query = '', limit = 40, authorFingerprint = '') => ipcRenderer.invoke('browser:hyperwebPostSearch', {
      query,
      limit,
      author_fingerprint: authorFingerprint,
    }),
    hyperwebMembersList: () => ipcRenderer.invoke('browser:hyperwebMembersList'),
    hyperwebChatSend: (payload = {}) => ipcRenderer.invoke('browser:hyperwebChatSend', payload || {}),
    hyperwebChatHistory: (payload = {}) => ipcRenderer.invoke('browser:hyperwebChatHistory', payload || {}),
    hyperwebChatMarkRead: (payload = {}) => ipcRenderer.invoke('browser:hyperwebChatMarkRead', payload || {}),
    hyperwebChatRoomCreate: (payload = {}) => ipcRenderer.invoke('browser:hyperwebChatRoomCreate', payload || {}),
    hyperwebChatRoomsList: () => ipcRenderer.invoke('browser:hyperwebChatRoomsList'),
    hyperwebShareReference: (srId, recipientIds = []) => ipcRenderer.invoke('browser:hyperwebShareReference', {
      sr_id: srId,
      recipient_ids: recipientIds,
    }),
    hyperwebListShares: () => ipcRenderer.invoke('browser:hyperwebListShares'),
    hyperwebAcceptShareWrite: (shareId) => ipcRenderer.invoke('browser:hyperwebAcceptShareWrite', { share_id: shareId }),
    hyperwebDeclineShareWrite: (shareId) => ipcRenderer.invoke('browser:hyperwebDeclineShareWrite', { share_id: shareId }),
    hyperwebRevokeShare: (shareId) => ipcRenderer.invoke('browser:hyperwebRevokeShare', { share_id: shareId }),
    hyperwebDeleteShare: (shareId) => ipcRenderer.invoke('browser:hyperwebDeleteShare', { share_id: shareId }),
    hyperwebListSharedRooms: () => ipcRenderer.invoke('browser:hyperwebListSharedRooms'),
    hyperwebOpenSharedRoom: (roomId) => ipcRenderer.invoke('browser:hyperwebOpenSharedRoom', { room_id: roomId }),
    hyperwebCollabApplyUpdate: (roomId, update = {}) => ipcRenderer.invoke('browser:hyperwebCollabApplyUpdate', {
      room_id: roomId,
      update,
    }),

    trustCommonsStatus: () => ipcRenderer.invoke('browser:trustCommonsStatus'),
    trustCommonsConnect: () => ipcRenderer.invoke('browser:trustCommonsConnect'),

    onNavigate: (callback) => {
      ipcRenderer.removeAllListeners('browser:did-navigate');
      ipcRenderer.on('browser:did-navigate', (_event, data) => callback(data));
    },
    onTitleUpdate: (callback) => {
      ipcRenderer.removeAllListeners('browser:title-updated');
      ipcRenderer.on('browser:title-updated', (_event, data) => callback(data));
    },
    onLoadingChange: (callback) => {
      ipcRenderer.removeAllListeners('browser:loading');
      ipcRenderer.on('browser:loading', (_event, data) => callback(data));
    },
    onAudible: (callback) => {
      ipcRenderer.removeAllListeners('browser:audible');
      ipcRenderer.on('browser:audible', (_event, data) => callback(data));
    },
    onChatStream: (callback) => {
      ipcRenderer.removeAllListeners('browser:chatStream');
      ipcRenderer.on('browser:chatStream', (_event, data) => callback(data));
    },
    onCrawlerStream: (callback) => {
      ipcRenderer.removeAllListeners('browser:crawlerStream');
      ipcRenderer.on('browser:crawlerStream', (_event, data) => callback(data));
    },
    onHyperwebChat: (callback) => {
      ipcRenderer.removeAllListeners('browser:hyperwebChat');
      ipcRenderer.on('browser:hyperwebChat', (_event, data) => callback(data));
    },
    onShortcutCommand: (callback) => {
      ipcRenderer.removeAllListeners('browser:shortcut-command');
      ipcRenderer.on('browser:shortcut-command', (_event, data) => callback(data));
    },
    onMarkerUpdate: (callback) => {
      ipcRenderer.removeAllListeners('browser:marker-update');
      ipcRenderer.on('browser:marker-update', (_event, data) => callback(data));
    },
  },
  tabs: {
    create: (url) => ipcRenderer.invoke('tabs:create', url),
    close: (tabId) => ipcRenderer.invoke('tabs:close', tabId),
    switch: (tabId) => ipcRenderer.invoke('tabs:switch', tabId),
    getAll: () => ipcRenderer.invoke('tabs:getAll'),
    navigate: (tabId, url) => ipcRenderer.invoke('tabs:navigate', tabId, url),
    getActive: () => ipcRenderer.invoke('tabs:getActive'),
  },
});
