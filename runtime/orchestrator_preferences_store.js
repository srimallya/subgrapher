const fs = require('fs');
const path = require('path');

function nowTs() {
  return Date.now();
}

function createOrchestratorPreferencesStore(options = {}) {
  const userDataPath = String(options.userDataPath || '').trim();
  const filePath = path.join(userDataPath || process.cwd(), 'orchestrator_preferences.json');

  function readState() {
    try {
      if (!fs.existsSync(filePath)) return { version: 1, users: {} };
      const parsed = JSON.parse(fs.readFileSync(filePath, 'utf8'));
      return {
        version: 1,
        users: (parsed && typeof parsed.users === 'object' && parsed.users) ? parsed.users : {},
      };
    } catch (_) {
      return { version: 1, users: {} };
    }
  }

  function writeState(state = {}) {
    const next = {
      version: 1,
      users: (state && typeof state.users === 'object' && state.users) ? state.users : {},
    };
    fs.writeFileSync(filePath, JSON.stringify(next, null, 2), 'utf8');
  }

  function normalizeScope(raw = '') {
    return String(raw || '').trim().toLowerCase();
  }

  function normalizePatch(raw = {}) {
    const src = (raw && typeof raw === 'object') ? raw : {};
    return {
      ...src,
      updated_at: nowTs(),
    };
  }

  return {
    filePath,
    get(userScope = '') {
      const scope = normalizeScope(userScope);
      if (!scope) return null;
      const state = readState();
      const value = state.users[scope];
      return (value && typeof value === 'object') ? value : null;
    },
    upsert(userScope = '', patch = {}, metadata = {}) {
      const scope = normalizeScope(userScope);
      if (!scope) return { ok: false, message: 'user_scope is required.' };
      const state = readState();
      const current = (state.users[scope] && typeof state.users[scope] === 'object') ? state.users[scope] : {};
      const next = {
        created_at: Number(current.created_at || nowTs()),
        updated_at: nowTs(),
        user_scope: scope,
        chat_id: String((metadata && metadata.chat_id) || current.chat_id || '').trim(),
        username: String((metadata && metadata.username) || current.username || '').trim(),
        preferences: {
          ...(current.preferences && typeof current.preferences === 'object' ? current.preferences : {}),
          ...normalizePatch(patch),
        },
      };
      state.users[scope] = next;
      writeState(state);
      return { ok: true, preference: next };
    },
    list(limit = 200) {
      const state = readState();
      const rows = Object.values(state.users || {}).filter((item) => item && typeof item === 'object');
      rows.sort((a, b) => Number((b && b.updated_at) || 0) - Number((a && a.updated_at) || 0));
      return {
        ok: true,
        users: rows.slice(0, Math.max(1, Math.min(2000, Number(limit) || 200))),
      };
    },
    delete(userScope = '') {
      const scope = normalizeScope(userScope);
      if (!scope) return { ok: false, message: 'user_scope is required.' };
      const state = readState();
      if (Object.prototype.hasOwnProperty.call(state.users, scope)) {
        delete state.users[scope];
        writeState(state);
      }
      return { ok: true };
    },
  };
}

module.exports = {
  createOrchestratorPreferencesStore,
};
