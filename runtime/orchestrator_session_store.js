const fs = require('fs');
const path = require('path');

function createOrchestratorSessionStore(options = {}) {
  const userDataPath = String(options.userDataPath || '').trim();
  const filePath = path.join(userDataPath || process.cwd(), 'orchestrator_sessions.json');

  function readState() {
    try {
      if (!fs.existsSync(filePath)) return { version: 1, sessions: {} };
      const parsed = JSON.parse(fs.readFileSync(filePath, 'utf8'));
      return {
        version: 1,
        sessions: (parsed && typeof parsed.sessions === 'object') ? parsed.sessions : {},
      };
    } catch (_) {
      return { version: 1, sessions: {} };
    }
  }

  function writeState(state) {
    const next = {
      version: 1,
      sessions: (state && typeof state.sessions === 'object') ? state.sessions : {},
    };
    fs.writeFileSync(filePath, JSON.stringify(next, null, 2), 'utf8');
  }

  return {
    filePath,
    get(key) {
      const sessionKey = String(key || '').trim();
      if (!sessionKey) return null;
      const state = readState();
      const entry = state.sessions[sessionKey];
      if (!entry || typeof entry !== 'object') return null;
      return entry;
    },
    set(key, value) {
      const sessionKey = String(key || '').trim();
      if (!sessionKey) return { ok: false, message: 'key is required.' };
      const state = readState();
      state.sessions[sessionKey] = (value && typeof value === 'object') ? value : {};
      writeState(state);
      return { ok: true };
    },
    delete(key) {
      const sessionKey = String(key || '').trim();
      if (!sessionKey) return { ok: false, message: 'key is required.' };
      const state = readState();
      if (Object.prototype.hasOwnProperty.call(state.sessions, sessionKey)) {
        delete state.sessions[sessionKey];
        writeState(state);
      }
      return { ok: true };
    },
  };
}

module.exports = {
  createOrchestratorSessionStore,
};
