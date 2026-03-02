const fs = require('fs');
const path = require('path');

function nowTs() {
  return Date.now();
}

function createOrchestratorUsersStore(options = {}) {
  const userDataPath = String(options.userDataPath || '').trim();
  const filePath = path.join(userDataPath || process.cwd(), 'orchestrator_users.json');

  function readState() {
    try {
      if (!fs.existsSync(filePath)) return { version: 1, users: [] };
      const parsed = JSON.parse(fs.readFileSync(filePath, 'utf8'));
      return {
        version: 1,
        users: Array.isArray(parsed && parsed.users) ? parsed.users : [],
      };
    } catch (_) {
      return { version: 1, users: [] };
    }
  }

  function writeState(state) {
    const next = {
      version: 1,
      users: Array.isArray(state && state.users) ? state.users : [],
    };
    fs.writeFileSync(filePath, JSON.stringify(next, null, 2), 'utf8');
  }

  function normalizeUser(input = {}, existing = null) {
    const base = (existing && typeof existing === 'object') ? existing : {};
    const src = (input && typeof input === 'object') ? input : {};
    return {
      chat_id: String(Object.prototype.hasOwnProperty.call(src, 'chat_id') ? src.chat_id : (base.chat_id || '')).trim(),
      telegram_username: String(Object.prototype.hasOwnProperty.call(src, 'telegram_username') ? src.telegram_username : (base.telegram_username || '')).trim(),
      user_id: String(Object.prototype.hasOwnProperty.call(src, 'user_id') ? src.user_id : (base.user_id || '')).trim(),
      created_at: Number(base.created_at || nowTs()),
      updated_at: nowTs(),
      prompts_today: Number(base.prompts_today || 0),
      prompts_total: Number(base.prompts_total || 0),
      last_prompt_day: String(base.last_prompt_day || ''),
      tokens_total: Number(base.tokens_total || 0),
    };
  }

  function getByChatId(chatId) {
    const id = String(chatId || '').trim();
    if (!id) return null;
    const state = readState();
    return (state.users || []).find((u) => String((u && u.chat_id) || '') === id) || null;
  }

  function getByUserId(userId) {
    const id = String(userId || '').trim().toLowerCase();
    if (!id) return null;
    const state = readState();
    return (state.users || []).find((u) => String((u && u.user_id) || '').trim().toLowerCase() === id) || null;
  }

  function listUsers() {
    const state = readState();
    const users = (state.users || []).slice().sort((a, b) => Number((b && b.updated_at) || 0) - Number((a && a.updated_at) || 0));
    return { ok: true, users };
  }

  function register(payload = {}) {
    const chatId = String(payload.chat_id || '').trim();
    const userId = String(payload.user_id || '').trim();
    if (!chatId || !userId) return { ok: false, message: 'chat_id and user_id are required.' };
    const state = readState();
    state.users = Array.isArray(state.users) ? state.users : [];

    const existingByUser = state.users.find((u) => String((u && u.user_id) || '').trim().toLowerCase() === userId.toLowerCase());
    if (existingByUser && String(existingByUser.chat_id || '') !== chatId) {
      return { ok: false, message: 'This user_id is already taken.' };
    }

    const idx = state.users.findIndex((u) => String((u && u.chat_id) || '') === chatId);
    if (idx >= 0) {
      const next = normalizeUser(payload, state.users[idx]);
      state.users[idx] = next;
      writeState(state);
      return { ok: true, user: next, updated: true };
    }

    const created = normalizeUser(payload, null);
    state.users.push(created);
    writeState(state);
    return { ok: true, user: created, created: true };
  }

  function incrementUsage(chatId, metrics = {}) {
    const id = String(chatId || '').trim();
    if (!id) return { ok: false, message: 'chat_id is required.' };
    const state = readState();
    const idx = (state.users || []).findIndex((u) => String((u && u.chat_id) || '') === id);
    if (idx < 0) return { ok: false, message: 'User not found.' };
    const user = normalizeUser(state.users[idx], state.users[idx]);
    const today = new Date().toISOString().slice(0, 10);
    if (user.last_prompt_day !== today) {
      user.prompts_today = 0;
      user.last_prompt_day = today;
    }
    user.prompts_today += 1;
    user.prompts_total += 1;
    user.tokens_total += Math.max(0, Number(metrics.tokens || 0));
    user.updated_at = nowTs();
    state.users[idx] = user;
    writeState(state);
    return { ok: true, user };
  }

  function revokeByChatId(chatId) {
    const id = String(chatId || '').trim();
    if (!id) return { ok: false, message: 'chat_id is required.' };
    const state = readState();
    state.users = Array.isArray(state.users) ? state.users : [];
    const idx = state.users.findIndex((u) => String((u && u.chat_id) || '') === id);
    if (idx < 0) return { ok: false, message: 'User not found.' };
    const [removed] = state.users.splice(idx, 1);
    writeState(state);
    return { ok: true, user: removed || null };
  }

  return {
    filePath,
    listUsers,
    getByChatId,
    getByUserId,
    register,
    incrementUsage,
    revokeByChatId,
  };
}

module.exports = {
  createOrchestratorUsersStore,
};
