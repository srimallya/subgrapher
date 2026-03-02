const fs = require('fs');
const path = require('path');

function nowTs() {
  return Date.now();
}

function randomRef(prefix = 'sec') {
  const rand = Math.random().toString(36).slice(2, 10);
  return `${prefix}_${nowTs()}_${rand}`;
}

function ensureDir(filePath) {
  const dir = path.dirname(String(filePath || ''));
  if (!dir) return;
  if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
}

function readJson(filePath) {
  try {
    if (!fs.existsSync(filePath)) return { version: 1, secrets: {} };
    const parsed = JSON.parse(fs.readFileSync(filePath, 'utf8'));
    const secrets = (parsed && typeof parsed.secrets === 'object' && parsed.secrets)
      ? parsed.secrets
      : {};
    return { version: 1, secrets };
  } catch (_) {
    return { version: 1, secrets: {} };
  }
}

function writeJson(filePath, state) {
  const next = {
    version: 1,
    secrets: (state && typeof state.secrets === 'object') ? state.secrets : {},
  };
  ensureDir(filePath);
  fs.writeFileSync(filePath, JSON.stringify(next, null, 2), 'utf8');
}

function createSecureSecretStore(options = {}) {
  const keychain = options.keychain;
  const safeStorage = options.safeStorage;
  const userDataPath = String(options.userDataPath || '').trim();
  const service = String(options.service || 'com.subgrapher.secrets').trim();
  const logger = options.logger || console;
  const filePath = path.join(userDataPath || process.cwd(), 'secure_secrets.json');

  function isMac() {
    return process.platform === 'darwin';
  }

  function canUseSafeStorage() {
    return !!(
      safeStorage
      && typeof safeStorage.isEncryptionAvailable === 'function'
      && safeStorage.isEncryptionAvailable()
      && typeof safeStorage.encryptString === 'function'
      && typeof safeStorage.decryptString === 'function'
    );
  }

  function upsertMeta(ref, patch = {}) {
    const cleanRef = String(ref || '').trim();
    if (!cleanRef) return;
    const state = readJson(filePath);
    const current = (state.secrets && state.secrets[cleanRef] && typeof state.secrets[cleanRef] === 'object')
      ? state.secrets[cleanRef]
      : { created_at: nowTs() };
    state.secrets[cleanRef] = {
      ...current,
      ...patch,
      ref: cleanRef,
      updated_at: nowTs(),
      created_at: Number(current.created_at || nowTs()),
    };
    writeJson(filePath, state);
  }

  function removeMeta(ref) {
    const cleanRef = String(ref || '').trim();
    if (!cleanRef) return;
    const state = readJson(filePath);
    if (Object.prototype.hasOwnProperty.call(state.secrets, cleanRef)) {
      delete state.secrets[cleanRef];
      writeJson(filePath, state);
    }
  }

  function getMeta(ref) {
    const cleanRef = String(ref || '').trim();
    if (!cleanRef) return null;
    const state = readJson(filePath);
    const entry = state.secrets[cleanRef];
    return (entry && typeof entry === 'object') ? entry : null;
  }

  function setSecret(ref, secret) {
    const cleanRef = String(ref || '').trim();
    const value = String(secret || '');
    if (!cleanRef || !value) {
      return { ok: false, message: 'ref and secret are required.' };
    }

    if (isMac() && keychain && typeof keychain.setSecret === 'function') {
      const setRes = keychain.setSecret(cleanRef, value, { service });
      if (setRes && setRes.ok) {
        upsertMeta(cleanRef, { backend: 'keychain' });
        return { ok: true, ref: cleanRef, backend: 'keychain' };
      }
      return {
        ok: false,
        message: String((setRes && setRes.message) || 'Unable to store keychain secret.'),
      };
    }

    if (!canUseSafeStorage()) {
      return { ok: false, message: 'Secure storage is unavailable on this platform.' };
    }

    try {
      const encrypted = safeStorage.encryptString(value);
      const encoded = Buffer.from(encrypted).toString('base64');
      upsertMeta(cleanRef, { backend: 'safe_storage', ciphertext_b64: encoded });
      return { ok: true, ref: cleanRef, backend: 'safe_storage' };
    } catch (err) {
      return { ok: false, message: String((err && err.message) || 'Unable to encrypt secret.') };
    }
  }

  function getSecret(ref) {
    const cleanRef = String(ref || '').trim();
    if (!cleanRef) return { ok: false, message: 'ref is required.' };

    const meta = getMeta(cleanRef);
    if (!meta || !meta.backend) {
      return { ok: false, message: 'Secret ref is not configured.' };
    }

    if (meta.backend === 'keychain') {
      if (!keychain || typeof keychain.getSecret !== 'function') {
        return { ok: false, message: 'Keychain module unavailable.' };
      }
      const res = keychain.getSecret(cleanRef, { service });
      if (!res || !res.ok || !res.secret) {
        return { ok: false, message: String((res && res.message) || 'Secret is not configured.') };
      }
      return { ok: true, secret: String(res.secret) };
    }

    if (meta.backend === 'safe_storage') {
      if (!canUseSafeStorage()) {
        return { ok: false, message: 'Secure storage is unavailable on this platform.' };
      }
      try {
        const encrypted = Buffer.from(String(meta.ciphertext_b64 || ''), 'base64');
        const secret = safeStorage.decryptString(encrypted);
        if (!secret) return { ok: false, message: 'Secret value is empty.' };
        return { ok: true, secret: String(secret) };
      } catch (err) {
        logger.warn('[secrets] decrypt failed:', String((err && err.message) || err));
        return { ok: false, message: 'Unable to decrypt secret.' };
      }
    }

    return { ok: false, message: 'Unsupported secret backend.' };
  }

  function clearSecret(ref) {
    const cleanRef = String(ref || '').trim();
    if (!cleanRef) return { ok: false, message: 'ref is required.' };

    const meta = getMeta(cleanRef);
    if (!meta) return { ok: true, missing: true };

    if (meta.backend === 'keychain' && keychain && typeof keychain.deleteSecret === 'function') {
      const res = keychain.deleteSecret(cleanRef, { service });
      if (!res || !res.ok) {
        return { ok: false, message: String((res && res.message) || 'Unable to clear keychain secret.') };
      }
    }

    removeMeta(cleanRef);
    return { ok: true };
  }

  function hasSecret(ref) {
    const cleanRef = String(ref || '').trim();
    if (!cleanRef) return false;
    const res = getSecret(cleanRef);
    return !!(res && res.ok && res.secret);
  }

  function createRef(prefix = 'sec') {
    return randomRef(prefix);
  }

  return {
    createRef,
    setSecret,
    getSecret,
    clearSecret,
    hasSecret,
    getMeta,
    filePath,
  };
}

module.exports = {
  createSecureSecretStore,
};
