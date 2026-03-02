const { execFileSync } = require('child_process');

const SERVICE_NAME = 'com.subgrapher.provider-keys';
const TRUSTCOMMONS_SERVICE = 'com.subgrapher.trustcommons';
const PROVIDER_KEY_SEP = '::';

function isMac() {
  return process.platform === 'darwin';
}

function runSecurity(args) {
  return execFileSync('security', args, { encoding: 'utf8', stdio: ['ignore', 'pipe', 'pipe'] });
}

function normalizeProvider(provider) {
  return String(provider || '').trim().toLowerCase();
}

function normalizeKeyId(keyId) {
  return String(keyId || '').trim().toLowerCase();
}

function providerAccount(provider, keyId = '') {
  const p = normalizeProvider(provider);
  const kid = normalizeKeyId(keyId);
  if (!p) return '';
  if (!kid) return p;
  return `${p}${PROVIDER_KEY_SEP}${kid}`;
}

function normalizeAccount(account) {
  return String(account || '').trim();
}

function normalizeService(serviceName, fallback = SERVICE_NAME) {
  const target = String(serviceName || '').trim();
  return target || fallback;
}

function isNotFoundError(err) {
  const stderr = String(err && err.stderr ? err.stderr : '').toLowerCase();
  const message = String(err && err.message ? err.message : '').toLowerCase();
  return stderr.includes('could not be found') || message.includes('could not be found');
}

function setProviderKey(provider, keyIdOrApiKey, apiKeyMaybe) {
  const p = normalizeProvider(provider);
  const hasLegacySignature = typeof apiKeyMaybe === 'undefined';
  const keyId = hasLegacySignature ? '' : normalizeKeyId(keyIdOrApiKey);
  const key = String(hasLegacySignature ? keyIdOrApiKey : apiKeyMaybe || '');
  if (!p || !key) {
    return { ok: false, message: 'provider and apiKey are required.' };
  }
  if (!isMac()) {
    return { ok: false, message: 'OS keychain storage is only implemented on macOS.' };
  }
  const account = providerAccount(p, keyId);
  if (!account) return { ok: false, message: 'provider is required.' };
  try {
    runSecurity(['add-generic-password', '-a', account, '-s', SERVICE_NAME, '-w', key, '-U']);
    return { ok: true, provider: p, key_id: keyId || '' };
  } catch (err) {
    return { ok: false, message: err.message || 'Failed to store provider key.' };
  }
}

function setLegacyProviderKey(provider, apiKey) {
  return setProviderKey(provider, '', apiKey);
}

function setSecret(account, secret, options = {}) {
  const a = normalizeAccount(account);
  const value = String(secret || '');
  const service = normalizeService(options.service, TRUSTCOMMONS_SERVICE);
  if (!a || !value) {
    return { ok: false, message: 'account and secret are required.' };
  }
  if (!isMac()) {
    return { ok: false, message: 'OS keychain storage is only implemented on macOS.' };
  }
  try {
    runSecurity(['add-generic-password', '-a', a, '-s', service, '-w', value, '-U']);
    return { ok: true };
  } catch (err) {
    return { ok: false, message: err.message || 'Failed to store keychain secret.' };
  }
}

function getSecret(account, options = {}) {
  const a = normalizeAccount(account);
  const service = normalizeService(options.service, TRUSTCOMMONS_SERVICE);
  if (!a) return { ok: false, message: 'account is required.' };
  if (!isMac()) {
    return { ok: false, message: 'OS keychain storage is only implemented on macOS.' };
  }
  try {
    const value = String(runSecurity(['find-generic-password', '-a', a, '-s', service, '-w']) || '').trim();
    if (!value) return { ok: false, message: 'Secret value is empty.' };
    return { ok: true, secret: value };
  } catch (err) {
    return { ok: false, message: err.message || 'Secret is not configured.' };
  }
}

function deleteSecret(account, options = {}) {
  const a = normalizeAccount(account);
  const service = normalizeService(options.service, TRUSTCOMMONS_SERVICE);
  if (!a) return { ok: false, message: 'account is required.' };
  if (!isMac()) {
    return { ok: false, message: 'OS keychain storage is only implemented on macOS.' };
  }
  try {
    runSecurity(['delete-generic-password', '-a', a, '-s', service]);
    return { ok: true };
  } catch (err) {
    if (isNotFoundError(err)) {
      return { ok: true, missing: true };
    }
    return { ok: false, message: err.message || 'Failed to delete keychain secret.' };
  }
}

function deleteProviderKey(provider, keyId = '') {
  const p = normalizeProvider(provider);
  const kid = normalizeKeyId(keyId);
  if (!p) return { ok: false, message: 'provider is required.' };
  if (!isMac()) {
    return { ok: false, message: 'OS keychain storage is only implemented on macOS.' };
  }
  const account = providerAccount(p, kid);
  try {
    runSecurity(['delete-generic-password', '-a', account, '-s', SERVICE_NAME]);
    return { ok: true, provider: p, key_id: kid || '' };
  } catch (err) {
    if (isNotFoundError(err)) {
      return { ok: true, missing: true, provider: p, key_id: kid || '' };
    }
    return { ok: false, message: err.message || 'Failed to delete provider key.' };
  }
}

function deleteLegacyProviderKey(provider) {
  return deleteProviderKey(provider, '');
}

function hasProviderKey(provider, keyId = '') {
  const p = normalizeProvider(provider);
  const kid = normalizeKeyId(keyId);
  if (!p || !isMac()) return false;
  const account = providerAccount(p, kid);
  try {
    runSecurity(['find-generic-password', '-a', account, '-s', SERVICE_NAME, '-w']);
    return true;
  } catch (_) {
    return false;
  }
}

function hasLegacyProviderKey(provider) {
  return hasProviderKey(provider, '');
}

function getProviderKey(provider, keyId = '') {
  const p = normalizeProvider(provider);
  const kid = normalizeKeyId(keyId);
  if (!p) return { ok: false, message: 'provider is required.' };
  if (!isMac()) {
    return { ok: false, message: 'OS keychain storage is only implemented on macOS.' };
  }
  const account = providerAccount(p, kid);
  try {
    const key = String(runSecurity(['find-generic-password', '-a', account, '-s', SERVICE_NAME, '-w']) || '').trim();
    if (!key) return { ok: false, message: 'Provider key is empty.' };
    return { ok: true, apiKey: key, provider: p, key_id: kid || '' };
  } catch (err) {
    return { ok: false, message: err.message || 'Provider key is not configured.' };
  }
}

function getLegacyProviderKey(provider) {
  return getProviderKey(provider, '');
}

function listConfiguredProviders(providers) {
  const list = Array.isArray(providers) ? providers : [];
  return list.map((provider) => {
    const id = normalizeProvider(provider);
    return {
      provider: id,
      configured: hasLegacyProviderKey(id),
    };
  });
}

module.exports = {
  PROVIDER_KEY_SEP,
  providerAccount,
  setProviderKey,
  setLegacyProviderKey,
  deleteProviderKey,
  deleteLegacyProviderKey,
  hasProviderKey,
  hasLegacyProviderKey,
  getProviderKey,
  getLegacyProviderKey,
  listConfiguredProviders,
  setSecret,
  getSecret,
  deleteSecret,
  TRUSTCOMMONS_SERVICE,
};
