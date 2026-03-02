const crypto = require('crypto');

const TRUSTCOMMONS_TOKEN_ACCOUNT = 'identity_token';
const TRUSTCOMMONS_REFRESH_ACCOUNT = 'refresh_token';

function nowTs() {
  return Date.now();
}

function normalizeLabel(value) {
  return String(value || '')
    .trim()
    .replace(/[^a-zA-Z0-9_-]+/g, '-')
    .replace(/^-+|-+$/g, '')
    .slice(0, 40);
}

function buildDisplayName(seed = '') {
  const base = normalizeLabel(seed) || 'subgrapher';
  const short = crypto.randomUUID().slice(0, 8);
  return `${base}-${short}`;
}

function makeIdentity(appName = 'subgrapher') {
  const identityId = `tcid_${crypto.randomUUID()}`;
  const displayName = buildDisplayName(appName);
  const token = crypto.randomBytes(32).toString('base64url');
  const refreshToken = crypto.randomBytes(32).toString('base64url');
  return {
    identity_id: identityId,
    display_name: displayName,
    token,
    refresh_token: refreshToken,
    created_at: nowTs(),
  };
}

function keychainServiceName() {
  return 'com.subgrapher.trustcommons';
}

function bootstrapTrustCommonsIdentity(keychain, settings = {}, options = {}) {
  const currentId = String((settings && settings.trustcommons_identity_id) || '').trim();
  const complete = !!(settings && settings.trustcommons_bootstrap_complete);
  const appLabel = String(options.appLabel || 'subgrapher').trim();

  if (complete && currentId) {
    const tokenRes = keychain.getSecret(currentId, { service: keychainServiceName() });
    const refreshRes = keychain.getSecret(`${currentId}:${TRUSTCOMMONS_REFRESH_ACCOUNT}`, { service: keychainServiceName() });
    return {
      ok: true,
      bootstrap_complete: true,
      created: false,
      identity: {
        identity_id: currentId,
        display_name: String((settings && settings.trustcommons_display_name) || '').trim() || currentId,
        token_available: !!(tokenRes && tokenRes.ok && tokenRes.secret),
        refresh_available: !!(refreshRes && refreshRes.ok && refreshRes.secret),
      },
      settings_patch: {},
    };
  }

  const identity = makeIdentity(appLabel);
  const tokenSave = keychain.setSecret(identity.identity_id, identity.token, { service: keychainServiceName() });
  if (!tokenSave || !tokenSave.ok) {
    return { ok: false, message: (tokenSave && tokenSave.message) || 'Unable to store Trust Commons token.' };
  }

  const refreshSave = keychain.setSecret(
    `${identity.identity_id}:${TRUSTCOMMONS_REFRESH_ACCOUNT}`,
    identity.refresh_token,
    { service: keychainServiceName() }
  );
  if (!refreshSave || !refreshSave.ok) {
    return { ok: false, message: (refreshSave && refreshSave.message) || 'Unable to store Trust Commons refresh token.' };
  }

  return {
    ok: true,
    bootstrap_complete: true,
    created: true,
    identity: {
      identity_id: identity.identity_id,
      display_name: identity.display_name,
      token_available: true,
      refresh_available: true,
    },
    settings_patch: {
      trustcommons_bootstrap_complete: true,
      trustcommons_identity_id: identity.identity_id,
      trustcommons_display_name: identity.display_name,
      trustcommons_bootstrap_at: identity.created_at,
    },
  };
}

function loadTrustCommonsIdentity(keychain, settings = {}) {
  const identityId = String((settings && settings.trustcommons_identity_id) || '').trim();
  if (!identityId) {
    return { ok: false, message: 'Trust Commons identity is not initialized.' };
  }

  const tokenRes = keychain.getSecret(identityId, { service: keychainServiceName() });
  if (!tokenRes || !tokenRes.ok || !tokenRes.secret) {
    return { ok: false, message: (tokenRes && tokenRes.message) || 'Trust Commons identity token is missing.' };
  }

  const refreshRes = keychain.getSecret(`${identityId}:${TRUSTCOMMONS_REFRESH_ACCOUNT}`, { service: keychainServiceName() });
  const refreshToken = (refreshRes && refreshRes.ok && refreshRes.secret) ? refreshRes.secret : '';

  return {
    ok: true,
    identity: {
      identity_id: identityId,
      display_name: String((settings && settings.trustcommons_display_name) || '').trim() || identityId,
      token: tokenRes.secret,
      refresh_token: refreshToken,
    },
  };
}

module.exports = {
  bootstrapTrustCommonsIdentity,
  loadTrustCommonsIdentity,
  keychainServiceName,
};
