const crypto = require('crypto');
const http = require('http');
const https = require('https');
const { URL, URLSearchParams } = require('url');

const GOOGLE_AUTH_URL = 'https://accounts.google.com/o/oauth2/v2/auth';
const GOOGLE_TOKEN_URL = 'https://oauth2.googleapis.com/token';
const GOOGLE_USERINFO_URL = 'https://www.googleapis.com/oauth2/v2/userinfo';
const GOOGLE_SCOPES = [
  'https://mail.google.com/',
  'openid',
  'email',
  'profile',
];

function randomUrlSafe(size = 48) {
  return crypto.randomBytes(size).toString('base64url');
}

function sha256Base64Url(value = '') {
  return crypto.createHash('sha256').update(String(value || ''), 'utf8').digest('base64url');
}

function requestJson(urlString, options = {}, body = '') {
  const url = new URL(urlString);
  const transport = url.protocol === 'https:' ? https : http;
  return new Promise((resolve, reject) => {
    const req = transport.request({
      protocol: url.protocol,
      hostname: url.hostname,
      port: url.port || (url.protocol === 'https:' ? 443 : 80),
      path: `${url.pathname}${url.search}`,
      method: options.method || 'GET',
      headers: options.headers || {},
    }, (res) => {
      const chunks = [];
      res.on('data', (chunk) => chunks.push(Buffer.from(chunk)));
      res.on('end', () => {
        const text = Buffer.concat(chunks).toString('utf8');
        let parsed = null;
        try {
          parsed = text ? JSON.parse(text) : {};
        } catch (_) {
          parsed = null;
        }
        if (res.statusCode >= 400) {
          reject(new Error(
            (parsed && (parsed.error_description || parsed.error)) || text || `HTTP ${res.statusCode}`
          ));
          return;
        }
        resolve(parsed || {});
      });
    });
    req.on('error', reject);
    if (body) req.write(body);
    req.end();
  });
}

async function exchangeGoogleAuthorizationCode({ clientId, clientSecret, code, codeVerifier, redirectUri }) {
  const form = new URLSearchParams({
    client_id: String(clientId || ''),
    client_secret: String(clientSecret || ''),
    code: String(code || ''),
    code_verifier: String(codeVerifier || ''),
    grant_type: 'authorization_code',
    redirect_uri: String(redirectUri || ''),
  });
  return requestJson(GOOGLE_TOKEN_URL, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/x-www-form-urlencoded',
      'Content-Length': Buffer.byteLength(form.toString()),
    },
  }, form.toString());
}

async function refreshGoogleAccessToken({ clientId, clientSecret, refreshToken }) {
  const form = new URLSearchParams({
    client_id: String(clientId || ''),
    client_secret: String(clientSecret || ''),
    refresh_token: String(refreshToken || ''),
    grant_type: 'refresh_token',
  });
  return requestJson(GOOGLE_TOKEN_URL, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/x-www-form-urlencoded',
      'Content-Length': Buffer.byteLength(form.toString()),
    },
  }, form.toString());
}

async function fetchGoogleUserInfo(accessToken = '') {
  return requestJson(GOOGLE_USERINFO_URL, {
    headers: {
      Authorization: `Bearer ${String(accessToken || '')}`,
    },
  });
}

async function startGoogleMailOAuthFlow({ clientId, clientSecret, openUrl }) {
  if (!clientId || !clientSecret) {
    throw new Error('Google client id and client secret are required.');
  }
  const state = randomUrlSafe(24);
  const codeVerifier = randomUrlSafe(48);
  const codeChallenge = sha256Base64Url(codeVerifier);
  const redirectServer = http.createServer();
  const authResult = await new Promise((resolve, reject) => {
    let settled = false;
    const finish = (value, error = null) => {
      if (settled) return;
      settled = true;
      try {
        redirectServer.close();
      } catch (_) {
        // ignore
      }
      if (error) reject(error);
      else resolve(value);
    };
    redirectServer.on('request', (req, res) => {
      try {
        const target = new URL(req.url, 'http://127.0.0.1');
        const code = String(target.searchParams.get('code') || '').trim();
        const returnedState = String(target.searchParams.get('state') || '').trim();
        const error = String(target.searchParams.get('error') || '').trim();
        res.statusCode = error ? 400 : 200;
        res.setHeader('Content-Type', 'text/html; charset=utf-8');
        res.end(error
          ? '<html><body><p>Google sign-in failed. You can close this window.</p></body></html>'
          : '<html><body><p>Google sign-in complete. You can close this window and return to Subgrapher.</p></body></html>');
        if (error) {
          finish(null, new Error(error));
          return;
        }
        if (!code || returnedState !== state) {
          finish(null, new Error('Google OAuth callback was invalid.'));
          return;
        }
        finish({ code });
      } catch (err) {
        finish(null, err);
      }
    });
    redirectServer.listen(0, '127.0.0.1', async () => {
      const address = redirectServer.address();
      const port = address && typeof address === 'object' ? address.port : 0;
      const redirectUri = `http://127.0.0.1:${port}/oauth/google/mail/callback`;
      const authUrl = new URL(GOOGLE_AUTH_URL);
      authUrl.search = new URLSearchParams({
        client_id: String(clientId || ''),
        redirect_uri: redirectUri,
        response_type: 'code',
        scope: GOOGLE_SCOPES.join(' '),
        access_type: 'offline',
        prompt: 'consent',
        state,
        code_challenge: codeChallenge,
        code_challenge_method: 'S256',
      }).toString();
      try {
        await openUrl(authUrl.toString());
      } catch (err) {
        finish(null, err);
      }
      const timeout = setTimeout(() => {
        finish(null, new Error('Google OAuth timed out.'));
      }, 180000);
      redirectServer.on('close', () => clearTimeout(timeout));
      redirectServer.__redirectUri = redirectUri;
    });
    redirectServer.on('error', reject);
  });

  const redirectUri = redirectServer.__redirectUri;
  const tokenSet = await exchangeGoogleAuthorizationCode({
    clientId,
    clientSecret,
    code: authResult.code,
    codeVerifier,
    redirectUri,
  });
  const userInfo = await fetchGoogleUserInfo(String(tokenSet.access_token || ''));
  return {
    access_token: String(tokenSet.access_token || ''),
    refresh_token: String(tokenSet.refresh_token || ''),
    expires_in: Number(tokenSet.expires_in || 0),
    token_type: String(tokenSet.token_type || ''),
    email: String(userInfo.email || '').trim(),
    name: String(userInfo.name || userInfo.email || '').trim(),
  };
}

module.exports = {
  fetchGoogleUserInfo,
  refreshGoogleAccessToken,
  startGoogleMailOAuthFlow,
};
