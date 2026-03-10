const fs = require('fs');
const path = require('path');
const crypto = require('crypto');

const { ImapClient } = require('./mail_imap');
const { computeMailConversationKey, normalizeWhitespace, parseRawEmailText } = require('./mail_parser');
const { buildRawMessage, sendMailViaSmtp } = require('./mail_smtp');

const SCHEMA_VERSION = 4;
const MAILBOX_ROLES = ['inbox', 'sent', 'drafts', 'archive', 'trash', 'junk'];
let sqlModulePromise = null;
const dbTaskByPath = new Map();

function nowTs() {
  return Date.now();
}

function makeId(prefix = 'mail') {
  return `${prefix}_${nowTs()}_${Math.random().toString(36).slice(2, 10)}`;
}

function ensureDirForFile(filePath) {
  const dir = path.dirname(String(filePath || ''));
  if (!dir) return;
  if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
}

async function getSqlModule() {
  if (!sqlModulePromise) {
    const initSqlJs = require('sql.js');
    const wasmPath = require.resolve('sql.js/dist/sql-wasm.wasm');
    const wasmDir = path.dirname(wasmPath);
    sqlModulePromise = initSqlJs({
      locateFile: (fileName) => path.join(wasmDir, fileName),
    });
  }
  return sqlModulePromise;
}

async function openDatabase(dbPath) {
  const SQL = await getSqlModule();
  if (fs.existsSync(dbPath)) {
    const bytes = fs.readFileSync(dbPath);
    return new SQL.Database(bytes);
  }
  return new SQL.Database();
}

function saveDatabase(db, dbPath) {
  ensureDirForFile(dbPath);
  fs.writeFileSync(dbPath, Buffer.from(db.export()));
}

function withDbLock(dbPath, taskFn) {
  const key = String(dbPath || '').trim();
  const prior = dbTaskByPath.get(key) || Promise.resolve();
  const next = prior.catch(() => {}).then(taskFn).finally(() => {
    if (dbTaskByPath.get(key) === next) dbTaskByPath.delete(key);
  });
  dbTaskByPath.set(key, next);
  return next;
}

function upsertMeta(db, key, value) {
  const stmt = db.prepare('INSERT INTO meta (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value');
  stmt.bind([String(key || ''), String(value || '')]);
  stmt.step();
  stmt.free();
}

function selectRows(db, sql, bind = []) {
  const stmt = db.prepare(sql);
  stmt.bind(bind);
  const rows = [];
  while (stmt.step()) rows.push(stmt.getAsObject());
  stmt.free();
  return rows;
}

function ensureColumn(db, table, columnName, columnSql) {
  const columns = new Set(selectRows(db, `PRAGMA table_info(${table})`).map((row) => String(row.name || '').trim()));
  if (columns.has(columnName)) return;
  db.run(`ALTER TABLE ${table} ADD COLUMN ${columnSql}`);
}

function tableExists(db, table) {
  const rows = selectRows(db, "SELECT name FROM sqlite_master WHERE type = 'table' AND name = ? LIMIT 1", [String(table || '').trim()]);
  return rows.length > 0;
}

function readSchemaVersion(db) {
  if (!tableExists(db, 'meta')) return 0;
  const rows = selectRows(db, "SELECT value FROM meta WHERE key = 'schema_version' LIMIT 1");
  const raw = Number((rows[0] && rows[0].value) || 0);
  return Number.isFinite(raw) ? Math.max(0, Math.round(raw)) : 0;
}

function getPrimaryKeyColumns(db, table) {
  return selectRows(db, `PRAGMA table_info(${table})`)
    .filter((row) => Number(row.pk || 0) > 0)
    .sort((a, b) => Number(a.pk || 0) - Number(b.pk || 0))
    .map((row) => String(row.name || '').trim());
}

function shouldResetDatabase(db) {
  const schemaVersion = readSchemaVersion(db);
  if (schemaVersion > 0 && schemaVersion < SCHEMA_VERSION) return true;
  if (tableExists(db, 'messages')) {
    const primaryKey = getPrimaryKeyColumns(db, 'messages');
    const expected = ['account_id', 'uid', 'mailbox'];
    if (primaryKey.length && (primaryKey.length !== expected.length || expected.some((name, index) => primaryKey[index] !== name))) {
      return true;
    }
  }
  return false;
}

function createMessagesTable(db, tableName = 'messages') {
  db.run(`
    CREATE TABLE IF NOT EXISTS ${tableName} (
      account_id TEXT NOT NULL,
      uid INTEGER NOT NULL,
      thread_id TEXT NOT NULL,
      thread_key TEXT NOT NULL,
      mailbox TEXT NOT NULL,
      mailbox_id TEXT NOT NULL DEFAULT '',
      mailbox_role TEXT NOT NULL DEFAULT '',
      message_id_header TEXT NOT NULL,
      in_reply_to TEXT NOT NULL,
      subject TEXT NOT NULL,
      sender TEXT NOT NULL,
      recipients TEXT NOT NULL,
      sent_at TEXT NOT NULL,
      sent_ts INTEGER NOT NULL DEFAULT 0,
      snippet TEXT NOT NULL,
      body_text TEXT NOT NULL,
      body_html TEXT NOT NULL DEFAULT '',
      raw_source TEXT NOT NULL,
      attachment_count INTEGER NOT NULL DEFAULT 0,
      searchable_text TEXT NOT NULL,
      flags_json TEXT NOT NULL DEFAULT '[]',
      is_unread INTEGER NOT NULL DEFAULT 1,
      is_flagged INTEGER NOT NULL DEFAULT 0,
      is_draft INTEGER NOT NULL DEFAULT 0,
      is_trashed INTEGER NOT NULL DEFAULT 0,
      is_archived INTEGER NOT NULL DEFAULT 0,
      draft_key TEXT NOT NULL DEFAULT '',
      created_at INTEGER NOT NULL,
      updated_at INTEGER NOT NULL,
      PRIMARY KEY (account_id, uid, mailbox),
      FOREIGN KEY (account_id) REFERENCES accounts(id) ON DELETE CASCADE
    );
  `);
}

function recreateMessagesTableIfNeeded(db) {
  const primaryKey = getPrimaryKeyColumns(db, 'messages');
  const expected = ['account_id', 'uid', 'mailbox'];
  if (primaryKey.length === expected.length && expected.every((name, index) => primaryKey[index] === name)) return;

  db.run('DROP INDEX IF EXISTS idx_messages_thread');
  db.run('DROP INDEX IF EXISTS idx_messages_account');
  db.run('DROP INDEX IF EXISTS idx_messages_sent');
  db.run('DROP INDEX IF EXISTS idx_messages_mailbox');
  db.run('DROP INDEX IF EXISTS idx_messages_account_uid_mailbox_unique');

  db.run('ALTER TABLE messages RENAME TO messages_legacy');
  createMessagesTable(db, 'messages');
  db.run(`
    INSERT INTO messages (
      account_id, uid, thread_id, thread_key, mailbox, mailbox_id, mailbox_role, message_id_header,
      in_reply_to, subject, sender, recipients, sent_at, sent_ts, snippet, body_text, body_html, raw_source,
      attachment_count, searchable_text, flags_json, is_unread, is_flagged, is_draft, is_trashed, is_archived,
      draft_key, created_at, updated_at
    )
    SELECT
      account_id, uid, thread_id, thread_key, mailbox, mailbox_id, mailbox_role, message_id_header,
      in_reply_to, subject, sender, recipients, sent_at, sent_ts, snippet, body_text, body_html, raw_source,
      attachment_count, searchable_text, flags_json, is_unread, is_flagged, is_draft, is_trashed, is_archived,
      draft_key, created_at, updated_at
    FROM messages_legacy
  `);
  db.run('DROP TABLE messages_legacy');
}

function ensureSchema(db) {
  db.run('PRAGMA foreign_keys = ON');
  db.run('CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT)');
  db.run(`
    CREATE TABLE IF NOT EXISTS accounts (
      id TEXT PRIMARY KEY,
      label TEXT NOT NULL,
      email TEXT NOT NULL,
      host TEXT NOT NULL,
      port INTEGER NOT NULL,
      username TEXT NOT NULL,
      mailbox TEXT NOT NULL,
      use_tls INTEGER NOT NULL DEFAULT 1,
      password_ref TEXT NOT NULL DEFAULT '',
      smtp_host TEXT NOT NULL DEFAULT '',
      smtp_port INTEGER NOT NULL DEFAULT 465,
      smtp_username TEXT NOT NULL DEFAULT '',
      smtp_password_ref TEXT NOT NULL DEFAULT '',
      smtp_use_tls INTEGER NOT NULL DEFAULT 1,
      smtp_starttls INTEGER NOT NULL DEFAULT 0,
      send_enabled INTEGER NOT NULL DEFAULT 1,
      sync_enabled INTEGER NOT NULL DEFAULT 1,
      sync_limit INTEGER NOT NULL DEFAULT 200,
      last_sync_at INTEGER NOT NULL DEFAULT 0,
      last_error TEXT NOT NULL DEFAULT '',
      created_at INTEGER NOT NULL,
      updated_at INTEGER NOT NULL,
      account_type TEXT NOT NULL DEFAULT 'manual_imap_smtp',
      provider TEXT NOT NULL DEFAULT 'generic',
      oauth_access_token_ref TEXT NOT NULL DEFAULT '',
      oauth_refresh_token_ref TEXT NOT NULL DEFAULT '',
      oauth_client_id_ref TEXT NOT NULL DEFAULT '',
      oauth_client_secret_ref TEXT NOT NULL DEFAULT '',
      oauth_token_expires_at INTEGER NOT NULL DEFAULT 0,
      capabilities_json TEXT NOT NULL DEFAULT '{}'
    );
  `);
  db.run(`
    CREATE TABLE IF NOT EXISTS mailboxes (
      id TEXT PRIMARY KEY,
      account_id TEXT NOT NULL,
      name TEXT NOT NULL,
      path TEXT NOT NULL,
      delimiter TEXT NOT NULL DEFAULT '',
      special_use TEXT NOT NULL DEFAULT '',
      sync_enabled INTEGER NOT NULL DEFAULT 1,
      message_count INTEGER NOT NULL DEFAULT 0,
      last_sync_at INTEGER NOT NULL DEFAULT 0,
      created_at INTEGER NOT NULL,
      updated_at INTEGER NOT NULL,
      UNIQUE (account_id, path),
      FOREIGN KEY (account_id) REFERENCES accounts(id) ON DELETE CASCADE
    );
  `);
  createMessagesTable(db);
  db.run('CREATE INDEX IF NOT EXISTS idx_messages_thread ON messages(thread_id)');
  db.run('CREATE INDEX IF NOT EXISTS idx_messages_account ON messages(account_id)');
  db.run('CREATE INDEX IF NOT EXISTS idx_messages_sent ON messages(sent_ts DESC)');
  db.run('CREATE INDEX IF NOT EXISTS idx_messages_mailbox ON messages(account_id, mailbox)');
  ensureColumn(db, 'accounts', 'account_type', "account_type TEXT NOT NULL DEFAULT 'manual_imap_smtp'");
  ensureColumn(db, 'accounts', 'provider', "provider TEXT NOT NULL DEFAULT 'generic'");
  ensureColumn(db, 'accounts', 'smtp_host', "smtp_host TEXT NOT NULL DEFAULT ''");
  ensureColumn(db, 'accounts', 'smtp_port', 'smtp_port INTEGER NOT NULL DEFAULT 465');
  ensureColumn(db, 'accounts', 'smtp_username', "smtp_username TEXT NOT NULL DEFAULT ''");
  ensureColumn(db, 'accounts', 'smtp_password_ref', "smtp_password_ref TEXT NOT NULL DEFAULT ''");
  ensureColumn(db, 'accounts', 'smtp_use_tls', 'smtp_use_tls INTEGER NOT NULL DEFAULT 1');
  ensureColumn(db, 'accounts', 'smtp_starttls', 'smtp_starttls INTEGER NOT NULL DEFAULT 0');
  ensureColumn(db, 'accounts', 'send_enabled', 'send_enabled INTEGER NOT NULL DEFAULT 1');
  ensureColumn(db, 'accounts', 'oauth_access_token_ref', "oauth_access_token_ref TEXT NOT NULL DEFAULT ''");
  ensureColumn(db, 'accounts', 'oauth_refresh_token_ref', "oauth_refresh_token_ref TEXT NOT NULL DEFAULT ''");
  ensureColumn(db, 'accounts', 'oauth_client_id_ref', "oauth_client_id_ref TEXT NOT NULL DEFAULT ''");
  ensureColumn(db, 'accounts', 'oauth_client_secret_ref', "oauth_client_secret_ref TEXT NOT NULL DEFAULT ''");
  ensureColumn(db, 'accounts', 'oauth_token_expires_at', 'oauth_token_expires_at INTEGER NOT NULL DEFAULT 0');
  ensureColumn(db, 'accounts', 'capabilities_json', "capabilities_json TEXT NOT NULL DEFAULT '{}'");
  ensureColumn(db, 'messages', 'mailbox_id', "mailbox_id TEXT NOT NULL DEFAULT ''");
  ensureColumn(db, 'messages', 'mailbox_role', "mailbox_role TEXT NOT NULL DEFAULT ''");
  ensureColumn(db, 'messages', 'body_html', "body_html TEXT NOT NULL DEFAULT ''");
  ensureColumn(db, 'messages', 'flags_json', "flags_json TEXT NOT NULL DEFAULT '[]'");
  ensureColumn(db, 'messages', 'is_unread', 'is_unread INTEGER NOT NULL DEFAULT 1');
  ensureColumn(db, 'messages', 'is_flagged', 'is_flagged INTEGER NOT NULL DEFAULT 0');
  ensureColumn(db, 'messages', 'is_draft', 'is_draft INTEGER NOT NULL DEFAULT 0');
  ensureColumn(db, 'messages', 'is_trashed', 'is_trashed INTEGER NOT NULL DEFAULT 0');
  ensureColumn(db, 'messages', 'is_archived', 'is_archived INTEGER NOT NULL DEFAULT 0');
  ensureColumn(db, 'messages', 'draft_key', "draft_key TEXT NOT NULL DEFAULT ''");
  recreateMessagesTableIfNeeded(db);
  db.run('CREATE UNIQUE INDEX IF NOT EXISTS idx_mailboxes_account_path_unique ON mailboxes(account_id, path)');
  upsertMeta(db, 'schema_version', String(SCHEMA_VERSION));
}

function parseDateTs(value = '') {
  const ts = Date.parse(String(value || ''));
  return Number.isFinite(ts) ? ts : 0;
}

function normalizeJsonObject(value, fallback = {}) {
  try {
    const parsed = JSON.parse(String(value || '{}'));
    return parsed && typeof parsed === 'object' ? parsed : fallback;
  } catch (_) {
    return fallback;
  }
}

function normalizeJsonArray(value, fallback = []) {
  try {
    const parsed = JSON.parse(String(value || '[]'));
    return Array.isArray(parsed) ? parsed : fallback;
  } catch (_) {
    return fallback;
  }
}

function inferProviderFromHost(value = '') {
  const host = String(value || '').trim().toLowerCase();
  if (host.includes('gmail.com') || host.includes('googlemail.com')) return 'gmail';
  if (host.includes('google')) return 'google_workspace';
  return 'generic';
}

function defaultCapabilities(provider = '') {
  const normalized = String(provider || '').trim().toLowerCase();
  if (normalized === 'gmail' || normalized === 'google_workspace') {
    return {
      auth_mode: normalized === 'gmail' || normalized === 'google_workspace' ? 'xoauth2_or_password' : 'password',
      supports_archive: true,
      supports_trash: true,
      supports_delete: true,
      supports_drafts: true,
      sent_folder: '',
      drafts_folder: '',
      archive_folder: '',
      trash_folder: '',
    };
  }
  return {
    auth_mode: 'password',
    supports_archive: false,
    supports_trash: true,
    supports_delete: true,
    supports_drafts: true,
    sent_folder: '',
    drafts_folder: '',
    archive_folder: '',
    trash_folder: '',
  };
}

function normalizeAccountRecord(row = {}) {
  const provider = String(row.provider || inferProviderFromHost(row.host) || 'generic').trim();
  const capabilities = {
    ...defaultCapabilities(provider),
    ...normalizeJsonObject(row.capabilities_json, {}),
  };
  return {
    id: String(row.id || '').trim(),
    label: String(row.label || '').trim(),
    email: String(row.email || '').trim(),
    host: String(row.host || '').trim(),
    port: Math.max(1, Math.round(Number(row.port || 993))),
    username: String(row.username || '').trim(),
    mailbox: String(row.mailbox || 'INBOX').trim() || 'INBOX',
    use_tls: !Object.prototype.hasOwnProperty.call(row, 'use_tls') || !!Number(row.use_tls || 0),
    password_ref: String(row.password_ref || '').trim(),
    smtp_host: String(row.smtp_host || '').trim(),
    smtp_port: Math.max(1, Math.round(Number(row.smtp_port || 465))),
    smtp_username: String(row.smtp_username || '').trim(),
    smtp_password_ref: String(row.smtp_password_ref || '').trim(),
    smtp_use_tls: !Object.prototype.hasOwnProperty.call(row, 'smtp_use_tls') || !!Number(row.smtp_use_tls || 0),
    smtp_starttls: !!Number(row.smtp_starttls || 0),
    send_enabled: !Object.prototype.hasOwnProperty.call(row, 'send_enabled') || !!Number(row.send_enabled || 0),
    sync_enabled: !Object.prototype.hasOwnProperty.call(row, 'sync_enabled') || !!Number(row.sync_enabled || 0),
    sync_limit: Math.max(1, Math.min(500, Math.round(Number(row.sync_limit || 200)))),
    last_sync_at: Number(row.last_sync_at || 0),
    last_error: String(row.last_error || '').trim(),
    created_at: Number(row.created_at || 0),
    updated_at: Number(row.updated_at || 0),
    account_type: String(row.account_type || 'manual_imap_smtp').trim() || 'manual_imap_smtp',
    provider,
    oauth_access_token_ref: String(row.oauth_access_token_ref || '').trim(),
    oauth_refresh_token_ref: String(row.oauth_refresh_token_ref || '').trim(),
    oauth_client_id_ref: String(row.oauth_client_id_ref || '').trim(),
    oauth_client_secret_ref: String(row.oauth_client_secret_ref || '').trim(),
    oauth_token_expires_at: Number(row.oauth_token_expires_at || 0),
    capabilities,
  };
}

function normalizeMailboxRecord(row = {}) {
  return {
    id: String(row.id || '').trim(),
    account_id: String(row.account_id || '').trim(),
    name: String(row.name || '').trim(),
    path: String(row.path || '').trim(),
    delimiter: String(row.delimiter || '').trim(),
    special_use: String(row.special_use || '').trim(),
    sync_enabled: !Object.prototype.hasOwnProperty.call(row, 'sync_enabled') || !!Number(row.sync_enabled || 0),
    message_count: Math.max(0, Number(row.message_count || 0)),
    last_sync_at: Number(row.last_sync_at || 0),
    created_at: Number(row.created_at || 0),
    updated_at: Number(row.updated_at || 0),
  };
}

function threadIdForAccount(accountId = '', threadKey = '') {
  return crypto.createHash('sha1').update(`${String(accountId || '')}:${String(threadKey || '')}`).digest('hex');
}

function parseRecipients(value = '') {
  try {
    const parsed = JSON.parse(String(value || '{}'));
    return {
      to: Array.isArray(parsed.to) ? parsed.to : [],
      cc: Array.isArray(parsed.cc) ? parsed.cc : [],
      bcc: Array.isArray(parsed.bcc) ? parsed.bcc : [],
    };
  } catch (_) {
    return { to: [], cc: [], bcc: [] };
  }
}

function normalizeComposeRecipients(input = {}) {
  const toList = Array.isArray(input.to) ? input.to : String(input.to || '').split(',');
  const ccList = Array.isArray(input.cc) ? input.cc : String(input.cc || '').split(',');
  const bccList = Array.isArray(input.bcc) ? input.bcc : String(input.bcc || '').split(',');
  const normalize = (items) => items.map((item) => String(item || '').trim()).filter(Boolean);
  return {
    to: normalize(toList),
    cc: normalize(ccList),
    bcc: normalize(bccList),
  };
}

function normalizeFlags(flags = []) {
  return Array.isArray(flags)
    ? flags.map((item) => String(item || '').trim()).filter(Boolean)
    : [];
}

function inferMailboxRole(mailbox = '', specialUse = '') {
  const explicit = String(specialUse || '').trim().toLowerCase();
  if (MAILBOX_ROLES.includes(explicit)) return explicit;
  const name = String(mailbox || '').trim().toLowerCase();
  if (name === 'inbox') return 'inbox';
  if (name.includes('draft')) return 'drafts';
  if (name.includes('trash') || name.includes('bin')) return 'trash';
  if (name.includes('sent')) return 'sent';
  if (name.includes('archive') || name.includes('all mail')) return 'archive';
  if (name.includes('spam') || name.includes('junk')) return 'junk';
  return '';
}

function flagsToState(flags = [], mailboxRole = '') {
  const lowerFlags = normalizeFlags(flags).map((item) => item.toLowerCase());
  return {
    flags_json: JSON.stringify(normalizeFlags(flags)),
    is_unread: lowerFlags.includes('\\seen') ? 0 : 1,
    is_flagged: lowerFlags.includes('\\flagged') ? 1 : 0,
    is_draft: lowerFlags.includes('\\draft') || mailboxRole === 'drafts' ? 1 : 0,
    is_trashed: lowerFlags.includes('\\deleted') || mailboxRole === 'trash' ? 1 : 0,
    is_archived: mailboxRole === 'archive' ? 1 : 0,
  };
}

function buildSearchableText(parsed = {}) {
  return normalizeWhitespace([
    parsed.subject,
    parsed.from,
    ...(Array.isArray(parsed.to) ? parsed.to : []),
    ...(Array.isArray(parsed.cc) ? parsed.cc : []),
    parsed.snippet,
    parsed.body_text,
  ].join(' ')).toLowerCase();
}

function normalizeConversationId(value = '') {
  return normalizeWhitespace(String(value || '').replace(/[<>]/g, '')).toLowerCase();
}

function resolveThreadKeyFromDatabase(db, account = {}, parsed = {}, rawSource = '') {
  const fallback = String(
    computeMailConversationKey(parsed, String(account.email || '').trim().toLowerCase())
  ).trim() || crypto.createHash('sha1').update(rawSource).digest('hex');
  if (!db) return fallback;
  const accountId = String(account.id || '').trim();
  if (!accountId) return fallback;

  const references = Array.isArray(parsed.references)
    ? parsed.references.map((item) => normalizeConversationId(item)).filter(Boolean)
    : [];
  const inReplyTo = normalizeConversationId(parsed.in_reply_to);
  const messageId = normalizeConversationId(parsed.message_id_header);
  const candidateIds = Array.from(new Set([
    ...references,
    inReplyTo,
    messageId,
  ].filter(Boolean)));

  for (const candidateId of candidateIds) {
    const rows = selectRows(
      db,
      'SELECT thread_key FROM messages WHERE account_id = ? AND LOWER(message_id_header) = ? LIMIT 1',
      [accountId, candidateId]
    );
    const threadKey = String((rows[0] && rows[0].thread_key) || '').trim();
    if (threadKey) return threadKey;
  }

  if (messageId) {
    const childRows = selectRows(
      db,
      'SELECT thread_key FROM messages WHERE account_id = ? AND LOWER(in_reply_to) = ? LIMIT 1',
      [accountId, messageId]
    );
    const childThreadKey = String((childRows[0] && childRows[0].thread_key) || '').trim();
    if (childThreadKey) return childThreadKey;
  }

  return fallback;
}

function buildParsedMessage(account = {}, mailbox = {}, uid = 0, rawSource = '', flags = [], db = null) {
  const parsed = parseRawEmailText(rawSource, `imap://${account.id}/${uid}`);
  const threadKey = resolveThreadKeyFromDatabase(db, account, parsed, rawSource);
  const mailboxName = String((mailbox && (mailbox.path || mailbox.name)) || account.mailbox || 'INBOX').trim() || 'INBOX';
  const mailboxRole = inferMailboxRole(mailboxName, mailbox.special_use);
  const sentTs = parseDateTs(parsed.sent_at);
  return {
    account_id: String(account.id || '').trim(),
    uid: Math.round(Number(uid || 0)),
    thread_key: threadKey,
    thread_id: threadIdForAccount(account.id, threadKey),
    mailbox: mailboxName,
    mailbox_id: String((mailbox && mailbox.id) || '').trim(),
    mailbox_role: mailboxRole,
    message_id_header: String(parsed.message_id_header || '').trim(),
    in_reply_to: String(parsed.in_reply_to || '').trim(),
    subject: String(parsed.subject || '').trim(),
    sender: String(parsed.from || '').trim(),
    recipients: JSON.stringify({
      to: Array.isArray(parsed.to) ? parsed.to : [],
      cc: Array.isArray(parsed.cc) ? parsed.cc : [],
      bcc: Array.isArray(parsed.bcc) ? parsed.bcc : [],
    }),
    sent_at: String(parsed.sent_at || '').trim(),
    sent_ts: sentTs,
    snippet: String(parsed.snippet || '').trim(),
    body_text: String(parsed.body_text || '').trim(),
    body_html: String(parsed.body_html || ''),
    raw_source: String(rawSource || ''),
    attachment_count: Array.isArray(parsed.attachments) ? parsed.attachments.length : 0,
    searchable_text: buildSearchableText(parsed),
    draft_key: String(parsed.message_id_header || '').trim() || `${account.id}:${mailboxName}:${uid}`,
    created_at: nowTs(),
    updated_at: nowTs(),
    ...flagsToState(flags, mailboxRole),
  };
}

function inferSmtpHost(host = '') {
  const clean = String(host || '').trim();
  if (!clean) return '';
  if (/^imap\./i.test(clean)) return clean.replace(/^imap\./i, 'smtp.');
  return clean;
}

function getDefaultMailboxSet(account = {}) {
  const primary = String(account.mailbox || 'INBOX').trim() || 'INBOX';
  const gmail = account.provider === 'gmail' || account.provider === 'google_workspace';
  const mailboxes = [
    { name: 'INBOX', path: 'INBOX', special_use: 'inbox' },
    { name: primary, path: primary, special_use: inferMailboxRole(primary, '') },
  ];
  if (gmail) {
    mailboxes.push(
      { name: '[Gmail]/Sent Mail', path: '[Gmail]/Sent Mail', special_use: 'sent' },
      { name: '[Gmail]/Drafts', path: '[Gmail]/Drafts', special_use: 'drafts' },
      { name: '[Gmail]/All Mail', path: '[Gmail]/All Mail', special_use: 'archive' },
      { name: '[Gmail]/Trash', path: '[Gmail]/Trash', special_use: 'trash' },
    );
  } else {
    mailboxes.push(
      { name: 'Sent', path: 'Sent', special_use: 'sent' },
      { name: 'Drafts', path: 'Drafts', special_use: 'drafts' },
      { name: 'Trash', path: 'Trash', special_use: 'trash' },
      { name: 'Archive', path: 'Archive', special_use: 'archive' },
    );
  }
  const seen = new Set();
  return mailboxes.filter((item) => {
    const key = String(item.path || '').trim().toLowerCase();
    if (!key || seen.has(key)) return false;
    seen.add(key);
    return true;
  });
}

function upsertAccount(db, account = {}) {
  const stmt = db.prepare(`
    INSERT INTO accounts (
      id, label, email, host, port, username, mailbox, use_tls, password_ref,
      smtp_host, smtp_port, smtp_username, smtp_password_ref, smtp_use_tls, smtp_starttls, send_enabled,
      sync_enabled, sync_limit, last_sync_at, last_error, created_at, updated_at,
      account_type, provider, oauth_access_token_ref, oauth_refresh_token_ref, oauth_client_id_ref,
      oauth_client_secret_ref, oauth_token_expires_at, capabilities_json
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(id) DO UPDATE SET
      label = excluded.label,
      email = excluded.email,
      host = excluded.host,
      port = excluded.port,
      username = excluded.username,
      mailbox = excluded.mailbox,
      use_tls = excluded.use_tls,
      password_ref = excluded.password_ref,
      smtp_host = excluded.smtp_host,
      smtp_port = excluded.smtp_port,
      smtp_username = excluded.smtp_username,
      smtp_password_ref = excluded.smtp_password_ref,
      smtp_use_tls = excluded.smtp_use_tls,
      smtp_starttls = excluded.smtp_starttls,
      send_enabled = excluded.send_enabled,
      sync_enabled = excluded.sync_enabled,
      sync_limit = excluded.sync_limit,
      last_sync_at = excluded.last_sync_at,
      last_error = excluded.last_error,
      updated_at = excluded.updated_at,
      account_type = excluded.account_type,
      provider = excluded.provider,
      oauth_access_token_ref = excluded.oauth_access_token_ref,
      oauth_refresh_token_ref = excluded.oauth_refresh_token_ref,
      oauth_client_id_ref = excluded.oauth_client_id_ref,
      oauth_client_secret_ref = excluded.oauth_client_secret_ref,
      oauth_token_expires_at = excluded.oauth_token_expires_at,
      capabilities_json = excluded.capabilities_json
  `);
  stmt.bind([
    account.id,
    account.label,
    account.email,
    account.host,
    account.port,
    account.username,
    account.mailbox,
    account.use_tls ? 1 : 0,
    account.password_ref || '',
    account.smtp_host || '',
    account.smtp_port || 465,
    account.smtp_username || '',
    account.smtp_password_ref || '',
    account.smtp_use_tls ? 1 : 0,
    account.smtp_starttls ? 1 : 0,
    account.send_enabled ? 1 : 0,
    account.sync_enabled ? 1 : 0,
    account.sync_limit,
    account.last_sync_at || 0,
    account.last_error || '',
    account.created_at || nowTs(),
    account.updated_at || nowTs(),
    account.account_type || 'manual_imap_smtp',
    account.provider || 'generic',
    account.oauth_access_token_ref || '',
    account.oauth_refresh_token_ref || '',
    account.oauth_client_id_ref || '',
    account.oauth_client_secret_ref || '',
    account.oauth_token_expires_at || 0,
    JSON.stringify(account.capabilities || defaultCapabilities(account.provider)),
  ]);
  stmt.step();
  stmt.free();
}

function upsertMailbox(db, mailbox = {}) {
  const stmt = db.prepare(`
    INSERT INTO mailboxes (
      id, account_id, name, path, delimiter, special_use, sync_enabled, message_count, last_sync_at, created_at, updated_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(account_id, path) DO UPDATE SET
      name = excluded.name,
      delimiter = excluded.delimiter,
      special_use = excluded.special_use,
      sync_enabled = excluded.sync_enabled,
      message_count = excluded.message_count,
      last_sync_at = excluded.last_sync_at,
      updated_at = excluded.updated_at
  `);
  stmt.bind([
    mailbox.id,
    mailbox.account_id,
    mailbox.name,
    mailbox.path,
    mailbox.delimiter || '',
    mailbox.special_use || '',
    mailbox.sync_enabled ? 1 : 0,
    mailbox.message_count || 0,
    mailbox.last_sync_at || 0,
    mailbox.created_at || nowTs(),
    mailbox.updated_at || nowTs(),
  ]);
  stmt.step();
  stmt.free();
}

function upsertMessage(db, message = {}) {
  const stmt = db.prepare(`
    INSERT INTO messages (
      account_id, uid, thread_id, thread_key, mailbox, mailbox_id, mailbox_role, message_id_header,
      in_reply_to, subject, sender, recipients, sent_at, sent_ts, snippet, body_text, body_html, raw_source,
      attachment_count, searchable_text, flags_json, is_unread, is_flagged, is_draft, is_trashed, is_archived,
      draft_key, created_at, updated_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(account_id, uid, mailbox) DO UPDATE SET
      thread_id = excluded.thread_id,
      thread_key = excluded.thread_key,
      mailbox_id = excluded.mailbox_id,
      mailbox_role = excluded.mailbox_role,
      message_id_header = excluded.message_id_header,
      in_reply_to = excluded.in_reply_to,
      subject = excluded.subject,
      sender = excluded.sender,
      recipients = excluded.recipients,
      sent_at = excluded.sent_at,
      sent_ts = excluded.sent_ts,
      snippet = excluded.snippet,
      body_text = excluded.body_text,
      body_html = excluded.body_html,
      raw_source = excluded.raw_source,
      attachment_count = excluded.attachment_count,
      searchable_text = excluded.searchable_text,
      flags_json = excluded.flags_json,
      is_unread = excluded.is_unread,
      is_flagged = excluded.is_flagged,
      is_draft = excluded.is_draft,
      is_trashed = excluded.is_trashed,
      is_archived = excluded.is_archived,
      draft_key = excluded.draft_key,
      updated_at = excluded.updated_at
  `);
  stmt.bind([
    message.account_id,
    message.uid,
    message.thread_id,
    message.thread_key,
    message.mailbox,
    message.mailbox_id || '',
    message.mailbox_role || '',
    message.message_id_header,
    message.in_reply_to,
    message.subject,
    message.sender,
    message.recipients,
    message.sent_at,
    message.sent_ts,
    message.snippet,
    message.body_text,
    message.body_html || '',
    message.raw_source,
    message.attachment_count,
    message.searchable_text,
    message.flags_json || '[]',
    message.is_unread ? 1 : 0,
    message.is_flagged ? 1 : 0,
    message.is_draft ? 1 : 0,
    message.is_trashed ? 1 : 0,
    message.is_archived ? 1 : 0,
    message.draft_key || '',
    message.created_at,
    message.updated_at,
  ]);
  stmt.step();
  stmt.free();
}

function groupThreads(rows = [], accountsById = new Map(), mailboxesByKey = new Map()) {
  const grouped = new Map();
  rows.forEach((row) => {
    const threadId = String(row.thread_id || '').trim();
    if (!threadId) return;
    const mailboxKey = `${String(row.account_id || '').trim()}:${String(row.mailbox || '').trim().toLowerCase()}`;
    const mailbox = mailboxesByKey.get(mailboxKey) || null;
    const current = grouped.get(threadId) || {
      id: threadId,
      account_id: String(row.account_id || '').trim(),
      account_label: '',
      account_email: '',
      mailbox: String(row.mailbox || '').trim(),
      mailbox_id: String(row.mailbox_id || '').trim(),
      mailbox_role: String(row.mailbox_role || '').trim(),
      subject: String(row.subject || '').trim(),
      from: String(row.sender || '').trim(),
      snippet: String(row.snippet || '').trim(),
      message_count: 0,
      attachment_count: 0,
      unread_count: 0,
      draft_count: 0,
      last_message_at: Number(row.sent_ts || row.updated_at || 0),
      participants: new Set(),
      capabilities: {},
    };
    current.message_count += 1;
    current.attachment_count += Math.max(0, Number(row.attachment_count || 0));
    current.unread_count += Number(row.is_unread || 0) ? 1 : 0;
    current.draft_count += Number(row.is_draft || 0) ? 1 : 0;
    current.participants.add(String(row.sender || '').trim());
    const recipients = parseRecipients(row.recipients);
    recipients.to.concat(recipients.cc || []).forEach((item) => current.participants.add(String(item || '').trim()));
    if (Number(row.sent_ts || row.updated_at || 0) >= current.last_message_at) {
      current.last_message_at = Number(row.sent_ts || row.updated_at || 0);
      current.subject = String(row.subject || '').trim() || current.subject;
      current.from = String(row.sender || '').trim() || current.from;
      current.snippet = String(row.snippet || '').trim() || current.snippet;
      current.mailbox = String(row.mailbox || '').trim() || current.mailbox;
      current.mailbox_id = String(row.mailbox_id || '').trim() || current.mailbox_id;
      current.mailbox_role = String(row.mailbox_role || '').trim() || current.mailbox_role;
    }
    current.capabilities = mailbox && mailbox.special_use
      ? { ...current.capabilities, special_use: mailbox.special_use }
      : current.capabilities;
    grouped.set(threadId, current);
  });
  return [...grouped.values()]
    .map((thread) => {
      const account = accountsById.get(thread.account_id) || {};
      return {
        ...thread,
        participants: [...thread.participants].filter(Boolean),
        account_label: String(account.label || '').trim(),
        account_email: String(account.email || '').trim(),
        capabilities: account.capabilities || {},
      };
    })
    .sort((a, b) => Number(b.last_message_at || 0) - Number(a.last_message_at || 0));
}

function makeOAuthConfig(account = {}, getSecretByRef) {
  const accessToken = account.oauth_access_token_ref ? getSecretByRef(account.oauth_access_token_ref) : null;
  return {
    accessToken: accessToken && accessToken.ok ? String(accessToken.secret || '').trim() : '',
    refreshTokenRef: String(account.oauth_refresh_token_ref || '').trim(),
    clientIdRef: String(account.oauth_client_id_ref || '').trim(),
    clientSecretRef: String(account.oauth_client_secret_ref || '').trim(),
    expiresAt: Number(account.oauth_token_expires_at || 0),
  };
}

function createMailStore(options = {}) {
  const userDataPath = String(options.userDataPath || process.cwd()).trim();
  const dbPath = path.join(userDataPath, 'mail_store.sqlite');
  const getSecretByRef = options.getSecretByRef;
  const setSecret = options.setSecret;
  const clearSecret = options.clearSecret;
  const refreshOAuthAccessToken = options.refreshOAuthAccessToken;

  async function withDatabase(taskFn) {
    return withDbLock(dbPath, async () => {
      let db = await openDatabase(dbPath);
      try {
        if (shouldResetDatabase(db)) {
          db.close();
          if (fs.existsSync(dbPath)) fs.unlinkSync(dbPath);
          db = await openDatabase(dbPath);
        }
        ensureSchema(db);
        const result = await taskFn(db);
        saveDatabase(db, dbPath);
        return result;
      } finally {
        db.close();
      }
    });
  }

  async function listAccounts() {
    return withDatabase(async (db) => (
      selectRows(db, 'SELECT * FROM accounts ORDER BY label COLLATE NOCASE ASC').map((row) => {
        const account = normalizeAccountRecord(row);
        return {
          ...account,
          password_configured: !!account.password_ref,
          smtp_password_configured: !!account.smtp_password_ref,
          oauth_configured: !!(account.oauth_access_token_ref || account.oauth_refresh_token_ref),
        };
      })
    ));
  }

  async function listMailboxes(accountId = '') {
    return withDatabase(async (db) => {
      const bind = [];
      let sql = 'SELECT * FROM mailboxes';
      if (String(accountId || '').trim()) {
        sql += ' WHERE account_id = ?';
        bind.push(String(accountId || '').trim());
      }
      sql += ' ORDER BY account_id ASC, CASE special_use WHEN \'inbox\' THEN 0 WHEN \'sent\' THEN 1 WHEN \'drafts\' THEN 2 WHEN \'archive\' THEN 3 WHEN \'trash\' THEN 4 ELSE 10 END, name COLLATE NOCASE ASC';
      return selectRows(db, sql, bind).map((row) => normalizeMailboxRecord(row));
    });
  }

  async function resolveAccountAuth(account = {}) {
    if (account.account_type === 'gmail_oauth') {
      let oauth = makeOAuthConfig(account, getSecretByRef);
      if (!oauth.accessToken && typeof refreshOAuthAccessToken === 'function') {
        const refreshed = await refreshOAuthAccessToken(account);
        if (refreshed && refreshed.ok) {
          oauth = makeOAuthConfig(refreshed.account || account, getSecretByRef);
        }
      } else if (oauth.refreshTokenRef && oauth.expiresAt && oauth.expiresAt <= nowTs() + 60_000 && typeof refreshOAuthAccessToken === 'function') {
        const refreshed = await refreshOAuthAccessToken(account);
        if (refreshed && refreshed.ok) oauth = makeOAuthConfig(refreshed.account || account, getSecretByRef);
      }
      if (!oauth.accessToken) return { ok: false, message: 'OAuth access token is missing.' };
      return { ok: true, type: 'xoauth2', access_token: oauth.accessToken };
    }
    const pwdRes = getSecretByRef(account.password_ref);
    if (!pwdRes || !pwdRes.ok || !pwdRes.secret) return { ok: false, message: (pwdRes && pwdRes.message) || 'Mailbox password is missing.' };
    return { ok: true, type: 'password', password: String(pwdRes.secret || '') };
  }

  async function saveAccount(input = {}) {
    const id = String(input.id || makeId('mailacct')).trim() || makeId('mailacct');
    const existingList = await listAccounts();
    const existing = existingList.find((item) => item.id === id) || null;
    const accountType = String(input.account_type || (existing && existing.account_type) || 'manual_imap_smtp').trim() || 'manual_imap_smtp';
    const provider = String(input.provider || (existing && existing.provider) || inferProviderFromHost(input.host || '')).trim() || 'generic';

    let passwordRef = String((existing && existing.password_ref) || '').trim();
    const password = String(input.password || '');
    if (password) {
      const setRes = setSecret(passwordRef, password, 'mailbox');
      if (!setRes || !setRes.ok) return { ok: false, message: (setRes && setRes.message) || 'Unable to store mailbox password.' };
      passwordRef = String(setRes.ref || '').trim();
    }

    let smtpPasswordRef = String((existing && existing.smtp_password_ref) || '').trim();
    const smtpPassword = String(input.smtp_password || '');
    if (smtpPassword) {
      const setRes = setSecret(smtpPasswordRef, smtpPassword, 'mailbox-smtp');
      if (!setRes || !setRes.ok) return { ok: false, message: (setRes && setRes.message) || 'Unable to store SMTP password.' };
      smtpPasswordRef = String(setRes.ref || '').trim();
    }

    let oauthAccessTokenRef = String((existing && existing.oauth_access_token_ref) || '').trim();
    let oauthRefreshTokenRef = String((existing && existing.oauth_refresh_token_ref) || '').trim();
    let oauthClientIdRef = String((existing && existing.oauth_client_id_ref) || '').trim();
    let oauthClientSecretRef = String((existing && existing.oauth_client_secret_ref) || '').trim();
    if (input.oauth_access_token) {
      const setRes = setSecret(oauthAccessTokenRef, input.oauth_access_token, 'mail-google-access');
      if (!setRes || !setRes.ok) return { ok: false, message: (setRes && setRes.message) || 'Unable to store access token.' };
      oauthAccessTokenRef = String(setRes.ref || '').trim();
    }
    if (input.oauth_refresh_token) {
      const setRes = setSecret(oauthRefreshTokenRef, input.oauth_refresh_token, 'mail-google-refresh');
      if (!setRes || !setRes.ok) return { ok: false, message: (setRes && setRes.message) || 'Unable to store refresh token.' };
      oauthRefreshTokenRef = String(setRes.ref || '').trim();
    }
    if (input.oauth_client_id) {
      const setRes = setSecret(oauthClientIdRef, input.oauth_client_id, 'mail-google-client');
      if (!setRes || !setRes.ok) return { ok: false, message: (setRes && setRes.message) || 'Unable to store client id.' };
      oauthClientIdRef = String(setRes.ref || '').trim();
    }
    if (input.oauth_client_secret) {
      const setRes = setSecret(oauthClientSecretRef, input.oauth_client_secret, 'mail-google-client-secret');
      if (!setRes || !setRes.ok) return { ok: false, message: (setRes && setRes.message) || 'Unable to store client secret.' };
      oauthClientSecretRef = String(setRes.ref || '').trim();
    }

    if (accountType === 'manual_imap_smtp' && !passwordRef) {
      return { ok: false, message: 'Mailbox password is required.' };
    }
    if (accountType === 'gmail_oauth' && !oauthAccessTokenRef && !oauthRefreshTokenRef) {
      return { ok: false, message: 'Google OAuth tokens are required.' };
    }
    if (!smtpPasswordRef) smtpPasswordRef = passwordRef;

    const smtpHost = String(input.smtp_host || (existing && existing.smtp_host) || inferSmtpHost(input.host || (existing && existing.host) || '')).trim();
    const smtpPort = Math.max(1, Math.round(Number(
      input.smtp_port
      || (existing && existing.smtp_port)
      || (Number(input.port || (existing && existing.port) || 993) === 587 ? 587 : 465)
    )));
    const capabilities = {
      ...defaultCapabilities(provider),
      ...((existing && existing.capabilities) || {}),
      ...((input && input.capabilities) || {}),
    };
    const account = {
      id,
      label: String(input.label || input.email || (existing && existing.label) || '').trim(),
      email: String(input.email || (existing && existing.email) || '').trim(),
      host: String(input.host || (existing && existing.host) || '').trim(),
      port: Math.max(1, Math.round(Number(input.port || (existing && existing.port) || 993))),
      username: String(input.username || (existing && existing.username) || input.email || '').trim(),
      mailbox: String(input.mailbox || (existing && existing.mailbox) || 'INBOX').trim() || 'INBOX',
      use_tls: Object.prototype.hasOwnProperty.call(input, 'use_tls') ? !!input.use_tls : (existing ? existing.use_tls : true),
      password_ref: passwordRef,
      smtp_host: smtpHost,
      smtp_port: smtpPort,
      smtp_username: String(input.smtp_username || (existing && existing.smtp_username) || input.username || input.email || '').trim(),
      smtp_password_ref: smtpPasswordRef,
      smtp_use_tls: Object.prototype.hasOwnProperty.call(input, 'smtp_use_tls') ? !!input.smtp_use_tls : (existing ? existing.smtp_use_tls : smtpPort === 465),
      smtp_starttls: Object.prototype.hasOwnProperty.call(input, 'smtp_starttls') ? !!input.smtp_starttls : (existing ? existing.smtp_starttls : smtpPort === 587),
      send_enabled: Object.prototype.hasOwnProperty.call(input, 'send_enabled') ? !!input.send_enabled : (existing ? existing.send_enabled : true),
      sync_enabled: Object.prototype.hasOwnProperty.call(input, 'sync_enabled') ? !!input.sync_enabled : (existing ? existing.sync_enabled : true),
      sync_limit: Math.max(1, Math.min(500, Math.round(Number(input.sync_limit || (existing && existing.sync_limit) || 200)))),
      last_sync_at: Number((existing && existing.last_sync_at) || 0),
      last_error: String((existing && existing.last_error) || '').trim(),
      created_at: Number((existing && existing.created_at) || nowTs()),
      updated_at: nowTs(),
      account_type: accountType,
      provider,
      oauth_access_token_ref: oauthAccessTokenRef,
      oauth_refresh_token_ref: oauthRefreshTokenRef,
      oauth_client_id_ref: oauthClientIdRef,
      oauth_client_secret_ref: oauthClientSecretRef,
      oauth_token_expires_at: Number(input.oauth_token_expires_at || (existing && existing.oauth_token_expires_at) || 0),
      capabilities,
    };
    if (!account.label || !account.email || !account.host || !account.username) {
      return { ok: false, message: 'Label, email, host, and username are required.' };
    }
    await withDatabase(async (db) => {
      upsertAccount(db, account);
      const knownMailboxes = selectRows(db, 'SELECT * FROM mailboxes WHERE account_id = ?', [account.id]).map((row) => normalizeMailboxRecord(row));
      if (!knownMailboxes.length) {
        getDefaultMailboxSet(account).forEach((candidate) => {
          upsertMailbox(db, {
            id: makeId('mailbox'),
            account_id: account.id,
            name: candidate.name,
            path: candidate.path,
            delimiter: '/',
            special_use: candidate.special_use || inferMailboxRole(candidate.path, ''),
            sync_enabled: true,
            message_count: 0,
            last_sync_at: 0,
            created_at: nowTs(),
            updated_at: nowTs(),
          });
        });
      }
    });
    return {
      ok: true,
      account: {
        ...account,
        password_configured: !!account.password_ref,
        smtp_password_configured: !!account.smtp_password_ref,
        oauth_configured: !!(account.oauth_access_token_ref || account.oauth_refresh_token_ref),
      },
    };
  }

  async function deleteAccount(accountId = '') {
    const id = String(accountId || '').trim();
    if (!id) return { ok: false, message: 'Account id is required.' };
    const accounts = await listAccounts();
    const existing = accounts.find((item) => item.id === id);
    if (!existing) return { ok: true, missing: true };
    [
      existing.password_ref,
      existing.smtp_password_ref,
      existing.oauth_access_token_ref,
      existing.oauth_refresh_token_ref,
      existing.oauth_client_id_ref,
      existing.oauth_client_secret_ref,
    ].filter(Boolean).forEach((ref) => clearSecret(ref));
    await withDatabase(async (db) => {
      const stmt = db.prepare('DELETE FROM accounts WHERE id = ?');
      stmt.bind([id]);
      stmt.step();
      stmt.free();
    });
    return { ok: true };
  }

  async function discoverMailboxes(account = {}, client = null) {
    let listed = [];
    if (client) {
      try {
        listed = await client.listMailboxes();
      } catch (_) {
        listed = [];
      }
    }
    if (!listed.length) listed = getDefaultMailboxSet(account);
    const mapped = [];
    const seen = new Set();
    listed.forEach((row) => {
      const pathValue = String((row && (row.path || row.name)) || '').trim();
      if (!pathValue) return;
      const key = pathValue.toLowerCase();
      if (seen.has(key)) return;
      seen.add(key);
      const role = inferMailboxRole(pathValue, row.special_use);
      mapped.push({
        id: makeId('mailbox'),
        account_id: account.id,
        name: String(row.name || pathValue).trim(),
        path: pathValue,
        delimiter: String(row.delimiter || '/').trim(),
        special_use: role,
        sync_enabled: MAILBOX_ROLES.includes(role) || role === '' ? true : false,
        message_count: 0,
        last_sync_at: 0,
        created_at: nowTs(),
        updated_at: nowTs(),
      });
    });
    return mapped;
  }

  async function syncAccount(accountId = '') {
    const id = String(accountId || '').trim();
    const accounts = await listAccounts();
    const account = accounts.find((item) => item.id === id);
    if (!account) return { ok: false, message: 'Mailbox account not found.' };
    const auth = await resolveAccountAuth(account);
    if (!auth.ok) return auth;
    const client = new ImapClient({ host: account.host, port: account.port, tls: account.use_tls });
    let syncedCount = 0;
    try {
      await client.connect();
      await client.capability();
      if (auth.type === 'xoauth2') await client.authenticateXoauth2(account.username, auth.access_token);
      else await client.login(account.username, auth.password);

      const discovered = await discoverMailboxes(account, client);
      await withDatabase(async (db) => {
        discovered.forEach((mailbox) => upsertMailbox(db, mailbox));
      });
      const mailboxRows = (await listMailboxes(account.id))
        .map((item) => ({
          ...item,
          sync_enabled: MAILBOX_ROLES.includes(item.special_use) || item.path.toLowerCase() === account.mailbox.toLowerCase() || item.sync_enabled,
        }))
        .filter((item) => item.sync_enabled);

      const capabilities = {
        ...account.capabilities,
        sent_folder: (mailboxRows.find((item) => item.special_use === 'sent') || {}).path || account.capabilities.sent_folder || '',
        drafts_folder: (mailboxRows.find((item) => item.special_use === 'drafts') || {}).path || account.capabilities.drafts_folder || '',
        archive_folder: (mailboxRows.find((item) => item.special_use === 'archive') || {}).path || account.capabilities.archive_folder || '',
        trash_folder: (mailboxRows.find((item) => item.special_use === 'trash') || {}).path || account.capabilities.trash_folder || '',
      };

      for (const mailbox of mailboxRows) {
        try {
          await client.selectMailbox(mailbox.path);
        } catch (_) {
          continue;
        }
        const allUids = await client.uidSearchAll();
        const numericUids = allUids.map((item) => Math.round(Number(item || 0))).filter(Boolean);
        const targetUids = numericUids.slice(-account.sync_limit);
        const seenUids = new Set(targetUids);
        await withDatabase(async (db) => {
          const existingRows = selectRows(
            db,
            'SELECT uid FROM messages WHERE account_id = ? AND LOWER(mailbox) = LOWER(?)',
            [account.id, mailbox.path]
          );
          existingRows.forEach((row) => {
            const uid = Math.round(Number(row.uid || 0));
            if (uid && !seenUids.has(uid)) {
              const delStmt = db.prepare('DELETE FROM messages WHERE account_id = ? AND uid = ? AND LOWER(mailbox) = LOWER(?)');
              delStmt.bind([account.id, uid, mailbox.path]);
              delStmt.step();
              delStmt.free();
            }
          });
        });
        for (const uid of targetUids) {
          const fetched = await client.fetchRawMessage(uid);
          await withDatabase(async (db) => {
            const message = buildParsedMessage(account, mailbox, uid, fetched.raw_source, fetched.flags || [], db);
            upsertMessage(db, message);
            upsertMailbox(db, {
              ...mailbox,
              message_count: numericUids.length,
              last_sync_at: nowTs(),
              updated_at: nowTs(),
            });
          });
          syncedCount += 1;
        }
      }

      await withDatabase(async (db) => {
        upsertAccount(db, {
          ...account,
          capabilities,
          last_sync_at: nowTs(),
          last_error: '',
          updated_at: nowTs(),
        });
      });
      return { ok: true, synced_count: syncedCount, mailboxes: await listMailboxes(account.id) };
    } catch (err) {
      await withDatabase(async (db) => {
        upsertAccount(db, {
          ...account,
          last_sync_at: Number(account.last_sync_at || 0),
          last_error: String((err && err.message) || 'Sync failed.'),
          updated_at: nowTs(),
        });
      });
      return { ok: false, message: String((err && err.message) || 'IMAP sync failed.') };
    } finally {
      await client.logout();
    }
  }

  async function searchThreads(options = {}) {
    const query = normalizeWhitespace((options && options.query) || '').toLowerCase();
    const limit = Math.max(1, Math.min(200, Math.round(Number((options && options.limit) || 80))));
    const accountId = String((options && options.account_id) || '').trim();
    const mailboxPath = String((options && options.mailbox_path) || '').trim();
    const smartView = String((options && options.smart_view) || '').trim().toLowerCase();
    return withDatabase(async (db) => {
      const accounts = selectRows(db, 'SELECT * FROM accounts WHERE sync_enabled = 1 ORDER BY label COLLATE NOCASE ASC');
      const accountsById = new Map(accounts.map((row) => {
        const normalized = normalizeAccountRecord(row);
        return [normalized.id, normalized];
      }));
      const mailboxRows = selectRows(db, 'SELECT * FROM mailboxes');
      const mailboxesByKey = new Map(mailboxRows.map((row) => {
        const mailbox = normalizeMailboxRecord(row);
        return [`${mailbox.account_id}:${mailbox.path.toLowerCase()}`, mailbox];
      }));
      let rows = selectRows(db, 'SELECT * FROM messages ORDER BY sent_ts DESC, updated_at DESC');
      if (accountId) rows = rows.filter((row) => String(row.account_id || '').trim() === accountId);
      if (mailboxPath) rows = rows.filter((row) => String(row.mailbox || '').trim().toLowerCase() === mailboxPath.toLowerCase());
      if (smartView === 'unread') rows = rows.filter((row) => !!Number(row.is_unread || 0));
      if (smartView && MAILBOX_ROLES.includes(smartView)) rows = rows.filter((row) => String(row.mailbox_role || '').trim().toLowerCase() === smartView);
      if (query) rows = rows.filter((row) => String(row.searchable_text || '').includes(query));
      const threads = groupThreads(rows, accountsById, mailboxesByKey).slice(0, limit);
      return { ok: true, items: threads, total: threads.length };
    });
  }

  async function getThread(threadId = '') {
    const id = String(threadId || '').trim();
    if (!id) return { ok: false, message: 'threadId is required.' };
    return withDatabase(async (db) => {
      const rows = selectRows(
        db,
        'SELECT m.*, a.label AS account_label, a.email AS account_email, a.capabilities_json AS account_capabilities_json FROM messages m JOIN accounts a ON a.id = m.account_id WHERE m.thread_id = ? ORDER BY m.sent_ts ASC, m.uid ASC',
        [id]
      );
      if (!rows.length) return { ok: false, message: 'Thread not found.' };
      const latest = rows[rows.length - 1];
      return {
        ok: true,
        thread: {
          id,
          account_id: String(latest.account_id || '').trim(),
          account_email: String(latest.account_email || '').trim(),
          subject: String(latest.subject || '').trim(),
          account_label: String(latest.account_label || '').trim(),
          mailbox: String(latest.mailbox || '').trim(),
          mailbox_role: String(latest.mailbox_role || '').trim(),
          capabilities: normalizeJsonObject(latest.account_capabilities_json, {}),
          messages: rows.map((row) => {
            const parsed = parseRawEmailText(String(row.raw_source || ''), `db://${row.account_id}/${row.uid}`);
            const recipients = parseRecipients(row.recipients);
            return {
              id: `${String(row.account_id || '').trim()}:${String(row.uid || '').trim()}:${String(row.mailbox || '').trim()}`,
              uid: Math.round(Number(row.uid || 0)),
              account_id: String(row.account_id || '').trim(),
              account_email: String(row.account_email || '').trim(),
              source_path: '',
              source_key: parsed.source_key || '',
              mail_message_id: '',
              account_name: String(row.account_label || '').trim(),
              mailbox_name: String(row.mailbox || '').trim(),
              mailbox_role: String(row.mailbox_role || '').trim(),
              message_id_header: String(row.message_id_header || '').trim(),
              in_reply_to: String(row.in_reply_to || '').trim(),
              references: Array.isArray(parsed.references) ? parsed.references : [],
              from: String(row.sender || '').trim(),
              to: recipients.to,
              cc: recipients.cc,
              bcc: recipients.bcc,
              subject: String(row.subject || '').trim(),
              sent_at: String(row.sent_at || '').trim(),
              sent_ts: Number(row.sent_ts || 0),
              body_text: String(row.body_text || '').trim(),
              body_html: String(row.body_html || ''),
              snippet: String(row.snippet || '').trim(),
              attachments: Array.isArray(parsed.attachments) ? parsed.attachments.map((attachment, index) => ({
                id: `${String(row.account_id || '').trim()}:${String(row.uid || '').trim()}:${index}`,
                file_name: String((attachment && attachment.file_name) || 'attachment').trim(),
                mime_type: String((attachment && attachment.mime_type) || 'application/octet-stream').trim(),
                size_bytes: Buffer.isBuffer(attachment.data) ? attachment.data.length : 0,
                content_id: String((attachment && attachment.content_id) || '').trim(),
                inline: !!(attachment && attachment.inline),
              })) : [],
              raw_source: String(row.raw_source || ''),
              flags: normalizeJsonArray(row.flags_json, []),
              is_unread: !!Number(row.is_unread || 0),
              is_flagged: !!Number(row.is_flagged || 0),
              is_draft: !!Number(row.is_draft || 0),
              is_trashed: !!Number(row.is_trashed || 0),
              is_archived: !!Number(row.is_archived || 0),
            };
          }),
        },
      };
    });
  }

  async function updateMessageState(accountId = '', mailboxPath = '', uid = 0, patch = {}) {
    const accounts = await listAccounts();
    const account = accounts.find((item) => item.id === String(accountId || '').trim());
    if (!account) return { ok: false, message: 'Mailbox account not found.' };
    const auth = await resolveAccountAuth(account);
    if (!auth.ok) return auth;
    const client = new ImapClient({ host: account.host, port: account.port, tls: account.use_tls });
    try {
      await client.connect();
      await client.capability();
      if (auth.type === 'xoauth2') await client.authenticateXoauth2(account.username, auth.access_token);
      else await client.login(account.username, auth.password);
      await client.selectMailbox(mailboxPath);
      if (Object.prototype.hasOwnProperty.call(patch, 'is_unread')) {
        await client.uidStoreFlags(uid, ['\\Seen'], patch.is_unread ? 'remove' : 'add');
      }
      if (Object.prototype.hasOwnProperty.call(patch, 'is_flagged')) {
        await client.uidStoreFlags(uid, ['\\Flagged'], patch.is_flagged ? 'add' : 'remove');
      }
      await withDatabase(async (db) => {
        const currentRow = selectRows(
          db,
          'SELECT is_unread, is_flagged FROM messages WHERE account_id = ? AND mailbox = ? AND uid = ? LIMIT 1',
          [account.id, mailboxPath, Math.round(Number(uid || 0))]
        )[0] || {};
        const stmt = db.prepare('UPDATE messages SET is_unread = ?, is_flagged = ?, updated_at = ? WHERE account_id = ? AND mailbox = ? AND uid = ?');
        stmt.bind([
          Object.prototype.hasOwnProperty.call(patch, 'is_unread') ? (patch.is_unread ? 1 : 0) : Number(currentRow.is_unread || 0),
          Object.prototype.hasOwnProperty.call(patch, 'is_flagged') ? (patch.is_flagged ? 1 : 0) : Number(currentRow.is_flagged || 0),
          nowTs(),
          account.id,
          mailboxPath,
          Math.round(Number(uid || 0)),
        ]);
        stmt.step();
        stmt.free();
      });
      return { ok: true };
    } catch (err) {
      return { ok: false, message: String((err && err.message) || 'Unable to update mail state.') };
    } finally {
      await client.logout();
    }
  }

  async function updateThreadState(threadId = '', patch = {}) {
    const thread = await getThread(threadId);
    if (!thread.ok || !thread.thread) return thread;
    const messages = Array.isArray(thread.thread.messages) ? thread.thread.messages : [];
    for (const message of messages) {
      if (Number(message.uid || 0) <= 0) continue;
      const stateRes = await updateMessageState(message.account_id, message.mailbox_name, Number(message.uid || 0), patch);
      if (!stateRes.ok) return stateRes;
    }
    return { ok: true };
  }

  async function moveThread(threadId = '', targetRole = '') {
    const thread = await getThread(threadId);
    if (!thread.ok || !thread.thread) return thread;
    const accountId = String((thread.thread && thread.thread.account_id) || '').trim();
    const accounts = await listAccounts();
    const account = accounts.find((item) => item.id === accountId);
    if (!account) return { ok: false, message: 'Mailbox account not found.' };
    const role = String(targetRole || '').trim().toLowerCase();
    const caps = account.capabilities || {};
    if (role === 'archive' && !caps.supports_archive) return { ok: false, message: 'Archive is not supported for this account.' };
    if (role === 'trash' && !caps.supports_trash) return { ok: false, message: 'Trash is not supported for this account.' };
    const mailboxRows = await listMailboxes(account.id);
    const targetMailbox = mailboxRows.find((item) => item.special_use === role);
    if (!targetMailbox) return { ok: false, message: `No ${role} mailbox is configured for this account.` };
    const auth = await resolveAccountAuth(account);
    if (!auth.ok) return auth;
    const client = new ImapClient({ host: account.host, port: account.port, tls: account.use_tls });
    try {
      await client.connect();
      await client.capability();
      if (auth.type === 'xoauth2') await client.authenticateXoauth2(account.username, auth.access_token);
      else await client.login(account.username, auth.password);
      for (const message of thread.thread.messages) {
        const uid = Number(message.uid || 0);
        if (uid <= 0) continue;
        await client.selectMailbox(message.mailbox_name);
        await client.uidMove(uid, targetMailbox.path);
      }
      await syncAccount(account.id);
      return { ok: true, target_mailbox: targetMailbox };
    } catch (err) {
      return { ok: false, message: String((err && err.message) || 'Unable to move thread.') };
    } finally {
      await client.logout();
    }
  }

  async function deleteThread(threadId = '') {
    return moveThread(threadId, 'trash');
  }

  async function sendMail(accountId = '', payload = {}) {
    const id = String(accountId || '').trim();
    if (!id) return { ok: false, message: 'Account id is required.' };
    const accounts = await listAccounts();
    const account = accounts.find((item) => item.id === id);
    if (!account) return { ok: false, message: 'Mailbox account not found.' };
    if (!account.send_enabled) return { ok: false, message: 'Sending is disabled for this account.' };
    const smtpHost = String(account.smtp_host || inferSmtpHost(account.host)).trim();
    if (!smtpHost) return { ok: false, message: 'SMTP host is missing for this account.' };

    const recipients = normalizeComposeRecipients(payload || {});
    const fromAddress = String((payload && payload.from) || account.email || '').trim();
    const subject = String((payload && payload.subject) || '').trim();
    const bodyText = String((payload && payload.body_text) || '');
    if (!fromAddress) return { ok: false, message: 'From address is required.' };
    if (!recipients.to.length && !recipients.cc.length && !recipients.bcc.length) {
      return { ok: false, message: 'At least one recipient is required.' };
    }

    let smtpAuth = null;
    if (account.account_type === 'gmail_oauth') {
      const auth = await resolveAccountAuth(account);
      if (!auth.ok) return auth;
      smtpAuth = { type: 'xoauth2', access_token: auth.access_token };
    } else {
      const smtpSecretRef = String(account.smtp_password_ref || account.password_ref || '').trim();
      const smtpSecret = getSecretByRef(smtpSecretRef);
      if (!smtpSecret || !smtpSecret.ok || !smtpSecret.secret) {
        return { ok: false, message: (smtpSecret && smtpSecret.message) || 'SMTP password is missing.' };
      }
      smtpAuth = { type: 'password', password: String(smtpSecret.secret || '') };
    }

    const attachments = Array.isArray(payload.attachments)
      ? payload.attachments.map((attachment) => ({
        file_name: String((attachment && attachment.file_name) || path.basename(String((attachment && attachment.source_path) || 'attachment'))).trim() || 'attachment',
        mime_type: String((attachment && attachment.mime_type) || 'application/octet-stream').trim(),
        content: attachment && attachment.content_base64
          ? Buffer.from(String(attachment.content_base64 || ''), 'base64')
          : fs.readFileSync(String((attachment && attachment.source_path) || '')),
      }))
      : [];

    const sendRes = await sendMailViaSmtp({
      host: smtpHost,
      port: account.smtp_port || 465,
      secure: !!account.smtp_use_tls,
      starttls: !!account.smtp_starttls,
      username: String(account.smtp_username || account.username || account.email || '').trim(),
      password: smtpAuth.type === 'password' ? smtpAuth.password : '',
      auth: smtpAuth,
      helo_name: smtpHost,
    }, {
      from: fromAddress,
      to: recipients.to,
      cc: recipients.cc,
      bcc: recipients.bcc,
      subject,
      body_text: bodyText,
      in_reply_to: String((payload && payload.in_reply_to) || '').trim(),
      references: Array.isArray(payload && payload.references) ? payload.references : [],
      message_id_header: String((payload && payload.message_id_header) || '').trim(),
      attachments,
    });

    const mailboxes = await listMailboxes(account.id);
    const sentMailbox = mailboxes.find((item) => item.special_use === 'sent') || null;
    const rawSource = String((sendRes && sendRes.raw_source) || '');
    if (sentMailbox) {
      const auth = await resolveAccountAuth(account);
      if (auth.ok) {
        const client = new ImapClient({ host: account.host, port: account.port, tls: account.use_tls });
        try {
          await client.connect();
          await client.capability();
          if (auth.type === 'xoauth2') await client.authenticateXoauth2(account.username, auth.access_token);
          else await client.login(account.username, auth.password);
          await client.appendMessage(sentMailbox.path, rawSource, { flags: ['\\Seen'] });
        } catch (_) {
          // Local reconciliation below still gives the user a visible sent message.
        } finally {
          await client.logout();
        }
      }
    }

    await withDatabase(async (db) => {
      const localUid = -Math.max(1, nowTs());
      const targetMailbox = sentMailbox || {
        id: '',
        path: account.capabilities.sent_folder || 'Sent',
        special_use: 'sent',
      };
      const message = buildParsedMessage(account, targetMailbox, localUid, rawSource, ['\\Seen']);
      upsertMessage(db, message);
    });

    return {
      ok: true,
      account,
      message_id_header: String((sendRes && sendRes.message_id_header) || '').trim(),
      sent_at: String((sendRes && sendRes.sent_at) || '').trim(),
    };
  }

  async function saveDraft(accountId = '', payload = {}) {
    const id = String(accountId || '').trim();
    if (!id) return { ok: false, message: 'Account id is required.' };
    const accounts = await listAccounts();
    const account = accounts.find((item) => item.id === id);
    if (!account) return { ok: false, message: 'Mailbox account not found.' };
    const mailboxes = await listMailboxes(account.id);
    const draftsMailbox = mailboxes.find((item) => item.special_use === 'drafts');
    if (!draftsMailbox) return { ok: false, message: 'No drafts mailbox is configured for this account.' };
    const auth = await resolveAccountAuth(account);
    if (!auth.ok) return auth;

    const recipients = normalizeComposeRecipients(payload || {});
    const built = buildRawMessage({
      from: String((payload && payload.from) || account.email || '').trim(),
      to: recipients.to,
      cc: recipients.cc,
      bcc: recipients.bcc,
      subject: String((payload && payload.subject) || '').trim(),
      body_text: String((payload && payload.body_text) || ''),
      in_reply_to: String((payload && payload.in_reply_to) || '').trim(),
      references: Array.isArray(payload && payload.references) ? payload.references : [],
      message_id_header: String((payload && payload.message_id_header) || '').trim(),
      attachments: Array.isArray(payload.attachments) ? payload.attachments.map((attachment) => ({
        file_name: String((attachment && attachment.file_name) || path.basename(String((attachment && attachment.source_path) || 'attachment'))).trim() || 'attachment',
        mime_type: String((attachment && attachment.mime_type) || 'application/octet-stream').trim(),
        content: attachment && attachment.content_base64
          ? Buffer.from(String(attachment.content_base64 || ''), 'base64')
          : fs.readFileSync(String((attachment && attachment.source_path) || '')),
      })) : [],
    });

    const client = new ImapClient({ host: account.host, port: account.port, tls: account.use_tls });
    try {
      await client.connect();
      await client.capability();
      if (auth.type === 'xoauth2') await client.authenticateXoauth2(account.username, auth.access_token);
      else await client.login(account.username, auth.password);
      await client.appendMessage(draftsMailbox.path, built.raw, { flags: ['\\Draft'] });
      await withDatabase(async (db) => {
        const localUid = -Math.max(1, nowTs());
        const message = buildParsedMessage(account, draftsMailbox, localUid, built.raw, ['\\Draft']);
        message.draft_key = String((payload && payload.draft_key) || built.messageId || '').trim();
        upsertMessage(db, message);
      });
      return { ok: true, draft_key: built.messageId };
    } catch (err) {
      return { ok: false, message: String((err && err.message) || 'Unable to save draft.') };
    } finally {
      await client.logout();
    }
  }

  async function exportThreads(threadIds = []) {
    const ids = Array.isArray(threadIds) ? threadIds.map((item) => String(item || '').trim()).filter(Boolean) : [];
    if (!ids.length) return [];
    return withDatabase(async (db) => {
      const out = [];
      for (const threadId of ids) {
        const rows = selectRows(
          db,
          'SELECT m.*, a.label AS account_label FROM messages m JOIN accounts a ON a.id = m.account_id WHERE m.thread_id = ? ORDER BY m.sent_ts ASC, m.uid ASC',
          [threadId]
        );
        if (!rows.length) continue;
        out.push({
          id: threadId,
          subject: String(rows[rows.length - 1].subject || '').trim(),
          messages: rows.map((row) => ({
            raw_source: String(row.raw_source || ''),
            account_name: String(row.account_label || '').trim(),
            mailbox_name: String(row.mailbox || '').trim(),
            mail_message_id: '',
          })),
        });
      }
      return out;
    });
  }

  return {
    dbPath,
    deleteAccount,
    deleteThread,
    exportThreads,
    getThread,
    listAccounts,
    listMailboxes,
    saveAccount,
    saveDraft,
    searchThreads,
    sendMail,
    syncAccount,
    updateThreadState,
    moveThread,
  };
}

module.exports = {
  createMailStore,
};
