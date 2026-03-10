const fs = require('fs');
const path = require('path');
const crypto = require('crypto');

const { ImapClient } = require('./mail_imap');
const { computeMailConversationKey, normalizeWhitespace, parseRawEmailText } = require('./mail_parser');
const { sendMailViaSmtp } = require('./mail_smtp');

const SCHEMA_VERSION = 2;
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
      password_ref TEXT NOT NULL,
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
      updated_at INTEGER NOT NULL
    );
  `);
  db.run(`
    CREATE TABLE IF NOT EXISTS messages (
      account_id TEXT NOT NULL,
      uid INTEGER NOT NULL,
      thread_id TEXT NOT NULL,
      thread_key TEXT NOT NULL,
      mailbox TEXT NOT NULL,
      message_id_header TEXT NOT NULL,
      in_reply_to TEXT NOT NULL,
      subject TEXT NOT NULL,
      sender TEXT NOT NULL,
      recipients TEXT NOT NULL,
      sent_at TEXT NOT NULL,
      sent_ts INTEGER NOT NULL DEFAULT 0,
      snippet TEXT NOT NULL,
      body_text TEXT NOT NULL,
      raw_source TEXT NOT NULL,
      attachment_count INTEGER NOT NULL DEFAULT 0,
      searchable_text TEXT NOT NULL,
      created_at INTEGER NOT NULL,
      updated_at INTEGER NOT NULL,
      PRIMARY KEY (account_id, uid),
      FOREIGN KEY (account_id) REFERENCES accounts(id) ON DELETE CASCADE
    );
  `);
  db.run('CREATE INDEX IF NOT EXISTS idx_messages_thread ON messages(thread_id)');
  db.run('CREATE INDEX IF NOT EXISTS idx_messages_account ON messages(account_id)');
  db.run('CREATE INDEX IF NOT EXISTS idx_messages_sent ON messages(sent_ts DESC)');
  const accountColumns = new Set(selectRows(db, 'PRAGMA table_info(accounts)').map((row) => String(row.name || '').trim()));
  const ensureAccountColumn = (name, sql) => {
    if (accountColumns.has(name)) return;
    db.run(`ALTER TABLE accounts ADD COLUMN ${sql}`);
  };
  ensureAccountColumn('smtp_host', "smtp_host TEXT NOT NULL DEFAULT ''");
  ensureAccountColumn('smtp_port', 'smtp_port INTEGER NOT NULL DEFAULT 465');
  ensureAccountColumn('smtp_username', "smtp_username TEXT NOT NULL DEFAULT ''");
  ensureAccountColumn('smtp_password_ref', "smtp_password_ref TEXT NOT NULL DEFAULT ''");
  ensureAccountColumn('smtp_use_tls', 'smtp_use_tls INTEGER NOT NULL DEFAULT 1');
  ensureAccountColumn('smtp_starttls', 'smtp_starttls INTEGER NOT NULL DEFAULT 0');
  ensureAccountColumn('send_enabled', 'send_enabled INTEGER NOT NULL DEFAULT 1');
  upsertMeta(db, 'schema_version', String(SCHEMA_VERSION));
}

function parseDateTs(value = '') {
  const ts = Date.parse(String(value || ''));
  return Number.isFinite(ts) ? ts : 0;
}

function normalizeAccountRecord(row = {}) {
  return {
    id: String(row.id || '').trim(),
    label: String(row.label || '').trim(),
    email: String(row.email || '').trim(),
    host: String(row.host || '').trim(),
    port: Math.max(1, Math.round(Number(row.port || 993))),
    username: String(row.username || '').trim(),
    mailbox: String(row.mailbox || 'INBOX').trim() || 'INBOX',
    use_tls: !!Number(row.use_tls || 0),
    password_ref: String(row.password_ref || '').trim(),
    smtp_host: String(row.smtp_host || '').trim(),
    smtp_port: Math.max(1, Math.round(Number(row.smtp_port || 465))),
    smtp_username: String(row.smtp_username || '').trim(),
    smtp_password_ref: String(row.smtp_password_ref || '').trim(),
    smtp_use_tls: !!Number(row.smtp_use_tls || 0),
    smtp_starttls: !!Number(row.smtp_starttls || 0),
    send_enabled: !Object.prototype.hasOwnProperty.call(row, 'send_enabled') || !!Number(row.send_enabled || 0),
    sync_enabled: !!Number(row.sync_enabled || 0),
    sync_limit: Math.max(1, Math.min(500, Math.round(Number(row.sync_limit || 200)))),
    last_sync_at: Number(row.last_sync_at || 0),
    last_error: String(row.last_error || '').trim(),
    created_at: Number(row.created_at || 0),
    updated_at: Number(row.updated_at || 0),
  };
}

function threadIdForAccount(accountId = '', threadKey = '') {
  return crypto.createHash('sha1').update(`${String(accountId || '')}:${String(threadKey || '')}`).digest('hex');
}

function buildParsedMessage(account = {}, uid = 0, rawSource = '', mailboxName = '') {
  const parsed = parseRawEmailText(rawSource, `imap://${account.id}/${uid}`);
  const threadKey = String(
    computeMailConversationKey(parsed, String(account.email || '').trim().toLowerCase())
  ).trim() || crypto.createHash('sha1').update(rawSource).digest('hex');
  const sentTs = parseDateTs(parsed.sent_at);
  return {
    account_id: String(account.id || '').trim(),
    uid: Math.round(Number(uid || 0)),
    thread_key: threadKey,
    thread_id: threadIdForAccount(account.id, threadKey),
    mailbox: String(mailboxName || account.mailbox || 'INBOX').trim() || 'INBOX',
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
    raw_source: String(rawSource || ''),
    attachment_count: Array.isArray(parsed.attachments) ? parsed.attachments.length : 0,
    searchable_text: normalizeWhitespace([
      parsed.subject,
      parsed.from,
      ...(Array.isArray(parsed.to) ? parsed.to : []),
      ...(Array.isArray(parsed.cc) ? parsed.cc : []),
      parsed.snippet,
      parsed.body_text,
    ].join(' ')).toLowerCase(),
    created_at: nowTs(),
    updated_at: nowTs(),
  };
}

function inferSmtpHost(host = '') {
  const clean = String(host || '').trim();
  if (!clean) return '';
  if (/^imap\./i.test(clean)) return clean.replace(/^imap\./i, 'smtp.');
  return clean;
}

function normalizeComposeRecipients(input = {}) {
  return {
    to: parseRecipients(JSON.stringify({ to: input.to })).to,
    cc: parseRecipients(JSON.stringify({ cc: input.cc })).cc,
    bcc: parseRecipients(JSON.stringify({ bcc: input.bcc })).bcc,
  };
}

function normalizeMailboxName(value = '') {
  return String(value || '').trim().toLowerCase();
}

function getMailboxCandidates(primaryMailbox = '') {
  const ordered = [];
  const seen = new Set();
  const push = (value) => {
    const mailbox = String(value || '').trim();
    if (!mailbox) return;
    const key = normalizeMailboxName(mailbox);
    if (seen.has(key)) return;
    seen.add(key);
    ordered.push(mailbox);
  };
  push(primaryMailbox || 'INBOX');
  [
    'INBOX',
    'Sent',
    'Sent Messages',
    'Sent Mail',
    'Sent Items',
    'INBOX.Sent',
    'INBOX.Sent Messages',
    '[Gmail]/Sent Mail',
  ].forEach(push);
  return ordered;
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

function selectRows(db, sql, bind = []) {
  const stmt = db.prepare(sql);
  stmt.bind(bind);
  const rows = [];
  while (stmt.step()) rows.push(stmt.getAsObject());
  stmt.free();
  return rows;
}

function upsertAccount(db, account = {}) {
  const stmt = db.prepare(`
    INSERT INTO accounts (
      id, label, email, host, port, username, mailbox, use_tls, password_ref,
      smtp_host, smtp_port, smtp_username, smtp_password_ref, smtp_use_tls, smtp_starttls, send_enabled,
      sync_enabled, sync_limit, last_sync_at, last_error, created_at, updated_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
      updated_at = excluded.updated_at
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
    account.password_ref,
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
  ]);
  stmt.step();
  stmt.free();
}

function upsertMessage(db, message = {}) {
  const stmt = db.prepare(`
    INSERT INTO messages (
      account_id, uid, thread_id, thread_key, mailbox, message_id_header,
      in_reply_to, subject, sender, recipients, sent_at, sent_ts, snippet,
      body_text, raw_source, attachment_count, searchable_text, created_at, updated_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(account_id, uid) DO UPDATE SET
      thread_id = excluded.thread_id,
      thread_key = excluded.thread_key,
      mailbox = excluded.mailbox,
      message_id_header = excluded.message_id_header,
      in_reply_to = excluded.in_reply_to,
      subject = excluded.subject,
      sender = excluded.sender,
      recipients = excluded.recipients,
      sent_at = excluded.sent_at,
      sent_ts = excluded.sent_ts,
      snippet = excluded.snippet,
      body_text = excluded.body_text,
      raw_source = excluded.raw_source,
      attachment_count = excluded.attachment_count,
      searchable_text = excluded.searchable_text,
      updated_at = excluded.updated_at
  `);
  stmt.bind([
    message.account_id,
    message.uid,
    message.thread_id,
    message.thread_key,
    message.mailbox,
    message.message_id_header,
    message.in_reply_to,
    message.subject,
    message.sender,
    message.recipients,
    message.sent_at,
    message.sent_ts,
    message.snippet,
    message.body_text,
    message.raw_source,
    message.attachment_count,
    message.searchable_text,
    message.created_at,
    message.updated_at,
  ]);
  stmt.step();
  stmt.free();
}

function groupThreads(rows = [], accountsById = new Map()) {
  const grouped = new Map();
  rows.forEach((row) => {
    const threadId = String(row.thread_id || '').trim();
    if (!threadId) return;
    const current = grouped.get(threadId) || {
      id: threadId,
      account_id: String(row.account_id || '').trim(),
      account_label: '',
      mailbox: String(row.mailbox || '').trim(),
      subject: String(row.subject || '').trim(),
      from: String(row.sender || '').trim(),
      snippet: String(row.snippet || '').trim(),
      message_count: 0,
      attachment_count: 0,
      last_message_at: Number(row.sent_ts || 0),
    };
    current.message_count += 1;
    current.attachment_count += Math.max(0, Number(row.attachment_count || 0));
    if (Number(row.sent_ts || 0) >= current.last_message_at) {
      current.last_message_at = Number(row.sent_ts || 0);
      current.subject = String(row.subject || '').trim() || current.subject;
      current.from = String(row.sender || '').trim() || current.from;
      current.snippet = String(row.snippet || '').trim() || current.snippet;
    }
    grouped.set(threadId, current);
  });
  return [...grouped.values()]
    .map((thread) => ({
      ...thread,
      account_label: String(((accountsById.get(thread.account_id) || {}).label) || '').trim(),
    }))
    .sort((a, b) => Number(b.last_message_at || 0) - Number(a.last_message_at || 0));
}

function createMailStore(options = {}) {
  const userDataPath = String(options.userDataPath || process.cwd()).trim();
  const dbPath = path.join(userDataPath, 'mail_store.sqlite');
  const getSecretByRef = options.getSecretByRef;
  const setSecret = options.setSecret;
  const clearSecret = options.clearSecret;

  async function withDatabase(taskFn) {
    return withDbLock(dbPath, async () => {
      const db = await openDatabase(dbPath);
      try {
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
        const normalized = normalizeAccountRecord(row);
        return {
          ...normalized,
          password_configured: !!normalized.password_ref,
          smtp_password_configured: !!normalized.smtp_password_ref,
        };
      })
    ));
  }

  async function saveAccount(input = {}) {
    const id = String(input.id || makeId('mailacct')).trim() || makeId('mailacct');
    const password = String(input.password || '');
    const smtpPassword = String(input.smtp_password || '');
    const existingList = await listAccounts();
    const existing = existingList.find((item) => item.id === id) || null;
    let passwordRef = String((existing && existing.password_ref) || '').trim();
    if (password) {
      const setRes = setSecret(passwordRef, password, 'mailbox');
      if (!setRes || !setRes.ok) return { ok: false, message: (setRes && setRes.message) || 'Unable to store mailbox password.' };
      passwordRef = String(setRes.ref || '').trim();
    }
    if (!passwordRef) return { ok: false, message: 'Mailbox password is required.' };
    let smtpPasswordRef = String((existing && existing.smtp_password_ref) || '').trim();
    if (smtpPassword) {
      const setRes = setSecret(smtpPasswordRef, smtpPassword, 'mailbox-smtp');
      if (!setRes || !setRes.ok) return { ok: false, message: (setRes && setRes.message) || 'Unable to store SMTP password.' };
      smtpPasswordRef = String(setRes.ref || '').trim();
    }
    if (!smtpPasswordRef) smtpPasswordRef = passwordRef;
    const smtpHost = String(input.smtp_host || (existing && existing.smtp_host) || inferSmtpHost(input.host || (existing && existing.host) || '')).trim();
    const smtpPort = Math.max(1, Math.round(Number(
      input.smtp_port
      || (existing && existing.smtp_port)
      || (Number(input.port || (existing && existing.port) || 993) === 587 ? 587 : 465)
    )));
    const smtpUseTls = Object.prototype.hasOwnProperty.call(input, 'smtp_use_tls')
      ? !!input.smtp_use_tls
      : ((existing && existing.smtp_use_tls) || smtpPort === 465);
    const smtpStarttls = Object.prototype.hasOwnProperty.call(input, 'smtp_starttls')
      ? !!input.smtp_starttls
      : ((existing && existing.smtp_starttls) || (!smtpUseTls && smtpPort === 587));
    const account = {
      id,
      label: String(input.label || input.email || '').trim(),
      email: String(input.email || '').trim(),
      host: String(input.host || '').trim(),
      port: Math.max(1, Math.round(Number(input.port || 993))),
      username: String(input.username || input.email || '').trim(),
      mailbox: String(input.mailbox || 'INBOX').trim() || 'INBOX',
      use_tls: input.use_tls !== false,
      password_ref: passwordRef,
      smtp_host: smtpHost,
      smtp_port: smtpPort,
      smtp_username: String(input.smtp_username || (existing && existing.smtp_username) || input.username || input.email || '').trim(),
      smtp_password_ref: smtpPasswordRef,
      smtp_use_tls: smtpUseTls,
      smtp_starttls: smtpStarttls,
      send_enabled: input.send_enabled !== false,
      sync_enabled: input.sync_enabled !== false,
      sync_limit: Math.max(1, Math.min(500, Math.round(Number(input.sync_limit || 200)))),
      last_sync_at: Number((existing && existing.last_sync_at) || 0),
      last_error: String((existing && existing.last_error) || '').trim(),
      created_at: Number((existing && existing.created_at) || nowTs()),
      updated_at: nowTs(),
    };
    if (!account.label || !account.email || !account.host || !account.username) {
      return { ok: false, message: 'Label, email, host, and username are required.' };
    }
    await withDatabase(async (db) => {
      upsertAccount(db, account);
    });
    return {
      ok: true,
      account: {
        ...account,
        password_configured: true,
        smtp_password_configured: !!account.smtp_password_ref,
      },
    };
  }

  async function deleteAccount(accountId = '') {
    const id = String(accountId || '').trim();
    if (!id) return { ok: false, message: 'Account id is required.' };
    const accounts = await listAccounts();
    const existing = accounts.find((item) => item.id === id);
    if (!existing) return { ok: true, missing: true };
    if (existing.password_ref) clearSecret(existing.password_ref);
    if (existing.smtp_password_ref && existing.smtp_password_ref !== existing.password_ref) clearSecret(existing.smtp_password_ref);
    await withDatabase(async (db) => {
      const stmt = db.prepare('DELETE FROM accounts WHERE id = ?');
      stmt.bind([id]);
      stmt.step();
      stmt.free();
    });
    return { ok: true };
  }

  async function syncAccount(accountId = '') {
    const id = String(accountId || '').trim();
    const accounts = await listAccounts();
    const account = accounts.find((item) => item.id === id);
    if (!account) return { ok: false, message: 'Mailbox account not found.' };
    const pwdRes = getSecretByRef(account.password_ref);
    if (!pwdRes || !pwdRes.ok || !pwdRes.secret) return { ok: false, message: (pwdRes && pwdRes.message) || 'Mailbox password is missing.' };
    const client = new ImapClient({ host: account.host, port: account.port, tls: account.use_tls });
    let syncedCount = 0;
    try {
      await client.connect();
      await client.login(account.username, pwdRes.secret);
      let openedMailboxCount = 0;
      for (const mailboxName of getMailboxCandidates(account.mailbox)) {
        try {
          await client.selectMailbox(mailboxName);
        } catch (_) {
          continue;
        }
        openedMailboxCount += 1;
        const allUids = await client.uidSearchAll();
        const numericUids = allUids.map((item) => Math.round(Number(item || 0))).filter(Boolean);
        const targetUids = numericUids.slice(-account.sync_limit);
        const seenUids = new Set(numericUids);
        await withDatabase(async (db) => {
          const existingRows = selectRows(
            db,
            'SELECT uid FROM messages WHERE account_id = ? AND LOWER(mailbox) = LOWER(?)',
            [account.id, mailboxName]
          );
          existingRows.forEach((row) => {
            const uid = Math.round(Number(row.uid || 0));
            if (uid && !seenUids.has(uid)) {
              const delStmt = db.prepare('DELETE FROM messages WHERE account_id = ? AND uid = ? AND LOWER(mailbox) = LOWER(?)');
              delStmt.bind([account.id, uid, mailboxName]);
              delStmt.step();
              delStmt.free();
            }
          });
          for (const uid of targetUids) {
            const rawSource = await client.fetchRawMessage(uid);
            const message = buildParsedMessage(account, uid, rawSource, mailboxName);
            upsertMessage(db, message);
            syncedCount += 1;
          }
        });
      }
      if (!openedMailboxCount) {
        throw new Error(`Unable to open mailbox "${account.mailbox}" or any sent mailbox for this account.`);
      }
      await withDatabase(async (db) => {
        upsertAccount(db, {
          ...account,
          last_sync_at: nowTs(),
          last_error: '',
          updated_at: nowTs(),
        });
      });
      return { ok: true, synced_count: syncedCount };
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
    return withDatabase(async (db) => {
      const accounts = selectRows(db, 'SELECT * FROM accounts WHERE sync_enabled = 1 ORDER BY label COLLATE NOCASE ASC');
      const accountsById = new Map(accounts.map((row) => {
        const normalized = normalizeAccountRecord(row);
        return [normalized.id, normalized];
      }));
      const rows = selectRows(
        db,
        'SELECT * FROM messages ORDER BY sent_ts DESC, updated_at DESC'
      );
      const filtered = query
        ? rows.filter((row) => String(row.searchable_text || '').includes(query))
        : rows;
      const threads = groupThreads(filtered, accountsById).slice(0, limit);
      return {
        ok: true,
        items: threads,
        total: threads.length,
      };
    });
  }

  async function getThread(threadId = '') {
    const id = String(threadId || '').trim();
    if (!id) return { ok: false, message: 'threadId is required.' };
    return withDatabase(async (db) => {
      const rows = selectRows(
        db,
        'SELECT m.*, a.label AS account_label, a.email AS account_email FROM messages m JOIN accounts a ON a.id = m.account_id WHERE m.thread_id = ? ORDER BY m.sent_ts ASC, m.uid ASC',
        [id]
      );
      if (!rows.length) return { ok: false, message: 'Thread not found.' };
      return {
        ok: true,
        thread: {
          id,
          account_id: String(rows[0].account_id || '').trim(),
          account_email: String(rows[0].account_email || '').trim(),
          subject: String(rows[rows.length - 1].subject || '').trim(),
          account_label: String(rows[0].account_label || '').trim(),
          mailbox: String(rows[0].mailbox || '').trim(),
          messages: rows.map((row) => ({
            recipients: parseRecipients(row.recipients),
            id: `${String(row.account_id || '').trim()}:${String(row.uid || '').trim()}`,
            account_id: String(row.account_id || '').trim(),
            account_email: String(row.account_email || '').trim(),
            source_path: '',
            source_key: '',
            mail_message_id: '',
            account_name: String(row.account_label || '').trim(),
            mailbox_name: String(row.mailbox || '').trim(),
            message_id_header: String(row.message_id_header || '').trim(),
            in_reply_to: String(row.in_reply_to || '').trim(),
            references: [],
            from: String(row.sender || '').trim(),
            to: parseRecipients(row.recipients).to,
            cc: parseRecipients(row.recipients).cc,
            bcc: parseRecipients(row.recipients).bcc,
            subject: String(row.subject || '').trim(),
            sent_at: String(row.sent_at || '').trim(),
            body_text: String(row.body_text || '').trim(),
            body_html: '',
            snippet: String(row.snippet || '').trim(),
            attachments: [],
            raw_source: String(row.raw_source || ''),
          })),
        },
      };
    });
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
    const smtpSecretRef = String(account.smtp_password_ref || account.password_ref || '').trim();
    const smtpSecret = getSecretByRef(smtpSecretRef);
    if (!smtpSecret || !smtpSecret.ok || !smtpSecret.secret) {
      return { ok: false, message: (smtpSecret && smtpSecret.message) || 'SMTP password is missing.' };
    }

    const recipients = normalizeComposeRecipients(payload || {});
    const fromAddress = String((payload && payload.from) || account.email || '').trim();
    const subject = String((payload && payload.subject) || '').trim();
    const bodyText = String((payload && payload.body_text) || '');
    if (!fromAddress) return { ok: false, message: 'From address is required.' };
    if (!recipients.to.length && !recipients.cc.length && !recipients.bcc.length) {
      return { ok: false, message: 'At least one recipient is required.' };
    }

    const sendRes = await sendMailViaSmtp({
      host: smtpHost,
      port: account.smtp_port || 465,
      secure: !!account.smtp_use_tls,
      starttls: !!account.smtp_starttls,
      username: String(account.smtp_username || account.username || account.email || '').trim(),
      password: smtpSecret.secret,
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
    });

    const localUid = -Math.max(1, nowTs());
    const rawWithTerminator = String((sendRes && sendRes.raw_source) || '');
    await withDatabase(async (db) => {
      const message = buildParsedMessage(account, localUid, rawWithTerminator, 'Sent');
      upsertMessage(db, message);
    });

    return {
      ok: true,
      account,
      message_id_header: String((sendRes && sendRes.message_id_header) || '').trim(),
      sent_at: String((sendRes && sendRes.sent_at) || '').trim(),
    };
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
    exportThreads,
    getThread,
    listAccounts,
    saveAccount,
    searchThreads,
    sendMail,
    syncAccount,
  };
}

module.exports = {
  createMailStore,
};
