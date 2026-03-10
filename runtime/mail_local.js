const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const { spawnSync } = require('child_process');

const LOCAL_MAIL_CACHE_TTL_MS = 5 * 60 * 1000;
const LOCAL_MAIL_MAX_SCAN_FILES = 2500;
const LOCAL_MAIL_EXTENSIONS = new Set(['.eml', '.emlx']);
const APPLE_SCRIPT_TIMEOUT_MS = 4000;
const APPLE_SCRIPT_FIELD_SEP = '\u001f';
const APPLE_SCRIPT_RECORD_SEP = '\u001e';

let localMailCache = {
  indexed_at: 0,
  root_path: '',
  files_scanned: 0,
  records: [],
  error: '',
  error_code: '',
};

function normalizeWhitespace(value = '') {
  return String(value || '').replace(/\s+/g, ' ').trim();
}

function escapeAppleScriptString(value = '') {
  return String(value || '').replace(/\\/g, '\\\\').replace(/"/g, '\\"');
}

function runAppleScript(script = '') {
  const target = String(script || '').trim();
  if (!target) return { ok: false, stdout: '', stderr: 'Missing AppleScript.', error_code: 'invalid_script' };
  try {
    const res = spawnSync('osascript', ['-e', target], {
      encoding: 'utf8',
      timeout: APPLE_SCRIPT_TIMEOUT_MS,
      maxBuffer: 8 * 1024 * 1024,
    });
    const stdout = String(res.stdout || '');
    const stderr = String(res.stderr || '').trim();
    if (res.error) {
      const code = String((res.error && res.error.code) || '').trim().toLowerCase();
      return {
        ok: false,
        stdout,
        stderr: stderr || String(res.error.message || 'AppleScript failed.'),
        error_code: code === 'etimedout' ? 'timeout' : 'osascript_failed',
      };
    }
    if (typeof res.status === 'number' && res.status !== 0) {
      const joined = `${stdout}\n${stderr}`.trim();
      const lower = joined.toLowerCase();
      const errorCode = lower.includes('not authorized') || lower.includes('automation')
        ? 'automation_denied'
        : 'osascript_failed';
      return { ok: false, stdout, stderr: joined || 'AppleScript failed.', error_code: errorCode };
    }
    return { ok: true, stdout, stderr: '', error_code: '' };
  } catch (err) {
    const message = String((err && err.message) || 'AppleScript failed.');
    return { ok: false, stdout: '', stderr: message, error_code: 'osascript_failed' };
  }
}

function parseAppleScriptList(raw = '') {
  return String(raw || '')
    .split(/\s*,\s*/)
    .map((item) => normalizeWhitespace(item))
    .filter(Boolean);
}

function parseAppleScriptRecords(raw = '', keys = []) {
  return String(raw || '')
    .split(APPLE_SCRIPT_RECORD_SEP)
    .map((record) => String(record || '').trim())
    .filter(Boolean)
    .map((record) => {
      const values = record.split(APPLE_SCRIPT_FIELD_SEP);
      const out = {};
      keys.forEach((key, index) => {
        out[key] = String(values[index] || '');
      });
      return out;
    });
}

function isPreferredMailAccount(name = '') {
  const value = normalizeWhitespace(name).toLowerCase();
  if (!value) return false;
  if (value === 'gmail') return false;
  if (value.includes('@gmail.com')) return false;
  if (value.includes('google')) return false;
  return true;
}

function prioritizeMailboxes(mailboxes = []) {
  const items = Array.isArray(mailboxes) ? mailboxes.map((item) => normalizeWhitespace(item)).filter(Boolean) : [];
  const preferred = ['INBOX', 'Inbox', 'Sent', 'Archive', 'All Mail', 'All Inboxes'];
  const order = new Map(preferred.map((name, index) => [name.toLowerCase(), index]));
  return [...items].sort((a, b) => {
    const aRank = order.has(a.toLowerCase()) ? order.get(a.toLowerCase()) : 100;
    const bRank = order.has(b.toLowerCase()) ? order.get(b.toLowerCase()) : 100;
    if (aRank !== bRank) return aRank - bRank;
    return a.localeCompare(b);
  }).slice(0, 6);
}

function decodeQuotedPrintable(value = '') {
  return String(value || '')
    .replace(/=\r?\n/g, '')
    .replace(/=([A-Fa-f0-9]{2})/g, (_m, hex) => String.fromCharCode(Number.parseInt(hex, 16)));
}

function decodeMimeWords(value = '') {
  return String(value || '').replace(/=\?([^?]+)\?([BQbq])\?([^?]*)\?=/g, (_m, _charset, encoding, content) => {
    try {
      if (String(encoding || '').toUpperCase() === 'B') {
        return Buffer.from(String(content || ''), 'base64').toString('utf8');
      }
      const qp = String(content || '').replace(/_/g, ' ');
      return decodeQuotedPrintable(qp);
    } catch (_) {
      return String(content || '');
    }
  });
}

function parseHeaderBlock(raw = '') {
  const lines = String(raw || '').replace(/\r/g, '').split('\n');
  const headers = {};
  let current = '';
  lines.forEach((line) => {
    if (!line) return;
    if (/^\s/.test(line) && current) {
      headers[current] = `${headers[current]} ${line.trim()}`.trim();
      return;
    }
    const idx = line.indexOf(':');
    if (idx <= 0) return;
    current = String(line.slice(0, idx) || '').trim().toLowerCase();
    const value = decodeMimeWords(String(line.slice(idx + 1) || '').trim());
    if (!headers[current]) {
      headers[current] = value;
    } else if (Array.isArray(headers[current])) {
      headers[current].push(value);
    } else {
      headers[current] = [headers[current], value];
    }
  });
  return headers;
}

function getHeader(headers, key) {
  const value = headers && headers[String(key || '').toLowerCase()];
  if (Array.isArray(value)) return String(value[value.length - 1] || '');
  return String(value || '');
}

function parseAddressList(value = '') {
  return String(value || '')
    .split(',')
    .map((item) => normalizeWhitespace(decodeMimeWords(item)))
    .filter(Boolean);
}

function splitHeadersAndBody(raw = '') {
  const text = String(raw || '');
  const marker = text.match(/\r?\n\r?\n/);
  if (!marker || marker.index == null) {
    return { headersRaw: text, bodyRaw: '' };
  }
  const idx = marker.index;
  const sepLen = marker[0].length;
  return {
    headersRaw: text.slice(0, idx),
    bodyRaw: text.slice(idx + sepLen),
  };
}

function getParam(value = '', key = '') {
  const pattern = new RegExp(`${String(key || '').replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}="([^"]+)"|${String(key || '').replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}=([^;\\s]+)`, 'i');
  const match = String(value || '').match(pattern);
  return String((match && (match[1] || match[2])) || '').trim();
}

function decodeBody(bodyRaw = '', encoding = '') {
  const normalized = String(encoding || '').trim().toLowerCase();
  if (normalized === 'base64') {
    try {
      return Buffer.from(String(bodyRaw || '').replace(/\s+/g, ''), 'base64');
    } catch (_) {
      return Buffer.from(String(bodyRaw || ''), 'utf8');
    }
  }
  if (normalized === 'quoted-printable') {
    return Buffer.from(decodeQuotedPrintable(String(bodyRaw || '')), 'utf8');
  }
  return Buffer.from(String(bodyRaw || ''), 'utf8');
}

function stripHtml(value = '') {
  return normalizeWhitespace(
    String(value || '')
      .replace(/<style[\s\S]*?<\/style>/gi, ' ')
      .replace(/<script[\s\S]*?<\/script>/gi, ' ')
      .replace(/<[^>]+>/g, ' ')
      .replace(/&nbsp;/gi, ' ')
      .replace(/&amp;/gi, '&')
      .replace(/&lt;/gi, '<')
      .replace(/&gt;/gi, '>')
      .replace(/&quot;/gi, '"')
  );
}

function parseMimePart(raw = '', out = null) {
  const target = out || { textParts: [], htmlParts: [], attachments: [] };
  const { headersRaw, bodyRaw } = splitHeadersAndBody(raw);
  const headers = parseHeaderBlock(headersRaw);
  const contentType = getHeader(headers, 'content-type') || 'text/plain';
  const disposition = getHeader(headers, 'content-disposition');
  const transferEncoding = getHeader(headers, 'content-transfer-encoding');
  const boundary = getParam(contentType, 'boundary');
  const mimeType = String(contentType.split(';')[0] || 'text/plain').trim().toLowerCase();

  if (mimeType.startsWith('multipart/') && boundary) {
    const marker = `--${boundary}`;
    const chunks = String(bodyRaw || '').split(marker).slice(1);
    chunks.forEach((chunk) => {
      const clean = chunk.replace(/^\r?\n/, '').replace(/\r?\n--\s*$/, '').trim();
      if (!clean || clean === '--') return;
      parseMimePart(clean, target);
    });
    return target;
  }

  const payload = decodeBody(bodyRaw, transferEncoding);
  const fileName = decodeMimeWords(getParam(disposition, 'filename') || getParam(contentType, 'name'));
  const inline = disposition.toLowerCase().startsWith('inline');
  if (fileName || disposition.toLowerCase().startsWith('attachment')) {
    target.attachments.push({
      file_name: fileName || `attachment${path.extname(fileName || '')}`,
      mime_type: mimeType || 'application/octet-stream',
      content_id: getHeader(headers, 'content-id').replace(/[<>]/g, ''),
      inline,
      data: payload,
    });
    return target;
  }

  if (mimeType === 'text/html') {
    target.htmlParts.push(payload.toString('utf8'));
    return target;
  }
  target.textParts.push(payload.toString('utf8'));
  return target;
}

function parseRawEmailText(raw = '', sourcePath = '') {
  const normalizedRaw = String(raw || '').replace(/\r\n/g, '\n');
  const { headersRaw, bodyRaw } = splitHeadersAndBody(normalizedRaw);
  const headers = parseHeaderBlock(headersRaw);
  const contentType = getHeader(headers, 'content-type') || 'text/plain';
  const parsed = parseMimePart(`${headersRaw}\n\n${bodyRaw}`);
  const textBody = normalizeWhitespace(parsed.textParts.join('\n\n'));
  const htmlBody = parsed.htmlParts.join('\n');
  const snippet = (textBody || stripHtml(htmlBody)).slice(0, 320);
  const subject = decodeMimeWords(getHeader(headers, 'subject') || path.basename(sourcePath || 'Mail'));
  const sourceKey = crypto.createHash('sha1').update(`${sourcePath}:${getHeader(headers, 'message-id')}:${subject}`).digest('hex');
  const references = [
    ...parseAddressList(getHeader(headers, 'references').replace(/[<>]/g, ' ').replace(/\s+/g, ',')),
  ];
  return {
    source_path: sourcePath,
    source_key: sourceKey,
    message_id_header: getHeader(headers, 'message-id').replace(/[<>]/g, '').trim(),
    in_reply_to: getHeader(headers, 'in-reply-to').replace(/[<>]/g, '').trim(),
    references,
    subject: normalizeWhitespace(subject),
    from: normalizeWhitespace(decodeMimeWords(getHeader(headers, 'from'))),
    to: parseAddressList(getHeader(headers, 'to')),
    cc: parseAddressList(getHeader(headers, 'cc')),
    bcc: parseAddressList(getHeader(headers, 'bcc')),
    sent_at: normalizeWhitespace(getHeader(headers, 'date')),
    body_text: textBody,
    body_html: htmlBody,
    snippet,
    attachments: parsed.attachments,
    mime_type: String(contentType.split(';')[0] || 'message/rfc822').trim().toLowerCase(),
  };
}

function parseEmailFile(filePath = '') {
  const target = String(filePath || '').trim();
  if (!target) throw new Error('filePath is required');
  const buffer = fs.readFileSync(target);
  const ext = String(path.extname(target) || '').trim().toLowerCase();
  let raw = buffer.toString('utf8');
  if (ext === '.emlx') {
    const newlineIdx = raw.indexOf('\n');
    const firstLine = newlineIdx >= 0 ? raw.slice(0, newlineIdx).trim() : '';
    const declared = Number(firstLine);
    if (Number.isFinite(declared) && declared > 0) {
      const remainder = buffer.subarray(newlineIdx + 1);
      raw = remainder.subarray(0, declared).toString('utf8');
    }
  }
  return parseRawEmailText(raw, target);
}

function listAppleMailAccounts() {
  const script = 'tell application "Mail" to get name of every account';
  const res = runAppleScript(script);
  if (!res.ok) return { ok: false, items: [], error: res.stderr, error_code: res.error_code };
  const items = parseAppleScriptList(res.stdout);
  return { ok: true, items, error: '', error_code: '' };
}

function listAppleMailboxes(accountName = '') {
  const account = String(accountName || '').trim();
  if (!account) return { ok: false, items: [], error: 'accountName is required.', error_code: 'invalid_account' };
  const script = `tell application "Mail" to get name of every mailbox of account "${escapeAppleScriptString(account)}"`;
  const res = runAppleScript(script);
  if (!res.ok) return { ok: false, items: [], error: res.stderr, error_code: res.error_code };
  const items = parseAppleScriptList(res.stdout);
  return { ok: true, items, error: '', error_code: '' };
}

function searchAppleMail(options = {}) {
  const query = normalizeWhitespace((options && options.query) || '');
  const limit = Math.max(1, Math.min(100, Math.round(Number((options && options.limit) || 40))));
  const selectedAccount = normalizeWhitespace((options && options.account_name) || '');
  const accountRes = listAppleMailAccounts();
  if (!accountRes.ok) {
    return {
      ok: false,
      items: [],
      total: 0,
      accounts: [],
      preferred_accounts: [],
      error: accountRes.error,
      error_code: accountRes.error_code,
    };
  }
  const preferredAccounts = accountRes.items.filter((item) => isPreferredMailAccount(item));
  const accounts = selectedAccount
    ? accountRes.items.filter((item) => item === selectedAccount)
    : (preferredAccounts.length ? preferredAccounts : accountRes.items);
  if (!query) {
    return {
      ok: true,
      items: [],
      total: 0,
      accounts: accountRes.items,
      preferred_accounts: preferredAccounts,
      error: '',
      error_code: '',
    };
  }

  const records = [];
  const seen = new Set();
  for (const accountName of accounts) {
    const mailboxRes = listAppleMailboxes(accountName);
    if (!mailboxRes.ok) continue;
    const mailboxNames = prioritizeMailboxes(mailboxRes.items);
    for (const mailboxName of mailboxNames) {
      if (records.length >= limit) break;
      const escapedAccount = escapeAppleScriptString(accountName);
      const escapedMailbox = escapeAppleScriptString(mailboxName);
      const escapedQuery = escapeAppleScriptString(query);
      const script = `
        tell application "Mail"
          set fieldSep to ASCII character 31
          set recordSep to ASCII character 30
          set outText to ""
          try
            set foundMessages to (messages of mailbox "${escapedMailbox}" of account "${escapedAccount}" whose subject contains "${escapedQuery}" or sender contains "${escapedQuery}")
          on error
            set foundMessages to {}
          end try
          repeat with m in foundMessages
            try
              set msgIdText to (id of m as text)
              set msgSubject to (subject of m as text)
              set msgSender to (sender of m as text)
              set msgDate to ""
              try
                set msgDate to (date sent of m as text)
              end try
              set msgContent to ""
              try
                set msgContent to (content of m as text)
              end try
              set outText to outText & msgIdText & fieldSep & msgSubject & fieldSep & msgSender & fieldSep & msgDate & fieldSep & msgContent & recordSep
            end try
          end repeat
          return outText
        end tell
      `;
      const res = runAppleScript(script);
      if (!res.ok) continue;
      const rows = parseAppleScriptRecords(res.stdout, ['mail_message_id', 'subject', 'from', 'sent_at', 'content']);
      rows.forEach((row) => {
        const mailMessageId = normalizeWhitespace(row.mail_message_id);
        if (!mailMessageId) return;
        const dedupeKey = `${accountName}:${mailboxName}:${mailMessageId}`;
        if (seen.has(dedupeKey) || records.length >= limit) return;
        seen.add(dedupeKey);
        records.push({
          source_id: crypto.createHash('sha1').update(dedupeKey).digest('hex'),
          mail_message_id: mailMessageId,
          account_name: accountName,
          mailbox_name: mailboxName,
          source_path: '',
          subject: normalizeWhitespace(row.subject) || 'Untitled thread',
          from: normalizeWhitespace(row.from),
          to: [],
          cc: [],
          sent_at: normalizeWhitespace(row.sent_at),
          snippet: normalizeWhitespace(row.content).slice(0, 320),
          thread_key: normalizeWhitespace(`${row.subject}:${row.from}:${mailMessageId}`),
          attachment_count: 0,
          source_kind: 'apple_mail_live',
        });
      });
    }
    if (records.length >= limit) break;
  }

  return {
    ok: true,
    items: records.slice(0, limit),
    total: records.length,
    accounts: accountRes.items,
    preferred_accounts: preferredAccounts,
    error: '',
    error_code: '',
  };
}

function loadAppleMailMessageSource(source = {}) {
  const accountName = normalizeWhitespace(source.account_name);
  const mailboxName = normalizeWhitespace(source.mailbox_name);
  const mailMessageId = normalizeWhitespace(source.mail_message_id);
  if (!accountName || !mailboxName || !mailMessageId) {
    return { ok: false, raw_source: '', error: 'Mail account, mailbox, and message id are required.', error_code: 'invalid_source' };
  }
  const script = `
    tell application "Mail"
      try
        set targetMessage to item 1 of (messages of mailbox "${escapeAppleScriptString(mailboxName)}" of account "${escapeAppleScriptString(accountName)}" whose id is ${Number(mailMessageId)})
        return source of targetMessage
      on error errText
        error errText
      end try
    end tell
  `;
  const res = runAppleScript(script);
  if (!res.ok) return { ok: false, raw_source: '', error: res.stderr, error_code: res.error_code };
  return { ok: true, raw_source: String(res.stdout || ''), error: '', error_code: '' };
}

function walkMailFiles(rootPath = '', limit = LOCAL_MAIL_MAX_SCAN_FILES) {
  const root = String(rootPath || '').trim();
  if (!root || !fs.existsSync(root)) return { files: [], error: '', error_code: '' };
  const out = [];
  const stack = [root];
  let error = '';
  let errorCode = '';
  while (stack.length > 0 && out.length < limit) {
    const current = stack.pop();
    let entries = [];
    try {
      entries = fs.readdirSync(current, { withFileTypes: true });
    } catch (err) {
      const code = String((err && err.code) || '').trim().toUpperCase();
      if (!error && (code === 'EPERM' || code === 'EACCES')) {
        error = 'Subgrapher does not have permission to read Apple Mail data. Grant Full Disk Access to your dev runner and Electron.';
        errorCode = 'permission_denied';
      }
      entries = [];
    }
    entries.forEach((entry) => {
      if (out.length >= limit) return;
      const fullPath = path.join(current, entry.name);
      if (entry.isDirectory()) {
        stack.push(fullPath);
        return;
      }
      const ext = String(path.extname(entry.name) || '').toLowerCase();
      if (!LOCAL_MAIL_EXTENSIONS.has(ext)) return;
      out.push(fullPath);
    });
  }
  return { files: out, error, error_code: errorCode };
}

function buildLocalMailIndex(options = {}) {
  const rootPath = String((options && options.rootPath) || path.join(process.env.HOME || '', 'Library', 'Mail')).trim();
  const force = !!(options && options.force);
  const now = Date.now();
  if (!force && localMailCache.root_path === rootPath && (now - Number(localMailCache.indexed_at || 0)) < LOCAL_MAIL_CACHE_TTL_MS) {
    return localMailCache;
  }
  if (!rootPath || !fs.existsSync(rootPath)) {
    localMailCache = {
      indexed_at: now,
      root_path: rootPath,
      files_scanned: 0,
      records: [],
      error: 'Local Mail store not found.',
      error_code: 'not_found',
    };
    return localMailCache;
  }
  const walked = walkMailFiles(rootPath, LOCAL_MAIL_MAX_SCAN_FILES);
  const files = Array.isArray(walked.files) ? walked.files : [];
  const records = [];
  files.forEach((filePath) => {
    try {
      const parsed = parseEmailFile(filePath);
      records.push({
        source_id: String(parsed.source_key || '').trim() || crypto.createHash('sha1').update(filePath).digest('hex'),
        source_path: filePath,
        message_id_header: parsed.message_id_header,
        subject: parsed.subject,
        from: parsed.from,
        to: parsed.to,
        cc: parsed.cc,
        sent_at: parsed.sent_at,
        snippet: parsed.snippet,
        thread_key: parsed.references[parsed.references.length - 1] || parsed.in_reply_to || parsed.message_id_header || normalizeWhitespace(`${parsed.subject}:${parsed.from}`),
        attachment_count: Array.isArray(parsed.attachments) ? parsed.attachments.length : 0,
      });
    } catch (_) {
      // Skip unreadable mail files.
    }
  });
  localMailCache = {
    indexed_at: now,
    root_path: rootPath,
    files_scanned: files.length,
    records,
    error: String(walked.error || ''),
    error_code: String(walked.error_code || ''),
  };
  return localMailCache;
}

function searchLocalMail(options = {}) {
  const live = searchAppleMail(options);
  const normalizedQuery = normalizeWhitespace((options && options.query) || '');
  if (live.ok && (!live.error_code && (live.items.length > 0 || !normalizedQuery))) {
    return {
      ok: true,
      indexed_at: Date.now(),
      root_path: 'Mail.app',
      files_scanned: 0,
      total: live.total,
      items: live.items,
      accounts: live.accounts,
      preferred_accounts: live.preferred_accounts,
      error: live.error,
      error_code: live.error_code,
      source: 'mail_app',
    };
  }

  const index = buildLocalMailIndex(options);
  const query = normalizedQuery.toLowerCase();
  const limit = Math.max(1, Math.min(100, Math.round(Number((options && options.limit) || 40))));
  const records = Array.isArray(index.records) ? index.records : [];
  const filtered = query
    ? records.filter((record) => {
      const blob = [
        record.subject,
        record.from,
        ...(Array.isArray(record.to) ? record.to : []),
        ...(Array.isArray(record.cc) ? record.cc : []),
        record.snippet,
      ].join(' ').toLowerCase();
      return blob.includes(query);
    })
    : records;
  return {
    ok: true,
    indexed_at: index.indexed_at,
    root_path: index.root_path,
    files_scanned: index.files_scanned,
    total: filtered.length,
    items: filtered.slice(0, limit),
    accounts: [],
    preferred_accounts: [],
    error: index.error,
    error_code: index.error_code,
    source: 'local_store',
  };
}

module.exports = {
  buildLocalMailIndex,
  listAppleMailAccounts,
  loadAppleMailMessageSource,
  parseEmailFile,
  parseRawEmailText,
  searchLocalMail,
};
