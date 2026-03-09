const fs = require('fs');
const path = require('path');
const crypto = require('crypto');

const LOCAL_MAIL_CACHE_TTL_MS = 5 * 60 * 1000;
const LOCAL_MAIL_MAX_SCAN_FILES = 2500;
const LOCAL_MAIL_EXTENSIONS = new Set(['.eml', '.emlx']);

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
  const index = buildLocalMailIndex(options);
  const query = normalizeWhitespace((options && options.query) || '').toLowerCase();
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
    error: index.error,
    error_code: index.error_code,
  };
}

module.exports = {
  buildLocalMailIndex,
  parseEmailFile,
  parseRawEmailText,
  searchLocalMail,
};
