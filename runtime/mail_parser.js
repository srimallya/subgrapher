const path = require('path');
const crypto = require('crypto');
const { TextDecoder } = require('util');

function normalizeWhitespace(value = '') {
  return String(value || '').replace(/\s+/g, ' ').trim();
}

function normalizeTextBody(value = '') {
  return String(value || '')
    .replace(/\r\n/g, '\n')
    .replace(/\r/g, '\n')
    .replace(/[ \t]+\n/g, '\n')
    .replace(/\n{3,}/g, '\n\n')
    .split('\n')
    .map((line) => line.replace(/[ \t]+$/g, ''))
    .join('\n')
    .trim();
}

function decodeQuotedPrintable(value = '') {
  return String(value || '')
    .replace(/=\r?\n/g, '')
    .replace(/=([A-Fa-f0-9]{2})/g, (_m, hex) => String.fromCharCode(Number.parseInt(hex, 16)));
}

function decodeQuotedPrintableToBuffer(value = '') {
  const input = String(value || '').replace(/=\r?\n/g, '');
  const bytes = [];
  for (let index = 0; index < input.length; index += 1) {
    const char = input[index];
    if (char === '=' && /^[A-Fa-f0-9]{2}$/.test(input.slice(index + 1, index + 3))) {
      bytes.push(Number.parseInt(input.slice(index + 1, index + 3), 16));
      index += 2;
      continue;
    }
    bytes.push(input.charCodeAt(index) & 0xff);
  }
  return Buffer.from(bytes);
}

function normalizeCharsetLabel(value = '') {
  const raw = String(value || '').trim().toLowerCase();
  if (!raw) return 'utf-8';
  if (raw === 'utf8') return 'utf-8';
  if (raw === 'us-ascii' || raw === 'ascii') return 'utf-8';
  if (raw === 'latin1' || raw === 'latin-1' || raw === 'iso-8859-1' || raw === 'iso8859-1') return 'iso-8859-1';
  if (raw === 'cp1252' || raw === 'windows1252' || raw === 'windows-1252') return 'windows-1252';
  if (raw === 'utf16' || raw === 'utf-16') return 'utf-16le';
  return raw;
}

function decodeWindows1252Buffer(buffer) {
  const chars = [];
  const table = {
    0x80: 0x20ac,
    0x82: 0x201a,
    0x83: 0x0192,
    0x84: 0x201e,
    0x85: 0x2026,
    0x86: 0x2020,
    0x87: 0x2021,
    0x88: 0x02c6,
    0x89: 0x2030,
    0x8a: 0x0160,
    0x8b: 0x2039,
    0x8c: 0x0152,
    0x8e: 0x017d,
    0x91: 0x2018,
    0x92: 0x2019,
    0x93: 0x201c,
    0x94: 0x201d,
    0x95: 0x2022,
    0x96: 0x2013,
    0x97: 0x2014,
    0x98: 0x02dc,
    0x99: 0x2122,
    0x9a: 0x0161,
    0x9b: 0x203a,
    0x9c: 0x0153,
    0x9e: 0x017e,
    0x9f: 0x0178,
  };
  for (const byte of buffer) {
    if (table[byte]) {
      chars.push(String.fromCodePoint(table[byte]));
      continue;
    }
    chars.push(String.fromCharCode(byte));
  }
  return chars.join('');
}

function mojibakeScore(value = '') {
  const text = String(value || '');
  if (!text) return 0;
  const suspicious = text.match(/(?:Ã.|Â.|â[\u0080-\u00bf€™œ\u009d€¢–—…]|ðŸ|ï¿½)/g) || [];
  const replacement = text.match(/\uFFFD/g) || [];
  return (suspicious.length * 3) + (replacement.length * 5);
}

function shouldPreferUtf8Fallback(decoded = '', utf8Candidate = '', charset = '') {
  const normalized = normalizeCharsetLabel(charset);
  if (!utf8Candidate || normalized === 'utf-8' || normalized === 'utf-16le') return false;
  if (!['iso-8859-1', 'windows-1252'].includes(normalized)) return false;
  return mojibakeScore(utf8Candidate) + 1 < mojibakeScore(decoded);
}

function decodeBufferWithCharset(buffer, charset = 'utf-8') {
  const payload = Buffer.isBuffer(buffer) ? buffer : Buffer.from(buffer || '');
  const normalized = normalizeCharsetLabel(charset);
  try {
    let decoded = '';
    if (normalized === 'windows-1252') decoded = decodeWindows1252Buffer(payload);
    else if (normalized === 'iso-8859-1') decoded = payload.toString('latin1');
    else {
      const decoder = new TextDecoder(normalized, { fatal: false });
      decoded = decoder.decode(payload);
    }
    if (shouldPreferUtf8Fallback(decoded, payload.toString('utf8'), normalized)) {
      return payload.toString('utf8');
    }
    return decoded;
  } catch (_) {
    try {
      return payload.toString('utf8');
    } catch (_) {
      return payload.toString('latin1');
    }
  }
}

function decodeMimeWords(value = '') {
  return String(value || '').replace(/=\?([^?]+)\?([BQbq])\?([^?]*)\?=/g, (_m, charset, encoding, content) => {
    try {
      const normalizedCharset = normalizeCharsetLabel(charset);
      if (String(encoding || '').toUpperCase() === 'B') {
        return decodeBufferWithCharset(Buffer.from(String(content || ''), 'base64'), normalizedCharset);
      }
      const qp = String(content || '').replace(/_/g, '=20');
      return decodeBufferWithCharset(decodeQuotedPrintableToBuffer(qp), normalizedCharset);
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
    if (!headers[current]) headers[current] = value;
    else if (Array.isArray(headers[current])) headers[current].push(value);
    else headers[current] = [headers[current], value];
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

function normalizeMessageId(value = '') {
  return normalizeWhitespace(String(value || '').replace(/[<>]/g, '')).toLowerCase();
}

function normalizeParticipantList(values = []) {
  return (Array.isArray(values) ? values : [])
    .map((item) => normalizeWhitespace(item).toLowerCase())
    .filter(Boolean)
    .sort();
}

function splitHeadersAndBody(raw = '') {
  const text = String(raw || '');
  const marker = text.match(/\r?\n\r?\n/);
  if (!marker || marker.index == null) return { headersRaw: text, bodyRaw: '' };
  const idx = marker.index;
  return {
    headersRaw: text.slice(0, idx),
    bodyRaw: text.slice(idx + marker[0].length),
  };
}

function getParam(value = '', key = '') {
  const safeKey = String(key || '').replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  const pattern = new RegExp(`${safeKey}="([^"]+)"|${safeKey}=([^;\\s]+)`, 'i');
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
    return decodeQuotedPrintableToBuffer(String(bodyRaw || ''));
  }
  return Buffer.from(String(bodyRaw || ''), 'latin1');
}

function stripHtml(value = '') {
  return normalizeTextBody(
    String(value || '')
      .replace(/<style[\s\S]*?<\/style>/gi, ' ')
      .replace(/<script[\s\S]*?<\/script>/gi, ' ')
      .replace(/<(br|\/p|\/div|\/li|\/tr|\/h[1-6])>/gi, '\n')
      .replace(/<li[^>]*>/gi, '- ')
      .replace(/<[^>]+>/g, ' ')
      .replace(/&nbsp;/gi, ' ')
      .replace(/&amp;/gi, '&')
      .replace(/&lt;/gi, '<')
      .replace(/&gt;/gi, '>')
      .replace(/&quot;/gi, '"')
      .replace(/&apos;/gi, '\'')
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
  const charset = getParam(contentType, 'charset') || 'utf-8';
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
      file_name: fileName || 'attachment',
      mime_type: mimeType || 'application/octet-stream',
      content_id: getHeader(headers, 'content-id').replace(/[<>]/g, ''),
      inline,
      data: payload,
    });
    return target;
  }
  if (mimeType === 'text/html') {
    target.htmlParts.push(decodeBufferWithCharset(payload, charset));
    return target;
  }
  target.textParts.push(decodeBufferWithCharset(payload, charset));
  return target;
}

function parseRawEmailText(raw = '', sourcePath = '') {
  const normalizedRaw = String(raw || '').replace(/\r\n/g, '\n');
  const { headersRaw, bodyRaw } = splitHeadersAndBody(normalizedRaw);
  const headers = parseHeaderBlock(headersRaw);
  const contentType = getHeader(headers, 'content-type') || 'text/plain';
  const parsed = parseMimePart(`${headersRaw}\n\n${bodyRaw}`);
  const htmlBody = parsed.htmlParts.join('\n');
  const textBody = normalizeTextBody(parsed.textParts.join('\n\n') || stripHtml(htmlBody));
  const snippet = normalizeWhitespace(textBody || stripHtml(htmlBody)).slice(0, 320);
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

function computeMailConversationKey(parsed = {}, accountEmail = '') {
  const references = Array.isArray(parsed.references)
    ? parsed.references.map((item) => normalizeMessageId(item)).filter(Boolean)
    : [];
  const inReplyTo = normalizeMessageId(parsed.in_reply_to);
  const messageId = normalizeMessageId(parsed.message_id_header);
  const canonicalRef = references[0] || inReplyTo || messageId;
  if (canonicalRef) return canonicalRef;

  const subject = normalizeWhitespace(parsed.subject || '')
    .replace(/^(?:re|fw|fwd)\s*:\s*/i, '')
    .toLowerCase();
  const participants = Array.from(new Set([
    ...normalizeParticipantList([parsed.from]),
    ...normalizeParticipantList(parsed.to),
    ...normalizeParticipantList(parsed.cc),
    normalizeWhitespace(accountEmail).toLowerCase(),
  ].filter(Boolean)));

  return normalizeWhitespace([subject, participants.join('|')].filter(Boolean).join(':'));
}

module.exports = {
  computeMailConversationKey,
  decodeMimeWords,
  normalizeTextBody,
  normalizeWhitespace,
  parseRawEmailText,
};
