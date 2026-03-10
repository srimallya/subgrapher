const path = require('path');
const crypto = require('crypto');

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
    return Buffer.from(decodeQuotedPrintable(String(bodyRaw || '')), 'utf8');
  }
  return Buffer.from(String(bodyRaw || ''), 'utf8');
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
      file_name: fileName || 'attachment',
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
  const textBody = normalizeTextBody(parsed.textParts.join('\n\n'));
  const htmlBody = parsed.htmlParts.join('\n');
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

function normalizeMailSubject(value = '') {
  return normalizeWhitespace(String(value || '').replace(/^(?:(?:re|fw|fwd)\s*:\s*)+/i, '')).toLowerCase();
}

function computeMailConversationKey(parsed = {}, fallbackSeed = '') {
  const references = Array.isArray(parsed.references)
    ? parsed.references.map((item) => String(item || '').trim()).filter(Boolean)
    : [];
  const rootReference = references[0] || '';
  if (rootReference) return rootReference;

  const replyParent = String(parsed.in_reply_to || '').trim();
  if (replyParent) return replyParent;

  const messageId = String(parsed.message_id_header || '').trim();
  if (messageId) return messageId;

  const normalizedSubject = normalizeMailSubject(parsed.subject || '');
  const participants = Array.from(new Set([
    String(parsed.from || '').trim().toLowerCase(),
    ...(Array.isArray(parsed.to) ? parsed.to : []).map((item) => String(item || '').trim().toLowerCase()),
    ...(Array.isArray(parsed.cc) ? parsed.cc : []).map((item) => String(item || '').trim().toLowerCase()),
  ].filter(Boolean))).sort();
  const heuristicKey = [normalizedSubject, participants.join('|'), String(fallbackSeed || '').trim()].filter(Boolean).join('::');
  return heuristicKey || 'mail-conversation';
}

module.exports = {
  computeMailConversationKey,
  decodeMimeWords,
  normalizeMailSubject,
  normalizeTextBody,
  normalizeWhitespace,
  parseRawEmailText,
};
