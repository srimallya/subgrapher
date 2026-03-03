const fs = require('fs');
const path = require('path');
const zlib = require('zlib');
const { spawnSync } = require('child_process');

const TEXT_EXTENSIONS = new Set([
  '.txt', '.md', '.markdown', '.json', '.yaml', '.yml', '.toml', '.ini', '.cfg',
  '.js', '.ts', '.tsx', '.jsx', '.mjs', '.cjs', '.py', '.go', '.rs', '.java', '.kt',
  '.c', '.cc', '.cpp', '.h', '.hpp', '.cs', '.rb', '.php', '.swift', '.sql', '.sh',
  '.html', '.css', '.scss', '.xml', '.csv', '.tsv', '.log', '.rst', '.svg',
]);

const IMAGE_EXTENSIONS = new Set([
  '.png', '.jpg', '.jpeg', '.gif', '.webp', '.bmp', '.tif', '.tiff', '.heic', '.heif',
]);

const BINARY_DOCUMENT_EXTENSIONS = new Set([
  '.pdf', '.doc', '.docx', '.xls', '.xlsx', '.ppt', '.pptx', '.rtf',
  '.odt', '.ods', '.odp', '.pages', '.numbers', '.key', '.msg', '.eml',
]);

const ALLOWED_CONTEXT_EXTENSIONS = new Set([
  ...Array.from(TEXT_EXTENSIONS),
  ...Array.from(IMAGE_EXTENSIONS),
  ...Array.from(BINARY_DOCUMENT_EXTENSIONS),
]);

function normalizeExt(ext = '') {
  return String(ext || '').trim().toLowerCase();
}

function isTextExtension(ext = '') {
  return TEXT_EXTENSIONS.has(normalizeExt(ext));
}

function isImageExtension(ext = '') {
  return IMAGE_EXTENSIONS.has(normalizeExt(ext));
}

function summarizeText(content, maxLen = 320) {
  const normalized = String(content || '').replace(/\s+/g, ' ').trim();
  if (!normalized) return '';
  const limit = Math.max(80, Math.round(Number(maxLen) || 320));
  if (normalized.length <= limit) return normalized;
  return `${normalized.slice(0, limit - 3)}...`;
}

function detectMimeType(ext = '', isText = false) {
  const normalized = normalizeExt(ext);
  if (normalized === '.md' || normalized === '.markdown') return 'text/markdown';
  if (normalized === '.json') return 'application/json';
  if (normalized === '.yaml' || normalized === '.yml') return 'application/yaml';
  if (normalized === '.toml') return 'application/toml';
  if (normalized === '.xml') return 'application/xml';
  if (normalized === '.csv') return 'text/csv';
  if (normalized === '.tsv') return 'text/tab-separated-values';
  if (normalized === '.html') return 'text/html';
  if (normalized === '.css' || normalized === '.scss') return 'text/css';
  if (normalized === '.svg') return 'image/svg+xml';

  if (normalized === '.pdf') return 'application/pdf';
  if (normalized === '.doc') return 'application/msword';
  if (normalized === '.docx') return 'application/vnd.openxmlformats-officedocument.wordprocessingml.document';
  if (normalized === '.xls') return 'application/vnd.ms-excel';
  if (normalized === '.xlsx') return 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet';
  if (normalized === '.ppt') return 'application/vnd.ms-powerpoint';
  if (normalized === '.pptx') return 'application/vnd.openxmlformats-officedocument.presentationml.presentation';
  if (normalized === '.rtf') return 'application/rtf';
  if (normalized === '.odt') return 'application/vnd.oasis.opendocument.text';
  if (normalized === '.ods') return 'application/vnd.oasis.opendocument.spreadsheet';
  if (normalized === '.odp') return 'application/vnd.oasis.opendocument.presentation';
  if (normalized === '.msg') return 'application/vnd.ms-outlook';
  if (normalized === '.eml') return 'message/rfc822';

  if (normalized === '.png') return 'image/png';
  if (normalized === '.jpg' || normalized === '.jpeg') return 'image/jpeg';
  if (normalized === '.gif') return 'image/gif';
  if (normalized === '.webp') return 'image/webp';
  if (normalized === '.bmp') return 'image/bmp';
  if (normalized === '.tif' || normalized === '.tiff') return 'image/tiff';
  if (normalized === '.heic') return 'image/heic';
  if (normalized === '.heif') return 'image/heif';

  return isText ? 'text/plain' : 'application/octet-stream';
}

function isLikelyTextMime(mimeType = '') {
  const mime = String(mimeType || '').trim().toLowerCase();
  if (!mime) return false;
  if (mime.startsWith('text/')) return true;
  if (mime === 'image/svg+xml') return true;
  return [
    'application/json',
    'application/xml',
    'application/yaml',
    'application/toml',
    'application/javascript',
    'application/x-javascript',
  ].some((prefix) => mime.startsWith(prefix));
}

function decodeXmlEntities(value = '') {
  return String(value || '')
    .replace(/&amp;/g, '&')
    .replace(/&lt;/g, '<')
    .replace(/&gt;/g, '>')
    .replace(/&quot;/g, '"')
    .replace(/&apos;/g, "'")
    .replace(/&#(\d+);/g, (_m, num) => {
      const code = Number(num);
      if (!Number.isFinite(code) || code <= 0 || code > 0x10FFFF) return '';
      try {
        return String.fromCodePoint(code);
      } catch (_) {
        return '';
      }
    })
    .replace(/&#x([0-9a-f]+);/gi, (_m, hex) => {
      const code = Number.parseInt(hex, 16);
      if (!Number.isFinite(code) || code <= 0 || code > 0x10FFFF) return '';
      try {
        return String.fromCodePoint(code);
      } catch (_) {
        return '';
      }
    });
}

function normalizeChunkText(value = '') {
  return String(value || '').replace(/\s+/g, ' ').trim();
}

function appendTextChunks(chunks = [], maxChars = 12_000) {
  const out = [];
  const seen = new Set();
  let total = 0;
  const limit = Math.max(200, Math.round(Number(maxChars) || 12_000));
  for (const chunk of (Array.isArray(chunks) ? chunks : [])) {
    const cleaned = normalizeChunkText(chunk);
    if (cleaned.length < 3) continue;
    const key = cleaned.toLowerCase();
    if (seen.has(key)) continue;
    seen.add(key);
    const remaining = limit - total;
    if (remaining <= 0) break;
    const clipped = cleaned.slice(0, remaining);
    if (!clipped) continue;
    out.push(clipped);
    total += clipped.length + 1;
  }
  return out.join('\n');
}

function parseZipCentralDirectory(buffer) {
  if (!Buffer.isBuffer(buffer) || buffer.length < 22) return [];
  const minOffset = Math.max(0, buffer.length - 66_000);
  let eocdOffset = -1;
  for (let offset = buffer.length - 22; offset >= minOffset; offset -= 1) {
    if (buffer.readUInt32LE(offset) === 0x06054b50) {
      eocdOffset = offset;
      break;
    }
  }
  if (eocdOffset < 0) return [];
  const totalEntries = buffer.readUInt16LE(eocdOffset + 10);
  const cdOffset = buffer.readUInt32LE(eocdOffset + 16);
  if (!Number.isFinite(cdOffset) || cdOffset <= 0 || cdOffset >= buffer.length) return [];
  const entries = [];
  let offset = cdOffset;
  while (offset + 46 <= buffer.length && entries.length < Math.max(1, totalEntries)) {
    if (buffer.readUInt32LE(offset) !== 0x02014b50) break;
    const compression = buffer.readUInt16LE(offset + 10);
    const compressedSize = buffer.readUInt32LE(offset + 20);
    const nameLen = buffer.readUInt16LE(offset + 28);
    const extraLen = buffer.readUInt16LE(offset + 30);
    const commentLen = buffer.readUInt16LE(offset + 32);
    const localHeaderOffset = buffer.readUInt32LE(offset + 42);
    const nameStart = offset + 46;
    const nameEnd = nameStart + nameLen;
    if (nameEnd > buffer.length) break;
    const name = buffer.toString('utf8', nameStart, nameEnd).replace(/\0/g, '');
    entries.push({
      name,
      compression,
      compressedSize,
      localHeaderOffset,
    });
    offset = nameEnd + extraLen + commentLen;
  }
  return entries;
}

function readZipEntryPayload(buffer, entry) {
  const localOffset = Number(entry && entry.localHeaderOffset);
  if (!Number.isFinite(localOffset) || localOffset < 0 || localOffset + 30 > buffer.length) return Buffer.alloc(0);
  if (buffer.readUInt32LE(localOffset) !== 0x04034b50) return Buffer.alloc(0);
  const nameLen = buffer.readUInt16LE(localOffset + 26);
  const extraLen = buffer.readUInt16LE(localOffset + 28);
  const dataStart = localOffset + 30 + nameLen + extraLen;
  const compressedSize = Number(entry && entry.compressedSize);
  if (!Number.isFinite(compressedSize) || compressedSize < 0) return Buffer.alloc(0);
  const dataEnd = dataStart + compressedSize;
  if (dataStart < 0 || dataEnd > buffer.length || dataStart >= dataEnd) return Buffer.alloc(0);
  const compressed = buffer.subarray(dataStart, dataEnd);
  const compression = Number(entry && entry.compression);
  if (compression === 0) return compressed;
  if (compression === 8) {
    try {
      return zlib.inflateRawSync(compressed);
    } catch (_) {
      return Buffer.alloc(0);
    }
  }
  return Buffer.alloc(0);
}

function extractDocxText(buffer, maxChars = 12_000) {
  const entries = parseZipCentralDirectory(buffer);
  const docEntries = entries.filter((entry) => /^word\/(document|comments|footnotes|endnotes|header\d+|footer\d+)\.xml$/i.test(String(entry.name || '')));
  if (docEntries.length === 0) return '';
  const chunks = [];
  docEntries.slice(0, 30).forEach((entry) => {
    const payload = readZipEntryPayload(buffer, entry);
    if (!payload.length) return;
    const xml = payload.toString('utf8');
    const tagMatches = xml.match(/<w:t[^>]*>[\s\S]*?<\/w:t>/g) || [];
    if (tagMatches.length > 0) {
      tagMatches.forEach((match) => {
        const value = String(match || '').replace(/^<w:t[^>]*>/i, '').replace(/<\/w:t>$/i, '');
        chunks.push(decodeXmlEntities(value));
      });
      return;
    }
    chunks.push(decodeXmlEntities(String(xml || '').replace(/<[^>]+>/g, ' ')));
  });
  return appendTextChunks(chunks, maxChars);
}

function extractPptxText(buffer, maxChars = 12_000) {
  const entries = parseZipCentralDirectory(buffer);
  const slideEntries = entries.filter((entry) => /^ppt\/slides\/slide\d+\.xml$/i.test(String(entry.name || '')));
  if (slideEntries.length === 0) return '';
  const chunks = [];
  slideEntries.slice(0, 40).forEach((entry) => {
    const payload = readZipEntryPayload(buffer, entry);
    if (!payload.length) return;
    const xml = payload.toString('utf8');
    const tagMatches = xml.match(/<a:t[^>]*>[\s\S]*?<\/a:t>/g) || [];
    if (tagMatches.length > 0) {
      tagMatches.forEach((match) => {
        const value = String(match || '').replace(/^<a:t[^>]*>/i, '').replace(/<\/a:t>$/i, '');
        chunks.push(decodeXmlEntities(value));
      });
      return;
    }
    chunks.push(decodeXmlEntities(String(xml || '').replace(/<[^>]+>/g, ' ')));
  });
  return appendTextChunks(chunks, maxChars);
}

function extractXlsxText(buffer, maxChars = 12_000) {
  const entries = parseZipCentralDirectory(buffer);
  const sheetEntries = entries.filter((entry) => /^xl\/(sharedStrings|worksheets\/sheet\d+)\.xml$/i.test(String(entry.name || '')));
  if (sheetEntries.length === 0) return '';
  const chunks = [];
  sheetEntries.slice(0, 32).forEach((entry) => {
    const payload = readZipEntryPayload(buffer, entry);
    if (!payload.length) return;
    const xml = payload.toString('utf8');
    const tagMatches = xml.match(/<(?:t|v)[^>]*>[\s\S]*?<\/(?:t|v)>/g) || [];
    if (tagMatches.length > 0) {
      tagMatches.forEach((match) => {
        const value = String(match || '').replace(/^<(?:t|v)[^>]*>/i, '').replace(/<\/(?:t|v)>$/i, '');
        chunks.push(decodeXmlEntities(value));
      });
      return;
    }
    chunks.push(decodeXmlEntities(String(xml || '').replace(/<[^>]+>/g, ' ')));
  });
  return appendTextChunks(chunks, maxChars);
}

function extractOpenDocumentText(buffer, maxChars = 12_000) {
  const entries = parseZipCentralDirectory(buffer);
  const contentEntry = entries.find((entry) => /^content\.xml$/i.test(String(entry.name || '')));
  if (!contentEntry) return '';
  const payload = readZipEntryPayload(buffer, contentEntry);
  if (!payload.length) return '';
  const xml = payload.toString('utf8');
  return appendTextChunks([decodeXmlEntities(xml.replace(/<[^>]+>/g, ' '))], maxChars);
}

function unescapePdfLiteral(value = '') {
  return String(value || '')
    .replace(/\\\r?\n/g, '')
    .replace(/\\n/g, '\n')
    .replace(/\\r/g, '\n')
    .replace(/\\t/g, '\t')
    .replace(/\\b/g, '')
    .replace(/\\f/g, '')
    .replace(/\\\(/g, '(')
    .replace(/\\\)/g, ')')
    .replace(/\\\\/g, '\\')
    .replace(/\\([0-7]{1,3})/g, (_m, oct) => {
      const code = Number.parseInt(oct, 8);
      if (!Number.isFinite(code) || code < 0 || code > 255) return '';
      return String.fromCharCode(code);
    });
}

function extractPdfTextWithPdftotext(filePath = '', maxChars = 12_000) {
  const target = String(filePath || '').trim();
  if (!target) return '';
  try {
    const probe = spawnSync('pdftotext', ['-v'], { encoding: 'utf8', timeout: 2000 });
    const probeStatus = Number(probe && probe.status);
    const probeOk = probeStatus === 0 || probeStatus === 1; // pdftotext -v commonly returns 0/1 by build
    if (!probeOk) return '';
  } catch (_) {
    return '';
  }
  try {
    const run = spawnSync(
      'pdftotext',
      ['-q', '-enc', 'UTF-8', '-layout', target, '-'],
      { encoding: 'utf8', maxBuffer: 8 * 1024 * 1024, timeout: 10_000 },
    );
    if (!run || Number(run.status) !== 0) return '';
    const raw = String(run.stdout || '').replace(/\u0000/g, '');
    const normalized = raw.replace(/\r/g, '').trim();
    if (!normalized) return '';
    return normalized.slice(0, Math.max(200, Math.round(Number(maxChars) || 12_000)));
  } catch (_) {
    return '';
  }
}

function extractPdfText(buffer, maxChars = 12_000, options = {}) {
  const filePath = String((options && options.filePath) || '').trim();
  const external = extractPdfTextWithPdftotext(filePath, maxChars);
  if (external) return external;
  if (!Buffer.isBuffer(buffer) || buffer.length === 0) return '';
  const text = buffer.toString('latin1');
  const matches = text.match(/\((?:\\.|[^\\()]){4,}\)/g) || [];
  const chunks = matches.map((match) => {
    const inner = String(match || '').slice(1, -1);
    return unescapePdfLiteral(inner);
  }).filter((item) => /[A-Za-z]/.test(String(item || '')));
  const combined = appendTextChunks(chunks, maxChars);
  if (combined) return combined;
  return extractBinaryTextFragments(buffer, maxChars);
}

function extractRtfText(buffer, maxChars = 12_000) {
  const raw = Buffer.isBuffer(buffer) ? buffer.toString('utf8') : '';
  if (!raw) return '';
  const cleaned = raw
    .replace(/\\'[0-9a-fA-F]{2}/g, ' ')
    .replace(/\\[a-z]+-?\d* ?/g, ' ')
    .replace(/[{}]/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
  return cleaned.slice(0, Math.max(200, Math.round(Number(maxChars) || 12_000)));
}

function extractBinaryTextFragments(buffer, maxChars = 6_000) {
  const sample = Buffer.isBuffer(buffer)
    ? buffer.subarray(0, Math.min(buffer.length, 220 * 1024))
    : Buffer.alloc(0);
  if (!sample.length) return '';
  const text = sample.toString('latin1');
  const matches = text.match(/[A-Za-z0-9][A-Za-z0-9_.,:/@%$()\- ]{5,}/g) || [];
  const rows = [];
  const seen = new Set();
  for (const item of matches) {
    const clean = normalizeChunkText(item);
    if (clean.length < 6) continue;
    const key = clean.toLowerCase();
    if (seen.has(key)) continue;
    seen.add(key);
    rows.push(clean);
    if (rows.length >= 120) break;
  }
  return appendTextChunks(rows, maxChars);
}

function readPngSize(buffer) {
  if (!Buffer.isBuffer(buffer) || buffer.length < 24) return null;
  const signature = '89504e470d0a1a0a';
  if (buffer.subarray(0, 8).toString('hex') !== signature) return null;
  return {
    width: buffer.readUInt32BE(16),
    height: buffer.readUInt32BE(20),
  };
}

function readGifSize(buffer) {
  if (!Buffer.isBuffer(buffer) || buffer.length < 10) return null;
  const header = buffer.toString('ascii', 0, 6);
  if (header !== 'GIF87a' && header !== 'GIF89a') return null;
  return {
    width: buffer.readUInt16LE(6),
    height: buffer.readUInt16LE(8),
  };
}

function readBmpSize(buffer) {
  if (!Buffer.isBuffer(buffer) || buffer.length < 26) return null;
  if (buffer.toString('ascii', 0, 2) !== 'BM') return null;
  return {
    width: Math.abs(buffer.readInt32LE(18)),
    height: Math.abs(buffer.readInt32LE(22)),
  };
}

function readJpegSize(buffer) {
  if (!Buffer.isBuffer(buffer) || buffer.length < 4) return null;
  if (buffer[0] !== 0xFF || buffer[1] !== 0xD8) return null;
  let offset = 2;
  while (offset + 9 < buffer.length) {
    if (buffer[offset] !== 0xFF) {
      offset += 1;
      continue;
    }
    const marker = buffer[offset + 1];
    if (marker === 0xD9 || marker === 0xDA) break;
    const length = buffer.readUInt16BE(offset + 2);
    if (!Number.isFinite(length) || length < 2) break;
    const isSof = (marker >= 0xC0 && marker <= 0xC3)
      || (marker >= 0xC5 && marker <= 0xC7)
      || (marker >= 0xC9 && marker <= 0xCB)
      || (marker >= 0xCD && marker <= 0xCF);
    if (isSof && offset + 8 < buffer.length) {
      return {
        height: buffer.readUInt16BE(offset + 5),
        width: buffer.readUInt16BE(offset + 7),
      };
    }
    offset += 2 + length;
  }
  return null;
}

function readWebpSize(buffer) {
  if (!Buffer.isBuffer(buffer) || buffer.length < 30) return null;
  if (buffer.toString('ascii', 0, 4) !== 'RIFF' || buffer.toString('ascii', 8, 12) !== 'WEBP') return null;
  const chunkType = buffer.toString('ascii', 12, 16);
  if (chunkType === 'VP8X' && buffer.length >= 30) {
    return {
      width: 1 + buffer.readUIntLE(24, 3),
      height: 1 + buffer.readUIntLE(27, 3),
    };
  }
  return null;
}

function describeImageBuffer(buffer, options = {}) {
  const ext = normalizeExt(options.ext || '');
  const filename = String(options.filename || 'image').trim() || 'image';
  const mime = String(options.mimeType || detectMimeType(ext, false)).trim() || 'image';

  const size = readPngSize(buffer)
    || readJpegSize(buffer)
    || readGifSize(buffer)
    || readWebpSize(buffer)
    || readBmpSize(buffer);
  const dimText = size && Number(size.width) > 0 && Number(size.height) > 0
    ? `${size.width}x${size.height}`
    : 'size unknown';

  return `${filename} (${mime}, ${dimText}). Use image analysis for full visual understanding.`;
}

function extractContextTextFromBuffer(buffer, options = {}) {
  const filename = String(options.filename || '').trim();
  const ext = normalizeExt(options.ext || (filename ? path.extname(filename) : ''));
  const isText = isTextExtension(ext);
  const mimeType = String(options.mimeType || detectMimeType(ext, isText)).trim();
  const maxChars = Math.max(200, Math.round(Number(options.maxChars) || 12_000));

  if (!Buffer.isBuffer(buffer) || buffer.length === 0) {
    return { text: '', mode: isText ? 'text' : 'binary', strategy: 'empty' };
  }

  if (isText || isLikelyTextMime(mimeType)) {
    const raw = buffer.toString('utf8').replace(/\u0000/g, '');
    return {
      text: raw.slice(0, maxChars),
      mode: 'text',
      strategy: 'utf8-text',
    };
  }

  if (isImageExtension(ext) || (mimeType.startsWith('image/') && mimeType !== 'image/svg+xml')) {
    return {
      text: describeImageBuffer(buffer, { filename: filename || path.basename(String(options.filePath || '')) || 'image', ext, mimeType }),
      mode: 'image',
      strategy: 'image-metadata',
    };
  }

  let extracted = '';
  let strategy = 'binary-fragments';

  if (ext === '.docx') {
    extracted = extractDocxText(buffer, maxChars);
    strategy = 'docx-xml';
  } else if (ext === '.pptx') {
    extracted = extractPptxText(buffer, maxChars);
    strategy = 'pptx-xml';
  } else if (ext === '.xlsx') {
    extracted = extractXlsxText(buffer, maxChars);
    strategy = 'xlsx-xml';
  } else if (ext === '.odt' || ext === '.ods' || ext === '.odp') {
    extracted = extractOpenDocumentText(buffer, maxChars);
    strategy = 'opendocument-xml';
  } else if (ext === '.rtf') {
    extracted = extractRtfText(buffer, maxChars);
    strategy = 'rtf-strip';
  } else if (ext === '.pdf' || mimeType === 'application/pdf') {
    extracted = extractPdfText(buffer, maxChars, options);
    strategy = extracted ? 'pdf-text' : 'pdf-literal-text';
  }

  if (!extracted) {
    extracted = extractBinaryTextFragments(buffer, maxChars);
    strategy = 'binary-fragments';
  }

  if (!extracted) {
    const label = filename || path.basename(String(options.filePath || '')) || 'file';
    const type = mimeType || (ext ? ext.slice(1).toUpperCase() : 'binary');
    extracted = `${label} (${type})`;
    strategy = 'filename-only';
  }

  return {
    text: extracted,
    mode: 'binary',
    strategy,
  };
}

function extractContextTextFromFile(filePath = '', options = {}) {
  const target = String(filePath || '').trim();
  if (!target || !fs.existsSync(target)) {
    return { text: '', mode: 'binary', strategy: 'missing-file' };
  }
  let buffer = Buffer.alloc(0);
  try {
    buffer = fs.readFileSync(target);
  } catch (_) {
    return { text: '', mode: 'binary', strategy: 'read-failed' };
  }
  const ext = normalizeExt(options.ext || path.extname(target));
  const filename = String(options.filename || path.basename(target));
  const mimeType = String(options.mimeType || detectMimeType(ext, isTextExtension(ext))).trim();
  return extractContextTextFromBuffer(buffer, {
    ...options,
    filePath: target,
    ext,
    filename,
    mimeType,
  });
}

module.exports = {
  TEXT_EXTENSIONS,
  IMAGE_EXTENSIONS,
  BINARY_DOCUMENT_EXTENSIONS,
  ALLOWED_CONTEXT_EXTENSIONS,
  isTextExtension,
  isImageExtension,
  detectMimeType,
  summarizeText,
  extractBinaryTextFragments,
  extractContextTextFromBuffer,
  extractContextTextFromFile,
};
