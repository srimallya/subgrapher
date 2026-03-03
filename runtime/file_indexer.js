const fs = require('fs');
const path = require('path');
const crypto = require('crypto');

const DEFAULT_TEXT_EXTENSIONS = new Set([
  '.txt', '.md', '.markdown', '.json', '.yaml', '.yml', '.toml', '.ini', '.cfg',
  '.js', '.ts', '.tsx', '.jsx', '.mjs', '.cjs', '.py', '.go', '.rs', '.java', '.kt',
  '.c', '.cc', '.cpp', '.h', '.hpp', '.cs', '.rb', '.php', '.swift', '.sql', '.sh',
  '.html', '.css', '.scss', '.xml', '.csv', '.tsv', '.log', '.rst'
]);

const DEFAULT_BINARY_EXTENSIONS = new Set([
  '.pdf', '.doc', '.docx', '.xls', '.xlsx', '.ppt', '.pptx', '.rtf',
  '.odt', '.ods', '.odp', '.pages', '.numbers', '.key', '.msg', '.eml',
]);

const DEFAULT_ALLOWED_EXTENSIONS = new Set([
  ...Array.from(DEFAULT_TEXT_EXTENSIONS),
  ...Array.from(DEFAULT_BINARY_EXTENSIONS),
]);

const SKIP_DIRS = new Set(['.git', '.hg', '.svn', 'node_modules', '.next', '.cache', 'dist', 'build', '__pycache__']);

function summarizeText(content) {
  const normalized = String(content || '').replace(/\s+/g, ' ').trim();
  if (!normalized) return '';
  if (normalized.length <= 320) return normalized;
  return `${normalized.slice(0, 320)}...`;
}

function detectMimeType(ext = '', isText = false) {
  if (ext === '.md' || ext === '.markdown') return 'text/markdown';
  if (ext === '.json') return 'application/json';
  if (ext === '.yaml' || ext === '.yml') return 'application/yaml';
  if (ext === '.toml') return 'application/toml';
  if (ext === '.xml') return 'application/xml';
  if (ext === '.csv') return 'text/csv';
  if (ext === '.tsv') return 'text/tab-separated-values';
  if (ext === '.html') return 'text/html';
  if (ext === '.css' || ext === '.scss') return 'text/css';
  if (ext === '.pdf') return 'application/pdf';
  if (ext === '.doc') return 'application/msword';
  if (ext === '.docx') return 'application/vnd.openxmlformats-officedocument.wordprocessingml.document';
  if (ext === '.xls') return 'application/vnd.ms-excel';
  if (ext === '.xlsx') return 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet';
  if (ext === '.ppt') return 'application/vnd.ms-powerpoint';
  if (ext === '.pptx') return 'application/vnd.openxmlformats-officedocument.presentationml.presentation';
  if (ext === '.rtf') return 'application/rtf';
  if (ext === '.odt') return 'application/vnd.oasis.opendocument.text';
  if (ext === '.ods') return 'application/vnd.oasis.opendocument.spreadsheet';
  if (ext === '.odp') return 'application/vnd.oasis.opendocument.presentation';
  if (ext === '.msg') return 'application/vnd.ms-outlook';
  if (ext === '.eml') return 'message/rfc822';
  return isText ? 'text/plain' : 'application/octet-stream';
}

function summarizeBinaryBuffer(buffer, filename = '', ext = '') {
  const maxProbeBytes = 160 * 1024;
  const sample = Buffer.isBuffer(buffer) ? buffer.subarray(0, Math.min(buffer.length, maxProbeBytes)) : Buffer.alloc(0);
  if (sample.length === 0) {
    return `${filename || 'file'} (${String(ext || '').replace(/^\./, '').toUpperCase() || 'BINARY'})`;
  }

  const text = sample.toString('latin1');
  const matches = text.match(/[A-Za-z0-9][A-Za-z0-9_.,:/\-@()#%$ ]{5,}/g) || [];
  const unique = [];
  const seen = new Set();
  for (const raw of matches) {
    const cleaned = String(raw || '').replace(/\s+/g, ' ').trim();
    if (cleaned.length < 6) continue;
    const key = cleaned.toLowerCase();
    if (seen.has(key)) continue;
    seen.add(key);
    unique.push(cleaned);
    if (unique.length >= 14) break;
  }
  const inferred = summarizeText(unique.join(' '));
  if (inferred) return inferred;
  return `${filename || 'file'} (${String(ext || '').replace(/^\./, '').toUpperCase() || 'BINARY'})`;
}

function indexFolderAsContext(rootPath, options = {}) {
  const absoluteRoot = path.resolve(String(rootPath || ''));
  const maxFiles = Number.isFinite(Number(options.maxFiles)) ? Math.max(1, Number(options.maxFiles)) : 250;
  const maxFileBytes = Number.isFinite(Number(options.maxFileBytes)) ? Math.max(1024, Number(options.maxFileBytes)) : 256 * 1024;
  const allowedExtensions = options.allowedExtensions instanceof Set
    ? options.allowedExtensions
    : DEFAULT_ALLOWED_EXTENSIONS;
  const textExtensions = options.textExtensions instanceof Set
    ? options.textExtensions
    : DEFAULT_TEXT_EXTENSIONS;

  const queue = [absoluteRoot];
  const files = [];
  const skipped = [];

  while (queue.length > 0 && files.length < maxFiles) {
    const current = queue.pop();
    let entries = [];
    try {
      entries = fs.readdirSync(current, { withFileTypes: true });
    } catch (_) {
      continue;
    }

    for (const entry of entries) {
      if (files.length >= maxFiles) break;
      const fullPath = path.join(current, entry.name);

      if (entry.isDirectory()) {
        if (SKIP_DIRS.has(entry.name)) continue;
        if (entry.name.startsWith('.')) continue;
        queue.push(fullPath);
        continue;
      }

      if (!entry.isFile()) continue;

      const ext = path.extname(entry.name).toLowerCase();
      if (!allowedExtensions.has(ext)) {
        skipped.push({ path: fullPath, reason: 'unsupported_extension' });
        continue;
      }

      let stat = null;
      try {
        stat = fs.statSync(fullPath);
      } catch (_) {
        skipped.push({ path: fullPath, reason: 'stat_failed' });
        continue;
      }

      if (!stat || !stat.isFile()) continue;
      if (stat.size > maxFileBytes) {
        skipped.push({ path: fullPath, reason: 'too_large' });
        continue;
      }

      let rawBuffer = null;
      try {
        rawBuffer = fs.readFileSync(fullPath);
      } catch (_) {
        skipped.push({ path: fullPath, reason: 'read_failed' });
        continue;
      }
      if (!rawBuffer || !Buffer.isBuffer(rawBuffer)) {
        skipped.push({ path: fullPath, reason: 'read_failed' });
        continue;
      }

      const relativePath = path.relative(absoluteRoot, fullPath) || entry.name;
      const isText = textExtensions.has(ext);
      let summary = '';
      if (isText) {
        summary = summarizeText(rawBuffer.toString('utf8'));
      } else {
        summary = summarizeBinaryBuffer(rawBuffer, entry.name, ext);
      }
      const hash = crypto.createHash('sha256').update(rawBuffer).digest('hex');

      files.push({
        absolute_path: fullPath,
        relative_path: relativePath,
        size_bytes: stat.size,
        mime_type: detectMimeType(ext, isText),
        summary,
        content_hash: hash,
      });
    }
  }

  return {
    root_path: absoluteRoot,
    files,
    skipped_count: skipped.length,
    skipped,
    truncated: files.length >= maxFiles,
  };
}

module.exports = {
  indexFolderAsContext,
};
