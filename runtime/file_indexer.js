const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const {
  TEXT_EXTENSIONS,
  ALLOWED_CONTEXT_EXTENSIONS,
  isTextExtension,
  detectMimeType,
  summarizeText,
  extractContextTextFromBuffer,
} = require('./context_file_support');

const SKIP_DIRS = new Set(['.git', '.hg', '.svn', 'node_modules', '.next', '.cache', 'dist', 'build', '__pycache__']);

function indexFolderAsContext(rootPath, options = {}) {
  const absoluteRoot = path.resolve(String(rootPath || ''));
  const maxFiles = Number.isFinite(Number(options.maxFiles)) ? Math.max(1, Number(options.maxFiles)) : 250;
  const maxFileBytes = Number.isFinite(Number(options.maxFileBytes)) ? Math.max(1024, Number(options.maxFileBytes)) : 256 * 1024;
  const allowedExtensions = options.allowedExtensions instanceof Set
    ? options.allowedExtensions
    : ALLOWED_CONTEXT_EXTENSIONS;
  const textExtensions = options.textExtensions instanceof Set
    ? options.textExtensions
    : TEXT_EXTENSIONS;

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
      const isText = textExtensions.has(ext) || isTextExtension(ext);
      const mimeType = detectMimeType(ext, isText);
      const extracted = extractContextTextFromBuffer(rawBuffer, {
        filename: entry.name,
        ext,
        mimeType,
        maxChars: 8_000,
      });
      const summary = summarizeText(
        String((extracted && extracted.text) || `${entry.name} (${String(ext || '').replace(/^\./, '').toUpperCase() || 'file'})`),
        560,
      );
      const hash = crypto.createHash('sha256').update(rawBuffer).digest('hex');

      files.push({
        absolute_path: fullPath,
        relative_path: relativePath,
        size_bytes: stat.size,
        mime_type: mimeType,
        summary,
        extract_strategy: String((extracted && extracted.strategy) || ''),
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
