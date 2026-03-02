const fs = require('fs');
const path = require('path');
const crypto = require('crypto');

const DEFAULT_ALLOWED_EXTENSIONS = new Set([
  '.txt', '.md', '.markdown', '.json', '.yaml', '.yml', '.toml', '.ini', '.cfg',
  '.js', '.ts', '.tsx', '.jsx', '.mjs', '.cjs', '.py', '.go', '.rs', '.java', '.kt',
  '.c', '.cc', '.cpp', '.h', '.hpp', '.cs', '.rb', '.php', '.swift', '.sql', '.sh',
  '.html', '.css', '.scss', '.xml', '.csv', '.tsv', '.log', '.rst'
]);

const SKIP_DIRS = new Set(['.git', '.hg', '.svn', 'node_modules', '.next', '.cache', 'dist', 'build', '__pycache__']);

function summarizeText(content) {
  const normalized = String(content || '').replace(/\s+/g, ' ').trim();
  if (!normalized) return '';
  if (normalized.length <= 320) return normalized;
  return `${normalized.slice(0, 320)}...`;
}

function indexFolderAsContext(rootPath, options = {}) {
  const absoluteRoot = path.resolve(String(rootPath || ''));
  const maxFiles = Number.isFinite(Number(options.maxFiles)) ? Math.max(1, Number(options.maxFiles)) : 250;
  const maxFileBytes = Number.isFinite(Number(options.maxFileBytes)) ? Math.max(1024, Number(options.maxFileBytes)) : 256 * 1024;
  const allowedExtensions = options.allowedExtensions instanceof Set
    ? options.allowedExtensions
    : DEFAULT_ALLOWED_EXTENSIONS;

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

      let raw = '';
      try {
        raw = fs.readFileSync(fullPath, 'utf8');
      } catch (_) {
        skipped.push({ path: fullPath, reason: 'read_failed' });
        continue;
      }

      const relativePath = path.relative(absoluteRoot, fullPath) || entry.name;
      const hash = crypto.createHash('sha256').update(raw, 'utf8').digest('hex');

      files.push({
        absolute_path: fullPath,
        relative_path: relativePath,
        size_bytes: stat.size,
        mime_type: ext === '.md' || ext === '.markdown' ? 'text/markdown' : 'text/plain',
        summary: summarizeText(raw),
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
