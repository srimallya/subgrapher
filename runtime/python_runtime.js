const fs = require('fs');
const path = require('path');
const { checkPythonRuntime } = require('./python_sandbox');

const DEFAULT_CACHE_TTL_MS = 30_000;

function normalizeRole(value) {
  const role = String(value || '').trim().toLowerCase();
  return role === 'viz' ? 'viz' : 'tool';
}

function readJsonSafe(filePath) {
  try {
    if (!filePath || !fs.existsSync(filePath)) return null;
    const raw = fs.readFileSync(filePath, 'utf8');
    return JSON.parse(raw);
  } catch (_) {
    return null;
  }
}

function listUnique(items = []) {
  const out = [];
  const seen = new Set();
  items.forEach((item) => {
    const value = String(item || '').trim();
    if (!value || seen.has(value)) return;
    seen.add(value);
    out.push(value);
  });
  return out;
}

class PythonRuntimeResolver {
  constructor(options = {}) {
    const opts = (options && typeof options === 'object') ? options : {};
    this.app = opts.app || null;
    this.projectRoot = String(opts.projectRoot || process.cwd());
    this.cacheTtlMs = Number.isFinite(Number(opts.cacheTtlMs))
      ? Math.max(5_000, Math.round(Number(opts.cacheTtlMs)))
      : DEFAULT_CACHE_TTL_MS;
    this.cache = new Map();
  }

  _isPackaged() {
    return !!(this.app && this.app.isPackaged);
  }

  _bundledRootDir() {
    if (this._isPackaged()) {
      return path.join(process.resourcesPath, 'python');
    }
    return path.join(this.projectRoot, 'build', 'bundled-python', 'current');
  }

  _manifestPath() {
    return path.join(this._bundledRootDir(), 'runtime-manifest.json');
  }

  _candidateRelPaths(role, manifest) {
    const normalizedRole = normalizeRole(role);
    const candidates = [];
    const roleEntry = manifest && manifest.roles && manifest.roles[normalizedRole];
    if (roleEntry && typeof roleEntry === 'object') {
      candidates.push(roleEntry.interpreter_rel_path);
      candidates.push(roleEntry.python_rel_path);
    }
    if (manifest && manifest.python && typeof manifest.python === 'object') {
      candidates.push(manifest.python.interpreter_rel_path);
      candidates.push(manifest.python.python_rel_path);
    }
    candidates.push(manifest && manifest.interpreter_rel_path);

    if (process.platform === 'win32') {
      candidates.push('python/install/python.exe', 'python/python.exe', 'install/python.exe');
    } else {
      candidates.push('python/install/bin/python3', 'python/bin/python3', 'install/bin/python3');
    }
    return listUnique(candidates);
  }

  _systemCandidates() {
    if (process.platform === 'win32') {
      return ['python3', 'python'];
    }
    return ['python3', 'python'];
  }

  async _checkCandidate(pythonBin, source, role, extraMessage = '') {
    const check = await checkPythonRuntime({ pythonBin, timeoutMs: 6_000 });
    return {
      ok: !!(check && check.ok),
      source,
      role: normalizeRole(role),
      python_bin: String(pythonBin || ''),
      version: String((check && check.version) || ''),
      message: check && check.ok
        ? String(extraMessage || '').trim()
        : String((check && check.message) || extraMessage || 'python unavailable'),
    };
  }

  _getCacheKey(role) {
    return normalizeRole(role);
  }

  _getCached(role) {
    const key = this._getCacheKey(role);
    const entry = this.cache.get(key);
    if (!entry || typeof entry !== 'object') return null;
    if ((Date.now() - Number(entry.ts || 0)) > this.cacheTtlMs) return null;
    return entry.value || null;
  }

  _setCached(role, value) {
    const key = this._getCacheKey(role);
    this.cache.set(key, { ts: Date.now(), value });
  }

  async resolve(role = 'tool', options = {}) {
    const normalizedRole = normalizeRole(role);
    const opts = (options && typeof options === 'object') ? options : {};
    if (!opts.bypassCache) {
      const cached = this._getCached(normalizedRole);
      if (cached) return cached;
    }

    const packaged = this._isPackaged();
    const bundledRoot = this._bundledRootDir();
    const manifestPath = this._manifestPath();
    const manifest = readJsonSafe(manifestPath);
    const bundledErrors = [];

    if (fs.existsSync(bundledRoot)) {
      const relPaths = this._candidateRelPaths(normalizedRole, manifest);
      for (const rel of relPaths) {
        const abs = path.join(bundledRoot, rel);
        if (!fs.existsSync(abs)) continue;
        const check = await this._checkCandidate(abs, 'bundled', normalizedRole, '');
        if (check.ok) {
          this._setCached(normalizedRole, check);
          return check;
        }
        bundledErrors.push(`bundled interpreter failed (${abs}): ${check.message}`);
      }
      if (bundledErrors.length === 0) {
        bundledErrors.push(`bundled runtime found at ${bundledRoot} but no interpreter candidate matched`);
      }
    } else if (packaged) {
      bundledErrors.push(`bundled runtime missing at ${bundledRoot}`);
    }

    const systemCandidates = this._systemCandidates();
    for (const candidate of systemCandidates) {
      const fallbackPrefix = packaged
        ? `Bundled Python runtime unavailable; using system ${candidate} fallback.`
        : '';
      const system = await this._checkCandidate(candidate, 'system', normalizedRole, fallbackPrefix);
      if (system.ok) {
        const suffix = bundledErrors.length > 0 ? ` ${bundledErrors.join(' | ')}` : '';
        const message = `${String(system.message || '').trim()}${suffix}`.trim();
        const result = { ...system, message };
        this._setCached(normalizedRole, result);
        return result;
      }
    }

    const result = {
      ok: false,
      source: 'none',
      role: normalizedRole,
      python_bin: '',
      version: '',
      message: [
        'No usable Python runtime found.',
        bundledErrors.join(' | '),
        `Tried: ${this._systemCandidates().join(', ')}`,
      ].filter(Boolean).join(' '),
    };
    this._setCached(normalizedRole, result);
    return result;
  }

  async diagnostics(options = {}) {
    return {
      tool: await this.resolve('tool', options),
      viz: await this.resolve('viz', options),
    };
  }
}

function createPythonRuntimeResolver(options = {}) {
  return new PythonRuntimeResolver(options);
}

module.exports = {
  createPythonRuntimeResolver,
  normalizeRole,
};
