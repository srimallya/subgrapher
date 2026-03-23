const fs = require('fs');
const path = require('path');
const { pipeline } = require('stream/promises');
const { Readable } = require('stream');
const { execFileSync } = require('child_process');

function nowTs() {
  return Date.now();
}

function normalizeWhitespace(value = '') {
  return String(value || '').replace(/\s+/g, ' ').trim();
}

function readJsonSafe(filePath = '') {
  try {
    if (!filePath || !fs.existsSync(filePath)) return null;
    return JSON.parse(fs.readFileSync(filePath, 'utf8'));
  } catch (_) {
    return null;
  }
}

function ensureDir(dirPath = '') {
  if (!dirPath) return;
  fs.mkdirSync(dirPath, { recursive: true });
}

function rmrf(targetPath = '') {
  if (!targetPath) return;
  fs.rmSync(targetPath, { recursive: true, force: true });
}

function copyDirContents(srcDir, destDir) {
  ensureDir(destDir);
  for (const entry of fs.readdirSync(srcDir, { withFileTypes: true })) {
    const from = path.join(srcDir, entry.name);
    const to = path.join(destDir, entry.name);
    if (entry.isDirectory()) {
      copyDirContents(from, to);
      continue;
    }
    ensureDir(path.dirname(to));
    fs.copyFileSync(from, to);
    try {
      fs.chmodSync(to, fs.statSync(from).mode);
    } catch (_) {
      // noop on platforms/filesystems that do not allow chmod here
    }
  }
}

function listFilesRecursive(rootDir = '') {
  if (!rootDir || !fs.existsSync(rootDir)) return [];
  const out = [];
  const stack = [rootDir];
  while (stack.length) {
    const current = stack.pop();
    const entries = fs.readdirSync(current, { withFileTypes: true });
    for (const entry of entries) {
      const abs = path.join(current, entry.name);
      if (entry.isDirectory()) stack.push(abs);
      else out.push(abs);
    }
  }
  return out;
}

function createSafeLogger(logFn = null) {
  return (message = '', meta = null) => {
    if (typeof logFn !== 'function') return;
    try {
      logFn(message, meta);
    } catch (err) {
      if (err && err.code === 'EPIPE') return;
      throw err;
    }
  };
}

class BundledLlmBootstrapManager {
  constructor(options = {}) {
    this.projectRoot = path.resolve(String(options.projectRoot || process.cwd()));
    this.userDataPath = path.resolve(String(options.userDataPath || process.cwd()));
    this.platformKey = String(options.platformKey || `${process.platform}-${process.arch}`).trim();
    this.fetchImpl = typeof options.fetchImpl === 'function' ? options.fetchImpl : fetch;
    this.log = createSafeLogger(options.log);
    this.onReady = typeof options.onReady === 'function' ? options.onReady : null;
    this.state = {
      bootstrap_state: 'missing',
      bootstrap_progress_percent: 0,
      bootstrap_current_label: '',
      bootstrap_started_at: 0,
      bootstrap_finished_at: 0,
      bootstrap_error: '',
      download_root: this.currentRootDir(),
    };
    this.activePromise = null;
  }

  manifestPath() {
    return path.join(this.projectRoot, 'runtime', 'models', 'policy_manifest.json');
  }

  projectManifest() {
    return readJsonSafe(this.manifestPath()) || {};
  }

  bootstrapAssets() {
    const manifest = this.projectManifest();
    const assets = (manifest && manifest.bootstrap_assets && typeof manifest.bootstrap_assets === 'object')
      ? manifest.bootstrap_assets
      : {};
    return assets[this.platformKey] || null;
  }

  rootDir() {
    return path.join(this.userDataPath, 'bundled-llm');
  }

  currentRootDir() {
    return path.join(this.rootDir(), 'current');
  }

  downloadsDir() {
    return path.join(this.rootDir(), 'downloads');
  }

  tempRootDir() {
    return path.join(this.rootDir(), 'tmp');
  }

  runtimeManifestPath(rootDir = this.currentRootDir()) {
    return path.join(rootDir, 'runtime-manifest.json');
  }

  currentDiagnostics() {
    return {
      ok: true,
      ...this.state,
      download_root: this.currentRootDir(),
    };
  }

  _setState(patch = {}) {
    this.state = {
      ...this.state,
      ...patch,
      download_root: this.currentRootDir(),
    };
  }

  hasReadyAssets() {
    const manifest = readJsonSafe(this.runtimeManifestPath()) || {};
    const executableRel = String(
      (manifest.executable_rel_path && (manifest.executable_rel_path[this.platformKey.startsWith('win') ? 'win32' : 'default'] || manifest.executable_rel_path.default))
      || manifest.executable_rel_path
      || ''
    ).trim();
    const modelRel = String(manifest.model_rel_path || '').trim();
    if (!executableRel || !modelRel) return false;
    return fs.existsSync(path.join(this.currentRootDir(), executableRel))
      && fs.existsSync(path.join(this.currentRootDir(), modelRel));
  }

  async ensureBundledLlmAssets(options = {}) {
    const force = !!options.force;
    if (!force && this.hasReadyAssets()) {
      this._setState({
        bootstrap_state: 'ready',
        bootstrap_progress_percent: 100,
        bootstrap_current_label: '',
        bootstrap_error: '',
        bootstrap_finished_at: this.state.bootstrap_finished_at || nowTs(),
      });
      return this.currentDiagnostics();
    }
    if (this.activePromise && !force) return this.activePromise;
    this.activePromise = this._runBootstrap(force)
      .finally(() => {
        this.activePromise = null;
      });
    return this.activePromise;
  }

  async _runBootstrap(force = false) {
    const assetConfig = this.bootstrapAssets();
    if (!assetConfig) {
      this._setState({
        bootstrap_state: 'error',
        bootstrap_error: `unsupported bundled LLM target: ${this.platformKey}`,
        bootstrap_finished_at: nowTs(),
      });
      return this.currentDiagnostics();
    }
    const startedAt = nowTs();
    this._setState({
      bootstrap_state: 'downloading',
      bootstrap_progress_percent: 0,
      bootstrap_current_label: 'Preparing bundled LLM assets',
      bootstrap_error: '',
      bootstrap_started_at: startedAt,
      bootstrap_finished_at: 0,
    });
    this.log('bootstrap:start', { platform: this.platformKey, force });
    const rootDir = this.rootDir();
    const downloadsDir = this.downloadsDir();
    const tmpRoot = this.tempRootDir();
    const tmpDir = path.join(tmpRoot, `run-${startedAt}`);
    const runtimeArchivePath = path.join(downloadsDir, String(assetConfig.runtime_archive_name || 'runtime-archive'));
    const modelPath = path.join(downloadsDir, String(assetConfig.model_filename || 'model.gguf'));
    try {
      ensureDir(rootDir);
      ensureDir(downloadsDir);
      rmrf(tmpDir);
      ensureDir(tmpDir);
      if (force) rmrf(this.currentRootDir());

      await this._downloadWithProgress(String(assetConfig.runtime_url || ''), runtimeArchivePath, {
        label: 'Downloading LLM runtime',
        fromPercent: 0,
        toPercent: 45,
      });
      await this._downloadWithProgress(String(assetConfig.model_url || ''), modelPath, {
        label: 'Downloading LLM model',
        fromPercent: 45,
        toPercent: 90,
      });

      this._setState({
        bootstrap_state: 'extracting',
        bootstrap_progress_percent: 92,
        bootstrap_current_label: 'Extracting runtime',
      });

      const extractDir = path.join(tmpDir, 'extract');
      ensureDir(extractDir);
      this._extractArchive(runtimeArchivePath, extractDir, String(assetConfig.runtime_archive_type || ''));
      const runtimeExecutable = listFilesRecursive(extractDir)
        .find((filePath) => path.basename(filePath) === String(assetConfig.runtime_executable_name || '').trim());
      if (!runtimeExecutable) {
        throw new Error(`runtime executable not found after extraction: ${String(assetConfig.runtime_executable_name || '').trim() || '[unknown]'}`);
      }
      const runtimeRoot = path.dirname(runtimeExecutable);
      const stagedRoot = path.join(tmpDir, 'current');
      const stagedEngineDir = path.join(stagedRoot, 'engine', 'llama');
      const stagedModelDir = path.join(stagedRoot, 'models');
      copyDirContents(runtimeRoot, stagedEngineDir);
      ensureDir(stagedModelDir);
      fs.copyFileSync(modelPath, path.join(stagedModelDir, String(assetConfig.model_filename || 'Qwen3.5-0.8B-Q8_0.gguf')));
      this._writeRuntimeManifest(stagedRoot, assetConfig);

      this._setState({
        bootstrap_state: 'extracting',
        bootstrap_progress_percent: 97,
        bootstrap_current_label: 'Finalizing bundled LLM',
      });
      rmrf(this.currentRootDir());
      ensureDir(path.dirname(this.currentRootDir()));
      fs.renameSync(stagedRoot, this.currentRootDir());

      this._setState({
        bootstrap_state: 'ready',
        bootstrap_progress_percent: 100,
        bootstrap_current_label: '',
        bootstrap_error: '',
        bootstrap_finished_at: nowTs(),
      });
      this.log('bootstrap:ready', {
        platform: this.platformKey,
        root: this.currentRootDir(),
      });
      if (typeof this.onReady === 'function') {
        Promise.resolve().then(() => this.onReady(this.currentDiagnostics())).catch(() => {});
      }
      return this.currentDiagnostics();
    } catch (err) {
      this._setState({
        bootstrap_state: 'error',
        bootstrap_current_label: '',
        bootstrap_error: String((err && err.message) || 'bundled_llm_bootstrap_failed'),
        bootstrap_finished_at: nowTs(),
      });
      this.log('bootstrap:error', {
        platform: this.platformKey,
        error: this.state.bootstrap_error,
      });
      return this.currentDiagnostics();
    } finally {
      rmrf(tmpDir);
    }
  }

  async _downloadWithProgress(url = '', outPath = '', options = {}) {
    const targetUrl = String(url || '').trim();
    if (!targetUrl) throw new Error('download url is required');
    const res = await this.fetchImpl(targetUrl, {
      headers: {
        'user-agent': 'Subgrapher/2.2.3',
      },
    });
    if (!res || !res.ok || !res.body) {
      throw new Error(`download failed for ${targetUrl}: HTTP ${(res && res.status) || 'unknown'}`);
    }
    ensureDir(path.dirname(outPath));
    const tmpPath = `${outPath}.part`;
    const totalBytes = Math.max(0, Number(res.headers.get('content-length') || 0) || 0);
    let written = 0;
    const fromPercent = Math.max(0, Math.min(100, Number(options.fromPercent || 0) || 0));
    const toPercent = Math.max(fromPercent, Math.min(100, Number(options.toPercent || fromPercent) || fromPercent));
    const label = String(options.label || 'Downloading').trim();
    const writable = fs.createWriteStream(tmpPath);
    const reader = Readable.fromWeb(res.body);
    reader.on('data', (chunk) => {
      written += Buffer.byteLength(chunk);
      const ratio = totalBytes > 0 ? Math.min(1, written / totalBytes) : 0;
      const nextPercent = Math.round(fromPercent + ((toPercent - fromPercent) * ratio));
      this._setState({
        bootstrap_state: 'downloading',
        bootstrap_progress_percent: nextPercent,
        bootstrap_current_label: label,
      });
    });
    await pipeline(reader, writable);
    fs.renameSync(tmpPath, outPath);
    this._setState({
      bootstrap_state: 'downloading',
      bootstrap_progress_percent: toPercent,
      bootstrap_current_label: label,
    });
  }

  _extractArchive(archivePath = '', outDir = '', archiveType = '') {
    const type = String(archiveType || '').trim().toLowerCase();
    if (type === 'tar.gz') {
      execFileSync('tar', ['-xzf', archivePath, '-C', outDir], { stdio: 'ignore' });
      return;
    }
    if (type === 'zip') {
      if (process.platform === 'win32') {
        execFileSync('powershell', ['-NoProfile', '-Command', `Expand-Archive -Path "${archivePath}" -DestinationPath "${outDir}" -Force`], { stdio: 'ignore' });
        return;
      }
      execFileSync('unzip', ['-q', archivePath, '-d', outDir], { stdio: 'ignore' });
      return;
    }
    throw new Error(`unsupported runtime archive type: ${type || '[empty]'}`);
  }

  _writeRuntimeManifest(rootDir = '', assetConfig = {}) {
    const projectManifest = this.projectManifest();
    const executableName = String(assetConfig.runtime_executable_name || '').trim();
    const runtimeManifest = {
      bundled: true,
      backend: String(projectManifest.backend || 'llama.cpp-cli'),
      target: this.platformKey,
      model_id: String(projectManifest.model_id || ''),
      model_name: String(projectManifest.model_name || projectManifest.model_id || ''),
      model_repo: String(assetConfig.model_repo || ''),
      tasks: Array.isArray(projectManifest.tasks) ? projectManifest.tasks.slice() : ['note_policy_classification', 'rss_article_cleanup_summary'],
      schema_version: Number(projectManifest.schema_version || 1) || 1,
      prompt_versions: {
        ...((projectManifest && projectManifest.prompt_versions) || {}),
      },
      executable_rel_path: {
        default: path.posix.join('engine', 'llama', executableName),
        win32: path.posix.join('engine', 'llama', String(assetConfig.runtime_executable_name || 'llama-cli.exe').trim()),
      },
      model_rel_path: path.posix.join('models', String(assetConfig.model_filename || 'Qwen3.5-0.8B-Q8_0.gguf')),
      runtime_release: String(assetConfig.runtime_release || ''),
      timeout_ms: Number(projectManifest.timeout_ms || 60_000) || 60_000,
      max_retries: Number(projectManifest.max_retries || 10) || 10,
      ctx_size: Number(projectManifest.ctx_size || 2048) || 2048,
      seed: Number(projectManifest.seed || 7) || 7,
      prepared_at: new Date().toISOString(),
    };
    fs.writeFileSync(this.runtimeManifestPath(rootDir), `${JSON.stringify(runtimeManifest, null, 2)}\n`, 'utf8');
  }
}

function createBundledLlmBootstrapManager(options = {}) {
  return new BundledLlmBootstrapManager(options);
}

module.exports = {
  createBundledLlmBootstrapManager,
};
