#!/usr/bin/env node

const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const http = require('http');
const https = require('https');
const { spawnSync } = require('child_process');

const REPO_ROOT = path.resolve(__dirname, '..');
const MANIFEST_PATH = path.join(REPO_ROOT, 'config', 'bundled_python_manifest.json');
const REQUIREMENTS_PATH = path.join(REPO_ROOT, 'config', 'bundled_python_requirements.txt');
const BUILD_ROOT = path.join(REPO_ROOT, 'build', 'bundled-python');
const CACHE_DIR = path.join(BUILD_ROOT, 'cache');
const OUTPUT_DIR = path.join(BUILD_ROOT, 'current');

const IMPORT_SMOKE_PACKAGES = ['pygame', 'numpy', 'matplotlib', 'pandas', 'scipy', 'seaborn', 'plotly'];

function fail(message) {
  throw new Error(String(message || 'Unknown error'));
}

function parseArgs() {
  const args = process.argv.slice(2);
  const out = {};
  for (let i = 0; i < args.length; i += 1) {
    const token = String(args[i] || '');
    if (token === '--target') {
      out.target = String(args[i + 1] || '').trim();
      i += 1;
    }
  }
  return out;
}

function currentTargetKey() {
  return `${process.platform}-${process.arch}`;
}

function canExecuteTargetInterpreter(target) {
  const normalized = String(target || '').trim().toLowerCase();
  if (!normalized) return false;
  if (normalized.startsWith('win32-')) return process.platform === 'win32';
  if (normalized.startsWith('darwin-')) return process.platform === 'darwin';
  if (normalized.startsWith('linux-')) return process.platform === 'linux';
  return false;
}

function readJson(filePath) {
  return JSON.parse(fs.readFileSync(filePath, 'utf8'));
}

function ensureDir(dirPath) {
  fs.mkdirSync(dirPath, { recursive: true });
}

function rmrf(dirPath) {
  try {
    fs.rmSync(dirPath, {
      recursive: true,
      force: true,
      maxRetries: 8,
      retryDelay: 200,
    });
    return;
  } catch (err) {
    if (!fs.existsSync(dirPath)) return;
    if (process.platform === 'win32') {
      const normalized = String(dirPath || '').replace(/\//g, '\\');
      const cmdRes = spawnSync('cmd.exe', ['/d', '/s', '/c', `rmdir /s /q "${normalized}"`], {
        stdio: 'pipe',
        encoding: 'utf8',
      });
      if (cmdRes.error) throw cmdRes.error;
      if (Number(cmdRes.status) === 0 || !fs.existsSync(dirPath)) return;
    } else {
      const rmRes = spawnSync('rm', ['-rf', dirPath], {
        stdio: 'pipe',
        encoding: 'utf8',
      });
      if (rmRes.error) throw rmRes.error;
      if (Number(rmRes.status) === 0 || !fs.existsSync(dirPath)) return;
    }
    throw err;
  }
}

function toPosix(relPath) {
  return String(relPath || '').split(path.sep).join('/');
}

function sha256File(filePath) {
  const hash = crypto.createHash('sha256');
  const fd = fs.openSync(filePath, 'r');
  const chunk = Buffer.allocUnsafe(1024 * 1024);
  try {
    let bytesRead = 0;
    do {
      bytesRead = fs.readSync(fd, chunk, 0, chunk.length, null);
      if (bytesRead > 0) hash.update(chunk.subarray(0, bytesRead));
    } while (bytesRead > 0);
  } finally {
    fs.closeSync(fd);
  }
  return hash.digest('hex');
}

function downloadFile(urlText, destination, redirects = 0) {
  return new Promise((resolve, reject) => {
    if (redirects > 8) {
      reject(new Error(`Too many redirects while downloading ${urlText}`));
      return;
    }
    const urlObj = new URL(urlText);
    const client = urlObj.protocol === 'http:' ? http : https;
    const req = client.get(urlObj, (res) => {
      const status = Number(res.statusCode || 0);
      const location = String(res.headers.location || '').trim();
      if (status >= 300 && status < 400 && location) {
        res.resume();
        const nextUrl = new URL(location, urlObj).toString();
        downloadFile(nextUrl, destination, redirects + 1).then(resolve).catch(reject);
        return;
      }
      if (status !== 200) {
        res.resume();
        reject(new Error(`Download failed (${status}) for ${urlText}`));
        return;
      }
      const tmpPath = `${destination}.tmp`;
      const file = fs.createWriteStream(tmpPath);
      res.pipe(file);
      file.on('finish', () => {
        file.close(() => {
          fs.renameSync(tmpPath, destination);
          resolve();
        });
      });
      file.on('error', (err) => {
        try { fs.unlinkSync(tmpPath); } catch (_) {}
        reject(err);
      });
    });
    req.on('error', reject);
  });
}

function runOrThrow(command, args, options = {}) {
  const result = spawnSync(command, args, {
    cwd: options.cwd || REPO_ROOT,
    env: {
      ...process.env,
      ...(options.env || {}),
    },
    stdio: 'pipe',
    encoding: 'utf8',
  });
  if (result.error) throw result.error;
  if (Number(result.status) !== 0) {
    const stderr = String(result.stderr || '').trim();
    const stdout = String(result.stdout || '').trim();
    fail(`Command failed: ${command} ${args.join(' ')}\n${stderr || stdout || 'Unknown command error'}`);
  }
  return result;
}

function tryRun(command, args, options = {}) {
  const result = spawnSync(command, args, {
    cwd: options.cwd || REPO_ROOT,
    env: {
      ...process.env,
      ...(options.env || {}),
    },
    stdio: 'pipe',
    encoding: 'utf8',
  });
  if (result.error) {
    return { ok: false, error: result.error };
  }
  return {
    ok: Number(result.status) === 0,
    status: Number(result.status || 0),
    stdout: String(result.stdout || ''),
    stderr: String(result.stderr || ''),
  };
}

function findBundledInterpreter(rootDir, target) {
  const normalizedTarget = String(target || '').trim().toLowerCase();
  const preferred = normalizedTarget.startsWith('win32')
    ? [
      path.join('python', 'install', 'python.exe'),
      path.join('python', 'python.exe'),
      path.join('install', 'python.exe'),
    ]
    : [
      path.join('python', 'install', 'bin', 'python3'),
      path.join('python', 'bin', 'python3'),
      path.join('install', 'bin', 'python3'),
    ];

  for (const rel of preferred) {
    const abs = path.join(rootDir, rel);
    if (fs.existsSync(abs)) return abs;
  }

  const targetName = normalizedTarget.startsWith('win32') ? 'python.exe' : 'python3';
  const stack = [rootDir];
  while (stack.length > 0) {
    const current = stack.pop();
    const entries = fs.readdirSync(current, { withFileTypes: true });
    for (const entry of entries) {
      const abs = path.join(current, entry.name);
      if (entry.isDirectory()) {
        stack.push(abs);
        continue;
      }
      if (!entry.isFile()) continue;
      if (entry.name === targetName) return abs;
    }
  }
  return '';
}

async function main() {
  const args = parseArgs();
  const target = String(args.target || currentTargetKey()).trim();
  if (!target) fail('Target is required.');

  if (!fs.existsSync(MANIFEST_PATH)) fail(`Missing manifest: ${MANIFEST_PATH}`);
  if (!fs.existsSync(REQUIREMENTS_PATH)) fail(`Missing requirements: ${REQUIREMENTS_PATH}`);

  const manifest = readJson(MANIFEST_PATH);
  const targetCfg = manifest && manifest.targets && manifest.targets[target];
  if (!targetCfg) fail(`Unsupported target "${target}" in ${MANIFEST_PATH}`);
  if (!canExecuteTargetInterpreter(target)) {
    fail(
      `Target "${target}" cannot be prepared on host "${process.platform}-${process.arch}". `
      + 'Run this step on a native host/runner for the target platform.',
    );
  }

  const artifactUrl = String(targetCfg.url || '').trim();
  const expectedSha = String(targetCfg.sha256 || '').trim().toLowerCase();
  if (!artifactUrl || !expectedSha) fail(`Manifest entry for ${target} must include url and sha256.`);

  ensureDir(CACHE_DIR);
  const archiveName = path.basename(new URL(artifactUrl).pathname);
  const archivePath = path.join(CACHE_DIR, archiveName);

  if (fs.existsSync(archivePath)) {
    const existingSha = sha256File(archivePath);
    if (existingSha !== expectedSha) {
      fs.unlinkSync(archivePath);
    }
  }
  if (!fs.existsSync(archivePath)) {
    console.log(`[python-bundle] Downloading ${artifactUrl}`);
    await downloadFile(artifactUrl, archivePath);
  }

  const actualSha = sha256File(archivePath);
  if (actualSha !== expectedSha) {
    fail(`Checksum mismatch for ${archiveName}. Expected ${expectedSha}, got ${actualSha}`);
  }

  rmrf(OUTPUT_DIR);
  ensureDir(OUTPUT_DIR);

  console.log(`[python-bundle] Extracting ${archiveName}`);
  runOrThrow('tar', ['-xzf', archivePath, '-C', OUTPUT_DIR]);

  const pythonAbs = findBundledInterpreter(OUTPUT_DIR, target);
  if (!pythonAbs) fail(`Unable to locate bundled python interpreter after extraction in ${OUTPUT_DIR}`);

  if (process.platform !== 'win32') {
    try {
      fs.chmodSync(pythonAbs, 0o755);
    } catch (_) {
      // noop
    }
  }

  const pipCheck = tryRun(pythonAbs, ['-m', 'pip', '--version']);
  if (!pipCheck.ok) {
    console.log('[python-bundle] Bootstrapping pip via ensurepip');
    runOrThrow(pythonAbs, ['-m', 'ensurepip', '--upgrade']);
  }

  console.log('[python-bundle] Installing pinned requirements');
  runOrThrow(pythonAbs, ['-m', 'pip', 'install', '--upgrade', '--disable-pip-version-check', '-r', REQUIREMENTS_PATH]);

  const smokeScript = `import ${IMPORT_SMOKE_PACKAGES.join(', ')}\nprint("subgrapher-python-bundle-ok")`;
  console.log('[python-bundle] Running import smoke check');
  runOrThrow(pythonAbs, ['-c', smokeScript]);

  const versionRes = runOrThrow(pythonAbs, ['--version']);
  const versionLine = `${String(versionRes.stdout || '').trim()}\n${String(versionRes.stderr || '').trim()}`
    .split('\n')
    .map((line) => line.trim())
    .find((line) => /^python\s+\d+/i.test(line))
    || '';

  const interpreterRelPath = toPosix(path.relative(OUTPUT_DIR, pythonAbs));
  const runtimeManifest = {
    version: 1,
    generated_at: new Date().toISOString(),
    target,
    source: {
      release_tag: String(manifest.release_tag || ''),
      python_version: String(manifest.python_version || ''),
      artifact_url: artifactUrl,
      artifact_sha256: expectedSha,
      artifact_filename: archiveName,
    },
    python: {
      version: versionLine || String(manifest.python_version || ''),
      interpreter_rel_path: interpreterRelPath,
    },
    roles: {
      tool: { interpreter_rel_path: interpreterRelPath },
      viz: { interpreter_rel_path: interpreterRelPath },
    },
    packages: IMPORT_SMOKE_PACKAGES,
  };

  fs.writeFileSync(
    path.join(OUTPUT_DIR, 'runtime-manifest.json'),
    JSON.stringify(runtimeManifest, null, 2),
    'utf8',
  );

  console.log(`[python-bundle] Prepared runtime for ${target}`);
  console.log(`[python-bundle] Interpreter: ${pythonAbs}`);
}

main().catch((err) => {
  const message = String((err && err.stack) || (err && err.message) || err || 'unknown error');
  console.error(`[python-bundle] ${message}`);
  process.exit(1);
});
