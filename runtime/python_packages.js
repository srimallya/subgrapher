const fs = require('fs');
const path = require('path');
const { spawn } = require('child_process');

const DEFAULT_ALLOWLIST_PATH = path.join(__dirname, 'python_allowlist.json');
const DEFAULT_TIMEOUT_MS = 90_000;

function normalizePackageName(value) {
  return String(value || '').trim().toLowerCase().replace(/[^a-z0-9_.-]/g, '');
}

function loadAllowlist(allowlistPath = DEFAULT_ALLOWLIST_PATH) {
  try {
    const raw = fs.readFileSync(allowlistPath, 'utf8');
    const parsed = JSON.parse(raw);
    if (!Array.isArray(parsed)) return [];
    const set = new Set();
    parsed.forEach((item) => {
      const name = normalizePackageName(item);
      if (name) set.add(name);
    });
    return Array.from(set.values());
  } catch (_) {
    return [];
  }
}

function splitAllowedAndRejected(packages = [], allowlist = []) {
  const allow = new Set(Array.isArray(allowlist) ? allowlist.map((name) => normalizePackageName(name)) : []);
  const requested = Array.isArray(packages) ? packages : [];
  const installed = [];
  const rejected = [];
  requested.forEach((pkg) => {
    const clean = normalizePackageName(pkg);
    if (!clean) return;
    if (allow.has(clean)) {
      if (!installed.includes(clean)) installed.push(clean);
      return;
    }
    if (!rejected.includes(clean)) rejected.push(clean);
  });
  return { installed, rejected };
}

async function installAllowedPackages(opts = {}) {
  const options = (opts && typeof opts === 'object') ? opts : {};
  const pythonBin = String(options.pythonBin || 'python3').trim() || 'python3';
  const timeoutMs = Number.isFinite(Number(options.timeoutMs))
    ? Math.max(5_000, Math.round(Number(options.timeoutMs)))
    : DEFAULT_TIMEOUT_MS;
  const cwd = String(options.cwd || process.cwd());
  const allowlist = loadAllowlist(String(options.allowlistPath || DEFAULT_ALLOWLIST_PATH));
  const { installed, rejected } = splitAllowedAndRejected(options.packages, allowlist);

  if (installed.length === 0) {
    return {
      ok: false,
      installed: [],
      rejected,
      stdout: '',
      stderr: rejected.length > 0
        ? `Rejected package(s): ${rejected.join(', ')}`
        : 'No allowlisted packages requested.',
      timed_out: false,
      allowlist,
    };
  }

  return new Promise((resolve) => {
    const args = ['-m', 'pip', 'install', '--upgrade', '--disable-pip-version-check', ...installed];
    const env = {
      PATH: process.env.PATH || '',
      HOME: process.env.HOME || process.cwd(),
      PYTHONNOUSERSITE: '1',
      PIP_DISABLE_PIP_VERSION_CHECK: '1',
    };

    const proc = spawn(pythonBin, args, {
      cwd,
      env,
      stdio: ['ignore', 'pipe', 'pipe'],
      windowsHide: true,
    });

    let stdout = '';
    let stderr = '';
    let timedOut = false;
    const maxCapture = 500_000;

    const timer = setTimeout(() => {
      timedOut = true;
      try {
        proc.kill('SIGKILL');
      } catch (_) {
        // noop
      }
    }, timeoutMs);

    proc.stdout.on('data', (chunk) => {
      stdout += String(chunk || '');
      if (stdout.length > maxCapture) {
        stdout = stdout.slice(-maxCapture);
      }
    });

    proc.stderr.on('data', (chunk) => {
      stderr += String(chunk || '');
      if (stderr.length > maxCapture) {
        stderr = stderr.slice(-maxCapture);
      }
    });

    proc.on('error', (err) => {
      clearTimeout(timer);
      resolve({
        ok: false,
        installed: [],
        rejected,
        stdout,
        stderr: `${stderr}\n${String((err && err.message) || 'pip invocation failed.')}`.trim(),
        timed_out: false,
        allowlist,
      });
    });

    proc.on('close', (code) => {
      clearTimeout(timer);
      if (timedOut) {
        resolve({
          ok: false,
          installed: [],
          rejected,
          stdout,
          stderr: `${stderr}\nPackage install timed out.`.trim(),
          timed_out: true,
          allowlist,
        });
        return;
      }

      const ok = Number(code) === 0;
      resolve({
        ok,
        installed: ok ? installed : [],
        rejected,
        stdout,
        stderr,
        timed_out: false,
        allowlist,
      });
    });
  });
}

module.exports = {
  loadAllowlist,
  installAllowedPackages,
  splitAllowedAndRejected,
};
