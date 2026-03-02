#!/usr/bin/env node

const fs = require('fs');
const path = require('path');
const { spawnSync } = require('child_process');

const REPO_ROOT = path.resolve(__dirname, '..');
const OUTPUT_DIR = path.join(REPO_ROOT, 'build', 'bundled-python', 'current');

function run(command, args) {
  return spawnSync(command, args, {
    cwd: REPO_ROOT,
    stdio: 'inherit',
    shell: false,
    env: {
      ...process.env,
    },
  });
}

function writeFallbackManifest() {
  try {
    fs.rmSync(OUTPUT_DIR, { recursive: true, force: true });
  } catch (_) {
    // noop
  }
  fs.mkdirSync(OUTPUT_DIR, { recursive: true });
  const manifest = {
    version: 1,
    generated_at: new Date().toISOString(),
    target: 'win32-x64',
    mode: 'fallback_placeholder',
    note: 'Bundled Windows Python preparation failed; packaged app should fallback to system python3 at runtime.',
    roles: {
      tool: {},
      viz: {},
    },
    python: {},
    packages: [],
  };
  fs.writeFileSync(
    path.join(OUTPUT_DIR, 'runtime-manifest.json'),
    JSON.stringify(manifest, null, 2),
    'utf8',
  );
  const readme = [
    '# Subgrapher Windows Python Placeholder',
    '',
    'Bundled Python preparation failed for this build.',
    'Runtime will fallback to system python3 if available.',
    'Pygame/data stack may be unavailable until bundled runtime is restored.',
    '',
  ].join('\n');
  fs.writeFileSync(path.join(OUTPUT_DIR, 'README_PLACEHOLDER.txt'), readme, 'utf8');
}

function main() {
  const prep = run('node', ['scripts/prepare_bundled_python.js', '--target', 'win32-x64']);
  if (!prep.error && Number(prep.status || 0) === 0) {
    process.exit(0);
  }
  console.warn('[python-bundle] Windows bundled runtime prep failed; continuing with system-python fallback placeholder.');
  writeFallbackManifest();
  process.exit(0);
}

main();
