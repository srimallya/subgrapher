const fs = require('fs');
const path = require('path');
const os = require('os');
const { execFileSync } = require('child_process');

const REPO_ROOT = path.resolve(__dirname, '..');
const BUILD_ROOT = path.join(REPO_ROOT, 'build', 'bundled-llm');
const CACHE_ROOT = path.join(BUILD_ROOT, 'cache');
const OUTPUT_DIR = path.join(BUILD_ROOT, 'current');
const POLICY_MANIFEST_PATH = path.join(REPO_ROOT, 'runtime', 'models', 'policy_manifest.json');

function parseArgs(argv = []) {
  const out = {};
  for (let i = 0; i < argv.length; i += 1) {
    const token = String(argv[i] || '').trim();
    if (token === '--target') out.target = String(argv[i + 1] || '').trim();
  }
  return out;
}

function detectTarget() {
  if (process.platform === 'darwin' && process.arch === 'arm64') return 'darwin-arm64';
  if (process.platform === 'win32' && process.arch === 'x64') return 'win-x64';
  throw new Error(`Unsupported host for bundled LLM prep: ${process.platform}-${process.arch}`);
}

function readJson(filePath) {
  return JSON.parse(fs.readFileSync(filePath, 'utf8'));
}

function resolveTargetConfig(target = '') {
  const manifest = readJson(POLICY_MANIFEST_PATH);
  const assets = manifest && manifest.bootstrap_assets && typeof manifest.bootstrap_assets === 'object'
    ? manifest.bootstrap_assets
    : {};
  const targetConfig = assets[target];
  if (!targetConfig) {
    throw new Error(`Unsupported bundled LLM target: ${target}`);
  }
  return {
    target,
    runtimeUrl: String(targetConfig.runtime_url || '').trim(),
    runtimeArchiveName: String(targetConfig.runtime_archive_name || '').trim(),
    runtimeArchiveType: String(targetConfig.runtime_archive_type || '').trim(),
    runtimeExecutableName: String(targetConfig.runtime_executable_name || '').trim(),
    runtimeRelease: String(targetConfig.runtime_release || '').trim(),
    modelUrl: String(targetConfig.model_url || '').trim(),
    modelFilename: String(targetConfig.model_filename || '').trim(),
    modelRepo: String(targetConfig.model_repo || '').trim(),
  };
}

function ensureDir(dirPath) {
  fs.mkdirSync(dirPath, { recursive: true });
}

function rmrf(targetPath) {
  fs.rmSync(targetPath, { recursive: true, force: true });
}

function run(cmd, args, options = {}) {
  execFileSync(cmd, args, {
    stdio: 'inherit',
    ...options,
  });
}

function downloadFile(url, outPath) {
  ensureDir(path.dirname(outPath));
  if (fs.existsSync(outPath) && fs.statSync(outPath).size > 0) return;
  run('curl', ['-L', '--fail', '--silent', '--show-error', url, '-o', outPath]);
}

function copyDirContents(srcDir, destDir) {
  ensureDir(destDir);
  const entries = fs.readdirSync(srcDir, { withFileTypes: true });
  for (const entry of entries) {
    const from = path.join(srcDir, entry.name);
    const to = path.join(destDir, entry.name);
    if (entry.isDirectory()) {
      copyDirContents(from, to);
    } else {
      ensureDir(path.dirname(to));
      fs.copyFileSync(from, to);
      try {
        fs.chmodSync(to, fs.statSync(from).mode);
      } catch (_) {
        // ignore chmod failures on Windows
      }
    }
  }
}

function listFilesRecursive(rootDir) {
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

function extractArchive(archivePath, outDir) {
  rmrf(outDir);
  ensureDir(outDir);
  if (archivePath.endsWith('.tar.gz')) {
    run('tar', ['-xzf', archivePath, '-C', outDir]);
    return;
  }
  if (archivePath.endsWith('.zip')) {
    if (process.platform === 'win32') {
      run('powershell', ['-NoProfile', '-Command', `Expand-Archive -Path "${archivePath}" -DestinationPath "${outDir}" -Force`]);
      return;
    }
    run('unzip', ['-q', archivePath, '-d', outDir]);
    return;
  }
  throw new Error(`Unsupported archive format: ${archivePath}`);
}

function installLlamaRuntime(target = '', targetConfig = {}) {
  const archivePath = path.join(CACHE_ROOT, String(targetConfig.runtimeArchiveName || 'runtime-archive'));
  const extractDir = path.join(CACHE_ROOT, `${target}-extract`);
  const engineDir = path.join(OUTPUT_DIR, 'engine', 'llama');

  downloadFile(String(targetConfig.runtimeUrl || ''), archivePath);
  extractArchive(archivePath, extractDir);
  const files = listFilesRecursive(extractDir);
  const executablePath = files.find((filePath) => path.basename(filePath) === String(targetConfig.runtimeExecutableName || '').trim());
  if (!executablePath) {
    throw new Error(`Could not locate ${String(targetConfig.runtimeExecutableName || '').trim()} after extracting ${String(targetConfig.runtimeArchiveName || '').trim()}`);
  }
  const runtimeRoot = path.dirname(executablePath);
  rmrf(engineDir);
  copyDirContents(runtimeRoot, engineDir);
  return {
    releaseTag: String(targetConfig.runtimeRelease || '').trim(),
    executableRelPath: path.posix.join('engine', 'llama', String(targetConfig.runtimeExecutableName || '').trim()),
  };
}

function installModel(targetConfig = {}) {
  const modelDir = path.join(OUTPUT_DIR, 'models');
  const modelPath = path.join(modelDir, String(targetConfig.modelFilename || 'Qwen3.5-0.8B-Q8_0.gguf'));
  downloadFile(String(targetConfig.modelUrl || '').trim(), modelPath);
  return {
    modelRelPath: path.posix.join('models', String(targetConfig.modelFilename || 'Qwen3.5-0.8B-Q8_0.gguf')),
  };
}

function writeManifest(target = '', targetConfig = {}, runtimeInfo = {}, modelInfo = {}) {
  const manifestPath = path.join(OUTPUT_DIR, 'runtime-manifest.json');
  const manifest = {
    bundled: true,
    backend: 'llama.cpp-cli',
    target,
    model_id: 'qwen3.5-0.8b-q8_0',
    model_name: 'Qwen3.5 0.8B Q8_0',
    model_repo: String(targetConfig.modelRepo || '').trim(),
    tasks: [
      'note_policy_classification',
      'rss_article_cleanup_summary',
    ],
    schema_version: 1,
    prompt_versions: {
      note_policy_classification: 1,
      rss_article_cleanup_summary: 1,
    },
    executable_rel_path: {
      default: runtimeInfo.executableRelPath,
      win32: runtimeInfo.executableRelPath,
    },
    model_rel_path: modelInfo.modelRelPath,
    llama_cpp_release: runtimeInfo.releaseTag,
    timeout_ms: 60000,
    max_retries: 10,
    ctx_size: 2048,
    seed: 7,
    prepared_at: new Date().toISOString(),
  };
  fs.writeFileSync(manifestPath, `${JSON.stringify(manifest, null, 2)}\n`, 'utf8');
}

function writeReadme(target = '') {
  const readmePath = path.join(OUTPUT_DIR, 'README_PLACEHOLDER.txt');
  const targetConfig = resolveTargetConfig(target);
  const text = [
    'Bundled small-LLM runtime assets prepared for packaging.',
    '',
    `Target: ${target}`,
    `Model: ${String(targetConfig.modelRepo || '').trim()}/${String(targetConfig.modelFilename || '').trim()}`,
    `Runtime: ${String(targetConfig.runtimeRelease || '').trim()}`,
    '',
  ].join('\n');
  fs.writeFileSync(readmePath, text, 'utf8');
}

function main() {
  const args = parseArgs(process.argv.slice(2));
  const target = args.target || detectTarget();
  const targetConfig = resolveTargetConfig(target);
  ensureDir(CACHE_ROOT);
  rmrf(OUTPUT_DIR);
  ensureDir(OUTPUT_DIR);

  const runtimeInfo = installLlamaRuntime(target, targetConfig);
  const modelInfo = installModel(targetConfig);
  writeManifest(target, targetConfig, runtimeInfo, modelInfo);
  writeReadme(target);
  console.log(`Bundled LLM assets prepared for ${target} at ${OUTPUT_DIR}`);
}

main();
