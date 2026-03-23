const fs = require('fs');
const path = require('path');
const os = require('os');
const { execFileSync } = require('child_process');

const REPO_ROOT = path.resolve(__dirname, '..');
const BUILD_ROOT = path.join(REPO_ROOT, 'build', 'bundled-llm');
const CACHE_ROOT = path.join(BUILD_ROOT, 'cache');
const OUTPUT_DIR = path.join(BUILD_ROOT, 'current');
const MODEL_FILENAME = 'Qwen3.5-0.8B-Q8_0.gguf';
const MODEL_REPO = 'lmstudio-community/Qwen3.5-0.8B-GGUF';
const MODEL_URL = `https://huggingface.co/${MODEL_REPO}/resolve/main/${MODEL_FILENAME}?download=1`;
const GITHUB_RELEASE_API = 'https://api.github.com/repos/ggml-org/llama.cpp/releases/latest';

const TARGETS = {
  'darwin-arm64': {
    githubAssetPattern: /llama-b\d+-bin-macos-arm64\.tar\.gz$/i,
    executableName: 'llama-cli',
  },
  'win-x64': {
    githubAssetPattern: /llama-b\d+-bin-win-cpu-x64\.zip$/i,
    executableName: 'llama-cli.exe',
  },
};

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

function capture(cmd, args, options = {}) {
  return execFileSync(cmd, args, {
    encoding: 'utf8',
    stdio: ['ignore', 'pipe', 'inherit'],
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

function fetchLatestRelease() {
  const raw = capture('curl', ['-L', '--fail', '--silent', '--show-error', GITHUB_RELEASE_API]);
  return JSON.parse(raw);
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

function resolveAssetForTarget(target = '') {
  const config = TARGETS[target];
  if (!config) throw new Error(`Unsupported bundled LLM target: ${target}`);
  const release = fetchLatestRelease();
  const assets = Array.isArray(release.assets) ? release.assets : [];
  const asset = assets.find((item) => config.githubAssetPattern.test(String(item && item.name || '')));
  if (!asset) {
    throw new Error(`Could not find llama.cpp asset for ${target} in release ${release.tag_name || 'latest'}`);
  }
  return {
    tag: String(release.tag_name || '').trim(),
    assetName: String(asset.name || '').trim(),
    assetUrl: String(asset.browser_download_url || '').trim(),
    executableName: config.executableName,
  };
}

function installLlamaRuntime(target = '') {
  const release = resolveAssetForTarget(target);
  const archivePath = path.join(CACHE_ROOT, release.assetName);
  const extractDir = path.join(CACHE_ROOT, `${target}-extract`);
  const engineDir = path.join(OUTPUT_DIR, 'engine', 'llama');

  downloadFile(release.assetUrl, archivePath);
  extractArchive(archivePath, extractDir);
  const files = listFilesRecursive(extractDir);
  const executablePath = files.find((filePath) => path.basename(filePath) === release.executableName);
  if (!executablePath) {
    throw new Error(`Could not locate ${release.executableName} after extracting ${release.assetName}`);
  }
  const runtimeRoot = path.dirname(executablePath);
  rmrf(engineDir);
  copyDirContents(runtimeRoot, engineDir);
  return {
    releaseTag: release.tag,
    executableRelPath: path.posix.join('engine', 'llama', release.executableName),
  };
}

function installModel() {
  const modelDir = path.join(OUTPUT_DIR, 'models');
  const modelPath = path.join(modelDir, MODEL_FILENAME);
  downloadFile(MODEL_URL, modelPath);
  return {
    modelRelPath: path.posix.join('models', MODEL_FILENAME),
  };
}

function writeManifest(target = '', runtimeInfo = {}, modelInfo = {}) {
  const manifestPath = path.join(OUTPUT_DIR, 'runtime-manifest.json');
  const manifest = {
    bundled: true,
    backend: 'llama.cpp-cli',
    target,
    model_id: 'qwen3.5-0.8b-q8_0',
    model_name: 'Qwen3.5 0.8B Q8_0',
    model_repo: MODEL_REPO,
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
  const text = [
    'Bundled small-LLM runtime assets prepared for packaging.',
    '',
    `Target: ${target}`,
    `Model: ${MODEL_REPO}/${MODEL_FILENAME}`,
    'Runtime: ggml-org/llama.cpp latest release asset',
    '',
  ].join('\n');
  fs.writeFileSync(readmePath, text, 'utf8');
}

function main() {
  const args = parseArgs(process.argv.slice(2));
  const target = args.target || detectTarget();
  if (!TARGETS[target]) {
    throw new Error(`Unsupported --target value: ${target}`);
  }
  ensureDir(CACHE_ROOT);
  rmrf(OUTPUT_DIR);
  ensureDir(OUTPUT_DIR);

  const runtimeInfo = installLlamaRuntime(target);
  const modelInfo = installModel();
  writeManifest(target, runtimeInfo, modelInfo);
  writeReadme(target);
  console.log(`Bundled LLM assets prepared for ${target} at ${OUTPUT_DIR}`);
}

main();
