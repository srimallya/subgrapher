const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const readline = require('readline');
const { spawn } = require('child_process');

const DEFAULT_TIMEOUT_MS = 10_000;
const MAX_CAPTURE_BYTES = 500_000;

function nowTs() {
  return Date.now();
}

function makeExecutionId() {
  return `exec_${crypto.randomUUID()}`;
}

function sanitizeCode(code) {
  return String(code || '').replace(/^\uFEFF/, '');
}

function containsRestrictedInstall(code) {
  const text = String(code || '').toLowerCase();
  if (!text) return false;
  return (
    /\bpip\s+install\b/.test(text)
    || /python\s+-m\s+pip\s+install/.test(text)
    || /subprocess\.[a-z_]+\([^)]*pip\s+install/.test(text)
    || /os\.system\([^)]*pip\s+install/.test(text)
  );
}

function buildStubs(toolNames = []) {
  if (!Array.isArray(toolNames) || toolNames.length === 0) return '';
  const lines = [
    '# ── Lumino programmatic tool stubs (injected) ──────────────────────────',
    'import sys as _lumino_sys',
    'import json as _lumino_json',
    'import uuid as _lumino_uuid',
    '',
    'def _lumino_rpc(_name, **kwargs):',
    '    _id = str(_lumino_uuid.uuid4())',
    '    _lumino_sys.stdout.write("__RPC__:" + _lumino_json.dumps({"id": _id, "name": _name, "args": kwargs}) + "\\n")',
    '    _lumino_sys.stdout.flush()',
    '    _line = _lumino_sys.stdin.readline()',
    '    _res = _lumino_json.loads(_line)',
    '    if _res.get("error"):',
    '        raise RuntimeError("Lumino tool [" + _name + "]: " + str(_res["error"]))',
    '    return _res.get("result")',
    '',
  ];
  toolNames.forEach((name) => {
    const safeName = String(name || '').replace(/[^a-zA-Z0-9_]/g, '_');
    lines.push(`def ${safeName}(**kwargs): return _lumino_rpc(${JSON.stringify(name)}, **kwargs)`);
  });
  lines.push('# ────────────────────────────────────────────────────────────────────────', '');
  return lines.join('\n');
}

function buildGuardedScript(userCodeRaw) {
  const userCode = sanitizeCode(userCodeRaw).replace(/plt\.show\s*\(\s*\)/g, 'plt.savefig("output.png", dpi=150, bbox_inches="tight")');
  const usesMatplotlib = /\bmatplotlib\b|\bplt\./.test(userCode);
  const matplotlibSetup = usesMatplotlib
    ? [
      'try:',
      '    import matplotlib',
      '    matplotlib.use("Agg")',
      '    import matplotlib.pyplot as plt',
      'except Exception:',
      '    plt = None',
      '',
    ]
    : [
      'plt = None',
      '',
    ];
  return [
    'import os',
    'import sys',
    'import time',
    'import json',
    'import traceback',
    'import pathlib',
    'import builtins',
    '',
    'os.environ.setdefault("SDL_VIDEODRIVER", "dummy")',
    'os.environ.setdefault("SDL_AUDIODRIVER", "dummy")',
    '',
    ...matplotlibSetup,
    '_SANDBOX_ROOT = os.path.abspath(os.getcwd())',
    '_ORIG_OPEN = builtins.open',
    '_ORIG_OS_OPEN = os.open',
    '_ORIG_UNLINK = os.unlink',
    '_ORIG_REMOVE = os.remove',
    '_ORIG_RENAME = os.rename',
    '_ORIG_MKDIR = os.mkdir',
    '_ORIG_MAKEDIRS = os.makedirs',
    '_ORIG_RMDIR = os.rmdir',
    '_ORIG_LISTDIR = os.listdir',
    '_ORIG_SCANDIR = os.scandir',
    '',
    'def _resolve_path(target):',
    '    try:',
    '        raw = os.fspath(target)',
    '    except Exception:',
    '        raw = str(target)',
    '    return os.path.abspath(raw)',
    '',
    'def _is_within_sandbox(abs_path):',
    '    return abs_path == _SANDBOX_ROOT or abs_path.startswith(_SANDBOX_ROOT + os.sep)',
    '',
    'def _resolve_for_read(target):',
    '    return _resolve_path(target)',
    '',
    'def _resolve_for_write(target):',
    '    abs_path = _resolve_path(target)',
    '    if _is_within_sandbox(abs_path):',
    '        return abs_path',
    '    raise PermissionError(f"sandbox policy denied write path: {abs_path}")',
    '',
    'def _mode_is_write(mode_value):',
    '    mode_text = str(mode_value or "r")',
    '    return any(ch in mode_text for ch in ("w", "a", "x", "+"))',
    '',
    'def _flags_include_write(flags):',
    '    write_bits = 0',
    '    write_bits |= getattr(os, "O_WRONLY", 0)',
    '    write_bits |= getattr(os, "O_RDWR", 0)',
    '    write_bits |= getattr(os, "O_CREAT", 0)',
    '    write_bits |= getattr(os, "O_TRUNC", 0)',
    '    write_bits |= getattr(os, "O_APPEND", 0)',
    '    write_bits |= getattr(os, "O_TMPFILE", 0)',
    '    return bool(int(flags) & int(write_bits))',
    '',
    'def _guard_open(file, *args, **kwargs):',
    '    mode_value = kwargs.get("mode", args[0] if len(args) > 0 else "r")',
    '    resolver = _resolve_for_write if _mode_is_write(mode_value) else _resolve_for_read',
    '    return _ORIG_OPEN(resolver(file), *args, **kwargs)',
    '',
    'def _guard_os_open(file, flags, mode=0o777, *, dir_fd=None):',
    '    if dir_fd is not None:',
    '        return _ORIG_OS_OPEN(file, flags, mode, dir_fd=dir_fd)',
    '    resolver = _resolve_for_write if _flags_include_write(flags) else _resolve_for_read',
    '    return _ORIG_OS_OPEN(resolver(file), flags, mode)',
    '',
    'def _guard_unlink(path_value, *args, **kwargs):',
    '    return _ORIG_UNLINK(_resolve_for_write(path_value), *args, **kwargs)',
    '',
    'def _guard_remove(path_value, *args, **kwargs):',
    '    return _ORIG_REMOVE(_resolve_for_write(path_value), *args, **kwargs)',
    '',
    'def _guard_rename(src, dst, *args, **kwargs):',
    '    return _ORIG_RENAME(_resolve_for_write(src), _resolve_for_write(dst), *args, **kwargs)',
    '',
    'def _guard_mkdir(path_value, *args, **kwargs):',
    '    return _ORIG_MKDIR(_resolve_for_write(path_value), *args, **kwargs)',
    '',
    'def _guard_makedirs(name, mode=0o777, exist_ok=False):',
    '    return _ORIG_MAKEDIRS(_resolve_for_write(name), mode=mode, exist_ok=exist_ok)',
    '',
    'def _guard_rmdir(path_value, *args, **kwargs):',
    '    return _ORIG_RMDIR(_resolve_for_write(path_value), *args, **kwargs)',
    '',
    'def _guard_listdir(path_value="."):',
    '    return _ORIG_LISTDIR(_resolve_for_read(path_value))',
    '',
    'def _guard_scandir(path_value="."):',
    '    return _ORIG_SCANDIR(_resolve_for_read(path_value))',
    '',
    'builtins.open = _guard_open',
    'os.open = _guard_os_open',
    'os.unlink = _guard_unlink',
    'os.remove = _guard_remove',
    'os.rename = _guard_rename',
    'os.mkdir = _guard_mkdir',
    'os.makedirs = _guard_makedirs',
    'os.rmdir = _guard_rmdir',
    'os.listdir = _guard_listdir',
    'os.scandir = _guard_scandir',
    '',
    '_PYGAME_FORCED_SIZE = None',
    '_PYGAME_KEYS_DOWN = set()',
    '_PYGAME_MOUSE_BUTTONS_DOWN = set()',
    '_PYGAME_MOUSE_POS = (0, 0)',
    '',
    'def _resolve_pygame_key_code(event, pygame):',
    '    key_name = str(event.get("key") or "").strip().lower()',
    '    key_code_text = str(event.get("code") or "").strip()',
    '    direct_map = {',
    '        "arrowleft": "left",',
    '        "arrowright": "right",',
    '        "arrowup": "up",',
    '        "arrowdown": "down",',
    '        "enter": "return",',
    '        "esc": "escape",',
    '        " ": "space",',
    '    }',
    '    if key_name in direct_map:',
    '        key_name = direct_map[key_name]',
    '    if not key_name and key_code_text:',
    '        code_map = {',
    '            "ArrowLeft": "left",',
    '            "ArrowRight": "right",',
    '            "ArrowUp": "up",',
    '            "ArrowDown": "down",',
    '            "Enter": "return",',
    '            "Escape": "escape",',
    '            "Space": "space",',
    '            "Tab": "tab",',
    '            "Backspace": "backspace",',
    '            "Delete": "delete",',
    '            "ShiftLeft": "left shift",',
    '            "ShiftRight": "right shift",',
    '            "ControlLeft": "left ctrl",',
    '            "ControlRight": "right ctrl",',
    '            "AltLeft": "left alt",',
    '            "AltRight": "right alt",',
    '            "MetaLeft": "left meta",',
    '            "MetaRight": "right meta",',
    '        }',
    '        key_name = code_map.get(key_code_text, "")',
    '    if not key_name:',
    '        return 0',
    '    try:',
    '        return int(pygame.key.key_code(key_name))',
    '    except Exception:',
    '        pass',
    '    try:',
    '        attr = "K_" + str(key_name).replace(" ", "_").replace("-", "_").lower()',
    '        return int(getattr(pygame, attr, 0) or 0)',
    '    except Exception:',
    '        return 0',
    '',
    'def _post_pygame_input_event(event):',
    '    if not isinstance(event, dict):',
    '        return',
    '    kind = str(event.get("type") or "").strip().lower()',
    '    if not kind:',
    '        return',
    '    global _PYGAME_KEYS_DOWN',
    '    global _PYGAME_MOUSE_BUTTONS_DOWN',
    '    global _PYGAME_MOUSE_POS',
    '    try:',
    '        import pygame',
    '    except Exception:',
    '        return',
    '    try:',
    '        if not pygame.get_init():',
    '            pygame.init()',
    '    except Exception:',
    '        return',
    '    try:',
    '        if kind == "keydown" or kind == "keyup":',
    '            key_code = _resolve_pygame_key_code(event, pygame)',
    '            if key_code <= 0:',
    '                return',
    '            event_type = pygame.KEYDOWN if kind == "keydown" else pygame.KEYUP',
    '            payload = {"key": key_code, "unicode": str(event.get("text") or "")}',
    '            if kind == "keydown":',
    '                _PYGAME_KEYS_DOWN.add(int(key_code))',
    '            else:',
    '                _PYGAME_KEYS_DOWN.discard(int(key_code))',
    '            pygame.event.post(pygame.event.Event(event_type, payload))',
    '        elif kind == "mousedown" or kind == "mouseup":',
    '            event_type = pygame.MOUSEBUTTONDOWN if kind == "mousedown" else pygame.MOUSEBUTTONUP',
    '            x = int(event.get("x", 0))',
    '            y = int(event.get("y", 0))',
    '            button = int(event.get("button", 1))',
    '            _PYGAME_MOUSE_POS = (x, y)',
    '            if kind == "mousedown":',
    '                _PYGAME_MOUSE_BUTTONS_DOWN.add(max(1, button))',
    '            else:',
    '                _PYGAME_MOUSE_BUTTONS_DOWN.discard(max(1, button))',
    '            pygame.event.post(pygame.event.Event(event_type, {"pos": (x, y), "button": button}))',
    '        elif kind == "mousemove":',
    '            x = int(event.get("x", 0))',
    '            y = int(event.get("y", 0))',
    '            rel_x = int(event.get("rel_x", 0))',
    '            rel_y = int(event.get("rel_y", 0))',
    '            _PYGAME_MOUSE_POS = (x, y)',
    '            pygame.event.post(pygame.event.Event(pygame.MOUSEMOTION, {"pos": (x, y), "rel": (rel_x, rel_y), "buttons": (1 in _PYGAME_MOUSE_BUTTONS_DOWN, 2 in _PYGAME_MOUSE_BUTTONS_DOWN, 3 in _PYGAME_MOUSE_BUTTONS_DOWN)}))',
    '        elif kind == "wheel":',
    '            dx = int(event.get("x", 0))',
    '            dy = int(event.get("y", 0))',
    '            pygame.event.post(pygame.event.Event(pygame.MOUSEWHEEL, {"x": dx, "y": dy}))',
    '        elif kind == "clear_input":',
    '            _PYGAME_KEYS_DOWN.clear()',
    '            _PYGAME_MOUSE_BUTTONS_DOWN.clear()',
    '        elif kind == "resize":',
    '            width = max(160, int(event.get("width", 0)))',
    '            height = max(120, int(event.get("height", 0)))',
    '            global _PYGAME_FORCED_SIZE',
    '            _PYGAME_FORCED_SIZE = (width, height)',
    '            flags = 0',
    '            try:',
    '                surface = pygame.display.get_surface()',
    '                if surface is not None:',
    '                    flags = int(surface.get_flags())',
    '            except Exception:',
    '                flags = 0',
    '            try:',
    '                pygame.display.set_mode((width, height), flags)',
    '            except Exception:',
    '                pygame.display.set_mode((width, height))',
    '            try:',
    '                pygame.event.post(pygame.event.Event(pygame.VIDEORESIZE, {"w": width, "h": height, "size": (width, height)}))',
    '            except Exception:',
    '                pass',
    '    except Exception:',
    '        pass',
    '',
    'def _install_pygame_input_state_overrides():',
    '    try:',
    '        import pygame',
    '    except Exception:',
    '        return',
    '    try:',
    '        key_mod = pygame.key',
    '        _orig_key_get_pressed = getattr(key_mod, "get_pressed", None)',
    '        def _key_get_pressed_wrapper():',
    '            snapshot = set(int(v) for v in _PYGAME_KEYS_DOWN)',
    '            class _SyntheticPressed:',
    '                def __getitem__(self, key):',
    '                    try:',
    '                        return 1 if int(key) in snapshot else 0',
    '                    except Exception:',
    '                        return 0',
    '                def __len__(self):',
    '                    return 4096',
    '            wrapped = _SyntheticPressed()',
    '            try:',
    '                if callable(_orig_key_get_pressed):',
    '                    _orig_key_get_pressed()',
    '            except Exception:',
    '                pass',
    '            return wrapped',
    '        key_mod.get_pressed = _key_get_pressed_wrapper',
    '    except Exception:',
    '        pass',
    '    try:',
    '        mouse_mod = pygame.mouse',
    '        def _mouse_get_pos_wrapper():',
    '            try:',
    '                x, y = _PYGAME_MOUSE_POS',
    '                return (int(x), int(y))',
    '            except Exception:',
    '                return (0, 0)',
    '        def _mouse_get_pressed_wrapper(num_buttons=3):',
    '            count = 3',
    '            try:',
    '                count = max(1, int(num_buttons))',
    '            except Exception:',
    '                count = 3',
    '            values = []',
    '            for idx in range(count):',
    '                btn = idx + 1',
    '                values.append(1 if btn in _PYGAME_MOUSE_BUTTONS_DOWN else 0)',
    '            return tuple(values)',
    '        mouse_mod.get_pos = _mouse_get_pos_wrapper',
    '        mouse_mod.get_pressed = _mouse_get_pressed_wrapper',
    '    except Exception:',
    '        pass',
    '',
    'def _seed_pygame_input_events_from_env():',
    '    raw = os.environ.get("SUBGRAPHER_PYGAME_INPUT_JSON", "").strip()',
    '    if not raw:',
    '        return',
    '    try:',
    '        events = json.loads(raw)',
    '    except Exception:',
    '        return',
    '    if not isinstance(events, list) or not events:',
    '        return',
    '    for event in events[:200]:',
    '        _post_pygame_input_event(event)',
    '',
    'def _start_pygame_stdin_event_thread():',
    '    raw = os.environ.get("SUBGRAPHER_PYGAME_STDIN_EVENTS", "").strip().lower()',
    '    if raw not in ("1", "true", "yes", "on"):',
    '        return',
    '    try:',
    '        import threading',
    '    except Exception:',
    '        return',
    '    def _worker():',
    '        while True:',
    '            try:',
    '                line = sys.stdin.readline()',
    '            except Exception:',
    '                break',
    '            if not line:',
    '                break',
    '            text = str(line).strip()',
    '            if not text:',
    '                continue',
    '            try:',
    '                payload = json.loads(text)',
    '            except Exception:',
    '                continue',
    '            if isinstance(payload, list):',
    '                for item in payload[:400]:',
    '                    _post_pygame_input_event(item)',
    '            else:',
    '                _post_pygame_input_event(payload)',
    '    try:',
    '        thread = threading.Thread(target=_worker, daemon=True)',
    '        thread.start()',
    '    except Exception:',
    '        pass',
    '',
    'def _install_pygame_set_mode_override_from_env():',
    '    raw_w = os.environ.get("SUBGRAPHER_PYGAME_WIDTH", "").strip()',
    '    raw_h = os.environ.get("SUBGRAPHER_PYGAME_HEIGHT", "").strip()',
    '    if not raw_w or not raw_h:',
    '        return',
    '    try:',
    '        target_w = max(160, int(raw_w))',
    '        target_h = max(120, int(raw_h))',
    '    except Exception:',
    '        return',
    '    try:',
    '        global _PYGAME_FORCED_SIZE',
    '        _PYGAME_FORCED_SIZE = (target_w, target_h)',
    '    except Exception:',
    '        _PYGAME_FORCED_SIZE = None',
    '    try:',
    '        import pygame',
    '    except Exception:',
    '        return',
    '    try:',
    '        display = pygame.display',
    '        _orig_set_mode = getattr(display, "set_mode", None)',
    '        if not callable(_orig_set_mode):',
    '            return',
    '        def _set_mode_wrapper(size, *args, **kwargs):',
    '            forced = _PYGAME_FORCED_SIZE if isinstance(_PYGAME_FORCED_SIZE, tuple) and len(_PYGAME_FORCED_SIZE) == 2 else None',
    '            requested = forced if forced else size',
    '            return _orig_set_mode(requested, *args, **kwargs)',
    '        display.set_mode = _set_mode_wrapper',
    '    except Exception:',
    '        pass',
    '',
    '_AUTO_SNAPSHOT_LAST_TS = 0.0',
    '',
    'def _save_current_pygame_surface_snapshot(force=False):',
    '    global _AUTO_SNAPSHOT_LAST_TS',
    '    try:',
    '        import pygame',
    '    except Exception:',
    '        return',
    '    try:',
    '        if not pygame.get_init():',
    '            return',
    '        surface = pygame.display.get_surface()',
    '        if surface is None:',
    '            return',
    '        now = time.time()',
    '        if (not force) and (now - float(_AUTO_SNAPSHOT_LAST_TS or 0.0) < 0.03):',
    '            return',
    '        pygame.image.save(surface, os.path.join(_SANDBOX_ROOT, "output.png"))',
    '        _AUTO_SNAPSHOT_LAST_TS = now',
    '    except Exception:',
    '        pass',
    '',
    'def _install_pygame_capture_hooks():',
    '    try:',
    '        import pygame',
    '    except Exception:',
    '        return',
    '    try:',
    '        display = pygame.display',
    '    except Exception:',
    '        return',
    '    try:',
    '        _orig_flip = getattr(display, "flip", None)',
    '        if callable(_orig_flip):',
    '            def _flip_wrapper(*args, **kwargs):',
    '                result = _orig_flip(*args, **kwargs)',
    '                _save_current_pygame_surface_snapshot()',
    '                return result',
    '            display.flip = _flip_wrapper',
    '    except Exception:',
    '        pass',
    '    try:',
    '        _orig_update = getattr(display, "update", None)',
    '        if callable(_orig_update):',
    '            def _update_wrapper(*args, **kwargs):',
    '                result = _orig_update(*args, **kwargs)',
    '                _save_current_pygame_surface_snapshot()',
    '                return result',
    '            display.update = _update_wrapper',
    '    except Exception:',
    '        pass',
    '',
    'def _auto_save_pygame_png_if_needed():',
    '    _save_current_pygame_surface_snapshot(force=True)',
    '    out_path = os.path.join(_SANDBOX_ROOT, "output.png")',
    '    if os.path.exists(out_path):',
    '        return',
    '    try:',
    '        import pygame',
    '    except Exception:',
    '        return',
    '    try:',
    '        if not pygame.get_init():',
    '            return',
    '        surface = pygame.display.get_surface()',
    '        if surface is None:',
    '            return',
    '        pygame.image.save(surface, out_path)',
    '    except Exception:',
    '        pass',
    '',
    'def _auto_save_matplotlib_png_if_needed():',
    '    if plt is None:',
    '        return',
    '    try:',
    '        has_figures = bool(plt.get_fignums())',
    '    except Exception:',
    '        has_figures = False',
    '    if not has_figures:',
    '        return',
    '    out_path = os.path.join(_SANDBOX_ROOT, "output.png")',
    '    if os.path.exists(out_path):',
    '        return',
    '    try:',
    '        plt.savefig(out_path, dpi=150, bbox_inches="tight")',
    '    except Exception:',
    '        pass',
    '',
    'EXIT_CODE = 0',
    'try:',
    '    _install_pygame_set_mode_override_from_env()',
    '    _install_pygame_input_state_overrides()',
    '    _seed_pygame_input_events_from_env()',
    '    _start_pygame_stdin_event_thread()',
    '    _install_pygame_capture_hooks()',
    userCode.split('\n').map((line) => `    ${line}`).join('\n'),
    'except Exception:',
    '    traceback.print_exc()',
    '    EXIT_CODE = 1',
    'finally:',
    '    _auto_save_pygame_png_if_needed()',
    '    _auto_save_matplotlib_png_if_needed()',
    '    try:',
    '        if plt is not None:',
    '            plt.close("all")',
    '    except Exception:',
    '        pass',
    '    try:',
    '        import pygame',
    '        if pygame.get_init():',
    '            pygame.quit()',
    '    except Exception:',
    '        pass',
    'sys.exit(EXIT_CODE)',
    '',
  ].join('\n');
}

function checkPythonRuntime(opts = {}) {
  const pythonBin = String(opts.pythonBin || 'python3').trim() || 'python3';
  const timeoutMs = Number.isFinite(Number(opts.timeoutMs))
    ? Math.max(2_000, Math.round(Number(opts.timeoutMs)))
    : 6_000;
  return new Promise((resolve) => {
    const proc = spawn(pythonBin, ['--version'], {
      stdio: ['ignore', 'pipe', 'pipe'],
      windowsHide: true,
    });
    let stdout = '';
    let stderr = '';
    let timedOut = false;
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
    });
    proc.stderr.on('data', (chunk) => {
      stderr += String(chunk || '');
    });

    proc.on('error', (err) => {
      clearTimeout(timer);
      resolve({ ok: false, version: '', message: String((err && err.message) || 'python unavailable') });
    });

    proc.on('close', (code) => {
      clearTimeout(timer);
      if (timedOut) {
        resolve({ ok: false, version: '', message: 'python check timed out' });
        return;
      }
      const raw = `${stdout}\n${stderr}`.trim();
      const version = raw.split('\n').find((line) => /python\s+\d+/i.test(line)) || raw;
      resolve({ ok: Number(code) === 0, version: String(version || '').trim(), message: Number(code) === 0 ? '' : (raw || 'python check failed') });
    });
  });
}

function cleanupStaleSandboxes(basePath, opts = {}) {
  const root = path.join(String(basePath || ''), 'sandbox');
  if (!root || !fs.existsSync(root)) return { ok: true, removed: 0 };
  const maxAgeMs = Number.isFinite(Number(opts.maxAgeMs))
    ? Math.max(60_000, Math.round(Number(opts.maxAgeMs)))
    : 48 * 60 * 60 * 1000;
  const now = nowTs();
  let removed = 0;

  const refDirs = fs.readdirSync(root, { withFileTypes: true }).filter((entry) => entry.isDirectory());
  refDirs.forEach((refDir) => {
    const refPath = path.join(root, refDir.name);
    const execDirs = fs.readdirSync(refPath, { withFileTypes: true }).filter((entry) => entry.isDirectory());
    execDirs.forEach((execDir) => {
      const dirPath = path.join(refPath, execDir.name);
      try {
        const stat = fs.statSync(dirPath);
        if ((now - Number(stat.mtimeMs || 0)) < maxAgeMs) return;
        fs.rmSync(dirPath, { recursive: true, force: true });
        removed += 1;
      } catch (_) {
        // noop
      }
    });
  });

  return { ok: true, removed };
}

async function executePython(code, options = {}) {
  const opts = (options && typeof options === 'object') ? options : {};
  const basePath = String(opts.basePath || process.cwd());
  const srId = String(opts.srId || 'global').replace(/[^a-zA-Z0-9_-]/g, '_') || 'global';
  const executionId = String(opts.executionId || makeExecutionId()).replace(/[^a-zA-Z0-9_-]/g, '_');
  const timeoutMs = Number.isFinite(Number(opts.timeoutMs))
    ? Math.max(1_500, Math.round(Number(opts.timeoutMs)))
    : DEFAULT_TIMEOUT_MS;
  const pythonBin = String(opts.pythonBin || 'python3').trim() || 'python3';
  const extraEnv = (opts.extraEnv && typeof opts.extraEnv === 'object') ? opts.extraEnv : {};

  const sandboxDir = path.join(basePath, 'sandbox', srId, executionId);
  fs.mkdirSync(sandboxDir, { recursive: true });

  const rawCode = sanitizeCode(code);
  if (!rawCode.trim()) {
    return {
      ok: false,
      stdout: '',
      stderr: 'Python code is required.',
      png_base64: null,
      png_path: null,
      timed_out: false,
      execution_id: executionId,
      sandbox_dir: sandboxDir,
      exit_code: null,
    };
  }

  if (containsRestrictedInstall(rawCode)) {
    return {
      ok: false,
      stdout: '',
      stderr: 'Direct package installs are blocked in run_python. Use pip_install tool.',
      png_base64: null,
      png_path: null,
      timed_out: false,
      execution_id: executionId,
      sandbox_dir: sandboxDir,
      exit_code: null,
    };
  }

  const scriptPath = path.join(sandboxDir, 'script.py');
  const outputPngPath = path.join(sandboxDir, 'output.png');
  const wrappedScript = buildGuardedScript(rawCode);
  fs.writeFileSync(scriptPath, wrappedScript, 'utf8');

  return new Promise((resolve) => {
    const mplConfigDir = path.join(sandboxDir, '.mplconfig');
    try {
      fs.mkdirSync(mplConfigDir, { recursive: true });
    } catch (_) {
      // noop
    }
    const sanitizedExtraEnv = {};
    Object.entries(extraEnv).forEach(([key, value]) => {
      const envKey = String(key || '').trim();
      if (!envKey) return;
      if (value === undefined || value === null) return;
      sanitizedExtraEnv[envKey] = String(value);
    });
    const env = {
      PATH: process.env.PATH || '',
      HOME: sandboxDir,
      PYTHONNOUSERSITE: '1',
      PYTHONUNBUFFERED: '1',
      MPLBACKEND: 'Agg',
      MPLCONFIGDIR: mplConfigDir,
      SUBGRAPHER_SANDBOX_ROOT: sandboxDir,
      ...sanitizedExtraEnv,
    };

    const proc = spawn(pythonBin, ['-I', 'script.py'], {
      cwd: sandboxDir,
      env,
      stdio: ['ignore', 'pipe', 'pipe'],
      windowsHide: true,
    });

    let stdout = '';
    let stderr = '';
    let timedOut = false;
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
      if (stdout.length > MAX_CAPTURE_BYTES) {
        stdout = stdout.slice(-MAX_CAPTURE_BYTES);
      }
    });

    proc.stderr.on('data', (chunk) => {
      stderr += String(chunk || '');
      if (stderr.length > MAX_CAPTURE_BYTES) {
        stderr = stderr.slice(-MAX_CAPTURE_BYTES);
      }
    });

    proc.on('error', (err) => {
      clearTimeout(timer);
      resolve({
        ok: false,
        stdout,
        stderr: `${stderr}\n${String((err && err.message) || 'python execution failed.')}`.trim(),
        png_base64: null,
        png_path: null,
        timed_out: false,
        execution_id: executionId,
        sandbox_dir: sandboxDir,
        exit_code: null,
      });
    });

    proc.on('close', (code) => {
      clearTimeout(timer);
      let pngBase64 = null;
      let pngPath = null;
      if (fs.existsSync(outputPngPath)) {
        try {
          const pngBytes = fs.readFileSync(outputPngPath);
          pngBase64 = pngBytes.toString('base64');
          pngPath = outputPngPath;
        } catch (_) {
          pngBase64 = null;
          pngPath = null;
        }
      }

      resolve({
        ok: !timedOut && Number(code) === 0,
        stdout,
        stderr,
        png_base64: pngBase64,
        png_path: pngPath,
        timed_out: timedOut,
        execution_id: executionId,
        sandbox_dir: sandboxDir,
        exit_code: Number.isFinite(Number(code)) ? Number(code) : null,
      });
    });
  });
}

function spawnPythonInteractiveProcess(code, options = {}) {
  const opts = (options && typeof options === 'object') ? options : {};
  const basePath = String(opts.basePath || process.cwd());
  const srId = String(opts.srId || 'global').replace(/[^a-zA-Z0-9_-]/g, '_') || 'global';
  const executionId = String(opts.executionId || makeExecutionId()).replace(/[^a-zA-Z0-9_-]/g, '_');
  const timeoutMs = Number.isFinite(Number(opts.timeoutMs))
    ? Math.max(5_000, Math.round(Number(opts.timeoutMs)))
    : 20 * 60 * 1000;
  const pythonBin = String(opts.pythonBin || 'python3').trim() || 'python3';
  const extraEnv = (opts.extraEnv && typeof opts.extraEnv === 'object') ? opts.extraEnv : {};

  const sandboxDir = path.join(basePath, 'sandbox', srId, executionId);
  fs.mkdirSync(sandboxDir, { recursive: true });

  const rawCode = sanitizeCode(code);
  if (!rawCode.trim()) {
    return {
      ok: false,
      error: 'Python code is required.',
      execution_id: executionId,
      sandbox_dir: sandboxDir,
      output_png_path: path.join(sandboxDir, 'output.png'),
    };
  }
  if (containsRestrictedInstall(rawCode)) {
    return {
      ok: false,
      error: 'Direct package installs are blocked in run_python. Use pip_install tool.',
      execution_id: executionId,
      sandbox_dir: sandboxDir,
      output_png_path: path.join(sandboxDir, 'output.png'),
    };
  }

  const scriptPath = path.join(sandboxDir, 'script.py');
  const outputPngPath = path.join(sandboxDir, 'output.png');
  const wrappedScript = buildGuardedScript(rawCode);
  fs.writeFileSync(scriptPath, wrappedScript, 'utf8');

  const mplConfigDir = path.join(sandboxDir, '.mplconfig');
  try {
    fs.mkdirSync(mplConfigDir, { recursive: true });
  } catch (_) {
    // noop
  }
  const sanitizedExtraEnv = {};
  Object.entries(extraEnv).forEach(([key, value]) => {
    const envKey = String(key || '').trim();
    if (!envKey) return;
    if (value === undefined || value === null) return;
    sanitizedExtraEnv[envKey] = String(value);
  });
  const env = {
    PATH: process.env.PATH || '',
    HOME: sandboxDir,
    PYTHONNOUSERSITE: '1',
    PYTHONUNBUFFERED: '1',
    MPLBACKEND: 'Agg',
    MPLCONFIGDIR: mplConfigDir,
    SUBGRAPHER_SANDBOX_ROOT: sandboxDir,
    SUBGRAPHER_PYGAME_STDIN_EVENTS: '1',
    ...sanitizedExtraEnv,
  };

  let proc;
  try {
    proc = spawn(pythonBin, ['-I', 'script.py'], {
      cwd: sandboxDir,
      env,
      stdio: ['pipe', 'pipe', 'pipe'],
      windowsHide: true,
    });
  } catch (err) {
    return {
      ok: false,
      error: String((err && err.message) || 'python execution failed.'),
      execution_id: executionId,
      sandbox_dir: sandboxDir,
      output_png_path: outputPngPath,
    };
  }

  let stdout = '';
  let stderr = '';
  let timedOut = false;
  let exitCode = null;
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
    if (stdout.length > MAX_CAPTURE_BYTES) stdout = stdout.slice(-MAX_CAPTURE_BYTES);
  });

  proc.stderr.on('data', (chunk) => {
    stderr += String(chunk || '');
    if (stderr.length > MAX_CAPTURE_BYTES) stderr = stderr.slice(-MAX_CAPTURE_BYTES);
  });

  const resultPromise = new Promise((resolve) => {
    proc.on('error', (err) => {
      clearTimeout(timer);
      resolve({
        ok: false,
        stdout,
        stderr: `${stderr}\n${String((err && err.message) || 'python execution failed.')}`.trim(),
        png_base64: null,
        png_path: null,
        timed_out: false,
        execution_id: executionId,
        sandbox_dir: sandboxDir,
        exit_code: null,
      });
    });

    proc.on('close', (code) => {
      clearTimeout(timer);
      exitCode = Number.isFinite(Number(code)) ? Number(code) : null;
      let pngBase64 = null;
      let pngPath = null;
      if (fs.existsSync(outputPngPath)) {
        try {
          const pngBytes = fs.readFileSync(outputPngPath);
          pngBase64 = pngBytes.toString('base64');
          pngPath = outputPngPath;
        } catch (_) {
          pngBase64 = null;
          pngPath = null;
        }
      }
      resolve({
        ok: !timedOut && Number(code) === 0,
        stdout,
        stderr,
        png_base64: pngBase64,
        png_path: pngPath,
        timed_out: timedOut,
        execution_id: executionId,
        sandbox_dir: sandboxDir,
        exit_code: exitCode,
      });
    });
  });

  const sendInputEvents = (events = []) => {
    if (!proc || proc.killed || !proc.stdin || proc.stdin.destroyed) return false;
    const payload = Array.isArray(events) ? events.slice(0, 400) : [events];
    if (payload.length === 0) return true;
    try {
      proc.stdin.write(`${JSON.stringify(payload)}\n`);
      return true;
    } catch (_) {
      return false;
    }
  };

  const stop = () => {
    try {
      if (proc && !proc.killed) proc.kill('SIGKILL');
      return true;
    } catch (_) {
      return false;
    }
  };

  const readLatestPng = () => {
    if (!fs.existsSync(outputPngPath)) return { ok: false, png_base64: '', png_path: '' };
    try {
      const bytes = fs.readFileSync(outputPngPath);
      return {
        ok: true,
        png_base64: bytes.toString('base64'),
        png_path: outputPngPath,
      };
    } catch (_) {
      return { ok: false, png_base64: '', png_path: '' };
    }
  };

  return {
    ok: true,
    process: proc,
    execution_id: executionId,
    sandbox_dir: sandboxDir,
    output_png_path: outputPngPath,
    result: resultPromise,
    sendInputEvents,
    stop,
    readLatestPng,
    getStdio: () => ({ stdout, stderr, timed_out: timedOut, exit_code: exitCode }),
  };
}

// executePythonWithBridge: same as executePython but opens stdin as a pipe so
// Python stub functions can emit __RPC__: lines and receive tool results back.
async function executePythonWithBridge(code, options = {}) {
  const opts = (options && typeof options === 'object') ? options : {};
  const basePath = String(opts.basePath || process.cwd());
  const srId = String(opts.srId || 'global').replace(/[^a-zA-Z0-9_-]/g, '_') || 'global';
  const executionId = String(opts.executionId || makeExecutionId()).replace(/[^a-zA-Z0-9_-]/g, '_');
  const timeoutMs = Number.isFinite(Number(opts.timeoutMs))
    ? Math.max(1_500, Math.round(Number(opts.timeoutMs)))
    : DEFAULT_TIMEOUT_MS;
  const pythonBin = String(opts.pythonBin || 'python3').trim() || 'python3';
  const extraEnv = (opts.extraEnv && typeof opts.extraEnv === 'object') ? opts.extraEnv : {};
  const toolBridge = typeof opts.toolBridge === 'function' ? opts.toolBridge : null;
  const bridgeToolNames = Array.isArray(opts.bridgeToolNames) ? opts.bridgeToolNames : [];

  const sandboxDir = path.join(basePath, 'sandbox', srId, executionId);
  fs.mkdirSync(sandboxDir, { recursive: true });

  const rawCode = sanitizeCode(code);
  if (!rawCode.trim()) {
    return {
      ok: false, stdout: '', stderr: 'Python code is required.',
      png_base64: null, png_path: null, timed_out: false,
      execution_id: executionId, sandbox_dir: sandboxDir, exit_code: null,
    };
  }
  if (containsRestrictedInstall(rawCode)) {
    return {
      ok: false, stdout: '', stderr: 'Direct package installs are blocked in run_python. Use pip_install tool.',
      png_base64: null, png_path: null, timed_out: false,
      execution_id: executionId, sandbox_dir: sandboxDir, exit_code: null,
    };
  }

  // Prepend stub functions then wrap with standard guards
  const stubCode = buildStubs(bridgeToolNames);
  const wrappedScript = buildGuardedScript(stubCode + rawCode);
  const scriptPath = path.join(sandboxDir, 'script.py');
  const outputPngPath = path.join(sandboxDir, 'output.png');
  fs.writeFileSync(scriptPath, wrappedScript, 'utf8');

  return new Promise((resolve) => {
    const mplConfigDir = path.join(sandboxDir, '.mplconfig');
    try {
      fs.mkdirSync(mplConfigDir, { recursive: true });
    } catch (_) {
      // noop
    }
    const sanitizedExtraEnv = {};
    Object.entries(extraEnv).forEach(([key, value]) => {
      const envKey = String(key || '').trim();
      if (!envKey) return;
      if (value === undefined || value === null) return;
      sanitizedExtraEnv[envKey] = String(value);
    });
    const env = {
      PATH: process.env.PATH || '',
      HOME: sandboxDir,
      PYTHONNOUSERSITE: '1',
      PYTHONUNBUFFERED: '1',
      MPLBACKEND: 'Agg',
      MPLCONFIGDIR: mplConfigDir,
      SUBGRAPHER_SANDBOX_ROOT: sandboxDir,
      ...sanitizedExtraEnv,
    };

    const proc = spawn(pythonBin, ['-I', 'script.py'], {
      cwd: sandboxDir,
      env,
      stdio: ['pipe', 'pipe', 'pipe'],  // stdin open for tool result replies
      windowsHide: true,
    });

    let stdout = '';
    let stderr = '';
    let timedOut = false;

    const timer = setTimeout(() => {
      timedOut = true;
      try { proc.kill('SIGKILL'); } catch (_) {}
    }, timeoutMs);

    // Process stdout line-by-line: intercept __RPC__: lines, pass rest through
    const rl = readline.createInterface({ input: proc.stdout, crlfDelay: Infinity });
    rl.on('line', (line) => {
      if (toolBridge && line.startsWith('__RPC__:')) {
        let req;
        try { req = JSON.parse(line.slice(8)); } catch (_) {
          try { proc.stdin.write(JSON.stringify({ id: null, result: null, error: 'Malformed RPC' }) + '\n'); } catch (__) {}
          return;
        }
        Promise.resolve().then(() => toolBridge(req)).then((result) => {
          try { proc.stdin.write(JSON.stringify({ id: req.id, result: result ?? null, error: null }) + '\n'); } catch (_) {}
        }).catch((err) => {
          try { proc.stdin.write(JSON.stringify({ id: req.id, result: null, error: String((err && err.message) || 'bridge error') }) + '\n'); } catch (_) {}
        });
      } else {
        stdout += line + '\n';
        if (stdout.length > MAX_CAPTURE_BYTES) stdout = stdout.slice(-MAX_CAPTURE_BYTES);
      }
    });

    proc.stderr.on('data', (chunk) => {
      stderr += String(chunk || '');
      if (stderr.length > MAX_CAPTURE_BYTES) stderr = stderr.slice(-MAX_CAPTURE_BYTES);
    });

    proc.on('error', (err) => {
      clearTimeout(timer);
      rl.close();
      resolve({
        ok: false, stdout,
        stderr: `${stderr}\n${String((err && err.message) || 'python execution failed.')}`.trim(),
        png_base64: null, png_path: null, timed_out: false,
        execution_id: executionId, sandbox_dir: sandboxDir, exit_code: null,
      });
    });

    proc.on('close', (code) => {
      clearTimeout(timer);
      rl.close();
      let pngBase64 = null;
      let pngPath = null;
      if (fs.existsSync(outputPngPath)) {
        try {
          const pngBytes = fs.readFileSync(outputPngPath);
          pngBase64 = pngBytes.toString('base64');
          pngPath = outputPngPath;
        } catch (_) {}
      }
      resolve({
        ok: !timedOut && Number(code) === 0,
        stdout, stderr, png_base64: pngBase64, png_path: pngPath,
        timed_out: timedOut, execution_id: executionId, sandbox_dir: sandboxDir,
        exit_code: Number.isFinite(Number(code)) ? Number(code) : null,
      });
    });
  });
}

class PythonSandboxManager {
  constructor(options = {}) {
    const opts = (options && typeof options === 'object') ? options : {};
    this.basePath = String(opts.basePath || process.cwd());
    this.pythonBin = String(opts.pythonBin || 'python3');
    this.maxPerReference = Number.isFinite(Number(opts.maxPerReference))
      ? Math.max(1, Math.round(Number(opts.maxPerReference)))
      : 2;
    this.maxGlobal = Number.isFinite(Number(opts.maxGlobal))
      ? Math.max(1, Math.round(Number(opts.maxGlobal)))
      : 6;
    this.defaultTimeoutMs = Number.isFinite(Number(opts.defaultTimeoutMs))
      ? Math.max(1_500, Math.round(Number(opts.defaultTimeoutMs)))
      : DEFAULT_TIMEOUT_MS;
    this.maxQueue = Number.isFinite(Number(opts.maxQueue))
      ? Math.max(1, Math.round(Number(opts.maxQueue)))
      : 120;
    this.queue = [];
    this.runningGlobal = 0;
    this.runningByRef = new Map();
  }

  getStatus() {
    return {
      queue_length: this.queue.length,
      running_global: this.runningGlobal,
      running_by_ref: Array.from(this.runningByRef.entries()).map(([srId, count]) => ({ sr_id: srId, count })),
      max_global: this.maxGlobal,
      max_per_reference: this.maxPerReference,
    };
  }

  _getRefRunning(srId) {
    const key = String(srId || 'global').trim() || 'global';
    return Number(this.runningByRef.get(key) || 0);
  }

  _incRefRunning(srId) {
    const key = String(srId || 'global').trim() || 'global';
    this.runningByRef.set(key, this._getRefRunning(key) + 1);
  }

  _decRefRunning(srId) {
    const key = String(srId || 'global').trim() || 'global';
    const next = this._getRefRunning(key) - 1;
    if (next <= 0) {
      this.runningByRef.delete(key);
      return;
    }
    this.runningByRef.set(key, next);
  }

  _canRun(srId) {
    if (this.runningGlobal >= this.maxGlobal) return false;
    if (this._getRefRunning(srId) >= this.maxPerReference) return false;
    return true;
  }

  _drain() {
    if (this.queue.length === 0) return;
    let madeProgress = true;
    while (madeProgress && this.queue.length > 0 && this.runningGlobal < this.maxGlobal) {
      madeProgress = false;
      const runnableIdx = this.queue.findIndex((job) => this._canRun(job.srId));
      if (runnableIdx < 0) break;
      const [job] = this.queue.splice(runnableIdx, 1);
      this.runningGlobal += 1;
      this._incRefRunning(job.srId);
      madeProgress = true;

      const execFn = job.toolBridge ? executePythonWithBridge : executePython;
      execFn(job.code, {
        basePath: this.basePath,
        srId: job.srId,
        executionId: job.executionId,
        timeoutMs: job.timeoutMs,
        pythonBin: this.pythonBin,
        extraEnv: (job.extraEnv && typeof job.extraEnv === 'object') ? job.extraEnv : {},
        toolBridge: job.toolBridge || null,
        bridgeToolNames: job.bridgeToolNames || [],
      }).then((result) => {
        job.resolve(result);
      }).catch((err) => {
        job.resolve({
          ok: false,
          stdout: '',
          stderr: String((err && err.message) || 'Python execution failed.'),
          png_base64: null,
          png_path: null,
          timed_out: false,
          execution_id: job.executionId,
          sandbox_dir: path.join(this.basePath, 'sandbox', job.srId, job.executionId),
          exit_code: null,
        });
      }).finally(() => {
        this.runningGlobal = Math.max(0, this.runningGlobal - 1);
        this._decRefRunning(job.srId);
        this._drain();
      });
    }
  }

  enqueue(payload = {}) {
    const input = (payload && typeof payload === 'object') ? payload : {};
    const srId = String(input.srId || input.sr_id || 'global').replace(/[^a-zA-Z0-9_-]/g, '_') || 'global';
    const code = String(input.code || '');
    const timeoutMs = Number.isFinite(Number(input.timeoutMs))
      ? Math.max(1_500, Math.round(Number(input.timeoutMs)))
      : this.defaultTimeoutMs;
    const executionId = String(input.executionId || makeExecutionId()).replace(/[^a-zA-Z0-9_-]/g, '_');
    const toolBridge = typeof input.toolBridge === 'function' ? input.toolBridge : null;
    const bridgeToolNames = Array.isArray(input.bridgeToolNames) ? input.bridgeToolNames : [];
    const extraEnv = (input.extraEnv && typeof input.extraEnv === 'object') ? input.extraEnv : {};

    if (this.queue.length >= this.maxQueue) {
      return Promise.resolve({
        ok: false,
        stdout: '',
        stderr: 'Python sandbox queue is full. Try again shortly.',
        png_base64: null,
        png_path: null,
        timed_out: false,
        execution_id: executionId,
        sandbox_dir: path.join(this.basePath, 'sandbox', srId, executionId),
        exit_code: null,
      });
    }

    return new Promise((resolve) => {
      this.queue.push({
        srId,
        code,
        timeoutMs,
        executionId,
        toolBridge,
        bridgeToolNames,
        extraEnv,
        resolve,
      });
      this._drain();
    });
  }
}

module.exports = {
  PythonSandboxManager,
  checkPythonRuntime,
  cleanupStaleSandboxes,
  executePython,
  spawnPythonInteractiveProcess,
  executePythonWithBridge,
};
