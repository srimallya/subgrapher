const fs = require('fs');
const path = require('path');
const { spawn } = require('child_process');

class MCPManager {
  constructor(configPath) {
    this.configPath = configPath;
    this.enabled = false;
    this.servers = [];
    this.runtime = new Map();
    this.loadConfig();
  }

  loadConfig() {
    try {
      if (!fs.existsSync(this.configPath)) {
        this.servers = [];
        return;
      }
      const raw = fs.readFileSync(this.configPath, 'utf8');
      const parsed = JSON.parse(raw);
      this.servers = Array.isArray(parsed.servers) ? parsed.servers.map((s) => ({
        id: String((s && s.id) || '').trim(),
        name: String((s && s.name) || '').trim() || String((s && s.id) || '').trim(),
        command: String((s && s.command) || '').trim(),
        args: Array.isArray(s && s.args) ? s.args.map((v) => String(v)) : [],
        cwd: String((s && s.cwd) || '').trim(),
        env: (s && typeof s.env === 'object' && s.env) ? s.env : {},
        autostart: !!(s && s.autostart),
      })).filter((s) => s.id && s.command) : [];
    } catch (_) {
      this.servers = [];
    }
  }

  getServerById(serverId) {
    const id = String(serverId || '').trim();
    return this.servers.find((server) => server.id === id) || null;
  }

  buildStatus() {
    return {
      enabled: this.enabled,
      servers: this.servers.map((server) => {
        const runtime = this.runtime.get(server.id);
        return {
          id: server.id,
          name: server.name,
          autostart: server.autostart,
          running: !!(runtime && runtime.running),
          pid: runtime && runtime.pid ? runtime.pid : null,
          last_error: runtime && runtime.lastError ? runtime.lastError : null,
          command: `${server.command} ${(server.args || []).join(' ')}`.trim(),
        };
      }),
    };
  }

  startServer(serverId) {
    const server = this.getServerById(serverId);
    if (!server) {
      return { ok: false, message: 'MCP server not found.' };
    }
    const current = this.runtime.get(server.id);
    if (current && current.running) {
      return { ok: true, already_running: true };
    }

    const mergedEnv = { ...process.env, ...server.env };
    const cwd = server.cwd ? path.resolve(server.cwd) : process.cwd();

    try {
      const child = spawn(server.command, server.args || [], {
        cwd,
        env: mergedEnv,
        stdio: ['pipe', 'pipe', 'pipe'],
      });
      const runtime = {
        child,
        running: true,
        pid: child.pid,
        lastError: null,
      };
      this.runtime.set(server.id, runtime);

      child.on('error', (err) => {
        runtime.running = false;
        runtime.lastError = err && err.message ? err.message : 'Process start error';
      });

      child.on('exit', (code, signal) => {
        runtime.running = false;
        if (code !== 0) {
          runtime.lastError = `Exited code=${code} signal=${signal || 'none'}`;
        }
      });

      child.stderr.on('data', (chunk) => {
        const msg = String(chunk || '').trim();
        if (!msg) return;
        runtime.lastError = msg.slice(-400);
      });

      return { ok: true, pid: child.pid };
    } catch (err) {
      this.runtime.set(server.id, {
        child: null,
        running: false,
        pid: null,
        lastError: err && err.message ? err.message : 'Failed to spawn MCP server',
      });
      return { ok: false, message: err && err.message ? err.message : 'Failed to spawn MCP server' };
    }
  }

  stopServer(serverId) {
    const runtime = this.runtime.get(String(serverId || '').trim());
    if (!runtime || !runtime.child) return { ok: true, already_stopped: true };
    try {
      runtime.child.kill('SIGTERM');
      runtime.running = false;
      return { ok: true };
    } catch (err) {
      return { ok: false, message: err && err.message ? err.message : 'Failed to stop server' };
    }
  }

  startAutostartServers() {
    if (!this.enabled) return { ok: true, started: [] };
    const started = [];
    for (const server of this.servers) {
      if (!server.autostart) continue;
      const res = this.startServer(server.id);
      if (res.ok) started.push(server.id);
    }
    return { ok: true, started };
  }

  stopAll() {
    const ids = Array.from(this.runtime.keys());
    ids.forEach((id) => this.stopServer(id));
    return { ok: true };
  }

  setEnabled(enabled) {
    this.enabled = !!enabled;
    if (!this.enabled) {
      this.stopAll();
      return { ok: true, status: this.buildStatus() };
    }
    this.startAutostartServers();
    return { ok: true, status: this.buildStatus() };
  }
}

module.exports = {
  MCPManager,
};
