const net = require('net');
const tls = require('tls');

const IMAP_TIMEOUT_MS = 15000;

function quoteImap(value = '') {
  return `"${String(value || '').replace(/\\/g, '\\\\').replace(/"/g, '\\"')}"`;
}

class ImapClient {
  constructor(options = {}) {
    this.host = String(options.host || '').trim();
    this.port = Number(options.port || 993);
    this.useTls = options.tls !== false;
    this.socket = null;
    this.buffer = Buffer.alloc(0);
    this.tagCounter = 1;
    this.ended = false;
    this.waiters = [];
  }

  async connect() {
    if (!this.host) throw new Error('IMAP host is required.');
    this.socket = this.useTls
      ? tls.connect({ host: this.host, port: this.port, servername: this.host, rejectUnauthorized: true })
      : net.connect({ host: this.host, port: this.port });
    this.socket.setTimeout(IMAP_TIMEOUT_MS, () => {
      this.socket.destroy(new Error('IMAP connection timed out.'));
    });
    this.socket.on('data', (chunk) => {
      this.buffer = Buffer.concat([this.buffer, Buffer.from(chunk)]);
      this.flushWaiters();
    });
    this.socket.on('error', (err) => {
      this.ended = true;
      this.flushWaiters(err);
    });
    this.socket.on('close', () => {
      this.ended = true;
      this.flushWaiters();
    });
    await this.waitFor(() => this.buffer.includes(Buffer.from('\r\n')));
    const greeting = await this.readLine();
    if (!/^\*\s+(OK|PREAUTH)/i.test(greeting)) {
      throw new Error(`IMAP greeting failed: ${greeting}`);
    }
  }

  flushWaiters(error = null) {
    const pending = this.waiters.splice(0);
    pending.forEach((item) => item(error));
  }

  waitFor(predicate) {
    return new Promise((resolve, reject) => {
      const check = (error = null) => {
        if (error) {
          reject(error);
          return;
        }
        try {
          if (predicate()) {
            resolve();
            return;
          }
        } catch (err) {
          reject(err);
          return;
        }
        if (this.ended) {
          reject(new Error('IMAP connection ended.'));
          return;
        }
        this.waiters.push(check);
      };
      check();
    });
  }

  async readLine() {
    await this.waitFor(() => this.buffer.includes(Buffer.from('\r\n')));
    const idx = this.buffer.indexOf(Buffer.from('\r\n'));
    const line = this.buffer.subarray(0, idx).toString('utf8');
    this.buffer = this.buffer.subarray(idx + 2);
    return line;
  }

  async readBytes(size) {
    const target = Math.max(0, Number(size || 0));
    await this.waitFor(() => this.buffer.length >= target);
    const out = this.buffer.subarray(0, target);
    this.buffer = this.buffer.subarray(target);
    return out;
  }

  nextTag() {
    const tag = `A${String(this.tagCounter).padStart(4, '0')}`;
    this.tagCounter += 1;
    return tag;
  }

  async sendCommand(command, options = {}) {
    const tag = this.nextTag();
    const line = `${tag} ${String(command || '').trim()}\r\n`;
    this.socket.write(line, 'utf8');
    const lines = [];
    const literals = [];
    while (true) {
      const nextLine = await this.readLine();
      lines.push(nextLine);
      const literalMatch = nextLine.match(/\{(\d+)\}$/);
      if (literalMatch) {
        const literalSize = Number(literalMatch[1] || 0);
        literals.push(await this.readBytes(literalSize));
      }
      if (new RegExp(`^${tag}\\s+`, 'i').test(nextLine)) {
        const ok = new RegExp(`^${tag}\\s+OK\\b`, 'i').test(nextLine);
        if (!ok) {
          throw new Error(`IMAP command failed: ${nextLine}`);
        }
        return { tag, lines, literals };
      }
      if (!options.collectLiterals && literals.length > 0) {
        // no-op, still need tagged completion
      }
    }
  }

  async login(username, password) {
    await this.sendCommand(`LOGIN ${quoteImap(username)} ${quoteImap(password)}`);
  }

  async selectMailbox(mailbox) {
    await this.sendCommand(`SELECT ${quoteImap(mailbox || 'INBOX')}`);
  }

  async uidSearchAll() {
    const res = await this.sendCommand('UID SEARCH ALL');
    const searchLine = res.lines.find((line) => /^\*\s+SEARCH\b/i.test(line)) || '';
    return String(searchLine.replace(/^\*\s+SEARCH\s*/i, '') || '')
      .trim()
      .split(/\s+/)
      .map((item) => Number(item))
      .filter((item) => Number.isFinite(item) && item > 0);
  }

  async fetchRawMessage(uid) {
    const res = await this.sendCommand(`UID FETCH ${Math.round(Number(uid || 0))} (UID RFC822)`, { collectLiterals: true });
    const raw = Buffer.concat(res.literals).toString('utf8');
    if (!raw) throw new Error(`No RFC822 payload returned for UID ${uid}.`);
    return raw;
  }

  async logout() {
    if (!this.socket || this.ended) return;
    try {
      await this.sendCommand('LOGOUT');
    } catch (_) {
      // ignore
    }
    try {
      this.socket.end();
    } catch (_) {
      // ignore
    }
  }
}

module.exports = {
  ImapClient,
};
