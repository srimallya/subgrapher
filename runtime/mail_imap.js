const net = require('net');
const tls = require('tls');

const IMAP_TIMEOUT_MS = 20000;

function quoteImap(value = '') {
  return `"${String(value || '').replace(/\\/g, '\\\\').replace(/"/g, '\\"')}"`;
}

function encodeXoauth2(username = '', accessToken = '') {
  return Buffer.from(`user=${username}\u0001auth=Bearer ${accessToken}\u0001\u0001`, 'utf8').toString('base64');
}

function decodeModifiedUtf7(value = '') {
  return String(value || '').replace(/&([^-]*)-/g, (_match, chunk) => {
    if (!chunk) return '&';
    const normalized = String(chunk || '').replace(/,/g, '/');
    try {
      const bytes = Buffer.from(normalized, 'base64');
      const swapped = Buffer.alloc(bytes.length);
      for (let i = 0; i < bytes.length; i += 2) {
        swapped[i] = bytes[i + 1] || 0;
        swapped[i + 1] = bytes[i] || 0;
      }
      return swapped.toString('utf16le').replace(/\u0000/g, '');
    } catch (_) {
      return `&${chunk}-`;
    }
  });
}

function parseListLine(line = '') {
  const text = String(line || '').trim();
  const match = text.match(/^\*\s+LIST\s+\(([^)]*)\)\s+(".*?"|NIL)\s+(.+)$/i);
  if (!match) return null;
  const flags = String(match[1] || '')
    .split(/\s+/)
    .map((item) => item.trim())
    .filter(Boolean);
  const delimiterRaw = String(match[2] || '').trim();
  const delimiter = /^NIL$/i.test(delimiterRaw) ? '' : delimiterRaw.replace(/^"|"$/g, '');
  const nameRaw = String(match[3] || '').trim();
  const mailboxName = decodeModifiedUtf7(nameRaw.replace(/^"|"$/g, ''));
  const lowerFlags = flags.map((item) => item.toLowerCase());
  let specialUse = '';
  if (lowerFlags.includes('\\inbox')) specialUse = 'inbox';
  else if (lowerFlags.includes('\\sent')) specialUse = 'sent';
  else if (lowerFlags.includes('\\drafts')) specialUse = 'drafts';
  else if (lowerFlags.includes('\\trash')) specialUse = 'trash';
  else if (lowerFlags.includes('\\archive') || lowerFlags.includes('\\all')) specialUse = 'archive';
  else if (lowerFlags.includes('\\junk') || lowerFlags.includes('\\spam')) specialUse = 'junk';
  return {
    flags,
    delimiter,
    name: mailboxName,
    special_use: specialUse,
  };
}

function parseFetchEnvelopeHeader(line = '', key = '') {
  const regex = new RegExp(`${String(key || '').replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}\\s+([^\\s\\)]+)`, 'i');
  const match = String(line || '').match(regex);
  return String((match && match[1]) || '').trim();
}

function parseFetchFlags(line = '') {
  const match = String(line || '').match(/FLAGS\s+\(([^)]*)\)/i);
  if (!match) return [];
  return String(match[1] || '')
    .split(/\s+/)
    .map((item) => item.trim())
    .filter(Boolean);
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
    this.capabilities = new Set();
    this.selectedMailbox = '';
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
    this.extractCapabilities([greeting]);
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

  extractCapabilities(lines = []) {
    (Array.isArray(lines) ? lines : []).forEach((line) => {
      const match = String(line || '').match(/CAPABILITY\s+(.+)$/i);
      if (!match) return;
      String(match[1] || '')
        .trim()
        .split(/\s+/)
        .map((item) => item.trim().toUpperCase())
        .filter(Boolean)
        .forEach((item) => this.capabilities.add(item));
    });
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
      if (options.continueLine && nextLine === '+') {
        this.socket.write(String(options.continueLine), 'utf8');
      } else if (options.continueLine && /^\+\s/i.test(nextLine)) {
        this.socket.write(String(options.continueLine), 'utf8');
      }
      if (new RegExp(`^${tag}\\s+`, 'i').test(nextLine)) {
        this.extractCapabilities(lines);
        const ok = new RegExp(`^${tag}\\s+OK\\b`, 'i').test(nextLine);
        if (!ok) throw new Error(`IMAP command failed: ${nextLine}`);
        return { tag, lines, literals };
      }
    }
  }

  async capability() {
    const res = await this.sendCommand('CAPABILITY');
    this.extractCapabilities(res.lines);
    return [...this.capabilities];
  }

  async login(username, password) {
    await this.sendCommand(`LOGIN ${quoteImap(username)} ${quoteImap(password)}`);
  }

  async authenticateXoauth2(username, accessToken) {
    await this.sendCommand(`AUTHENTICATE XOAUTH2 ${encodeXoauth2(username, accessToken)}`);
  }

  async selectMailbox(mailbox) {
    await this.sendCommand(`SELECT ${quoteImap(mailbox || 'INBOX')}`);
    this.selectedMailbox = String(mailbox || 'INBOX').trim() || 'INBOX';
  }

  async listMailboxes() {
    const res = await this.sendCommand('LIST "" "*"');
    return res.lines
      .filter((line) => /^\*\s+LIST\b/i.test(line))
      .map((line) => parseListLine(line))
      .filter(Boolean);
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

  async uidFetchMetadata(uids = []) {
    const uidList = Array.isArray(uids) ? uids.map((item) => Math.round(Number(item || 0))).filter(Boolean) : [];
    if (!uidList.length) return [];
    const res = await this.sendCommand(`UID FETCH ${uidList.join(',')} (UID FLAGS RFC822.SIZE BODY.PEEK[HEADER.FIELDS (MESSAGE-ID SUBJECT FROM DATE IN-REPLY-TO REFERENCES)])`, {
      collectLiterals: true,
    });
    const lines = res.lines.filter((line) => /^\*\s+\d+\s+FETCH\b/i.test(line));
    return lines.map((line, index) => ({
      uid: Math.round(Number(parseFetchEnvelopeHeader(line, 'UID') || 0)),
      flags: parseFetchFlags(line),
      size_bytes: Math.max(0, Number(parseFetchEnvelopeHeader(line, 'RFC822.SIZE') || 0)),
      header_source: Buffer.isBuffer(res.literals[index]) ? res.literals[index].toString('utf8') : '',
    })).filter((item) => item.uid > 0);
  }

  async fetchRawMessage(uid) {
    const res = await this.sendCommand(`UID FETCH ${Math.round(Number(uid || 0))} (UID FLAGS RFC822)`, { collectLiterals: true });
    const raw = Buffer.concat(res.literals).toString('utf8');
    if (!raw) throw new Error(`No RFC822 payload returned for UID ${uid}.`);
    const fetchLine = res.lines.find((line) => /^\*\s+\d+\s+FETCH\b/i.test(line)) || '';
    return {
      raw_source: raw,
      flags: parseFetchFlags(fetchLine),
      uid: Math.round(Number(parseFetchEnvelopeHeader(fetchLine, 'UID') || uid || 0)),
    };
  }

  async uidStoreFlags(uid, flags = [], mode = 'replace') {
    const cleanUid = Math.round(Number(uid || 0));
    if (!cleanUid) throw new Error('UID is required.');
    const cleanFlags = Array.isArray(flags) ? flags.map((item) => String(item || '').trim()).filter(Boolean) : [];
    const action = mode === 'add' ? '+FLAGS.SILENT' : (mode === 'remove' ? '-FLAGS.SILENT' : 'FLAGS.SILENT');
    await this.sendCommand(`UID STORE ${cleanUid} ${action} (${cleanFlags.join(' ')})`);
  }

  async uidCopy(uid, mailbox) {
    await this.sendCommand(`UID COPY ${Math.round(Number(uid || 0))} ${quoteImap(mailbox || 'INBOX')}`);
  }

  async uidMove(uid, mailbox) {
    const cleanUid = Math.round(Number(uid || 0));
    const target = String(mailbox || '').trim() || 'INBOX';
    if (this.capabilities.has('MOVE')) {
      await this.sendCommand(`UID MOVE ${cleanUid} ${quoteImap(target)}`);
      return;
    }
    await this.uidCopy(cleanUid, target);
    await this.uidStoreFlags(cleanUid, ['\\Deleted'], 'add');
    await this.sendCommand('EXPUNGE');
  }

  async appendMessage(mailbox, rawSource, options = {}) {
    const payload = Buffer.from(String(rawSource || ''), 'utf8');
    const flags = Array.isArray(options.flags) ? options.flags.map((item) => String(item || '').trim()).filter(Boolean) : [];
    const flagPart = flags.length ? ` (${flags.join(' ')})` : '';
    const datePart = options.internalDate ? ` "${String(options.internalDate).replace(/"/g, '')}"` : '';
    const tag = this.nextTag();
    const command = `${tag} APPEND ${quoteImap(mailbox || 'INBOX')}${flagPart}${datePart} {${payload.length}}\r\n`;
    this.socket.write(command, 'utf8');
    while (true) {
      const line = await this.readLine();
      if (/^\+\s?/i.test(line)) {
        this.socket.write(payload);
        this.socket.write('\r\n', 'utf8');
        break;
      }
      if (new RegExp(`^${tag}\\s+(NO|BAD)\\b`, 'i').test(line)) {
        throw new Error(`IMAP APPEND failed: ${line}`);
      }
    }
    while (true) {
      const line = await this.readLine();
      if (new RegExp(`^${tag}\\s+OK\\b`, 'i').test(line)) return true;
      if (new RegExp(`^${tag}\\s+(NO|BAD)\\b`, 'i').test(line)) {
        throw new Error(`IMAP APPEND failed: ${line}`);
      }
    }
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
