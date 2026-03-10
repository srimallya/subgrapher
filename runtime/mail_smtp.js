const net = require('net');
const tls = require('tls');
const crypto = require('crypto');

function nowTs() {
  return Date.now();
}

function buildLineReader(socket) {
  let buffer = '';
  const queue = [];
  const waiters = [];

  const flush = () => {
    while (waiters.length && queue.length) {
      const resolve = waiters.shift();
      resolve(queue.shift());
    }
  };

  socket.on('data', (chunk) => {
    buffer += Buffer.isBuffer(chunk) ? chunk.toString('utf8') : String(chunk || '');
    let marker = buffer.indexOf('\r\n');
    while (marker >= 0) {
      const line = buffer.slice(0, marker);
      buffer = buffer.slice(marker + 2);
      queue.push(line);
      marker = buffer.indexOf('\r\n');
    }
    flush();
  });

  return async function readLine() {
    if (queue.length) return queue.shift();
    return new Promise((resolve) => {
      waiters.push(resolve);
    });
  };
}

async function readResponse(readLine) {
  const lines = [];
  while (true) {
    const line = await readLine();
    if (typeof line !== 'string') throw new Error('SMTP connection closed.');
    lines.push(line);
    if (/^\d{3} /.test(line)) {
      return {
        code: Number(line.slice(0, 3)),
        lines,
        text: lines.join('\n'),
      };
    }
  }
}

function writeLine(socket, line) {
  return new Promise((resolve, reject) => {
    socket.write(`${String(line || '')}\r\n`, (err) => {
      if (err) reject(err);
      else resolve();
    });
  });
}

async function sendCommand(socket, readLine, command, allowedCodes = [250]) {
  await writeLine(socket, command);
  const response = await readResponse(readLine);
  if (!allowedCodes.includes(response.code)) {
    throw new Error(response.text || `SMTP command failed: ${command}`);
  }
  return response;
}

function parseEhloCapabilities(response = {}) {
  const capabilities = new Set();
  const authMechanisms = new Set();
  const lines = Array.isArray(response.lines) ? response.lines : [];
  lines.slice(1).forEach((line) => {
    const payload = String(line || '').replace(/^\d{3}[ -]?/, '').trim();
    if (!payload) return;
    const [name, ...rest] = payload.split(/\s+/);
    const upper = String(name || '').toUpperCase();
    capabilities.add(upper);
    if (upper === 'AUTH') {
      rest.forEach((item) => authMechanisms.add(String(item || '').toUpperCase()));
    }
  });
  return { capabilities, authMechanisms };
}

function createMessageId(domainHint = 'localhost') {
  const host = String(domainHint || 'localhost').replace(/[^a-z0-9.-]+/gi, '') || 'localhost';
  return `${crypto.randomBytes(12).toString('hex')}@${host}`;
}

function encodeHeader(value = '') {
  return String(value || '').replace(/\r?\n/g, ' ').trim();
}

function normalizeAddressList(value) {
  if (Array.isArray(value)) return value.map((item) => String(item || '').trim()).filter(Boolean);
  return String(value || '')
    .split(',')
    .map((item) => String(item || '').trim())
    .filter(Boolean);
}

function buildRawMessage(message = {}, options = {}) {
  const from = String(message.from || '').trim();
  const to = normalizeAddressList(message.to);
  const cc = normalizeAddressList(message.cc);
  const bcc = normalizeAddressList(message.bcc);
  if (!from) throw new Error('From address is required.');
  if (!to.length && !cc.length && !bcc.length) throw new Error('At least one recipient is required.');

  const domainHint = String(options.domainHint || from.split('@')[1] || 'localhost').trim();
  const messageId = String(message.message_id_header || createMessageId(domainHint)).replace(/[<>]/g, '');
  const sentDate = new Date(Number(options.sentTs || nowTs())).toUTCString();
  const body = String(message.body_text || '').replace(/\r\n/g, '\n').replace(/\r/g, '\n');
  const normalizedBody = body.split('\n').map((line) => (
    line.startsWith('.') ? `.${line}` : line
  )).join('\r\n');

  const headers = [
    `From: ${encodeHeader(from)}`,
    to.length ? `To: ${encodeHeader(to.join(', '))}` : '',
    cc.length ? `Cc: ${encodeHeader(cc.join(', '))}` : '',
    `Subject: ${encodeHeader(message.subject || '')}`,
    `Date: ${sentDate}`,
    `Message-ID: <${messageId}>`,
    message.in_reply_to ? `In-Reply-To: <${String(message.in_reply_to || '').replace(/[<>]/g, '')}>` : '',
    Array.isArray(message.references) && message.references.length
      ? `References: ${message.references.map((item) => `<${String(item || '').replace(/[<>]/g, '')}>`).join(' ')}`
      : '',
    'MIME-Version: 1.0',
    'Content-Type: text/plain; charset=UTF-8',
    'Content-Transfer-Encoding: 8bit',
  ].filter(Boolean);

  return {
    envelope: {
      from,
      to: [...to, ...cc, ...bcc],
    },
    raw: `${headers.join('\r\n')}\r\n\r\n${normalizedBody}\r\n.`,
    messageId,
    sentAt: sentDate,
  };
}

async function authenticate(socket, readLine, authMechanisms, username, password) {
  const user = String(username || '').trim();
  const pass = String(password || '');
  if (!user || !pass) throw new Error('SMTP credentials are missing.');

  if (authMechanisms.has('PLAIN')) {
    const token = Buffer.from(`\u0000${user}\u0000${pass}`, 'utf8').toString('base64');
    await sendCommand(socket, readLine, `AUTH PLAIN ${token}`, [235]);
    return;
  }
  if (authMechanisms.has('LOGIN') || !authMechanisms.size) {
    await sendCommand(socket, readLine, 'AUTH LOGIN', [334]);
    await sendCommand(socket, readLine, Buffer.from(user, 'utf8').toString('base64'), [334]);
    await sendCommand(socket, readLine, Buffer.from(pass, 'utf8').toString('base64'), [235]);
    return;
  }
  throw new Error(`Unsupported SMTP auth mechanisms: ${Array.from(authMechanisms).join(', ')}`);
}

function connectSocket(options = {}) {
  return new Promise((resolve, reject) => {
    const port = Math.max(1, Number(options.port || 465));
    const host = String(options.host || '').trim();
    if (!host) {
      reject(new Error('SMTP host is required.'));
      return;
    }
    const onError = (err) => reject(err);
    const socket = options.secure
      ? tls.connect({ host, port, servername: host }, () => resolve(socket))
      : net.connect({ host, port }, () => resolve(socket));
    socket.once('error', onError);
    socket.once('connect', () => {
      socket.removeListener('error', onError);
    });
  });
}

async function upgradeToTls(socket, host) {
  return new Promise((resolve, reject) => {
    const secureSocket = tls.connect({
      socket,
      servername: String(host || '').trim() || undefined,
    }, () => resolve(secureSocket));
    secureSocket.once('error', reject);
  });
}

async function sendMailViaSmtp(config = {}, message = {}) {
  const built = buildRawMessage(message, { domainHint: config.helo_name || config.host });
  let socket = await connectSocket({ host: config.host, port: config.port, secure: !!config.secure });
  let readLine = buildLineReader(socket);
  try {
    const greeting = await readResponse(readLine);
    if (greeting.code !== 220) throw new Error(greeting.text || 'SMTP server rejected connection.');

    let ehlo = await sendCommand(socket, readLine, `EHLO ${String(config.helo_name || 'subgrapher.local').trim()}`, [250]);
    let capabilities = parseEhloCapabilities(ehlo);

    if (!config.secure && config.starttls) {
      await sendCommand(socket, readLine, 'STARTTLS', [220]);
      socket = await upgradeToTls(socket, config.host);
      readLine = buildLineReader(socket);
      ehlo = await sendCommand(socket, readLine, `EHLO ${String(config.helo_name || 'subgrapher.local').trim()}`, [250]);
      capabilities = parseEhloCapabilities(ehlo);
    }

    await authenticate(socket, readLine, capabilities.authMechanisms, config.username, config.password);
    await sendCommand(socket, readLine, `MAIL FROM:<${built.envelope.from}>`, [250]);
    for (const recipient of built.envelope.to) {
      await sendCommand(socket, readLine, `RCPT TO:<${recipient}>`, [250, 251]);
    }
    await sendCommand(socket, readLine, 'DATA', [354]);
    await writeLine(socket, built.raw);
    const dataResponse = await readResponse(readLine);
    if (dataResponse.code !== 250) throw new Error(dataResponse.text || 'SMTP DATA failed.');
    await sendCommand(socket, readLine, 'QUIT', [221]);
    socket.end();
    return {
      ok: true,
      message_id_header: built.messageId,
      sent_at: built.sentAt,
      raw_source: built.raw.replace(/\r\n\.$/, '\r\n'),
    };
  } catch (err) {
    try {
      socket.end();
    } catch (_) {
      // noop
    }
    throw err;
  }
}

module.exports = {
  sendMailViaSmtp,
};
