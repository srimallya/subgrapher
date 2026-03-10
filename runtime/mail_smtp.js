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
      queue.push(buffer.slice(0, marker));
      buffer = buffer.slice(marker + 2);
      marker = buffer.indexOf('\r\n');
    }
    flush();
  });

  return async function readLine() {
    if (queue.length) return queue.shift();
    return new Promise((resolve) => waiters.push(resolve));
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
    const [capability, ...rest] = payload.split(/\s+/);
    const upper = String(capability || '').toUpperCase();
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

function toBase64(bufferOrString) {
  return Buffer.isBuffer(bufferOrString)
    ? bufferOrString.toString('base64')
    : Buffer.from(String(bufferOrString || ''), 'utf8').toString('base64');
}

function chunkBase64(value = '', width = 76) {
  const clean = String(value || '');
  const out = [];
  for (let idx = 0; idx < clean.length; idx += width) {
    out.push(clean.slice(idx, idx + width));
  }
  return out.join('\r\n');
}

function guessContentDisposition(attachment = {}) {
  const inline = !!attachment.inline;
  return inline ? 'inline' : 'attachment';
}

function buildRawMessage(message = {}, options = {}) {
  const from = String(message.from || '').trim();
  const to = normalizeAddressList(message.to);
  const cc = normalizeAddressList(message.cc);
  const bcc = normalizeAddressList(message.bcc);
  if (!from) throw new Error('From address is required.');
  if (!to.length && !cc.length && !bcc.length) throw new Error('At least one recipient is required.');

  const attachments = Array.isArray(message.attachments) ? message.attachments.filter(Boolean) : [];
  const domainHint = String(options.domainHint || from.split('@')[1] || 'localhost').trim();
  const messageId = String(message.message_id_header || createMessageId(domainHint)).replace(/[<>]/g, '');
  const sentDate = new Date(Number(options.sentTs || nowTs())).toUTCString();
  const bodyText = String(message.body_text || '').replace(/\r\n/g, '\n').replace(/\r/g, '\n');
  const escapedBody = bodyText.split('\n').map((line) => (line.startsWith('.') ? `.${line}` : line)).join('\r\n');

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
  ].filter(Boolean);

  let bodyPart = '';
  if (attachments.length > 0) {
    const boundary = `subgrapher_mixed_${crypto.randomBytes(8).toString('hex')}`;
    headers.push(`Content-Type: multipart/mixed; boundary="${boundary}"`);
    const parts = [
      `--${boundary}`,
      'Content-Type: text/plain; charset=UTF-8',
      'Content-Transfer-Encoding: 8bit',
      '',
      escapedBody,
    ];
    attachments.forEach((attachment) => {
      const fileName = String((attachment && attachment.file_name) || 'attachment').trim() || 'attachment';
      const mimeType = String((attachment && attachment.mime_type) || 'application/octet-stream').trim() || 'application/octet-stream';
      const data = Buffer.isBuffer(attachment.data)
        ? attachment.data
        : Buffer.from(String((attachment && attachment.data_base64) || ''), 'base64');
      const disposition = guessContentDisposition(attachment);
      parts.push(
        `--${boundary}`,
        `Content-Type: ${mimeType}; name="${encodeHeader(fileName)}"`,
        'Content-Transfer-Encoding: base64',
        `Content-Disposition: ${disposition}; filename="${encodeHeader(fileName)}"`,
        '',
        chunkBase64(toBase64(data)),
      );
    });
    parts.push(`--${boundary}--`, '');
    bodyPart = parts.join('\r\n');
  } else {
    headers.push('Content-Type: text/plain; charset=UTF-8');
    headers.push('Content-Transfer-Encoding: 8bit');
    bodyPart = `${escapedBody}\r\n`;
  }

  const rawWithoutTerminator = `${headers.join('\r\n')}\r\n\r\n${bodyPart}`;
  return {
    envelope: {
      from,
      to: [...to, ...cc, ...bcc],
    },
    raw: rawWithoutTerminator,
    dataTerminated: `${rawWithoutTerminator}\r\n.`,
    messageId,
    sentAt: sentDate,
  };
}

async function authenticate(socket, readLine, authMechanisms, auth = {}) {
  const username = String(auth.username || '').trim();
  if (auth.type === 'xoauth2') {
    const accessToken = String(auth.access_token || '').trim();
    if (!username || !accessToken) throw new Error('SMTP XOAUTH2 credentials are missing.');
    const token = Buffer.from(`user=${username}\u0001auth=Bearer ${accessToken}\u0001\u0001`, 'utf8').toString('base64');
    await sendCommand(socket, readLine, `AUTH XOAUTH2 ${token}`, [235]);
    return;
  }

  const password = String(auth.password || '');
  if (!username || !password) throw new Error('SMTP credentials are missing.');

  if (authMechanisms.has('PLAIN')) {
    const token = Buffer.from(`\u0000${username}\u0000${password}`, 'utf8').toString('base64');
    await sendCommand(socket, readLine, `AUTH PLAIN ${token}`, [235]);
    return;
  }
  await sendCommand(socket, readLine, 'AUTH LOGIN', [334]);
  await sendCommand(socket, readLine, Buffer.from(username, 'utf8').toString('base64'), [334]);
  await sendCommand(socket, readLine, Buffer.from(password, 'utf8').toString('base64'), [235]);
}

function connectSocket(options = {}) {
  return new Promise((resolve, reject) => {
    const port = Math.max(1, Number(options.port || 465));
    const host = String(options.host || '').trim();
    if (!host) {
      reject(new Error('SMTP host is required.'));
      return;
    }
    const onConnected = () => {
      socket.removeListener('error', onError);
      resolve(socket);
    };
    const onError = (err) => {
      reject(err);
    };
    const socket = options.secure
      ? tls.connect({ host, port, servername: host }, onConnected)
      : net.connect({ host, port }, onConnected);
    socket.once('error', onError);
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

    await authenticate(socket, readLine, capabilities.authMechanisms, config.auth || {
      type: config.auth_type || 'password',
      username: config.username,
      password: config.password,
      access_token: config.access_token,
    });
    await sendCommand(socket, readLine, `MAIL FROM:<${built.envelope.from}>`, [250]);
    for (const recipient of built.envelope.to) {
      await sendCommand(socket, readLine, `RCPT TO:<${recipient}>`, [250, 251]);
    }
    await sendCommand(socket, readLine, 'DATA', [354]);
    await writeLine(socket, built.dataTerminated);
    const final = await readResponse(readLine);
    if (final.code !== 250) throw new Error(final.text || 'SMTP DATA failed.');
    await sendCommand(socket, readLine, 'QUIT', [221]);
    socket.end();
    return {
      ok: true,
      message_id_header: built.messageId,
      sent_at: built.sentAt,
      raw_source: built.raw,
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
  buildRawMessage,
  sendMailViaSmtp,
};
