const fs = require('fs');
const path = require('path');

class TelegramService {
  constructor(options = {}) {
    this.logger = options.logger || console;
    this.tokenProvider = typeof options.tokenProvider === 'function' ? options.tokenProvider : (() => '');
    this.onMessage = typeof options.onMessage === 'function' ? options.onMessage : null;
    this.pollIntervalSec = Math.max(1, Number(options.pollIntervalSec || 1));
    this.offset = Number(options.initialOffset || 0) || 0;
    this.running = false;
    this.loopPromise = null;
    this.lastError = '';
    this.lastPollAt = 0;
    this.backoffSec = 1;
  }

  setOffset(nextOffset) {
    this.offset = Math.max(0, Number(nextOffset || 0));
  }

  setPollIntervalSec(value) {
    this.pollIntervalSec = Math.max(1, Number(value || 1));
  }

  async sleep(ms) {
    await new Promise((resolve) => setTimeout(resolve, Math.max(0, Number(ms) || 0)));
  }

  async callApi(method, payload = {}, timeoutMs = 30_000) {
    const token = String(this.tokenProvider() || '').trim();
    if (!token) {
      return { ok: false, message: 'Telegram token not configured.' };
    }
    const url = `https://api.telegram.org/bot${token}/${method}`;
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), Math.max(2_000, Number(timeoutMs || 30_000)));
    try {
      const response = await fetch(url, {
        method: 'POST',
        headers: {
          'content-type': 'application/json',
        },
        body: JSON.stringify(payload || {}),
        signal: controller.signal,
      });
      const raw = await response.text();
      let json = null;
      try {
        json = raw ? JSON.parse(raw) : null;
      } catch (_) {
        json = null;
      }
      if (!response.ok || !(json && json.ok)) {
        return {
          ok: false,
          status: response.status,
          message: String((json && json.description) || raw || `Telegram API ${method} failed.`),
          raw,
          json,
        };
      }
      return { ok: true, result: json.result, raw, json };
    } catch (err) {
      return { ok: false, message: String((err && err.message) || 'Telegram request failed.') };
    } finally {
      clearTimeout(timeout);
    }
  }

  async callApiMultipart(method, formData, timeoutMs = 45_000) {
    const token = String(this.tokenProvider() || '').trim();
    if (!token) {
      return { ok: false, message: 'Telegram token not configured.' };
    }
    const url = `https://api.telegram.org/bot${token}/${method}`;
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), Math.max(2_000, Number(timeoutMs || 45_000)));
    try {
      const response = await fetch(url, {
        method: 'POST',
        body: formData,
        signal: controller.signal,
      });
      const raw = await response.text();
      let json = null;
      try {
        json = raw ? JSON.parse(raw) : null;
      } catch (_) {
        json = null;
      }
      if (!response.ok || !(json && json.ok)) {
        return {
          ok: false,
          status: response.status,
          message: String((json && json.description) || raw || `Telegram API ${method} failed.`),
          raw,
          json,
        };
      }
      return { ok: true, result: json.result, raw, json };
    } catch (err) {
      return { ok: false, message: String((err && err.message) || 'Telegram request failed.') };
    } finally {
      clearTimeout(timeout);
    }
  }

  async getMe() {
    return this.callApi('getMe', {}, 12_000);
  }

  async sendMessage(chatId, text, options = {}) {
    const chat_id = String(chatId || '').trim();
    const messageText = String(text || '').trim();
    if (!chat_id || !messageText) {
      return { ok: false, message: 'chat_id and text are required.' };
    }
    const payload = {
      chat_id,
      text: messageText,
      disable_web_page_preview: !!options.disable_web_page_preview,
    };
    if (options.parse_mode) payload.parse_mode = String(options.parse_mode);
    return this.callApi('sendMessage', payload, 20_000);
  }

  async sendPhoto(chatId, source = {}, options = {}) {
    const chat_id = String(chatId || '').trim();
    const payload = (source && typeof source === 'object') ? source : {};
    if (!chat_id) return { ok: false, message: 'chat_id is required.' };

    const form = new FormData();
    form.append('chat_id', chat_id);
    if (options.caption) form.append('caption', String(options.caption));
    if (options.parse_mode) form.append('parse_mode', String(options.parse_mode));
    if (Object.prototype.hasOwnProperty.call(options, 'disable_notification')) {
      form.append('disable_notification', options.disable_notification ? 'true' : 'false');
    }

    const fileId = String(payload.file_id || '').trim();
    const localPath = String(payload.local_path || '').trim();
    const buffer = Buffer.isBuffer(payload.buffer) ? payload.buffer : null;
    const filenameInput = String(payload.filename || '').trim();

    if (fileId) {
      form.append('photo', fileId);
      return this.callApiMultipart('sendPhoto', form, 30_000);
    }

    let fileBuffer = null;
    let filename = filenameInput || 'photo.jpg';
    if (localPath) {
      if (!fs.existsSync(localPath)) return { ok: false, message: 'Local photo path does not exist.' };
      fileBuffer = fs.readFileSync(localPath);
      const base = path.basename(localPath);
      if (base) filename = base;
    } else if (buffer) {
      fileBuffer = buffer;
    } else {
      return { ok: false, message: 'photo source requires local_path, buffer, or file_id.' };
    }
    const blob = new Blob([fileBuffer], { type: 'image/jpeg' });
    form.append('photo', blob, filename);
    return this.callApiMultipart('sendPhoto', form, 45_000);
  }

  async getFile(fileId = '') {
    const file_id = String(fileId || '').trim();
    if (!file_id) return { ok: false, message: 'file_id is required.' };
    return this.callApi('getFile', { file_id }, 20_000);
  }

  async downloadFile(filePath = '') {
    const token = String(this.tokenProvider() || '').trim();
    if (!token) return { ok: false, message: 'Telegram token not configured.' };
    const file_path = String(filePath || '').trim();
    if (!file_path) return { ok: false, message: 'file_path is required.' };
    const url = `https://api.telegram.org/file/bot${token}/${file_path.replace(/^\/+/, '')}`;
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 45_000);
    try {
      const response = await fetch(url, { signal: controller.signal });
      if (!response || !response.ok) {
        return { ok: false, status: response ? response.status : 0, message: `Telegram file download failed (${response ? response.status : 'unknown'}).` };
      }
      const arr = await response.arrayBuffer();
      return { ok: true, buffer: Buffer.from(arr), file_path };
    } catch (err) {
      return { ok: false, message: String((err && err.message) || 'Telegram file download failed.') };
    } finally {
      clearTimeout(timeout);
    }
  }

  async pollUpdates() {
    const payload = {
      timeout: 25,
      offset: this.offset + 1,
      allowed_updates: ['message'],
    };
    return this.callApi('getUpdates', payload, 35_000);
  }

  async loop() {
    while (this.running) {
      this.lastPollAt = Date.now();
      const res = await this.pollUpdates();
      if (!this.running) break;

      if (!res || !res.ok) {
        this.lastError = String((res && res.message) || 'Telegram polling failed.');
        this.logger.warn('[telegram] poll failed:', this.lastError);
        this.backoffSec = Math.min(30, Math.max(1, this.backoffSec * 2));
        await this.sleep(this.backoffSec * 1000);
        continue;
      }

      this.lastError = '';
      this.backoffSec = 1;
      const updates = Array.isArray(res.result) ? res.result : [];
      for (const update of updates) {
        const updateId = Number((update && update.update_id) || 0);
        if (updateId > this.offset) this.offset = updateId;
        if (!this.onMessage) continue;
        const message = (update && update.message && typeof update.message === 'object') ? update.message : null;
        if (!message) continue;
        try {
          await this.onMessage(message, update);
        } catch (err) {
          this.logger.warn('[telegram] onMessage failed:', String((err && err.message) || err));
        }
      }

      await this.sleep(this.pollIntervalSec * 1000);
    }
  }

  start() {
    if (this.running) return { ok: true, running: true };
    this.running = true;
    this.loopPromise = this.loop().catch((err) => {
      this.lastError = String((err && err.message) || 'Telegram loop crashed.');
      this.logger.warn('[telegram] loop crashed:', this.lastError);
      this.running = false;
    });
    return { ok: true, running: true };
  }

  async stop() {
    this.running = false;
    if (this.loopPromise) {
      try {
        await this.loopPromise;
      } catch (_) {
        // noop
      }
      this.loopPromise = null;
    }
    return { ok: true, running: false };
  }

  status() {
    return {
      ok: true,
      running: this.running,
      offset: this.offset,
      poll_interval_sec: this.pollIntervalSec,
      last_error: this.lastError,
      last_poll_at: this.lastPollAt,
    };
  }
}

module.exports = {
  TelegramService,
};
