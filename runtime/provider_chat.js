const DEFAULT_TIMEOUT_MS = 45_000;

function normalizeText(value) {
  return String(value || '').trim();
}

function toModelId(model) {
  const raw = normalizeText(model);
  if (!raw) return '';
  return raw.replace(/^models\//, '');
}

function normalizeBaseUrl(value, fallback = '') {
  const raw = normalizeText(value || fallback);
  if (!raw) return normalizeText(fallback);
  return raw.replace(/\/+$/, '');
}

async function readResponseJson(response) {
  const raw = await response.text();
  let json = null;
  try {
    json = raw ? JSON.parse(raw) : null;
  } catch (_) {
    json = null;
  }
  return { raw, json };
}

function extractErrorMessage(raw, json, fallback) {
  if (json && json.error) {
    if (typeof json.error === 'string') return json.error;
    if (typeof json.error.message === 'string') return json.error.message;
  }
  if (json && typeof json.message === 'string') return json.message;
  const text = normalizeText(raw);
  if (text) return text.slice(0, 240);
  return fallback;
}

function extractOpenAiDelta(payload) {
  const choices = Array.isArray(payload && payload.choices) ? payload.choices : [];
  const choice = choices[0] || null;
  const delta = choice && choice.delta ? choice.delta.content : '';
  if (typeof delta === 'string') return delta;
  if (Array.isArray(delta)) {
    return delta.map((part) => {
      if (typeof part === 'string') return part;
      if (part && typeof part.text === 'string') return part.text;
      return '';
    }).join('');
  }
  return '';
}

function extractOpenAiFinal(payload) {
  const choices = Array.isArray(payload && payload.choices) ? payload.choices : [];
  const choice = choices[0] || null;
  const content = choice && choice.message ? choice.message.content : '';
  if (typeof content === 'string') return content;
  if (Array.isArray(content)) {
    return content.map((part) => {
      if (typeof part === 'string') return part;
      if (part && typeof part.text === 'string') return part.text;
      return '';
    }).join('');
  }
  return '';
}

function extractAnthropicDelta(payload) {
  const type = String((payload && payload.type) || '').trim().toLowerCase();
  if (type !== 'content_block_delta') return '';
  const delta = payload && payload.delta ? payload.delta.text : '';
  return typeof delta === 'string' ? delta : '';
}

function extractAnthropicFinal(payload) {
  const content = Array.isArray(payload && payload.content) ? payload.content : [];
  return content.map((block) => {
    if (!block || typeof block !== 'object') return '';
    if (String(block.type || '').toLowerCase() !== 'text') return '';
    return String(block.text || '');
  }).join('');
}

function extractGoogleText(payload) {
  const candidates = Array.isArray(payload && payload.candidates) ? payload.candidates : [];
  const first = candidates[0] || null;
  const parts = Array.isArray(first && first.content && first.content.parts) ? first.content.parts : [];
  return parts.map((part) => String((part && part.text) || '')).join('');
}

function safeJsonParse(raw, fallback = {}) {
  if (raw == null) return fallback;
  if (typeof raw === 'object') return raw;
  const text = String(raw || '').trim();
  if (!text) return fallback;
  try {
    return JSON.parse(text);
  } catch (_) {
    return fallback;
  }
}

function normalizeOpenAiToolCalls(payload) {
  const choices = Array.isArray(payload && payload.choices) ? payload.choices : [];
  const message = (choices[0] && choices[0].message) ? choices[0].message : {};
  const toolCalls = Array.isArray(message.tool_calls) ? message.tool_calls : [];
  return toolCalls.map((call, index) => ({
    id: String((call && call.id) || `tool_${index}`),
    function: {
      name: String((call && call.function && call.function.name) || ''),
      arguments: String((call && call.function && call.function.arguments) || '{}'),
    },
  })).filter((call) => call.function.name);
}

function normalizeAnthropicToolCalls(payload) {
  const content = Array.isArray(payload && payload.content) ? payload.content : [];
  return content
    .filter((block) => String((block && block.type) || '').toLowerCase() === 'tool_use')
    .map((block, index) => ({
      id: String((block && block.id) || `tool_${index}`),
      function: {
        name: String((block && block.name) || ''),
        arguments: JSON.stringify((block && block.input && typeof block.input === 'object') ? block.input : {}),
      },
    }))
    .filter((call) => call.function.name);
}

function normalizeAnthropicText(payload) {
  const content = Array.isArray(payload && payload.content) ? payload.content : [];
  return content
    .filter((block) => String((block && block.type) || '').toLowerCase() === 'text')
    .map((block) => String((block && block.text) || ''))
    .join('');
}

function normalizeToolSchemaForOpenAi(tools = []) {
  return (Array.isArray(tools) ? tools : []).map((tool) => ({
    type: 'function',
    function: {
      name: String((tool && tool.name) || ''),
      description: String((tool && tool.description) || ''),
      parameters: (tool && tool.parameters && typeof tool.parameters === 'object')
        ? tool.parameters
        : { type: 'object', properties: {} },
    },
  })).filter((tool) => tool.function.name);
}

function normalizeToolSchemaForAnthropic(tools = []) {
  return (Array.isArray(tools) ? tools : []).map((tool) => ({
    name: String((tool && tool.name) || ''),
    description: String((tool && tool.description) || ''),
    input_schema: (tool && tool.parameters && typeof tool.parameters === 'object')
      ? tool.parameters
      : { type: 'object', properties: {} },
  })).filter((tool) => tool.name);
}

function normalizeMessagesForOpenAi(messages = []) {
  const list = Array.isArray(messages) ? messages : [];
  return list.map((message) => {
    const role = String((message && message.role) || 'user').toLowerCase();
    if (role === 'assistant' && Array.isArray(message.tool_calls) && message.tool_calls.length > 0) {
      return {
        role: 'assistant',
        content: String(message.content || ''),
        tool_calls: message.tool_calls.map((call, index) => ({
          id: String((call && call.id) || `tool_${index}`),
          type: 'function',
          function: {
            name: String((call && call.function && call.function.name) || ''),
            arguments: String((call && call.function && call.function.arguments) || '{}'),
          },
        })),
      };
    }
    if (role === 'tool') {
      return {
        role: 'tool',
        tool_call_id: String((message && message.tool_call_id) || ''),
        content: String((message && message.content) || ''),
      };
    }
    if (role === 'assistant') {
      return { role: 'assistant', content: String((message && message.content) || '') };
    }
    return { role: 'user', content: String((message && message.content) || '') };
  });
}

function normalizeMessagesForAnthropic(messages = []) {
  const list = Array.isArray(messages) ? messages : [];
  const out = [];
  list.forEach((message) => {
    const role = String((message && message.role) || '').toLowerCase();
    if (role === 'user') {
      out.push({ role: 'user', content: String((message && message.content) || '') });
      return;
    }
    if (role === 'assistant') {
      const toolCalls = Array.isArray(message.tool_calls) ? message.tool_calls : [];
      if (toolCalls.length === 0) {
        out.push({ role: 'assistant', content: String((message && message.content) || '') });
        return;
      }
      const blocks = [];
      const text = String((message && message.content) || '').trim();
      if (text) blocks.push({ type: 'text', text });
      toolCalls.forEach((call, index) => {
        const id = String((call && call.id) || `tool_${index}`);
        const name = String((call && call.function && call.function.name) || '');
        const input = safeJsonParse(call && call.function ? call.function.arguments : '{}', {});
        if (!name) return;
        blocks.push({
          type: 'tool_use',
          id,
          name,
          input,
        });
      });
      out.push({ role: 'assistant', content: blocks });
      return;
    }
    if (role === 'tool') {
      out.push({
        role: 'user',
        content: [{
          type: 'tool_result',
          tool_use_id: String((message && message.tool_call_id) || ''),
          content: String((message && message.content) || ''),
        }],
      });
    }
  });
  return out;
}

async function streamSse(response, onEvent, signal) {
  if (!response.body || typeof response.body.getReader !== 'function') {
    throw new Error('Streaming response is not available.');
  }
  const reader = response.body.getReader();
  const decoder = new TextDecoder('utf-8');
  let buffer = '';

  while (true) {
    if (signal && signal.aborted) throw new Error('Request canceled.');
    const { done, value } = await reader.read();
    if (done) break;
    buffer += decoder.decode(value, { stream: true }).replace(/\r\n/g, '\n');
    while (buffer.includes('\n\n')) {
      const idx = buffer.indexOf('\n\n');
      const chunk = buffer.slice(0, idx);
      buffer = buffer.slice(idx + 2);
      if (!chunk.trim()) continue;
      let eventType = 'message';
      const dataLines = [];
      chunk.split('\n').forEach((line) => {
        if (!line || line.startsWith(':')) return;
        if (line.startsWith('event:')) {
          eventType = line.slice('event:'.length).trim() || 'message';
          return;
        }
        if (line.startsWith('data:')) {
          dataLines.push(line.slice('data:'.length).trimStart());
        }
      });
      const data = dataLines.join('\n');
      await onEvent({ event: eventType, data });
    }
  }

  if (buffer.trim()) {
    await onEvent({ event: 'message', data: buffer.trim() });
  }
}

async function openAiLikeChat(params, options = {}) {
  const provider = normalizeText(params.provider);
  const model = toModelId(params.model);
  const apiKey = String(params.apiKey || '').trim();
  const systemPrompt = String(params.systemPrompt || '').trim();
  const userPrompt = String(params.userPrompt || '').trim();
  const signal = options.signal;
  const onDelta = typeof options.onDelta === 'function' ? options.onDelta : null;
  const wantsStreaming = !!onDelta;
  const baseUrl = provider === 'cerebras'
    ? 'https://api.cerebras.ai'
    : (
      provider === 'lmstudio'
        ? normalizeBaseUrl(params.baseUrl || params.lmstudio_base_url, 'http://127.0.0.1:1234')
        : 'https://api.openai.com'
    );

  const body = {
    model,
    messages: [
      ...(systemPrompt ? [{ role: 'system', content: systemPrompt }] : []),
      { role: 'user', content: userPrompt },
    ],
    temperature: 0.2,
    stream: wantsStreaming,
  };

  const headers = {
    'content-type': 'application/json',
  };
  if (apiKey) headers.authorization = `Bearer ${apiKey}`;

  const response = await fetch(`${baseUrl}/v1/chat/completions`, {
    method: 'POST',
    headers,
    body: JSON.stringify(body),
    signal,
  });

  if (!response.ok) {
    const { raw, json } = await readResponseJson(response);
    throw new Error(extractErrorMessage(raw, json, `${provider} request failed (${response.status}).`));
  }

  if (!wantsStreaming) {
    const { json } = await readResponseJson(response);
    return {
      ok: true,
      text: extractOpenAiFinal(json),
      provider,
      model,
      streamed: false,
    };
  }

  let text = '';
  await streamSse(response, async ({ data }) => {
    if (!data) return;
    if (data === '[DONE]') return;
    let payload = null;
    try {
      payload = JSON.parse(data);
    } catch (_) {
      return;
    }
    const delta = extractOpenAiDelta(payload);
    if (!delta) return;
    text += delta;
    onDelta(delta);
  }, signal);

  return { ok: true, text, provider, model, streamed: true };
}

async function anthropicChat(params, options = {}) {
  const provider = normalizeText(params.provider);
  const model = toModelId(params.model);
  const apiKey = String(params.apiKey || '').trim();
  const systemPrompt = String(params.systemPrompt || '').trim();
  const userPrompt = String(params.userPrompt || '').trim();
  const signal = options.signal;
  const onDelta = typeof options.onDelta === 'function' ? options.onDelta : null;
  const wantsStreaming = !!onDelta;

  const body = {
    model,
    max_tokens: 1100,
    temperature: 0.2,
    stream: wantsStreaming,
    messages: [{ role: 'user', content: userPrompt }],
  };
  if (systemPrompt) body.system = systemPrompt;

  const response = await fetch('https://api.anthropic.com/v1/messages', {
    method: 'POST',
    headers: {
      'x-api-key': apiKey,
      'anthropic-version': '2023-06-01',
      'content-type': 'application/json',
    },
    body: JSON.stringify(body),
    signal,
  });

  if (!response.ok) {
    const { raw, json } = await readResponseJson(response);
    throw new Error(extractErrorMessage(raw, json, `anthropic request failed (${response.status}).`));
  }

  if (!wantsStreaming) {
    const { json } = await readResponseJson(response);
    return {
      ok: true,
      text: extractAnthropicFinal(json),
      provider,
      model,
      streamed: false,
    };
  }

  let text = '';
  await streamSse(response, async ({ data }) => {
    if (!data) return;
    let payload = null;
    try {
      payload = JSON.parse(data);
    } catch (_) {
      return;
    }
    const delta = extractAnthropicDelta(payload);
    if (!delta) return;
    text += delta;
    onDelta(delta);
  }, signal);

  return { ok: true, text, provider, model, streamed: true };
}

function buildGoogleRequestBody(systemPrompt, userPrompt) {
  const body = {
    contents: [{ role: 'user', parts: [{ text: userPrompt }] }],
  };
  if (systemPrompt) {
    body.systemInstruction = { role: 'system', parts: [{ text: systemPrompt }] };
  }
  return body;
}

async function googleStreamChat(params, options = {}) {
  const model = toModelId(params.model);
  const apiKey = String(params.apiKey || '').trim();
  const systemPrompt = String(params.systemPrompt || '').trim();
  const userPrompt = String(params.userPrompt || '').trim();
  const signal = options.signal;
  const onDelta = typeof options.onDelta === 'function' ? options.onDelta : null;
  if (!onDelta) {
    return { ok: false, message: 'No streaming handler configured.' };
  }

  const endpoint = `https://generativelanguage.googleapis.com/v1beta/models/${encodeURIComponent(model)}:streamGenerateContent?alt=sse&key=${encodeURIComponent(apiKey)}`;
  const response = await fetch(endpoint, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(buildGoogleRequestBody(systemPrompt, userPrompt)),
    signal,
  });

  if (!response.ok) {
    const { raw, json } = await readResponseJson(response);
    throw new Error(extractErrorMessage(raw, json, `google stream request failed (${response.status}).`));
  }

  let text = '';
  await streamSse(response, async ({ data }) => {
    if (!data || data === '[DONE]') return;
    let payload = null;
    try {
      payload = JSON.parse(data);
    } catch (_) {
      return;
    }
    const delta = extractGoogleText(payload);
    if (!delta) return;
    text += delta;
    onDelta(delta);
  }, signal);

  return { ok: true, text, provider: 'google', model, streamed: true };
}

async function googleNonStreamChat(params, options = {}) {
  const model = toModelId(params.model);
  const apiKey = String(params.apiKey || '').trim();
  const systemPrompt = String(params.systemPrompt || '').trim();
  const userPrompt = String(params.userPrompt || '').trim();
  const signal = options.signal;
  const endpoint = `https://generativelanguage.googleapis.com/v1beta/models/${encodeURIComponent(model)}:generateContent?key=${encodeURIComponent(apiKey)}`;
  const response = await fetch(endpoint, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(buildGoogleRequestBody(systemPrompt, userPrompt)),
    signal,
  });

  if (!response.ok) {
    const { raw, json } = await readResponseJson(response);
    throw new Error(extractErrorMessage(raw, json, `google request failed (${response.status}).`));
  }

  const { json } = await readResponseJson(response);
  return {
    ok: true,
    text: extractGoogleText(json),
    provider: 'google',
    model,
    streamed: false,
  };
}

async function chatWithProvider(params = {}, options = {}) {
  const provider = normalizeText(params.provider).toLowerCase();
  const model = toModelId(params.model);
  const apiKey = String(params.apiKey || '').trim();
  const baseUrl = normalizeBaseUrl(params.baseUrl || params.lmstudio_base_url, '');
  const systemPrompt = String(params.systemPrompt || '').trim();
  const userPrompt = String(params.userPrompt || '').trim();
  const timeoutMs = Number.isFinite(Number(options.timeoutMs))
    ? Math.max(5_000, Number(options.timeoutMs))
    : DEFAULT_TIMEOUT_MS;
  const onDelta = typeof options.onDelta === 'function' ? options.onDelta : null;
  const requiresApiKey = provider !== 'lmstudio';
  if (!provider || !model || (requiresApiKey && !apiKey)) {
    throw new Error('Provider and model are required. API key is required for remote providers.');
  }
  if (!userPrompt) {
    return { ok: true, text: '', provider, model, streamed: false };
  }

  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(new Error('Provider request timed out.')), timeoutMs);
  const upstream = options.signal;
  const onAbort = () => controller.abort(upstream.reason || new Error('Request canceled.'));
  if (upstream && typeof upstream.addEventListener === 'function') {
    if (upstream.aborted) controller.abort(upstream.reason || new Error('Request canceled.'));
    upstream.addEventListener('abort', onAbort, { once: true });
  }

  try {
    const request = { provider, model, apiKey, systemPrompt, userPrompt, baseUrl };
    if (provider === 'openai' || provider === 'cerebras' || provider === 'lmstudio') {
      return await openAiLikeChat(request, { signal: controller.signal, onDelta });
    }
    if (provider === 'anthropic') {
      return await anthropicChat(request, { signal: controller.signal, onDelta });
    }
    if (provider === 'google') {
      if (onDelta) {
        try {
          return await googleStreamChat(request, { signal: controller.signal, onDelta });
        } catch (_) {
          // Fallback to non-stream mode for Google providers.
        }
      }
      return await googleNonStreamChat(request, { signal: controller.signal });
    }
    throw new Error(`Unsupported provider: ${provider}`);
  } finally {
    clearTimeout(timeout);
    if (upstream && typeof upstream.removeEventListener === 'function') {
      upstream.removeEventListener('abort', onAbort);
    }
  }
}

async function callProviderWithTools(params = {}, options = {}) {
  const provider = normalizeText(params.provider).toLowerCase();
  const model = toModelId(params.model);
  const apiKey = String(params.apiKey || '').trim();
  const baseUrl = normalizeBaseUrl(params.baseUrl || params.lmstudio_base_url, '');
  const systemPrompt = String(params.systemPrompt || '').trim();
  const messages = Array.isArray(params.messages) ? params.messages : [];
  const tools = Array.isArray(params.tools) ? params.tools : [];
  const disableTools = !!(
    (options && Object.prototype.hasOwnProperty.call(options, 'disableTools') && options.disableTools)
    || (params && Object.prototype.hasOwnProperty.call(params, 'disableTools') && params.disableTools)
  );
  const timeoutMs = Number.isFinite(Number(options.timeoutMs || params.timeoutMs))
    ? Math.max(5_000, Number(options.timeoutMs || params.timeoutMs))
    : DEFAULT_TIMEOUT_MS;
  const upstream = options.signal || params.signal;

  const requiresApiKey = provider !== 'lmstudio';
  if (!provider || !model || (requiresApiKey && !apiKey)) {
    throw new Error('Provider and model are required. API key is required for remote providers.');
  }
  if (!['openai', 'anthropic', 'cerebras', 'lmstudio'].includes(provider)) {
    throw new Error(`Tool-calling is not supported for provider: ${provider}`);
  }

  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(new Error('Provider request timed out.')), timeoutMs);
  const onAbort = () => controller.abort(upstream.reason || new Error('Request canceled.'));
  if (upstream && typeof upstream.addEventListener === 'function') {
    if (upstream.aborted) controller.abort(upstream.reason || new Error('Request canceled.'));
    upstream.addEventListener('abort', onAbort, { once: true });
  }

  try {
    if (provider === 'openai' || provider === 'cerebras' || provider === 'lmstudio') {
      const endpoint = provider === 'cerebras'
        ? 'https://api.cerebras.ai/v1/chat/completions'
        : (
          provider === 'lmstudio'
            ? `${normalizeBaseUrl(baseUrl, 'http://127.0.0.1:1234')}/v1/chat/completions`
            : 'https://api.openai.com/v1/chat/completions'
        );
      const body = {
        model,
        messages: [
          ...(systemPrompt ? [{ role: 'system', content: systemPrompt }] : []),
          ...normalizeMessagesForOpenAi(messages),
        ],
        temperature: 0.2,
      };
      if (!disableTools) {
        body.tool_choice = 'auto';
        body.tools = normalizeToolSchemaForOpenAi(tools);
      }
      const headers = {
        'content-type': 'application/json',
      };
      if (apiKey) headers.authorization = `Bearer ${apiKey}`;
      const response = await fetch(endpoint, {
        method: 'POST',
        headers,
        body: JSON.stringify(body),
        signal: controller.signal,
      });
      if (!response.ok) {
        const { raw, json } = await readResponseJson(response);
        throw new Error(extractErrorMessage(raw, json, `${provider} request failed (${response.status}).`));
      }
      const { json } = await readResponseJson(response);
      return {
        ok: true,
        text: extractOpenAiFinal(json),
        tool_calls: disableTools ? [] : normalizeOpenAiToolCalls(json),
        provider,
        model,
      };
    }

    const body = {
      model,
      max_tokens: 1300,
      temperature: 0.2,
      messages: normalizeMessagesForAnthropic(messages),
    };
    if (!disableTools) {
      body.tools = normalizeToolSchemaForAnthropic(tools);
    }
    if (systemPrompt) body.system = systemPrompt;

    const response = await fetch('https://api.anthropic.com/v1/messages', {
      method: 'POST',
      headers: {
        'x-api-key': apiKey,
        'anthropic-version': '2023-06-01',
        'content-type': 'application/json',
      },
      body: JSON.stringify(body),
      signal: controller.signal,
    });
    if (!response.ok) {
      const { raw, json } = await readResponseJson(response);
      throw new Error(extractErrorMessage(raw, json, `anthropic request failed (${response.status}).`));
    }
    const { json } = await readResponseJson(response);
    return {
      ok: true,
      text: normalizeAnthropicText(json),
      tool_calls: disableTools ? [] : normalizeAnthropicToolCalls(json),
      provider,
      model,
    };
  } finally {
    clearTimeout(timeout);
    if (upstream && typeof upstream.removeEventListener === 'function') {
      upstream.removeEventListener('abort', onAbort);
    }
  }
}

module.exports = {
  callProviderWithTools,
  chatWithProvider,
};
