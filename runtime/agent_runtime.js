const crypto = require('crypto');

function id(prefix) {
  return `${prefix}_${crypto.randomUUID()}`;
}

function now() {
  return Date.now();
}

const EXPLICIT_ARTIFACT_SAVE_PHRASES = [
  'write it into an artifact',
  'write that into an artifact',
  'save it into an artifact',
  'save that into an artifact',
  'save it as an artifact',
  'write it as an artifact',
  'put it in an artifact',
  'put that in an artifact',
  'write to artifact',
  'save to artifact',
  'write into artifact',
  'save into artifact',
];

function parseChatCommand(message) {
  const raw = String(message || '').trim();
  const lower = raw.toLowerCase();
  const crawlCommand = parseCrawlCommand(raw);
  if (crawlCommand) return crawlCommand;

  if (EXPLICIT_ARTIFACT_SAVE_PHRASES.some((phrase) => lower.includes(phrase))) {
    return { type: 'artifact_from_last_assistant' };
  }

  if (lower.startsWith('/artifact ')) {
    const body = raw.slice('/artifact '.length).trim();
    const parts = body.split(':');
    const title = String(parts.shift() || 'Agent Note').trim() || 'Agent Note';
    const content = String(parts.join(':') || '').trim() || 'New artifact created by agent command.';
    return { type: 'artifact', title, content };
  }

  if (lower.startsWith('/viz ')) {
    const body = raw.slice('/viz '.length).trim();
    const chunks = body.split(/\s+/);
    const title = chunks.join(' ').trim() || 'Interactive Visualization';
    const html = [
      '<!doctype html>',
      '<html lang="en">',
      '<head>',
      '  <meta charset="utf-8" />',
      '  <meta name="viewport" content="width=device-width,initial-scale=1" />',
      `  <title>${title.replace(/</g, '&lt;').replace(/>/g, '&gt;')}</title>`,
      '  <style>',
      '    html, body { margin: 0; height: 100%; background: #0a0f1a; color: #e9f3ff; font-family: system-ui, -apple-system, Segoe UI, Roboto, sans-serif; }',
      '    #app { display: grid; place-items: center; height: 100%; }',
      '    .card { padding: 20px 24px; border: 1px solid #2a3a4f; border-radius: 12px; background: rgba(16, 27, 44, 0.88); }',
      '  </style>',
      '</head>',
      '<body>',
      '  <div id="app"><div class="card">Edit this HTML artifact and click Start to run.</div></div>',
      '</body>',
      '</html>',
    ].join('\n');
    return { type: 'artifact', artifactType: 'html', title, content: html };
  }

  if (lower.startsWith('/diff artifact ')) {
    const body = raw.slice('/diff artifact '.length).trim();
    const firstSpace = body.indexOf(' ');
    if (firstSpace > 0) {
      const artifactId = body.slice(0, firstSpace).trim();
      const patch = body.slice(firstSpace + 1).trim();
      if (artifactId && patch) {
        return { type: 'diff_artifact', artifactId, patch };
      }
    }
  }

  if (lower.includes('visualize') || lower.includes('visualise')) {
    return {
      type: 'artifact',
      artifactType: 'html',
      title: 'Generated Visualization',
      content: '<!doctype html><html><body style="margin:0;background:#0b1118;color:#edf3ff;display:grid;place-items:center;height:100vh;font-family:system-ui,sans-serif"><div>Interactive HTML artifact ready. Click Start to run.</div></body></html>',
    };
  }

  if (lower.includes('create artifact') || lower.includes('create note') || lower.includes('write note')) {
    return {
      type: 'artifact',
      title: 'Research Note',
      content: `Auto-note from prompt: ${raw}`,
    };
  }

  return { type: 'plain' };
}

function tokenizeCommand(raw) {
  const input = String(raw || '');
  const out = [];
  const re = /"([^"]*)"|'([^']*)'|(\S+)/g;
  let match;
  while ((match = re.exec(input)) !== null) {
    if (typeof match[1] === 'string') out.push(match[1]);
    else if (typeof match[2] === 'string') out.push(match[2]);
    else if (typeof match[3] === 'string') out.push(match[3]);
  }
  return out;
}

function parseCrawlFlags(tokens = []) {
  const flags = {
    depth: null,
    page_cap: null,
    mode: null,
    markdown_first: null,
    robots_policy: null,
  };
  const list = Array.isArray(tokens) ? tokens : [];

  for (let i = 0; i < list.length; i += 1) {
    const token = String(list[i] || '').trim();
    if (!token) continue;

    if (token === '--markdown-first') {
      flags.markdown_first = true;
      continue;
    }
    if (token === '--no-markdown-first') {
      flags.markdown_first = false;
      continue;
    }
    if (token === '--ignore-robots') {
      flags.robots_policy = 'ignore';
      continue;
    }
    if (token === '--respect-robots') {
      flags.robots_policy = 'respect';
      continue;
    }

    if (token === '--depth' || token.startsWith('--depth=')) {
      const value = token.includes('=') ? token.split('=').slice(1).join('=') : String(list[i + 1] || '').trim();
      if (!token.includes('=') && value) i += 1;
      const depth = Number(value);
      if (Number.isFinite(depth)) flags.depth = Math.max(1, Math.min(6, Math.round(depth)));
      continue;
    }

    if (
      token === '--pages'
      || token.startsWith('--pages=')
      || token === '--page-cap'
      || token.startsWith('--page-cap=')
    ) {
      const value = token.includes('=') ? token.split('=').slice(1).join('=') : String(list[i + 1] || '').trim();
      if (!token.includes('=') && value) i += 1;
      const pageCap = Number(value);
      if (Number.isFinite(pageCap)) flags.page_cap = Math.max(5, Math.min(300, Math.round(pageCap)));
      continue;
    }

    if (token === '--mode' || token.startsWith('--mode=')) {
      const value = token.includes('=') ? token.split('=').slice(1).join('=') : String(list[i + 1] || '').trim();
      if (!token.includes('=') && value) i += 1;
      const mode = String(value || '').trim().toLowerCase();
      if (mode === 'safe' || mode === 'broad') flags.mode = mode;
      continue;
    }

    if (token === '--robots' || token.startsWith('--robots=')) {
      const value = token.includes('=') ? token.split('=').slice(1).join('=') : String(list[i + 1] || '').trim();
      if (!token.includes('=') && value) i += 1;
      const robots = String(value || '').trim().toLowerCase();
      if (robots === 'ignore' || robots === 'respect') flags.robots_policy = robots;
    }
  }

  return flags;
}

function parseCrawlCommand(rawInput) {
  const raw = String(rawInput || '').trim();
  if (!raw) return null;
  if (!/^\/?crawl\b/i.test(raw)) return null;

  const tokens = tokenizeCommand(raw);
  if (!tokens.length) return null;
  const head = String(tokens[0] || '').toLowerCase();
  if (head !== '/crawl' && head !== 'crawl') return null;

  const args = tokens.slice(1);
  const sub = String(args[0] || '').trim().toLowerCase();
  if (sub === 'status') {
    return {
      type: 'crawl_status',
      job_id: String(args[1] || '').trim(),
    };
  }
  if (sub === 'stop' || sub === 'cancel') {
    return {
      type: 'crawl_stop',
      job_id: String(args[1] || '').trim(),
    };
  }

  let source_type = 'web';
  let cursor = 0;
  if (sub === 'local') {
    source_type = 'local';
    cursor = 1;
  } else if (sub === 'web') {
    source_type = 'web';
    cursor = 1;
  }

  const target = String(args[cursor] || '').trim();
  const rest = args.slice(cursor + 1);
  const flags = parseCrawlFlags(rest);

  if (source_type === 'local') {
    return {
      type: 'crawl_start',
      source_type,
      absolute_path: target,
      ...flags,
    };
  }

  return {
    type: 'crawl_start',
    source_type: 'web',
    url: target,
    ...flags,
  };
}

function summarizeScope(srId, srAllRefs) {
  const list = Array.isArray(srAllRefs) ? srAllRefs : [];
  const count = list.length;
  const active = list.find((ref) => String((ref && ref.id) || '') === String(srId || ''));
  const title = active ? String(active.title || 'Untitled') : 'Unknown';
  return { count, title };
}

function handleChat(payload = {}) {
  const message = String(payload.message || '').trim();
  const srId = String(payload.sr_id || '').trim();
  const provider = String(payload.provider || '').trim();
  const model = String(payload.model || '').trim();
  const srAllRefs = Array.isArray(payload.sr_all_refs) ? payload.sr_all_refs : [];
  const srArtifacts = Array.isArray(payload.sr_artifacts) ? payload.sr_artifacts : [];
  const srContextFiles = Array.isArray(payload.sr_context_files) ? payload.sr_context_files : [];
  const srChatThread = (payload.sr_chat_thread && typeof payload.sr_chat_thread === 'object')
    ? payload.sr_chat_thread
    : { messages: [] };

  const command = parseChatCommand(message);
  const scope = summarizeScope(srId, srAllRefs);

  const response = {
    message: '',
    pending_artifacts: [],
    pending_weight_updates: [],
    pending_decision_traces: [],
    pending_workspace_tabs: [],
    pending_diff_ops: [],
    pending_hyperweb_queries: [],
    pending_hyperweb_suggestions: [],
  };

  if (!message) {
    response.message = 'No message received.';
    return response;
  }

  if (command.type === 'artifact_from_last_assistant') {
    const threadMessages = Array.isArray(srChatThread.messages) ? srChatThread.messages : [];
    const lastAssistant = threadMessages.slice().reverse().find(
      (m) => String((m && m.role) || '') === 'assistant',
    );
    const lastText = lastAssistant ? String(lastAssistant.text || '').trim() : '';
    if (!lastText) {
      response.message = 'No assistant reply found in the current thread to write into an artifact.';
      return response;
    }
    const artifact = {
      id: id('artifact'),
      reference_id: srId,
      type: 'markdown',
      title: 'Research Memory',
      content: `<!-- subgrapher:memory -->\n\n${lastText}`,
      created_at: now(),
      updated_at: now(),
    };
    response.pending_artifacts.push(artifact);
    response.pending_workspace_tabs.push({
      type: 'artifact',
      reference_id: srId,
      artifact_id: artifact.id,
      title: artifact.title,
    });
    response.message = 'The latest assistant reply has been written into an artifact and opened.';
    return response;
  }

  if (command.type === 'artifact') {
    const artifact = {
      id: id('artifact'),
      reference_id: srId,
      type: String(command.artifactType || 'markdown').trim().toLowerCase() === 'html' ? 'html' : 'markdown',
      title: command.title,
      content: command.content,
      created_at: now(),
      updated_at: now(),
    };
    response.pending_artifacts.push(artifact);
    response.pending_workspace_tabs.push({
      type: 'artifact',
      reference_id: srId,
      artifact_id: artifact.id,
      title: artifact.title,
    });
    response.message = `Created artifact "${artifact.title}" in the active reference.`;
    return response;
  }

  if (command.type === 'diff_artifact') {
    response.pending_diff_ops.push({
      id: id('diff'),
      target_kind: 'artifact',
      reference_id: srId,
      target_id: command.artifactId,
      mode: 'append',
      patch: command.patch,
      summary: 'Append text to artifact from chat command.',
    });
    response.message = `Queued a diff operation for artifact ${command.artifactId}.`;
    return response;
  }

  const contextNote = srContextFiles.length > 0
    ? `I can see ${srContextFiles.length} imported context file(s).`
    : 'No context files are currently attached.';
  const artifactNote = srArtifacts.length > 0
    ? `${srArtifacts.length} artifact(s) are available in this workspace.`
    : 'No artifacts yet in this workspace.';

  const defaultMessage = [
    `Lumino is working in scoped reference mode for "${scope.title}" (${scope.count} visible reference nodes).`,
    provider && model ? `Provider/model: ${provider}/${model}.` : '',
    contextNote,
    artifactNote,
    'Use `/artifact title: content`, `/viz <title>` (creates runnable HTML artifact), or `/crawl <url>` / `/crawl status` / `/crawl stop` for direct workspace mutations.',
    'For direct corrections, prefer write_markdown_artifact/write_html_artifact with artifact_id. `/diff artifact <id> <text>` queues an append patch for manual apply.'
  ].filter(Boolean).join(' ');
  const overrideText = String(payload.assistant_text || '').trim();
  response.message = overrideText || defaultMessage;

  return response;
}

const AGENT_TOOLS = [
  // ── Direct tools (callable by the LLM) ──────────────────────────────────
  {
    name: 'run_python',
    allowed_callers: ['direct'],
    description: [
      'Execute Python code in a sandbox.',
      'Use this for data-in/data-out computation and transformations.',
      'The following stub functions are pre-injected and callable directly inside your code:',
      '  list_artifacts() -> list[{id, title, kind}]',
      '  read_artifact(artifact_id) -> {id, title, content, kind}',
      '  list_highlights() -> list[{id, source, text, ...}]',
      '  search_reference_graph(query) -> {nodes, edges}',
      '  list_context_files() -> list[{id, name, size_bytes, summary}]',
      '  read_context_file(file_id) -> {name, content}',
      'Use these stubs to access reference data inside Python. Process and filter results before printing — only stdout reaches your context.',
    ].join(' '),
    parameters: {
      type: 'object',
      properties: {
        code: { type: 'string', description: 'Python code to execute. May call stub functions for data access.' },
        reason: { type: 'string', description: 'Why this execution is needed.' },
      },
      required: ['code'],
      additionalProperties: false,
    },
  },
  {
    name: 'pip_install',
    allowed_callers: ['direct'],
    description: 'Install allowlisted Python packages.',
    parameters: {
      type: 'object',
      properties: {
        packages: {
          type: 'array',
          items: { type: 'string' },
          minItems: 1,
          description: 'Allowlisted package names to install.',
        },
      },
      required: ['packages'],
      additionalProperties: false,
    },
  },
  {
    name: 'save_skill',
    allowed_callers: ['direct'],
    description: 'Save reusable Python code as a skill.',
    parameters: {
      type: 'object',
      properties: {
        name: { type: 'string' },
        description: { type: 'string' },
        code: { type: 'string' },
        scope: { type: 'string', enum: ['local', 'global'] },
      },
      required: ['name', 'code'],
      additionalProperties: false,
    },
  },
  {
    name: 'run_skill',
    allowed_callers: ['direct'],
    description: 'Run a saved skill by name.',
    parameters: {
      type: 'object',
      properties: {
        name: { type: 'string' },
        scope: { type: 'string', enum: ['local', 'global'] },
        args: { type: 'object', additionalProperties: true },
      },
      required: ['name'],
      additionalProperties: false,
    },
  },
  {
    name: 'create_artifact',
    allowed_callers: ['direct'],
    description: 'Create or update an artifact. Use artifact_type="html" for runnable browser artifacts. Provide artifact_id to update in place.',
    parameters: {
      type: 'object',
      properties: {
        title: { type: 'string' },
        content: { type: 'string' },
        artifact_type: { type: 'string', enum: ['markdown', 'html'] },
        artifact_id: { type: 'string' },
      },
      required: ['title', 'content'],
      additionalProperties: false,
    },
  },
  {
    name: 'write_markdown_artifact',
    allowed_callers: ['direct'],
    description: [
      'Write or update a markdown artifact with full content.',
      'Use this for detailed deliverables — reports, essays, articles, or any long-form output the user requests.',
      'If artifact_id is omitted, runtime may update the active artifact for iterative fix/improve requests; otherwise a new deliverable artifact is created.',
      'Keep your chat reply concise and put the full content in the artifact.',
    ].join(' '),
    parameters: {
      type: 'object',
      properties: {
        title: { type: 'string', description: 'Artifact title.' },
        content: { type: 'string', description: 'Full markdown content for the artifact.' },
        artifact_id: { type: 'string', description: 'Optional ID of an existing artifact to update.' },
      },
      required: ['title', 'content'],
      additionalProperties: false,
    },
  },
  {
    name: 'write_html_artifact',
    allowed_callers: ['direct'],
    description: [
      'Write or update an executable HTML artifact with full content.',
      'Use this for dynamic interactive visualizations and games that run in the workspace.',
      'If artifact_id is omitted, runtime may update the active artifact for iterative fix/improve requests; otherwise a new HTML artifact is created.',
    ].join(' '),
    parameters: {
      type: 'object',
      properties: {
        title: { type: 'string', description: 'Artifact title.' },
        content: { type: 'string', description: 'Full HTML document content.' },
        artifact_id: { type: 'string', description: 'Optional ID of an existing artifact to update.' },
      },
      required: ['title', 'content'],
      additionalProperties: false,
    },
  },
  {
    name: 'web_search',
    allowed_callers: ['direct'],
    description: 'Search the web for a query using DuckDuckGo. Returns titles, URLs, and snippets from the top results.',
    parameters: {
      type: 'object',
      properties: {
        query: { type: 'string', description: 'The search query.' },
        max_results: { type: 'number', description: 'Maximum number of results to return (1–10, default 5).' },
      },
      required: ['query'],
      additionalProperties: false,
    },
  },
  {
    name: 'fetch_webpage',
    allowed_callers: ['direct'],
    description: 'Fetch the readable text content of a single webpage. Strips HTML tags and returns plain text.',
    parameters: {
      type: 'object',
      properties: {
        url: { type: 'string', description: 'Full URL of the page to fetch.' },
        max_length: { type: 'number', description: 'Maximum characters of text to return (default 4000, max 8000).' },
      },
      required: ['url'],
      additionalProperties: false,
    },
  },
  {
    name: 'open_web_tab',
    allowed_callers: ['direct'],
    description: 'Open a URL in a web tab in the active reference workspace.',
    parameters: {
      type: 'object',
      properties: {
        url: { type: 'string', description: 'Full http(s) URL to open in a workspace web tab.' },
        title: { type: 'string', description: 'Optional preferred tab title.' },
      },
      required: ['url'],
      additionalProperties: false,
    },
  },
  {
    name: 'add_web_highlight',
    allowed_callers: ['direct'],
    description: 'Add a web highlight to the active reference for a specific URL. Appends only when not already present.',
    parameters: {
      type: 'object',
      properties: {
        url: { type: 'string', description: 'Full http(s) URL the highlight belongs to.' },
        text: { type: 'string', description: 'Highlighted text snippet.' },
        context_before: { type: 'string', description: 'Optional context immediately before the text.' },
        context_after: { type: 'string', description: 'Optional context immediately after the text.' },
        web_start: { type: 'number', description: 'Optional absolute start offset in page text.' },
        web_end: { type: 'number', description: 'Optional absolute end offset in page text.' },
      },
      required: ['url', 'text'],
      additionalProperties: false,
    },
  },
  {
    name: 'add_artifact_highlight',
    allowed_callers: ['direct'],
    description: 'Add an artifact highlight by artifact ID and character bounds. Appends only when not already present.',
    parameters: {
      type: 'object',
      properties: {
        artifact_id: { type: 'string', description: 'Target artifact ID.' },
        artifact_start: { type: 'number', description: 'Start character offset in artifact content.' },
        artifact_end: { type: 'number', description: 'End character offset in artifact content.' },
        text: { type: 'string', description: 'Optional highlighted text. If omitted, derived from artifact content range.' },
      },
      required: ['artifact_id', 'artifact_start', 'artifact_end'],
      additionalProperties: false,
    },
  },
  {
    name: 'clear_highlights',
    allowed_callers: ['direct'],
    description: 'Clear highlights by target in the active reference. Use target_type=url with url, or target_type=artifact with artifact_id.',
    parameters: {
      type: 'object',
      properties: {
        target_type: { type: 'string', enum: ['url', 'artifact'] },
        url: { type: 'string', description: 'Required when target_type=url.' },
        artifact_id: { type: 'string', description: 'Required when target_type=artifact.' },
      },
      required: ['target_type'],
      additionalProperties: false,
    },
  },
  {
    name: 'search_local_evidence',
    allowed_callers: ['direct'],
    description: 'Rank local evidence from artifacts, highlights, and context files using BM25 + embeddings within Lumino scope.',
    parameters: {
      type: 'object',
      properties: {
        query: { type: 'string', description: 'Local evidence query text.' },
        top_k: { type: 'number', description: 'Maximum results to return (default 8, max 24).' },
        include_kinds: {
          type: 'array',
          items: { type: 'string', enum: ['artifact', 'highlight', 'context_file'] },
          description: 'Optional evidence kinds to include.',
        },
      },
      required: ['query'],
      additionalProperties: false,
    },
  },
  {
    name: 'finish',
    allowed_callers: ['direct'],
    description: 'Finish the agent loop with a final user-facing answer.',
    parameters: {
      type: 'object',
      properties: {
        message: { type: 'string' },
      },
      required: ['message'],
      additionalProperties: false,
    },
  },

  // ── Programmatic tools (also callable directly where enabled) ─────────────
  {
    name: 'list_artifacts',
    allowed_callers: ['direct', 'run_python'],
    description: 'List all artifacts in the active reference.',
    parameters: {
      type: 'object',
      properties: {},
      additionalProperties: false,
    },
  },
  {
    name: 'read_artifact',
    allowed_callers: ['direct', 'run_python'],
    description: 'Read the full content of an artifact by ID.',
    parameters: {
      type: 'object',
      properties: {
        artifact_id: { type: 'string', description: 'Artifact ID (from list_artifacts).' },
      },
      required: ['artifact_id'],
      additionalProperties: false,
    },
  },
  {
    name: 'list_highlights',
    allowed_callers: ['direct', 'run_python'],
    description: 'List all web/artifact highlights in the active reference.',
    parameters: {
      type: 'object',
      properties: {},
      additionalProperties: false,
    },
  },
  {
    name: 'search_reference_graph',
    allowed_callers: ['direct', 'run_python'],
    description: 'Search the reference knowledge graph nodes and edges by query string.',
    parameters: {
      type: 'object',
      properties: {
        query: { type: 'string', description: 'Search term to match against node/edge labels.' },
      },
      required: ['query'],
      additionalProperties: false,
    },
  },
  {
    name: 'list_context_files',
    allowed_callers: ['direct', 'run_python'],
    description: 'List all imported context files in the active reference.',
    parameters: {
      type: 'object',
      properties: {},
      additionalProperties: false,
    },
  },
  {
    name: 'read_context_file',
    allowed_callers: ['direct', 'run_python'],
    description: 'Read the full content of a context file by ID.',
    parameters: {
      type: 'object',
      properties: {
        file_id: { type: 'string', description: 'Context file ID (from list_context_files).' },
      },
      required: ['file_id'],
      additionalProperties: false,
    },
  },
];

function parseToolArguments(raw) {
  if (raw == null) return {};
  if (typeof raw === 'object') return raw;
  const text = String(raw || '').trim();
  if (!text) return {};
  try {
    return JSON.parse(text);
  } catch (_) {
    return {};
  }
}

function asToolCallId(toolCall, index = 0) {
  const directId = String((toolCall && toolCall.id) || '').trim();
  if (directId) return directId;
  return id(`tool_${index}`);
}

function normalizeToolCalls(toolCalls = []) {
  const list = Array.isArray(toolCalls) ? toolCalls : [];
  return list.map((item, index) => {
    const name = String((item && item.function && item.function.name) || item.name || '').trim();
    const argsRaw = (item && item.function && item.function.arguments) || item.arguments || '{}';
    return {
      id: asToolCallId(item, index),
      function: {
        name,
        arguments: (typeof argsRaw === 'string') ? argsRaw : JSON.stringify(argsRaw || {}),
      },
    };
  }).filter((item) => String((item && item.function && item.function.name) || '').trim());
}

function aggregatePending(target, source) {
  const src = (source && typeof source === 'object') ? source : {};
  [
    'pending_artifacts',
    'pending_workspace_tabs',
    'pending_diff_ops',
    'pending_hyperweb_queries',
    'pending_hyperweb_suggestions',
    'pending_weight_updates',
    'pending_decision_traces',
  ].forEach((key) => {
    if (!Array.isArray(target[key])) target[key] = [];
    const incoming = Array.isArray(src[key]) ? src[key] : [];
    incoming.forEach((item) => target[key].push(item));
  });
}

const LOCAL_EVIDENCE_TOOL_NAMES = new Set([
  'list_artifacts',
  'read_artifact',
  'list_highlights',
  'list_context_files',
  'read_context_file',
  'search_reference_graph',
  'search_local_evidence',
]);

const WEB_TOOL_NAMES = new Set(['web_search', 'fetch_webpage']);
const DELIVERABLE_TOOL_NAMES = new Set(['write_markdown_artifact', 'write_html_artifact', 'create_artifact']);

function asPositiveInt(value, fallback, min = 1, max = 8) {
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) return fallback;
  return Math.max(min, Math.min(max, Math.round(parsed)));
}

function normalizeCitationMode(value) {
  const mode = String(value || '').trim().toLowerCase();
  if (mode === 'hybrid') return 'hybrid';
  return 'hybrid';
}

function extractFootnoteNumbers(text = '') {
  const out = new Set();
  const source = String(text || '');
  const re = /\[(\d{1,3})\]/g;
  let match;
  while ((match = re.exec(source)) !== null) {
    const n = Number(match[1]);
    if (Number.isFinite(n) && n > 0) out.add(String(n));
  }
  return Array.from(out.values());
}

function extractSourcesSection(text = '') {
  const source = String(text || '');
  if (!source.trim()) return '';
  const match = source.match(/(?:^|\n)#{1,6}\s*Sources\b([\s\S]*)$/i);
  if (!match) return '';
  return String(match[1] || '').trim();
}

function hasQuotedClaims(text = '') {
  const source = String(text || '');
  if (!source.trim()) return false;
  if (/(?:^|\n)\s*>\s+\S+/m.test(source)) return true;
  return /["“][^"\n]{12,}["”]/.test(source);
}

function validateFootnoteCitationFormat(text = '') {
  const source = String(text || '').trim();
  if (!source) {
    return { ok: false, reason: 'empty_text' };
  }
  const footnotes = extractFootnoteNumbers(source);
  if (footnotes.length === 0) {
    return { ok: false, reason: 'no_footnotes' };
  }
  const sourcesSection = extractSourcesSection(source);
  if (!sourcesSection) {
    return { ok: false, reason: 'missing_sources_section' };
  }
  for (const num of footnotes) {
    const entryRe = new RegExp(`(?:^|\\n)\\s*\\[\\s*${num}\\s*\\]\\s+([^\\n]+)`, 'i');
    const entryMatch = sourcesSection.match(entryRe);
    if (!entryMatch) {
      return { ok: false, reason: `missing_source_entry_${num}` };
    }
    const entry = String(entryMatch[1] || '').trim();
    if (!/(https?:\/\/|artifact:|context_file:|highlight:|file:)/i.test(entry)) {
      return { ok: false, reason: `unresolvable_source_entry_${num}` };
    }
  }
  return { ok: true, footnote_count: footnotes.length };
}

function buildCitationKey(entry = {}) {
  if (!entry || typeof entry !== 'object') return '';
  const explicitKey = String(entry.source_key || '').trim();
  if (explicitKey) return explicitKey;
  const url = String(entry.url || '').trim();
  if (url) return `url:${url}`;
  const artifactId = String(entry.artifact_id || '').trim();
  if (artifactId) return `artifact:${artifactId}`;
  const contextFileId = String(entry.context_file_id || '').trim();
  if (contextFileId) return `context_file:${contextFileId}`;
  const locator = String(entry.source_locator || '').trim();
  if (locator) return `locator:${locator}`;
  const sourceId = String(entry.source_id || '').trim();
  if (sourceId) return `source_id:${sourceId}`;
  return '';
}

function extractCitationEntriesFromTool(toolName, toolOutput) {
  const name = String(toolName || '').trim();
  const output = (toolOutput && typeof toolOutput === 'object') ? toolOutput : {};
  const entries = [];

  if (name === 'search_local_evidence' && Array.isArray(output.citations)) {
    output.citations.forEach((item) => {
      if (!item || typeof item !== 'object') return;
      entries.push({
        source_key: String(item.source_key || '').trim(),
        source_locator: String(item.source_locator || '').trim(),
        url: String(item.url || '').trim(),
        artifact_id: String(item.artifact_id || '').trim(),
        context_file_id: String(item.context_file_id || '').trim(),
        marker_backed: !!item.marker_backed,
      });
    });
    return entries;
  }

  if (name === 'web_search' && Array.isArray(output.results)) {
    output.results.forEach((item) => {
      const url = String((item && item.url) || '').trim();
      if (!url) return;
      entries.push({
        source_key: `url:${url}`,
        source_locator: url,
        url,
        marker_backed: false,
      });
    });
    return entries;
  }

  if (name === 'fetch_webpage') {
    const url = String(output.url || '').trim();
    if (url) {
      entries.push({
        source_key: `url:${url}`,
        source_locator: url,
        url,
        marker_backed: false,
      });
    }
    return entries;
  }

  if (name === 'add_web_highlight') {
    const highlight = (output.highlight && typeof output.highlight === 'object') ? output.highlight : null;
    const url = String((highlight && highlight.url) || '').trim();
    if (url) {
      entries.push({
        source_key: `url:${url}`,
        source_locator: url,
        url,
        marker_backed: true,
      });
    }
    return entries;
  }

  if (name === 'add_artifact_highlight') {
    const highlight = (output.highlight && typeof output.highlight === 'object') ? output.highlight : null;
    const artifactId = String((highlight && highlight.artifact_id) || '').trim();
    if (artifactId) {
      entries.push({
        source_key: `artifact:${artifactId}`,
        source_locator: `artifact:${artifactId}`,
        artifact_id: artifactId,
        marker_backed: true,
      });
    }
    return entries;
  }

  if (name === 'list_highlights') {
    const list = Array.isArray(output.result) ? output.result : [];
    list.forEach((item) => {
      if (!item || typeof item !== 'object') return;
      const source = String(item.source || '').trim().toLowerCase();
      if (source === 'artifact') {
        const artifactId = String(item.artifact_id || '').trim();
        if (!artifactId) return;
        entries.push({
          source_key: `artifact:${artifactId}`,
          source_locator: `artifact:${artifactId}`,
          artifact_id: artifactId,
          marker_backed: true,
        });
        return;
      }
      const url = String(item.url || '').trim();
      if (!url) return;
      entries.push({
        source_key: `url:${url}`,
        source_locator: url,
        url,
        marker_backed: true,
      });
    });
    return entries;
  }

  return entries;
}

function evaluateCitationGate(policy, state = {}) {
  const target = (state && typeof state === 'object') ? state : {};
  const requiresCitations = !!(policy && policy.requiresCitations);
  const citationSourcesCount = asPositiveInt(target.citationSourcesCount, 0, 0, 999);
  const markerSourcesCount = asPositiveInt(target.markerSourcesCount, 0, 0, 999);
  const text = String(target.text || '').trim();
  const enforceFormat = !!target.enforceFormat;
  const requiredCount = !!(policy && policy.isDetailed)
    ? asPositiveInt(policy.minCitationsDetailed, 2, 1, 8)
    : asPositiveInt(policy.minCitationsShort, 1, 1, 8);
  const reasons = [];

  if (requiresCitations && citationSourcesCount < requiredCount) {
    reasons.push('citation_required');
  }

  if (requiresCitations && enforceFormat && text) {
    const formatCheck = validateFootnoteCitationFormat(text);
    if (!formatCheck.ok) {
      reasons.push('citation_format_required');
    }
    if (
      String((policy && policy.citationMode) || '') === 'hybrid'
      && !!(policy && policy.requireMarkerForQuotedClaims)
      && hasQuotedClaims(text)
      && markerSourcesCount === 0
    ) {
      reasons.push('marker_required_for_quotes');
    }
  }

  return {
    ok: reasons.length === 0,
    requiredCount,
    reasons,
  };
}

function normalizeResearchPolicy(input) {
  const policy = (input && typeof input === 'object') ? input : {};
  const rawRecoveryTurns = Number(policy.maxRecoveryTurns);
  return {
    isDetailed: !!policy.isDetailed,
    requiresWebResearch: !!policy.requiresWebResearch,
    requireDeliverableBeforeFinish: !!policy.requireDeliverableBeforeFinish,
    localEvidenceAvailable: !!policy.localEvidenceAvailable,
    requiresCitations: !!policy.requiresCitations,
    citationMode: normalizeCitationMode(policy.citationMode),
    minCitationsShort: asPositiveInt(policy.minCitationsShort, 1, 1, 8),
    minCitationsDetailed: asPositiveInt(policy.minCitationsDetailed, 2, 1, 8),
    requireMarkerForQuotedClaims: Object.prototype.hasOwnProperty.call(policy, 'requireMarkerForQuotedClaims')
      ? !!policy.requireMarkerForQuotedClaims
      : true,
    allowRecoveryReprompt: Object.prototype.hasOwnProperty.call(policy, 'allowRecoveryReprompt')
      ? !!policy.allowRecoveryReprompt
      : true,
    maxRecoveryTurns: Number.isFinite(rawRecoveryTurns)
      ? Math.max(0, Math.min(3, Math.round(rawRecoveryTurns)))
      : 1,
  };
}

function isSubstantiveFinalText(value) {
  const text = String(value || '').trim();
  if (!text) return false;
  if (/^agent loop completed\.?$/i.test(text)) return false;
  if (/^agent loop reached max turns/i.test(text)) return false;
  if (text.length >= 80) return true;
  const lines = text.split('\n').filter((line) => line.trim());
  return lines.length >= 3;
}

function isWebUnavailableResult(toolRes, toolOutput) {
  const explicit = !!(toolRes && toolRes.unavailable) || !!(toolOutput && toolOutput.unavailable);
  if (explicit) return true;
  const combined = [
    String((toolRes && toolRes.message) || ''),
    String((toolOutput && toolOutput.message) || ''),
    String((toolOutput && toolOutput.error) || ''),
  ].join(' ').toLowerCase();
  if (!combined) return false;
  return [
    'unavailable',
    'failed to fetch',
    'network',
    'timed out',
    'timeout',
    'econn',
    'enotfound',
    'no connected peers',
    'disconnected',
  ].some((item) => combined.includes(item));
}

function buildPolicyBlockedToolResult(message, code) {
  const text = String(message || 'Tool call blocked by policy.').trim();
  return {
    ok: false,
    policy_blocked: true,
    message: text,
    tool_output: {
      ok: false,
      policy_blocked: true,
      policy_code: String(code || 'policy_blocked').trim(),
      message: text,
    },
  };
}

function computeMissingPhase(policy, localEvidenceCount, webEvidenceCount, deliverableWritten, webUnavailable, citationGate = null) {
  if (policy.isDetailed && policy.localEvidenceAvailable && localEvidenceCount === 0) {
    return 'local';
  }
  if (policy.requiresWebResearch && webEvidenceCount === 0 && !webUnavailable) {
    return 'web';
  }
  const gate = (citationGate && typeof citationGate === 'object') ? citationGate : null;
  if (gate && Array.isArray(gate.reasons)) {
    if (gate.reasons.includes('citation_required')) return 'citation';
  }
  if (policy.requireDeliverableBeforeFinish && !deliverableWritten) {
    return 'deliverable';
  }
  if (gate && Array.isArray(gate.reasons)) {
    if (gate.reasons.includes('citation_format_required')) return 'citation_format';
    if (gate.reasons.includes('marker_required_for_quotes')) return 'marker';
  }
  return '';
}

function buildRecoveryPrompt(missingPhase) {
  const phase = String(missingPhase || '').trim().toLowerCase();
  if (phase === 'local') {
    return [
      'Continue the task.',
      'Required phase missing: workspace local evidence read.',
      'Call exactly one local evidence tool now (list_artifacts, read_artifact, list_highlights, list_context_files, read_context_file, or search_reference_graph).',
      'Do not call finish yet.',
    ].join(' ');
  }
  if (phase === 'web') {
    return [
      'Continue the task.',
      'Required phase missing: web research.',
      'Call at least one successful web tool now (web_search or fetch_webpage).',
      'Do not call finish yet.',
    ].join(' ');
  }
  if (phase === 'deliverable') {
    return [
      'Continue the task.',
      'Required phase missing: deliverable artifact writing.',
      'Call write_markdown_artifact now.',
      'Do not call finish yet.',
    ].join(' ');
  }
  if (phase === 'citation') {
    return [
      'Continue the task.',
      'Required phase missing: citation evidence.',
      'Call search_local_evidence now and gather enough sources before finishing.',
      'Do not call finish yet.',
    ].join(' ');
  }
  if (phase === 'citation_format') {
    return [
      'Continue the task.',
      'Required phase missing: citation format.',
      'Rewrite output with footnote markers like [1] and add a "## Sources" section with resolvable source entries.',
      'Do not call finish yet.',
    ].join(' ');
  }
  if (phase === 'marker') {
    return [
      'Continue the task.',
      'Required phase missing: marker-backed evidence for quoted claims.',
      'Add at least one marker-backed source via add_web_highlight or add_artifact_highlight, then synthesize again.',
      'Do not call finish yet.',
    ].join(' ');
  }
  return 'Continue and complete required policy phases before finishing.';
}

function isLikelyRateLimitError(err) {
  const message = String((err && err.message) || err || '').trim().toLowerCase();
  if (!message) return false;
  if (message.includes('request canceled') || message.includes('timed out')) return false;
  return (
    /\b429\b/.test(message)
    || message.includes('rate limit')
    || message.includes('too many requests')
    || message.includes('high traffic')
    || message.includes('overloaded')
    || message.includes('capacity')
  );
}

async function sleepWithSignal(ms, signal) {
  const waitMs = Math.max(0, Math.round(Number(ms) || 0));
  if (waitMs === 0) return;
  await new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      if (signal && typeof signal.removeEventListener === 'function') {
        signal.removeEventListener('abort', onAbort);
      }
      resolve();
    }, waitMs);
    const onAbort = () => {
      clearTimeout(timer);
      if (signal && typeof signal.removeEventListener === 'function') {
        signal.removeEventListener('abort', onAbort);
      }
      reject(new Error('Request canceled.'));
    };
    if (signal && typeof signal.addEventListener === 'function') {
      if (signal.aborted) {
        clearTimeout(timer);
        reject(new Error('Request canceled.'));
        return;
      }
      signal.addEventListener('abort', onAbort, { once: true });
    }
  });
}

async function executeAgenticLoop(options = {}) {
  const opts = (options && typeof options === 'object') ? options : {};
  const callProvider = typeof opts.callProviderWithTools === 'function' ? opts.callProviderWithTools : null;
  const executeTool = typeof opts.executeTool === 'function' ? opts.executeTool : null;
  if (!callProvider || !executeTool) {
    throw new Error('Agent loop requires callProviderWithTools and executeTool handlers.');
  }

  const onDelta = typeof opts.onDelta === 'function' ? opts.onDelta : null;
  const onStatus = typeof opts.onStatus === 'function' ? opts.onStatus : null;
  const signal = opts.signal;
  const provider = String(opts.provider || '').trim().toLowerCase();
  const model = String(opts.model || '').trim();
  const apiKey = String(opts.apiKey || '').trim();
  const systemPrompt = String(opts.systemPrompt || '').trim();
  const userPrompt = String(opts.userPrompt || '').trim();
  const maxTurns = Number.isFinite(Number(opts.maxTurns))
    ? Math.max(1, Math.min(16, Math.round(Number(opts.maxTurns))))
    : 8;
  const researchPolicy = normalizeResearchPolicy(opts.researchPolicy);
  const defaultDirectTools = AGENT_TOOLS.filter((t) => (t.allowed_callers ?? ['direct']).includes('direct'));
  const directTools = Array.isArray(opts.tools) && opts.tools.length > 0 ? opts.tools : defaultDirectTools;

  const aggregate = {
    message: '',
    pending_artifacts: [],
    pending_workspace_tabs: [],
    pending_diff_ops: [],
    pending_hyperweb_queries: [],
    pending_hyperweb_suggestions: [],
    pending_weight_updates: [],
    pending_decision_traces: [],
    tool_steps: [],
    turns: 0,
    stopped_reason: '',
    research_policy_state: {
      local_evidence_count: 0,
      web_evidence_count: 0,
      deliverable_written: false,
      web_unavailable: false,
      citation_sources_count: 0,
      marker_sources_count: 0,
      citation_gate_passed: true,
    },
  };

  const messages = [{ role: 'user', content: userPrompt }];
  let finalText = '';
  let finished = false;
  let localEvidenceCount = 0;
  let webEvidenceCount = 0;
  let deliverableWritten = false;
  let webUnavailable = false;
  let localBypassNoted = false;
  let recoveryTurnCount = 0;
  const citationSources = new Map();
  const markerSourceKeys = new Set();
  const citationBlockReasons = [];
  const blockedTools = [];
  const phaseAnnounced = {
    local: false,
    web: false,
    deliverable: false,
  };

  const emitStatus = (state, source, text, extra = {}) => {
    if (!onStatus) return;
    const payload = {
      state: String(state || 'info'),
      source: String(source || 'agent'),
      text: String(text || '').trim(),
      ...((extra && typeof extra === 'object') ? extra : {}),
    };
    if (!payload.text) return;
    onStatus(payload);
  };

  const emitPhase = (phaseName) => {
    const phase = String(phaseName || '').trim().toLowerCase();
    if (!phase || phaseAnnounced[phase]) return;
    if (phase === 'local') {
      emitStatus('info', 'agent', 'Reading workspace context...');
      phaseAnnounced.local = true;
      return;
    }
    if (phase === 'web') {
      emitStatus('info', 'agent', 'Web research phase...');
      phaseAnnounced.web = true;
      return;
    }
    if (phase === 'deliverable') {
      emitStatus('info', 'agent', 'Deliverable writing phase...');
      phaseAnnounced.deliverable = true;
    }
  };

  const callProviderWithBackoff = async (payload, providerOptions = {}) => {
    const maxTotalWaitMs = 45_000;
    const baseStepMs = 5_000;
    let totalWaitMs = 0;
    let attempt = 0;
    while (true) {
      if (signal && signal.aborted) throw new Error('Request canceled.');
      try {
        return await callProvider(payload, providerOptions);
      } catch (err) {
        if (!isLikelyRateLimitError(err)) throw err;
        const nextWaitMs = Math.min(baseStepMs * (attempt + 1), maxTotalWaitMs - totalWaitMs);
        if (nextWaitMs <= 0) throw err;
        attempt += 1;
        totalWaitMs += nextWaitMs;
        await sleepWithSignal(nextWaitMs, signal);
      }
    }
  };

  if (researchPolicy.isDetailed) {
    emitPhase('local');
  }

  for (let turn = 0; turn < maxTurns; turn += 1) {
    if (signal && signal.aborted) throw new Error('Request canceled.');
    aggregate.turns = turn + 1;
    emitStatus('info', 'agent', `Agent reasoning turn ${turn + 1}...`, { meta: { turn } });

    const providerRes = await callProviderWithBackoff({
      provider,
      model,
      apiKey,
      systemPrompt,
      messages,
      tools: directTools,
      signal,
    });

    const assistantText = String((providerRes && providerRes.text) || '').trim();
    const toolCalls = normalizeToolCalls(providerRes && providerRes.tool_calls);

    if (assistantText && onDelta) onDelta(assistantText);

    if (toolCalls.length === 0) {
      finalText = assistantText || finalText;
      const draftCitationGate = evaluateCitationGate(researchPolicy, {
        citationSourcesCount: citationSources.size,
        markerSourcesCount: markerSourceKeys.size,
        enforceFormat: !!researchPolicy.requiresCitations && !!assistantText,
        text: assistantText,
      });
      const missingPhase = computeMissingPhase(
        researchPolicy,
        localEvidenceCount,
        webEvidenceCount,
        deliverableWritten,
        webUnavailable,
        draftCitationGate,
      );
      if (
        missingPhase
        && researchPolicy.allowRecoveryReprompt
        && recoveryTurnCount < researchPolicy.maxRecoveryTurns
      ) {
        recoveryTurnCount += 1;
        const recoveryPrompt = buildRecoveryPrompt(missingPhase);
        messages.push({
          role: 'user',
          content: recoveryPrompt,
        });
        emitStatus('info', 'agent', 'Recovery: required phase still missing; requesting one more tool pass.', {
          meta: { turn, reason: 'recovery_reprompt', missing_phase: missingPhase, recovery_turns: recoveryTurnCount },
        });
        continue;
      }
      aggregate.stopped_reason = missingPhase ? 'required_phase_unmet_after_recovery' : 'no_tool_calls';
      emitStatus('info', 'agent', 'No additional tool calls needed; preparing final response.', {
        meta: { turn, reason: aggregate.stopped_reason, missing_phase: missingPhase || '' },
      });
      break;
    }

    messages.push({
      role: 'assistant',
      content: assistantText,
      tool_calls: toolCalls,
    });

    for (let i = 0; i < toolCalls.length; i += 1) {
      const toolCall = toolCalls[i];
      const toolName = String((toolCall && toolCall.function && toolCall.function.name) || '').trim();
      if (!toolName) continue;
      const args = parseToolArguments(toolCall.function.arguments);
      const isLocalEvidenceTool = LOCAL_EVIDENCE_TOOL_NAMES.has(toolName);
      const isWebTool = WEB_TOOL_NAMES.has(toolName);
      const isDeliverableTool = DELIVERABLE_TOOL_NAMES.has(toolName);
      const isFinishTool = toolName === 'finish';

      if (isLocalEvidenceTool) emitPhase('local');
      if (isWebTool) {
        if (researchPolicy.isDetailed && localEvidenceCount === 0 && !researchPolicy.localEvidenceAvailable) {
          if (!localBypassNoted) {
            emitStatus('info', 'agent', 'No local context available; proceeding to web research.');
            localBypassNoted = true;
          }
        }
      }
      if (isDeliverableTool || isFinishTool) emitPhase('deliverable');
      if (isWebTool && !(researchPolicy.isDetailed && localEvidenceCount === 0 && researchPolicy.localEvidenceAvailable)) {
        emitPhase('web');
      }

      emitStatus('start', 'tool', `Calling tool: ${toolName}`, {
        tool_name: toolName,
        meta: { turn, index: i, args },
      });

      let toolRes = null;
      if (
        researchPolicy.isDetailed
        && isWebTool
        && localEvidenceCount === 0
        && researchPolicy.localEvidenceAvailable
      ) {
        toolRes = buildPolicyBlockedToolResult(
          'Read workspace context first using local evidence tools before starting web research.',
          'local_evidence_required',
        );
      }
      if (
        !toolRes
        && researchPolicy.requiresWebResearch
        && (isDeliverableTool || isFinishTool)
        && webEvidenceCount === 0
        && !webUnavailable
      ) {
        toolRes = buildPolicyBlockedToolResult(
          'Complete at least one successful web research step before writing the deliverable or finishing.',
          'web_research_required',
        );
      }
      if (
        !toolRes
        && researchPolicy.requireDeliverableBeforeFinish
        && isFinishTool
        && !deliverableWritten
      ) {
        toolRes = buildPolicyBlockedToolResult(
          'Write the requested deliverable artifact before finishing.',
          'deliverable_required',
        );
      }
      if (
        !toolRes
        && researchPolicy.requiresCitations
        && (isDeliverableTool || isFinishTool)
      ) {
        const targetText = isDeliverableTool
          ? String(args.content || '').trim()
          : String(args.message || '').trim();
        const citationGate = evaluateCitationGate(researchPolicy, {
          citationSourcesCount: citationSources.size,
          markerSourcesCount: markerSourceKeys.size,
          enforceFormat: true,
          text: targetText,
        });
        if (!citationGate.ok) {
          const code = citationGate.reasons[0];
          if (code === 'citation_required') {
            toolRes = buildPolicyBlockedToolResult(
              `Add citations before ${isDeliverableTool ? 'writing the deliverable' : 'finishing'} (required: ${citationGate.requiredCount}, current: ${citationSources.size}). Use search_local_evidence first.`,
              'citation_required',
            );
          } else if (code === 'citation_format_required') {
            toolRes = buildPolicyBlockedToolResult(
              'Use footnote citation format ([1], [2], ...) and include a "## Sources" section with resolvable entries.',
              'citation_format_required',
            );
          } else if (code === 'marker_required_for_quotes') {
            toolRes = buildPolicyBlockedToolResult(
              'Quoted claims require marker-backed evidence. Add a marker-backed source via add_web_highlight or add_artifact_highlight.',
              'marker_required_for_quotes',
            );
          }
          if (code && !citationBlockReasons.includes(code)) {
            citationBlockReasons.push(code);
          }
        }
      }
      if (!toolRes) {
        toolRes = await executeTool({
          name: toolName,
          arguments: args,
          tool_call_id: toolCall.id,
          turn,
          index: i,
        });
      }

      const normalizedToolRes = (toolRes && typeof toolRes === 'object') ? toolRes : {};
      emitStatus(normalizedToolRes.ok === false ? 'error' : 'done', 'tool', String(normalizedToolRes.message || `${toolName} completed.`), {
        tool_name: toolName,
        meta: {
          turn,
          index: i,
          pending_workspace_tabs: Array.isArray(normalizedToolRes.pending_workspace_tabs)
            ? normalizedToolRes.pending_workspace_tabs
            : [],
        },
      });
      aggregate.tool_steps.push({
        turn,
        name: toolName,
        ok: normalizedToolRes.ok !== false,
        message: String(normalizedToolRes.message || ''),
      });
      if (normalizedToolRes.policy_blocked) {
        const policyCode = String(((normalizedToolRes.tool_output && normalizedToolRes.tool_output.policy_code) || '')).trim();
        blockedTools.push({
          turn,
          name: toolName,
          code: policyCode,
        });
        if (
          (policyCode === 'citation_required' || policyCode === 'citation_format_required' || policyCode === 'marker_required_for_quotes')
          && !citationBlockReasons.includes(policyCode)
        ) {
          citationBlockReasons.push(policyCode);
        }
      }
      aggregatePending(aggregate, normalizedToolRes);

      const toolOutput = normalizedToolRes.tool_output && typeof normalizedToolRes.tool_output === 'object'
        ? normalizedToolRes.tool_output
        : {
          ok: normalizedToolRes.ok !== false,
          message: String(normalizedToolRes.message || ''),
        };
      const toolOk = normalizedToolRes.ok !== false && toolOutput.ok !== false;
      if (toolOk) {
        const citationEntries = extractCitationEntriesFromTool(toolName, toolOutput);
        citationEntries.forEach((entry) => {
          const key = buildCitationKey(entry);
          if (!key) return;
          const normalized = {
            source_key: key,
            source_locator: String(entry.source_locator || '').trim(),
            url: String(entry.url || '').trim(),
            artifact_id: String(entry.artifact_id || '').trim(),
            context_file_id: String(entry.context_file_id || '').trim(),
            marker_backed: !!entry.marker_backed,
          };
          citationSources.set(key, normalized);
          if (normalized.marker_backed) {
            markerSourceKeys.add(key);
          }
        });
      }
      if (toolOk && isLocalEvidenceTool) localEvidenceCount += 1;
      if (toolOk && isWebTool) webEvidenceCount += 1;
      if (toolOk && isDeliverableTool) deliverableWritten = true;
      if (!toolOk && isWebTool && isWebUnavailableResult(normalizedToolRes, toolOutput)) {
        webUnavailable = true;
        emitStatus('info', 'agent', 'Web research is unavailable; final synthesis will proceed with current evidence.');
      }
      messages.push({
        role: 'tool',
        tool_call_id: String(toolCall.id || ''),
        content: JSON.stringify(toolOutput),
      });

      if (normalizedToolRes.finish === true) {
        finished = true;
        finalText = String(normalizedToolRes.final_message || assistantText || finalText).trim();
        aggregate.stopped_reason = 'finish_tool';
        emitStatus('info', 'agent', 'Agent marked the task complete.', {
          meta: { turn, reason: 'finish_tool' },
        });
        break;
      }
    }

    if (finished) break;
  }

  const shouldForceFinalization = !finished && !isSubstantiveFinalText(finalText);
  if (shouldForceFinalization) {
    const finalInstruction = [
      'Finalize now with available evidence. No more tool calls.',
      researchPolicy.requiresWebResearch && webEvidenceCount === 0 && webUnavailable
        ? 'Web research is currently unavailable. Mention this limitation explicitly in your final answer.'
        : '',
      'Provide a complete user-facing response.',
    ].filter(Boolean).join(' ');
    const toolSummaryLines = aggregate.tool_steps.slice(-24).map((step, idx) => {
      const name = String((step && step.name) || 'unknown');
      const status = (step && step.ok) ? 'ok' : 'error';
      const note = String((step && step.message) || '').trim();
      return `${idx + 1}. ${name} (${status})${note ? `: ${note}` : ''}`;
    });
    const finalizationMessages = [
      { role: 'user', content: userPrompt },
      {
        role: 'assistant',
        content: [
          'Tool execution summary:',
          toolSummaryLines.length > 0 ? toolSummaryLines.join('\n') : 'No tool execution steps were recorded.',
          finalText ? `Latest draft response: ${finalText}` : '',
        ].filter(Boolean).join('\n'),
      },
      { role: 'user', content: finalInstruction },
    ];
    try {
      emitStatus('info', 'agent', 'Finalizing response from available evidence with tools disabled.');
      const finalizationRes = await callProviderWithBackoff({
        provider,
        model,
        apiKey,
        systemPrompt,
        messages: finalizationMessages,
        tools: directTools,
        signal,
      }, {
        disableTools: true,
      });
      const forcedText = String((finalizationRes && finalizationRes.text) || '').trim();
      if (forcedText) {
        finalText = forcedText;
        aggregate.stopped_reason = aggregate.stopped_reason || 'forced_finalization';
      }
    } catch (finalizeErr) {
      emitStatus('error', 'agent', `Finalization pass failed: ${String((finalizeErr && finalizeErr.message) || 'unknown error')}`);
      aggregate.stopped_reason = aggregate.stopped_reason || 'forced_finalization_failed';
    }
  }
  if (!finalText) {
    if (aggregate.turns >= maxTurns) {
      finalText = 'I could not complete this within the agent turn limit.';
      aggregate.stopped_reason = aggregate.stopped_reason || 'max_turns';
    } else {
      finalText = 'Agent loop completed.';
      aggregate.stopped_reason = aggregate.stopped_reason || 'completed';
    }
  }

  aggregate.research_policy_state = {
    local_evidence_count: localEvidenceCount,
    web_evidence_count: webEvidenceCount,
    deliverable_written: deliverableWritten,
    web_unavailable: webUnavailable,
    citation_sources_count: citationSources.size,
    marker_sources_count: markerSourceKeys.size,
    citation_gate_passed: true,
  };
  const finalCitationGate = evaluateCitationGate(researchPolicy, {
    citationSourcesCount: citationSources.size,
    markerSourcesCount: markerSourceKeys.size,
    enforceFormat: !!researchPolicy.requiresCitations && !!String(finalText || '').trim(),
    text: finalText,
  });
  finalCitationGate.reasons.forEach((code) => {
    if (!citationBlockReasons.includes(code)) citationBlockReasons.push(code);
  });
  aggregate.research_policy_state.citation_gate_passed = finalCitationGate.ok;
  aggregate.policy_diagnostics = {
    missing_phase: computeMissingPhase(
      researchPolicy,
      localEvidenceCount,
      webEvidenceCount,
      deliverableWritten,
      webUnavailable,
      finalCitationGate,
    ),
    recovery_attempts: recoveryTurnCount,
    blocked_tools: blockedTools,
    citation_block_reasons: citationBlockReasons.slice(),
  };

  emitStatus('info', 'agent', 'Agent loop finished.', {
    meta: { turns: aggregate.turns, stopped_reason: aggregate.stopped_reason || 'completed' },
  });
  aggregate.message = finalText;
  return aggregate;
}

const PROGRAMMATIC_TOOL_NAMES = AGENT_TOOLS
  .filter((t) => Array.isArray(t.allowed_callers) && t.allowed_callers.includes('run_python'))
  .map((t) => t.name);

module.exports = {
  AGENT_TOOLS,
  PROGRAMMATIC_TOOL_NAMES,
  executeAgenticLoop,
  handleChat,
  parseChatCommand,
};
