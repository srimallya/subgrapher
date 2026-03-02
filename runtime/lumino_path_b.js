const crypto = require('crypto');

const PATH_B_DEFAULT_TOP_K = 12;

function nowTs() {
  return Date.now();
}

function toShortText(value, maxLen = 1800) {
  const text = String(value || '').replace(/\s+/g, ' ').trim();
  if (!text) return '';
  if (text.length <= maxLen) return text;
  return `${text.slice(0, Math.max(1, maxLen - 3))}...`;
}

function normalizeTopic(raw) {
  return String(raw || '')
    .toLowerCase()
    .replace(/https?:\/\/[^\s]+/g, ' ')
    .replace(/[^a-z0-9\s]/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function dayBucketFromTs(ts = nowTs()) {
  const d = new Date(Number(ts) || nowTs());
  const y = d.getUTCFullYear();
  const m = String(d.getUTCMonth() + 1).padStart(2, '0');
  const day = String(d.getUTCDate()).padStart(2, '0');
  return `${y}-${m}-${day}`;
}

function computeIdempotencyKey(params = {}) {
  const topic = normalizeTopic(params.topic || '');
  const source = String(params.source || 'orchestrator').trim().toLowerCase();
  const dayBucket = String(params.day_bucket || dayBucketFromTs()).trim();
  const scope = String(params.user_scope || 'default').trim().toLowerCase();
  const payload = `${topic}|${source}|${dayBucket}|${scope}`;
  return crypto.createHash('sha256').update(payload).digest('hex');
}

function toArray(value) {
  return Array.isArray(value) ? value : [];
}

function dedupeUrls(urls = []) {
  const out = [];
  const seen = new Set();
  toArray(urls).forEach((item) => {
    const url = String(item || '').trim();
    if (!url) return;
    const key = url.toLowerCase();
    if (seen.has(key)) return;
    seen.add(key);
    out.push(url);
  });
  return out;
}

function buildFollowUpQuestion(task = {}) {
  const hasScheduleIntent = /\b(schedule|daily|weekly|track|monitor|every day|every week)\b/i.test(String(task.user_message || ''));
  if (hasScheduleIntent) {
    return 'Should I create a scheduled tracker for this topic? You can use /job_create once you confirm.';
  }
  return 'Do you want me to keep tracking this topic with a scheduled job?';
}

function formatTelegramMessage(summary = '', citationUrls = []) {
  const cleanSummary = toShortText(summary || 'Research completed.', 1600);
  const uniqueCitations = dedupeUrls(citationUrls).slice(0, 3);
  if (uniqueCitations.length < 3) {
    const shortage = uniqueCitations.length;
    return {
      ok: false,
      citation_urls: uniqueCitations,
      text: [
        cleanSummary,
        '',
        `Need more reliable sources; only ${shortage} verified links found. Retrying requires broader query.`,
        ...(uniqueCitations.length > 0 ? ['', 'Sources:', ...uniqueCitations.map((url) => `- ${url}`)] : []),
      ].join('\n'),
    };
  }
  return {
    ok: true,
    citation_urls: uniqueCitations,
    text: [
      cleanSummary,
      '',
      'Sources:',
      ...uniqueCitations.map((url) => `- ${url}`),
    ].join('\n'),
  };
}

function buildPathBSystemPrompt() {
  return [
    'You are Path B, the global orchestrator for Subgrapher.',
    'Path A is scoped to one reference; you orchestrate globally.',
    'You must drive this task via tools and then call finish.',
    'Required workflow:',
    `1) global_reference_search (top_k=${PATH_B_DEFAULT_TOP_K})`,
    '2) read_reference_snapshot for candidate references',
    '3) choose exactly one: select_reference OR create_reference',
    '4) always do web_search',
    '5) verify links with verify_link until you have >= 3 verified links or retries are exhausted',
    '6) add_tab for verified links in selected/created reference',
    '7) delegate_path_a to run scoped deep research',
    '8) read_research_artifact for summary material',
    '9) finish with summary_for_telegram + citation_urls (prefer 3 verified URLs).',
    'Do not fabricate citations. Use only verified links.',
  ].join('\n');
}

function buildPathBUserPrompt(task = {}, session = null) {
  const lines = [
    `User request: ${String(task.message || '').trim() || '(empty request)'}`,
    `Source: ${String(task.source || 'telegram')}`,
    `User scope: ${String(task.user_scope || 'default')}`,
  ];
  const msgs = toArray(session && session.messages).slice(-4);
  if (msgs.length > 0) {
    lines.push('', 'Recent conversation context:');
    msgs.forEach((item, idx) => {
      lines.push(`${idx + 1}. User: ${toShortText(item.user_message || '', 180)}`);
      lines.push(`   Summary: ${toShortText(item.summary || '', 220)}`);
    });
  }
  lines.push('', 'Deliverable for Telegram: concise summary + 3 verified source URLs.');
  return lines.join('\n');
}

function buildPathADelegationPrompt(task = {}, orchestratorState = {}) {
  const message = String(task.message || '').trim();
  const mode = String(orchestratorState.chosen_mode || '').trim() || 'unknown';
  const rationale = String(orchestratorState.selection_rationale || '').trim();
  const verified = toArray(orchestratorState.verified_urls).slice(0, 8);
  const linksSection = verified.length > 0
    ? verified.map((item, idx) => {
      const title = toShortText((item && item.title) || `Verified Link ${idx + 1}`, 160);
      const url = String((item && item.url) || '').trim();
      const reason = toShortText((item && item.reason) || (item && item.relevance_reason) || '', 180);
      return `${idx + 1}. ${title}\nURL: ${url}\nWhy relevant: ${reason || 'Semantic match with user intent.'}`;
    }).join('\n\n')
    : 'No verified links were available.';
  return [
    '[Path B -> Path A delegation]',
    `User request: ${message || '(empty request)'}`,
    `Reference mode: ${mode}${rationale ? ` (${rationale})` : ''}`,
    '',
    'Verified links for this reference:',
    linksSection,
    '',
    'Execution requirements:',
    '- Produce a substantive research artifact in this reference.',
    '- Include an explicit concise telegram-ready summary paragraph at the end.',
    '- Keep all claims grounded in cited evidence.',
  ].join('\n');
}

function buildEnforcedPathADelegationPrompt(task = {}, orchestratorState = {}, modelPrompt = '') {
  const basePrompt = buildPathADelegationPrompt(task, orchestratorState);
  const customPrompt = String(modelPrompt || '').trim();
  if (!customPrompt) return basePrompt;
  const notes = toShortText(customPrompt, 900);
  return [
    basePrompt,
    '',
    'Additional orchestration notes:',
    notes,
  ].join('\n');
}

function parseToolArray(value) {
  if (!Array.isArray(value)) return [];
  return value.map((item) => String(item || '').trim()).filter(Boolean);
}

const PATH_B_TOOLS = [
  {
    name: 'global_reference_search',
    description: 'Run global top-K embedding search across all references.',
    parameters: {
      type: 'object',
      properties: {
        query: { type: 'string' },
        top_k: { type: 'number' },
      },
      required: ['query'],
      additionalProperties: false,
    },
  },
  {
    name: 'read_reference_snapshot',
    description: 'Read a compact snapshot of a reference by sr_id.',
    parameters: {
      type: 'object',
      properties: {
        sr_id: { type: 'string' },
      },
      required: ['sr_id'],
      additionalProperties: false,
    },
  },
  {
    name: 'select_reference',
    description: 'Select an existing reference for Path A scoped execution.',
    parameters: {
      type: 'object',
      properties: {
        sr_id: { type: 'string' },
        reason: { type: 'string' },
      },
      required: ['sr_id'],
      additionalProperties: false,
    },
  },
  {
    name: 'create_reference',
    description: 'Create a new reference when no existing reference is suitable.',
    parameters: {
      type: 'object',
      properties: {
        topic: { type: 'string' },
        reason: { type: 'string' },
      },
      required: ['topic'],
      additionalProperties: false,
    },
  },
  {
    name: 'web_search',
    description: 'Search the web for candidate links.',
    parameters: {
      type: 'object',
      properties: {
        query: { type: 'string' },
        max_results: { type: 'number' },
      },
      required: ['query'],
      additionalProperties: false,
    },
  },
  {
    name: 'verify_link',
    description: 'Verify that a URL truly matches user intent and is readable.',
    parameters: {
      type: 'object',
      properties: {
        url: { type: 'string' },
        query: { type: 'string' },
        intent_context: { type: 'string' },
      },
      required: ['url', 'query'],
      additionalProperties: false,
    },
  },
  {
    name: 'add_tab',
    description: 'Add a verified link as a web tab in the selected reference.',
    parameters: {
      type: 'object',
      properties: {
        sr_id: { type: 'string' },
        url: { type: 'string' },
        title: { type: 'string' },
      },
      required: ['sr_id', 'url'],
      additionalProperties: false,
    },
  },
  {
    name: 'delegate_path_a',
    description: 'Delegate deep scoped research and artifact generation to Path A.',
    parameters: {
      type: 'object',
      properties: {
        sr_id: { type: 'string' },
        worker_prompt: { type: 'string' },
      },
      required: ['sr_id'],
      additionalProperties: false,
    },
  },
  {
    name: 'read_research_artifact',
    description: 'Read the latest substantive research artifact from the selected reference.',
    parameters: {
      type: 'object',
      properties: {
        sr_id: { type: 'string' },
      },
      required: ['sr_id'],
      additionalProperties: false,
    },
  },
  {
    name: 'update_user_preferences',
    description: 'Update orchestrator preferences for this user scope.',
    parameters: {
      type: 'object',
      properties: {
        patch: { type: 'object', additionalProperties: true },
      },
      required: ['patch'],
      additionalProperties: false,
    },
  },
  {
    name: 'finish',
    description: 'Finish with telegram summary text and citation URLs.',
    parameters: {
      type: 'object',
      properties: {
        summary_for_telegram: { type: 'string' },
        citation_urls: {
          type: 'array',
          items: { type: 'string' },
        },
      },
      required: ['summary_for_telegram'],
      additionalProperties: false,
    },
  },
];

function createPathBExecutor(options = {}) {
  const enabled = options.enabled !== false;
  const globalReferenceSearch = typeof options.globalReferenceSearch === 'function' ? options.globalReferenceSearch : null;
  const readReferenceSnapshot = typeof options.readReferenceSnapshot === 'function' ? options.readReferenceSnapshot : null;
  const selectReference = typeof options.selectReference === 'function' ? options.selectReference : null;
  const createReference = typeof options.createReference === 'function' ? options.createReference : null;
  const webSearch = typeof options.webSearch === 'function' ? options.webSearch : null;
  const verifyLink = typeof options.verifyLink === 'function' ? options.verifyLink : null;
  const addTab = typeof options.addTab === 'function' ? options.addTab : null;
  const delegatePathA = typeof options.delegatePathA === 'function' ? options.delegatePathA : null;
  const readResearchArtifact = typeof options.readResearchArtifact === 'function' ? options.readResearchArtifact : null;
  const upsertUserPreferences = typeof options.upsertUserPreferences === 'function' ? options.upsertUserPreferences : null;
  const executeAgenticLoopFn = typeof options.executeAgenticLoop === 'function' ? options.executeAgenticLoop : null;
  const callProviderWithToolsFn = typeof options.callProviderWithTools === 'function' ? options.callProviderWithTools : null;
  const resolveRuntimeCredentials = typeof options.resolveRuntimeCredentials === 'function' ? options.resolveRuntimeCredentials : null;
  const sessionStore = options.sessionStore && typeof options.sessionStore === 'object' ? options.sessionStore : null;
  const logger = options.logger || console;

  const required = [
    globalReferenceSearch,
    readReferenceSnapshot,
    selectReference,
    createReference,
    webSearch,
    verifyLink,
    addTab,
    delegatePathA,
    readResearchArtifact,
    upsertUserPreferences,
    executeAgenticLoopFn,
    callProviderWithToolsFn,
    resolveRuntimeCredentials,
  ];
  if (required.some((item) => !item)) {
    throw new Error('createPathBExecutor missing required dependencies for orchestrator mode.');
  }

  function readSession(sessionKey) {
    if (!sessionStore || typeof sessionStore.get !== 'function') return null;
    return sessionStore.get(sessionKey);
  }

  function writeSession(sessionKey, sessionValue) {
    if (!sessionStore || typeof sessionStore.set !== 'function') return;
    sessionStore.set(sessionKey, sessionValue);
  }

  function clearSession(sessionKey) {
    if (!sessionStore || typeof sessionStore.delete !== 'function') return;
    sessionStore.delete(sessionKey);
  }

  async function executeLegacyPathBTask(input = {}, optionsIn = {}) {
    const payload = (input && typeof input === 'object') ? input : {};
    const topic = String(payload.message || '').trim();
    if (!topic) return { ok: false, message: 'Message is required.' };
    const userScope = String(payload.user_scope || payload.chat_id || payload.username || 'default').trim().toLowerCase();
    const source = String(payload.source || 'telegram').trim().toLowerCase();

    const global = await globalReferenceSearch({ query: topic, top_k: PATH_B_DEFAULT_TOP_K });
    const top = toArray(global && global.results).slice(0, 1);
    let chosen = null;
    if (top.length > 0) {
      chosen = await selectReference({
        sr_id: String((top[0] && top[0].sr_id) || '').trim(),
        reason: 'legacy_top_match',
        source,
        user_scope: userScope,
      });
    }
    if (!chosen || !chosen.ok || !chosen.sr_id) {
      chosen = await createReference({
        topic,
        reason: 'legacy_no_match',
        source,
        user_scope: userScope,
      });
    }
    if (!chosen || !chosen.ok || !chosen.sr_id) {
      return { ok: false, message: 'Unable to resolve reference.' };
    }
    const srId = String(chosen.sr_id || '').trim();
    const web = await webSearch({ query: topic, max_results: 8, provider_hint: String(payload.web_provider || '') });
    const urls = toArray(web && web.results).map((item) => String((item && item.url) || '').trim()).filter(Boolean).slice(0, 5);
    for (let i = 0; i < urls.length; i += 1) {
      await addTab({ sr_id: srId, url: urls[i], title: `Candidate ${i + 1}` });
    }
    const workerPrompt = buildPathADelegationPrompt(payload, {
      chosen_mode: chosen.created ? 'created' : 'reused',
      selection_rationale: String((chosen && chosen.message) || ''),
      verified_urls: toArray(web && web.results).slice(0, 5).map((item) => ({
        title: String((item && item.title) || ''),
        url: String((item && item.url) || ''),
        reason: String((item && item.snippet) || ''),
      })),
    });
    const delegated = await delegatePathA({
      sr_id: srId,
      worker_prompt: workerPrompt,
      provider: String(payload.provider || ''),
      model: String(payload.model || ''),
      run_id: String(payload.run_id || ''),
      source,
      user_scope: userScope,
      idempotency_key: computeIdempotencyKey({
        topic,
        source,
        user_scope: userScope,
        day_bucket: String(payload.day_bucket || dayBucketFromTs()),
      }),
    }, optionsIn);
    const artifact = await readResearchArtifact({ sr_id: srId, delegated_response: delegated });
    const summaryText = toShortText(String((artifact && artifact.summary) || (delegated && delegated.message) || 'Research completed.'), 1200);
    const citationUrls = dedupeUrls(toArray(web && web.results).map((item) => String((item && item.url) || '').trim())).slice(0, 3);
    const telegram = formatTelegramMessage(summaryText, citationUrls);
    return {
      ok: true,
      lane: 'path_b',
      chosen_sr_id: srId,
      chosen_mode: chosen.created ? 'created' : 'reused',
      verified_urls: toArray(web && web.results).slice(0, 3),
      telegram_citations: telegram.citation_urls,
      artifact_id_used_for_summary: String((artifact && artifact.artifact && artifact.artifact.id) || ''),
      delegated_response: delegated || null,
      research_artifact: artifact || null,
      telegram_summary: telegram.text,
      follow_up_question: buildFollowUpQuestion({ user_message: topic }),
      tool_trace: [],
      message: telegram.text,
    };
  }

  async function executePathBTask(input = {}, optionsIn = {}) {
    if (!enabled) {
      return executeLegacyPathBTask(input, optionsIn);
    }

    const payload = (input && typeof input === 'object') ? input : {};
    const userMessage = String(payload.message || '').trim();
    if (!userMessage) return { ok: false, message: 'Message is required.' };

    const runId = String(payload.run_id || `run_${nowTs()}`).trim();
    const source = String(payload.source || 'telegram').trim().toLowerCase();
    const userScope = String(payload.user_scope || payload.chat_id || payload.username || 'default').trim().toLowerCase();
    const sessionKey = String(payload.session_key || payload.chat_id || payload.username || userScope).trim();
    const idempotencyKey = computeIdempotencyKey({
      topic: userMessage,
      source,
      user_scope: userScope,
      day_bucket: String(payload.day_bucket || dayBucketFromTs()),
    });

    const provider = String(payload.provider || 'openai').trim().toLowerCase();
    const model = String(payload.model || '').trim();
    const creds = resolveRuntimeCredentials(provider, String(payload.provider_key_id || payload.key_id || ''));
    if (!creds || !creds.ok) {
      return { ok: false, message: String((creds && creds.message) || 'Provider credentials unavailable for Path B.') };
    }

    const priorSession = sessionKey ? readSession(sessionKey) : null;
    const orchestratorState = {
      selected_sr_id: '',
      chosen_mode: '',
      selection_rationale: '',
      global_results: [],
      web_results: [],
      verified_urls: [],
      delegated_response: null,
      research_artifact: null,
      final_payload: null,
      tool_trace: [],
      path_a_prompt: '',
    };

    const appendToolTrace = (entry) => {
      orchestratorState.tool_trace.push({
        ts: nowTs(),
        ...((entry && typeof entry === 'object') ? entry : {}),
      });
    };

    const toolExec = async (toolInput = {}) => {
      const name = String((toolInput && toolInput.name) || '').trim();
      const args = (toolInput && toolInput.arguments && typeof toolInput.arguments === 'object')
        ? toolInput.arguments
        : {};
      appendToolTrace({ name, args });

      if (name === 'global_reference_search') {
        const topK = Math.max(1, Math.min(40, Number(args.top_k || PATH_B_DEFAULT_TOP_K)));
        const res = await globalReferenceSearch({
          query: String(args.query || userMessage),
          top_k: topK,
          source,
          user_scope: userScope,
        });
        orchestratorState.global_results = toArray(res && res.results);
        return {
          ok: !!(res && res.ok),
          message: `Global reference search returned ${orchestratorState.global_results.length} result(s).`,
          tool_output: {
            ok: !!(res && res.ok),
            query: String(args.query || userMessage),
            results: orchestratorState.global_results,
          },
        };
      }

      if (name === 'read_reference_snapshot') {
        const srId = String(args.sr_id || '').trim();
        const res = await readReferenceSnapshot({ sr_id: srId });
        return {
          ok: !!(res && res.ok),
          message: res && res.ok ? `Read snapshot for ${srId}.` : String((res && res.message) || 'Unable to read reference snapshot.'),
          tool_output: res || { ok: false, message: 'Unable to read reference snapshot.' },
        };
      }

      if (name === 'select_reference') {
        const srId = String(args.sr_id || '').trim();
        const reason = String(args.reason || '').trim();
        const res = await selectReference({
          sr_id: srId,
          reason,
          run_id: runId,
          source,
          user_scope: userScope,
          idempotency_key: idempotencyKey,
        });
        if (res && res.ok && res.sr_id) {
          orchestratorState.selected_sr_id = String(res.sr_id || '').trim();
          orchestratorState.chosen_mode = 'reused';
          orchestratorState.selection_rationale = reason || String(res.message || '');
        }
        return {
          ok: !!(res && res.ok),
          message: res && res.ok ? `Selected reference ${String(res.sr_id || '')}.` : String((res && res.message) || 'Unable to select reference.'),
          tool_output: res || { ok: false, message: 'Unable to select reference.' },
        };
      }

      if (name === 'create_reference') {
        const topic = String(args.topic || userMessage).trim();
        const reason = String(args.reason || '').trim();
        const res = await createReference({
          topic,
          reason,
          run_id: runId,
          source,
          user_scope: userScope,
          idempotency_key: idempotencyKey,
        });
        if (res && res.ok && res.sr_id) {
          orchestratorState.selected_sr_id = String(res.sr_id || '').trim();
          orchestratorState.chosen_mode = 'created';
          orchestratorState.selection_rationale = reason || String(res.message || '');
        }
        return {
          ok: !!(res && res.ok),
          message: res && res.ok ? `Created reference ${String(res.sr_id || '')}.` : String((res && res.message) || 'Unable to create reference.'),
          tool_output: res || { ok: false, message: 'Unable to create reference.' },
        };
      }

      if (name === 'web_search') {
        const query = String(args.query || userMessage).trim();
        const maxResults = Math.max(8, Math.min(20, Number(args.max_results || 8)));
        const res = await webSearch({
          query,
          max_results: maxResults,
          provider_hint: String(payload.web_provider || '').trim().toLowerCase(),
          timeout_ms: Number(optionsIn.timeoutMs || 25_000),
        });
        orchestratorState.web_results = toArray(res && res.results);
        return {
          ok: !!(res && res.ok),
          message: `Web search returned ${orchestratorState.web_results.length} result(s).`,
          tool_output: res || { ok: false, message: 'Web search failed.' },
        };
      }

      if (name === 'verify_link') {
        const url = String(args.url || '').trim();
        const query = String(args.query || userMessage).trim();
        const intentContext = String(args.intent_context || orchestratorState.selection_rationale || '').trim();
        const res = await verifyLink({
          url,
          query,
          intent_context: intentContext,
        });
        if (res && res.ok && res.accepted) {
          const key = String((res.url || url) || '').trim().toLowerCase();
          const exists = orchestratorState.verified_urls.some((item) => String((item && item.url) || '').trim().toLowerCase() === key);
          if (!exists) {
            orchestratorState.verified_urls.push({
              title: String(res.title || '').trim(),
              url: String(res.url || url).trim(),
              reason: String(res.reason || '').trim(),
              relevance_score: Number(res.relevance_score || 0),
            });
            orchestratorState.verified_urls.sort((a, b) => Number((b && b.relevance_score) || 0) - Number((a && a.relevance_score) || 0));
          }
        }
        return {
          ok: !!(res && res.ok),
          message: res && res.ok
            ? (res.accepted ? `Verified ${url}.` : `Rejected ${url}.`)
            : String((res && res.message) || 'Link verification failed.'),
          tool_output: res || { ok: false, message: 'Link verification failed.' },
        };
      }

      if (name === 'add_tab') {
        const srId = String(args.sr_id || orchestratorState.selected_sr_id).trim();
        const url = String(args.url || '').trim();
        const title = String(args.title || '').trim();
        const res = await addTab({
          sr_id: srId,
          url,
          title,
          run_id: runId,
          source,
          user_scope: userScope,
          idempotency_key: idempotencyKey,
        });
        return {
          ok: !!(res && res.ok),
          message: res && res.ok ? `Added tab for ${url}.` : String((res && res.message) || 'Unable to add tab.'),
          tool_output: res || { ok: false, message: 'Unable to add tab.' },
        };
      }

      if (name === 'delegate_path_a') {
        const srId = String(args.sr_id || orchestratorState.selected_sr_id).trim();
        if (!srId) {
          return { ok: false, message: 'No selected reference. Call select_reference or create_reference first.', tool_output: { ok: false, message: 'No selected reference.' } };
        }
        const workerPrompt = buildEnforcedPathADelegationPrompt(
          payload,
          orchestratorState,
          String(args.worker_prompt || '').trim(),
        );
        orchestratorState.path_a_prompt = workerPrompt;
        // A new delegation invalidates any stale artifact read from earlier turns.
        orchestratorState.research_artifact = null;
        const res = await delegatePathA({
          sr_id: srId,
          worker_prompt: workerPrompt,
          provider: provider,
          model,
          run_id: runId,
          source,
          user_scope: userScope,
          idempotency_key: idempotencyKey,
        }, {
          timeoutMs: Number(optionsIn.timeoutMs || 120_000),
          signal: optionsIn.signal,
        });
        orchestratorState.delegated_response = res || null;
        return {
          ok: !!(res && res.ok !== false),
          message: `Delegated to Path A for ${srId}.`,
          tool_output: {
            ok: !!(res && res.ok !== false),
            sr_id: srId,
            response_message: String((res && res.message) || ''),
          },
        };
      }

      if (name === 'read_research_artifact') {
        const srId = String(args.sr_id || orchestratorState.selected_sr_id).trim();
        const res = await readResearchArtifact({
          sr_id: srId,
          delegated_response: orchestratorState.delegated_response,
        });
        // Keep only successful reads in state so fallback can retry failed/empty reads.
        orchestratorState.research_artifact = (res && res.ok) ? res : null;
        return {
          ok: !!(res && res.ok),
          message: res && res.ok ? 'Read research artifact.' : String((res && res.message) || 'Unable to read research artifact.'),
          tool_output: res || { ok: false, message: 'Unable to read research artifact.' },
        };
      }

      if (name === 'update_user_preferences') {
        const patch = (args.patch && typeof args.patch === 'object') ? args.patch : {};
        const mergedPatch = {
          ...patch,
          last_query: userMessage,
          last_query_at: nowTs(),
          last_verified_urls: orchestratorState.verified_urls.map((item) => String((item && item.url) || '').trim()).filter(Boolean).slice(0, 8),
        };
        const res = await upsertUserPreferences({
          user_scope: userScope,
          chat_id: String(payload.chat_id || ''),
          username: String(payload.username || ''),
          patch: mergedPatch,
        });
        return {
          ok: !!(res && res.ok),
          message: res && res.ok ? 'User preferences updated.' : String((res && res.message) || 'Unable to update preferences.'),
          tool_output: res || { ok: false, message: 'Unable to update preferences.' },
        };
      }

      if (name === 'finish') {
        const summary = String(args.summary_for_telegram || '').trim();
        const citationUrls = parseToolArray(args.citation_urls);
        orchestratorState.final_payload = {
          summary_for_telegram: summary,
          citation_urls: citationUrls,
        };
        return {
          ok: true,
          finish: true,
          final_message: summary || 'Path B finished.',
          message: 'Path B marked complete.',
          tool_output: {
            ok: true,
            summary_for_telegram: summary,
            citation_urls: citationUrls,
          },
        };
      }

      return {
        ok: false,
        message: `Unsupported Path B tool: ${name}`,
        tool_output: { ok: false, message: `Unsupported Path B tool: ${name}` },
      };
    };

    const userPrompt = buildPathBUserPrompt(payload, priorSession);
    const systemPrompt = buildPathBSystemPrompt();
    const agentResult = await executeAgenticLoopFn({
      provider,
      model,
      apiKey: String(creds.apiKey || ''),
      systemPrompt,
      userPrompt,
      signal: optionsIn.signal,
      maxTurns: Math.max(8, Math.min(16, Number(optionsIn.maxTurns || 12))),
      tools: PATH_B_TOOLS,
      researchPolicy: {
        isDetailed: false,
        requiresWebResearch: false,
        requireDeliverableBeforeFinish: false,
        localEvidenceAvailable: false,
        requiresCitations: false,
        citationMode: 'hybrid',
      },
      callProviderWithTools: async (providerPayload, providerOptions = {}) => callProviderWithToolsFn({
        ...((providerPayload && typeof providerPayload === 'object') ? providerPayload : {}),
        baseUrl: String((creds && creds.base_url) || '').trim(),
      }, {
        ...((providerOptions && typeof providerOptions === 'object') ? providerOptions : {}),
        signal: optionsIn.signal,
        timeoutMs: Number(optionsIn.timeoutMs || 120_000),
      }),
      executeTool: toolExec,
    });

    // Deterministic recovery so Path B remains reliable even if the model skips a required tool step.
    if (!String(orchestratorState.selected_sr_id || '').trim()) {
      const top = toArray(orchestratorState.global_results)[0] || null;
      if (top && top.sr_id) {
        await toolExec({
          name: 'select_reference',
          arguments: {
            sr_id: String(top.sr_id || ''),
            reason: 'fallback_select_top_match',
          },
        });
      } else {
        await toolExec({
          name: 'create_reference',
          arguments: {
            topic: userMessage,
            reason: 'fallback_no_global_match',
          },
        });
      }
    }

    if (toArray(orchestratorState.web_results).length === 0) {
      await toolExec({
        name: 'web_search',
        arguments: {
          query: userMessage,
          max_results: 8,
        },
      });
    }

    const runVerificationPass = async (queryText, rows = []) => {
      const urls = dedupeUrls(toArray(rows).map((item) => String((item && item.url) || '').trim())).slice(0, 16);
      for (let i = 0; i < urls.length; i += 1) {
        if (toArray(orchestratorState.verified_urls).length >= 3) break;
        await toolExec({
          name: 'verify_link',
          arguments: {
            url: urls[i],
            query: queryText,
            intent_context: String(orchestratorState.selection_rationale || userMessage),
          },
        });
      }
    };

    await runVerificationPass(userMessage, orchestratorState.web_results);
    if (toArray(orchestratorState.verified_urls).length < 3) {
      const broadenedQuery = `${userMessage} latest updates analysis`;
      await toolExec({
        name: 'web_search',
        arguments: {
          query: broadenedQuery,
          max_results: 8,
        },
      });
      await runVerificationPass(broadenedQuery, orchestratorState.web_results);
    }

    const selectedSrId = String(orchestratorState.selected_sr_id || '').trim();
    if (selectedSrId) {
      const toTab = toArray(orchestratorState.verified_urls).slice(0, 6);
      for (let i = 0; i < toTab.length; i += 1) {
        const item = toTab[i] || {};
        await toolExec({
          name: 'add_tab',
          arguments: {
            sr_id: selectedSrId,
            url: String(item.url || ''),
            title: String(item.title || item.url || `Verified Link ${i + 1}`),
          },
        });
      }
    }

    if (selectedSrId && !orchestratorState.delegated_response) {
      await toolExec({
        name: 'delegate_path_a',
        arguments: {
          sr_id: selectedSrId,
          worker_prompt: buildPathADelegationPrompt(payload, orchestratorState),
        },
      });
    }

    const hasResearchSummary = !!String(((orchestratorState.research_artifact && orchestratorState.research_artifact.summary) || '')).trim();
    if (selectedSrId && (!orchestratorState.research_artifact || !hasResearchSummary)) {
      await toolExec({
        name: 'read_research_artifact',
        arguments: {
          sr_id: selectedSrId,
        },
      });
    }

    const inferredTopics = normalizeTopic(userMessage).split(' ').filter((token) => token.length >= 4).slice(0, 8);
    const trackingPreference = /\b(track|monitor|daily|weekly|schedule|cron)\b/i.test(userMessage)
      ? 'tracking_requested'
      : 'none';
    const outputPreference = /\b(summary|summarize|brief|concise|telegram)\b/i.test(userMessage)
      ? 'summary'
      : 'detailed';
    await toolExec({
      name: 'update_user_preferences',
      arguments: {
        patch: {
          inferred_topics: inferredTopics,
          tracking_preference: trackingPreference,
          output_preference: outputPreference,
        },
      },
    });

    const summaryRaw = String(
      (orchestratorState.final_payload && orchestratorState.final_payload.summary_for_telegram)
      || ((orchestratorState.research_artifact && orchestratorState.research_artifact.summary) || '')
      || ((orchestratorState.delegated_response && orchestratorState.delegated_response.message) || '')
      || ((agentResult && agentResult.message) || '')
      || ''
    ).trim();
    const verifiedOnly = dedupeUrls(
      toArray(orchestratorState.verified_urls).map((item) => String((item && item.url) || '').trim()),
    );
    const requestedOrder = parseToolArray(orchestratorState.final_payload && orchestratorState.final_payload.citation_urls);
    const normalizedVerified = new Map();
    verifiedOnly.forEach((url) => {
      normalizedVerified.set(String(url || '').trim().toLowerCase(), url);
    });
    const orderedFromRequested = requestedOrder
      .map((url) => normalizedVerified.get(String(url || '').trim().toLowerCase()) || '')
      .filter(Boolean);
    const citationUrlsRaw = dedupeUrls([
      ...orderedFromRequested,
      ...verifiedOnly,
    ]);
    const telegram = formatTelegramMessage(summaryRaw, citationUrlsRaw);

    const artifactIdForSummary = String(
      (orchestratorState.research_artifact && orchestratorState.research_artifact.artifact && orchestratorState.research_artifact.artifact.id)
      || ''
    ).trim();
    const chosenSrId = String(orchestratorState.selected_sr_id || '').trim();
    const chosenMode = String(orchestratorState.chosen_mode || '').trim();
    const compactToolTrace = toArray((agentResult && agentResult.tool_steps) || []).map((step) => ({
      name: String((step && step.name) || ''),
      ok: !!(step && step.ok),
      message: toShortText((step && step.message) || '', 180),
    })).slice(-40);

    if (sessionKey) {
      const prev = priorSession || { messages: [], updated_at: 0 };
      const nextMessages = toArray(prev.messages).slice(-30);
      nextMessages.push({
        ts: nowTs(),
        user_message: userMessage,
        sr_id: chosenSrId,
        summary: toShortText(summaryRaw, 800),
      });
      writeSession(sessionKey, {
        messages: nextMessages.slice(-40),
        updated_at: nowTs(),
      });
    }

    logger.info('[path_b]', JSON.stringify({
      run_id: runId,
      chosen_sr_id: chosenSrId,
      chosen_mode: chosenMode,
      verified_count: toArray(orchestratorState.verified_urls).length,
      artifact_id: artifactIdForSummary,
      tool_steps: compactToolTrace.length,
    }));

    return {
      ok: true,
      lane: 'path_b',
      run_id: runId,
      chosen_sr_id: chosenSrId,
      chosen_mode: chosenMode || 'created',
      sr_id: chosenSrId,
      idempotency_key: idempotencyKey,
      verified_urls: toArray(orchestratorState.verified_urls).slice(0, 12),
      telegram_citations: telegram.citation_urls,
      artifact_id_used_for_summary: artifactIdForSummary,
      tool_trace: compactToolTrace,
      delegated_response: orchestratorState.delegated_response || null,
      research_artifact: orchestratorState.research_artifact || null,
      telegram_summary: telegram.text,
      follow_up_question: buildFollowUpQuestion({ user_message: userMessage }),
      message: telegram.text,
      agent_result: agentResult || null,
    };
  }

  return {
    executePathBTask,
    clearConversation(sessionKey = '') {
      const key = String(sessionKey || '').trim();
      if (key) clearSession(key);
      return { ok: true, session_key: key };
    },
    computeIdempotencyKey,
    PATH_B_TOOLS,
  };
}

module.exports = {
  createPathBExecutor,
  computeIdempotencyKey,
  normalizeTopic,
  PATH_B_TOOLS,
};
