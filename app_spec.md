# Subgrapher App Spec (Unified Artifact Runtime)

## Summary
Current app version: `2.2.6`

Subgrapher uses one artifact runtime for authored outputs:
- `markdown` artifacts for text/image docs
- `html` artifacts for interactive visualizations and games

Legacy pygame/viz tabs are removed from active runtime behavior.
Image analysis is unified: active provider native vision is attempted first, then LM Studio fallback is used when needed.
Hyperweb now runs as a pure P2P subsystem over Hyperswarm topics with durable trusted-peer inbox delivery for private traffic.
Workspace surfaces currently include:
- `status`
- `web`
- `artifact`
- `files`
- `skills`
- `mail`
- `notes`

## Artifact Data Model
Artifacts are stored as:
`{ id, type, title, content, created_at, updated_at }`

Where:
- `type = "markdown" | "html"`

## Runtime UX
### Status surface
- A dedicated `status` workspace surface provides a compact overview layer for the active session.
- The surface includes:
  - a live clock tied to the local user time
  - elapsed progress bars for day, week, month, and year
  - a lightweight task list where checked items are removed
  - a calendar with upcoming events and add/delete flows
  - a notifications list that routes into Mail or Hyperweb targets
  - a topic-filtered feed with search and open-in-reference behavior
- Feed items open as new web tabs in the active reference.
- Notifications are read as operational shortcuts, not passive display-only items.

### Notes surface
- A dedicated `notes` workspace surface provides an ambient writing and reading layer alongside the browser-first Workspace flow.
- Product role split:
  - `notes` is thought-first ambient computation for drafting and reading
  - `web` workspace tabs remain the browser-first research surface
- Bundled small-LLM routing:
  - Notes uses a bundled local GGUF model through an in-app runtime, not through Ollama or a separately installed model host.
  - The bundled model is task-scoped:
    - full-note policy classification before evidence retrieval
    - current-feed article cleanup/summarization for Status
  - This model is not a general chat surface inside the app; it is a local structured-output helper for routing and cleanup tasks.
- Notes behavior:
  - users write freely in markdown/plain note form without manually triggering fact-check actions
  - when writing pauses, the full note is classified first into a retrieval policy that controls freshness bias, source mix, contradiction scan, and fetch budget
  - factual claims are detected continuously from the current note body
  - web evidence retrieval runs in the background against detected claims
  - reliability is shown at two levels:
    - whole-note `Evidence Reliability`
    - local sentence/region evidence states
  - a persistent right-side evidence rail stays visible and lists supporting, contradicting, and contextual sources in realtime
  - clicking an evidence item focuses the linked note region
  - edit mode hides inline markers to keep writing distraction low; view mode restores evidence-linked highlights
- Claim/evidence model:
  - claim extraction is atomic enough to avoid coarse chunk-only judgments
  - UI grouping remains sentence/region based for readability, scoring, and navigation
  - contradictory claims should surface as risky
  - partially correct claims should surface as mixed or weak, not only binary false
  - supported claims can still receive rewrite/context nudges for dates, numbers, attribution, and scope
- Promotion:
  - `Create Reference` from a note preserves the note body, evidence summary, retrieved research links, and excerpt context when moving into Workspace
  - promoted references should open with the carried research context already attached rather than requiring the user to rediscover the same sources
- Refresh behavior:
  - Notes exposes explicit `Reanalyze` so an older note can discard prior evidence state and rerun policy classification plus evidence retrieval from scratch
  - freshness-sensitive notes can auto-refresh evidence while idle when the stored evidence TTL has expired

### Bundled local LLM
- Subgrapher ships a bundled small local GGUF model plus local inference runtime for product-internal structured tasks.
- Release installers stay lean and reliable:
  - the app downloads the pinned local runtime binary and GGUF model into app-managed local data on first launch
  - assets are resolved from app data first, then packaged resources if present
  - this removes live model/runtime fetching from release build time
- Current bundled tasks:
  - `note_policy_classification`
  - `rss_article_cleanup_summary`
- Bootstrap behavior:
  - Settings shows bundled-LLM lifecycle state and progress:
    - `missing`
    - `downloading`
    - `extracting`
    - `ready`
    - `error`
  - once bootstrap completes, the app automatically processes pending note analysis and feed-summary work
- Failure handling:
  - invalid JSON, too-short summaries, and similar small-model output failures are retried up to 10 times before fallback
  - deterministic fallback heuristics remain available when the bundled runtime is unavailable or still fails validation after retries
  - main-process logging failures such as closed stdio / `EPIPE` must not surface as fatal app popups

### Status feed cleanup
- The `status` feed stores both raw fetched article text and cleaned summaries.
- After crawler fetch, the bundled small LLM removes scraper noise and stores a concise factual gist for display/search.
- `Rerun Tasks` in Settings discards old derived note/feed LLM outputs and rebuilds them from source content.

### Markdown artifacts
- Code/text editor with debounced autosave.
- Existing markdown image preview behavior is preserved.

### HTML artifacts
- Preview-first lifecycle:
  - Opening an HTML artifact mounts the sandboxed iframe runtime automatically.
  - `Code` is a secondary toggle for editing source.
  - `Refresh` rerenders the current HTML on demand.
- Full workspace sizing:
  - Preview uses the full artifact workspace area (no centered fixed black viewport).
- Interaction:
  - Click-to-focus and native keyboard/mouse handling inside iframe.
- Edit behavior while running:
  - Saved edits rerender the active HTML preview automatically.
  - Leaving the artifact surface resets `Code` back to preview silently.

## Security Model (HTML Runtime)
Iframe sandbox policy:
- `allow-scripts allow-forms allow-pointer-lock allow-downloads`

Intentionally not allowed:
- `allow-same-origin`
- top-level navigation takeover
- Electron/Node access

## Data-at-Rest Security
- Core reference and metadata stores are encrypted at rest with versioned AES-256-GCM envelopes.
- Existing plaintext JSON stores are migrated in-place to encrypted format during normal reads.
- App data lock UX:
  - `Lock Now` clears in-memory app data key material.
  - `Unlock` requires system auth:
    - Touch ID on supported macOS devices.
    - system account password fallback.
- App settings and keychain-backed secret references remain separately managed for bootstrap/runtime availability.

## Legacy Migration
On reference load, legacy `tab_kind="viz"` entries are migration-converted:
- viz tab content is preserved into a markdown artifact
- any available python code/snapshot metadata is retained in artifact content
- viz tabs are removed after conversion

Migration is idempotent and safe to run repeatedly.

## Agent + Tooling Policy
- Dedicated pygame tabs are removed.
- `run_python` remains available and can execute pygame scripts when explicitly used by user code.
- Pygame sandbox hooks are lazy-enabled only when script content references/imports `pygame`.
- Agents create interactive outputs using HTML artifacts:
  - `write_html_artifact`
  - or `create_artifact` with `artifact_type: "html"`
- Python visual outputs are routed to artifacts, not viz tabs.
- `analyze_image` supports `image_url`, `local_path`, and `context_file_id` (mounted context files), and returns provider/fallback metadata.

## Web Search + Orchestration
- Path A runtime tooling, Path B orchestration, and Telegram-triggered jobs share the same web search backend contract.
- Search fallback chain:
  - DDG instant API
  - DDG HTML endpoint parser
  - Bing HTML parser fallback
- No-result search outcomes are represented as successful empty result sets, not transport failures.
- Explicit web/search intent requests in agent mode must complete at least one web-evidence step.
- If a model returns no tool calls while web phase is still missing, deterministic recovery executes:
  - `web_search`
  - optional top-result `fetch_webpage`
- Citation gate validation targets deliverable artifact content when an artifact is written, not just final chat summary text.

## Hyperweb P2P Runtime
- Transport:
  - Uses Hyperswarm topic discovery and direct peer sockets.
  - No TTC auth, no relay URL, and no central signaling dependency.
  - Built-in topics include:
    - one fixed public topic for public feed/lobby/reference gossip
    - deterministic per-share topics for trusted-peer shared workspaces
    - deterministic per-peer inbox topics for durable private delivery
- Security:
  - sender payloads are signed and verified against known peer signing keys
  - chat body is end-to-end encrypted (X25519 key agreement + AES-256-GCM envelope)
- UX:
  - Hyperweb supports a public lobby, public feed/reference discovery, trusted-peer DM, private share notices, and shared rooms.
  - Public features are best-effort and eventually consistent across online peers.
  - Trusted-peer private traffic uses durable inbox replay:
    - DM text
    - DM file attachments
    - share invite / accept / decline / revoke / delete notices
  - Delivery semantics:
    - `pending`: stored locally and waiting for peer replay/materialization
    - `delivered`: recipient materialized the inbox entry locally
    - `read`: recipient explicitly marked the DM as read
  - Live collaboration updates remain realtime/best-effort on share topics; they are not part of the durable inbox path.

## Local Context Ingestion + Abstraction
- Folder mounts are read-only indexed context with recursive traversal and extension filtering.
- Indexed types include text/code, common images, and binary document formats:
  - `.png`, `.jpg/.jpeg`, `.gif`, `.webp`, `.bmp`, `.tif/.tiff`, `.heic/.heif`
  - `.pdf`, `.doc/.docx`, `.xls/.xlsx`, `.ppt/.pptx`, `.rtf`
  - `.odt/.ods/.odp`, `.msg`, `.eml`
- Default ingest caps:
  - up to 500 files
  - up to 32MB per file
- Manual local context import uses the same extension allowlist as folder mounts.
- Preview no longer dumps raw bytes for binary files; images render in modal with extracted context, and docs/binary files show extracted text/metadata summaries with open-in-default-app fallback.
- With abstraction routing enabled for non-local providers, local files are summarized into an abstraction copy.
- Non-text local files (image/doc/pdf/binary) may be analyzed via LM Studio during abstraction construction.
- `read_context_file` returns extracted non-text content and auto-attempts LM Studio vision summaries for images (with metadata fallback if unavailable).

## Mail Runtime
- Storage model:
  - local SQLite mail store at `mail_store.sqlite` in app `userData`
  - normalized tables for accounts, mailboxes, and messages
  - threads are reconstructed from normalized local messages
- Account setup:
  - configured in `Settings -> Mail`
  - supports generic IMAP/SMTP accounts with password auth
  - supports Gmail / Google Workspace OAuth bootstrap from Settings
  - credentials and OAuth secrets are stored via OS keychain refs, not in reference data
- Active mail flow:
  - Subgrapher talks to IMAP/SMTP directly
  - Apple Mail folder scanning and AppleScript automation are not part of the active path
- Sync model:
  - main-process polling scheduler
  - controlled by global Mail settings
  - app-level polling interval shared by enabled accounts
  - triggered from Settings per account / selected accounts
  - also triggered from global Mail and reference mail surfaces
  - per-account sync state tracks:
    - `sync_state`
    - `last_sync_at`
    - `last_success_at`
    - `last_error`
    - `new_threads_count`
    - `new_messages_count`
    - `last_notified_at`
  - per-account notifications toggle in Settings
  - OS notifications fire for new inbound unread mail detected from local-store diffs after sync
  - notification click routes the user into global `Mail` on the target account/thread
  - no IMAP IDLE lifecycle yet
- Sync targets:
  - configured mailbox
  - discovered/common sent folders
  - discovered/common drafts, archive, and trash folders where available
- Goal:
  - reconstruct usable inbound + outbound conversation threads in the local store
- Mail surfaces:
  - reference `mail` tab:
    - search/actions
    - thread list
    - content preview
  - global `Mail` page:
    - account/folder navigation
    - thread list
    - content preview
    - composer
- Mail actions available now:
  - search local threads by query/account/folder/smart view
  - preview normalized thread content
  - compose and reply
  - save draft
  - send message
  - attach local files to outgoing mail
  - mark read/unread
  - archive when provider capabilities allow it
  - move thread to trash
  - attach selected synced threads into the active reference
  - reference mail attach flow:
    - user selects one or more synced threads in the full thread list
    - `Add Selected` attaches that set to the active reference and switches the reference mail tab into an attached-thread review view
    - `Back` returns to the full thread list without discarding the current attached selection, so the user can extend the set and attach again
- Path B mail actions available now:
  - `mail_search` over the normalized local mail store only
  - `mail_read_thread` for normalized thread retrieval
  - `mail_open_thread` to route the renderer into global `Mail`
- Search runs against Subgrapher's local mail database, not a live mail client process.
- Legacy local mail databases are not migrated forward; when the mail schema changes, Subgrapher resets `mail_store.sqlite` and rebuilds it from a fresh sync.
- Mail preview should preserve human-readable structure:
  - paragraph spacing
  - sender/recipient headers
  - quoted reply blocks
  - mailbox/account metadata

## Local Evidence RAG
- Local evidence retrieval is hybrid:
  - BM25 lexical score
  - semantic cosine score
- Persistent vector storage:
  - SQLite index per reference under `semantic_references/<ref>/rag/index.sqlite`
  - `documents`, `embeddings`, `graph_nodes`, `graph_edges`, and `graph_scores` tables with schema version marker
- Temporal graph sidecar:
  - anchor-scoped to the active Path-C reference
  - built from the full scoped local-evidence source set, not `reference_graph`
  - stores source-level nodes, shared-term source-to-source edges, and precomputed temporal centrality scores
  - rebuilt on manual reindex and whenever scoped reference evidence changes
- Embedding runtime:
  - primary: LM Studio `/v1/embeddings`
  - fallback: local hash embedding (`hybrid:local-hash-embedding-v1`)
- Agent local-evidence tools:
  - `search_local_evidence` remains the primary retrieval path
  - `expand_local_evidence_graph` provides optional one-hop graph expansion with `global`, `recent_30d`, and `recent_7d` signals
- Settings/runtime controls:
  - `rag_enabled`
  - `rag_embedding_model` (default: `text-embedding-nomic-embed-text-v1.5`)
  - `rag_top_k`
  - status + manual reindex action in Settings
  - RAG status now reports graph readiness plus graph node/edge counts

## IPC / Renderer Contract
Removed from renderer preload usage:
- `browser:vizStart`
- `browser:vizStop`
- `browser:vizInput`
- `browser:vizFrame`

`browser:srOpenVizTab` remains deprecated guidance only (no new viz tab creation).

Mail renderer/main contract now includes IMAP-backed local store operations:
- `browser:mailListAccounts`
- `browser:mailListMailboxes`
- `browser:mailStatus`
- `browser:mailStartGoogleOAuth`
- `browser:mailSaveAccount`
- `browser:mailDeleteAccount`
- `browser:mailSyncAccount`
- `browser:mailSearchLocalThreads`
- `browser:mailPreviewSource`
- `browser:mailSendMessage`
- `browser:mailSaveDraft`
- renderer event channel: `browser:mail-event`
  - `sync_status` updates for background/manual sync lifecycle
  - `open_thread` route payload for notification-driven Mail navigation
- `browser:mailUpdateThreadState`
- `browser:mailMoveThread`
- `browser:mailDeleteThread`
- `browser:mailPickAttachments`
- `browser:srAttachMailThreadsFromStore`

## Python Runtime + Packaging
- Packaged apps use immutable runtime policy (no runtime `pip install`).
- `browser:pipInstall` in packaged builds returns explicit immutable-runtime failure messaging.
- `browser:pythonCheck` returns role diagnostics:
  - `runtimes.tool`
  - `runtimes.viz` (legacy diagnostic compatibility)
- `requests` availability is hardened via bundled dependency and sandbox fallback shim.

## Windows EXE Mitigation (Degraded Mode)
If bundled Python preparation is unavailable for Windows build:
- EXE still builds with placeholder runtime payload.
- App falls back to system Python for non-visual Python features.
- HTML artifact runtime remains fully available (independent of Python).

## Build Commands
One-command rebuild:
- `npm run build:release`

Platform-specific:
- `npm run build:mac`
- `npm run build:win`
