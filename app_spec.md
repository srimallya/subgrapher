# Subgrapher App Spec (Unified Artifact Runtime)

## Summary
Subgrapher uses one artifact runtime for authored outputs:
- `markdown` artifacts for text/image docs
- `html` artifacts for interactive visualizations and games

Legacy pygame/viz tabs are removed from active runtime behavior.
Image analysis is unified: active provider native vision is attempted first, then LM Studio fallback is used when needed.
Hyperweb now includes E2E chat (`DM` + `room`) over the existing RTC data-channel path.
Workspace surfaces currently include:
- `web`
- `artifact`
- `files`
- `skills`
- `mail`

## Artifact Data Model
Artifacts are stored as:
`{ id, type, title, content, created_at, updated_at }`

Where:
- `type = "markdown" | "html"`

## Runtime UX
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

## Hyperweb Chat + Multi-Device Sync
- Transport:
  - Uses existing Hyperweb RTC data-channel protocol path (no additional transport).
- Chat protocol:
  - `hyperweb:chat_message`
  - `hyperweb:chat_ack` (delivery/read)
- Security:
  - sender payloads are signed and verified against known peer signing keys
  - chat body is end-to-end encrypted (X25519 key agreement + AES-256-GCM envelope)
- UX:
  - Hyperweb Chat tab supports direct messages, room messaging, online presence, and basic p2p file transfer.
- Multi-device sync policy:
  - controlled by Settings toggle `trustcommons_sync_enabled` (`Multi-Device Auto Sync`)
  - default `false` (opt-in)
  - enabling requires `trustcommons_identity_id`; without identity, sync remains disabled
  - current limitation: Hyperweb peer identity is device-local, so multiple devices for one user may appear as separate peer fingerprints.

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
  - manual only right now
  - triggered from Settings per account / selected accounts
  - also triggered from global Mail and reference mail surfaces
  - no background polling scheduler yet
  - no IMAP IDLE lifecycle yet
  - no OS new-mail notifications yet
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
  - `documents` + `embeddings` tables with schema version marker
- Embedding runtime:
  - primary: LM Studio `/v1/embeddings`
  - fallback: local hash embedding (`hybrid:local-hash-embedding-v1`)
- Settings/runtime controls:
  - `rag_enabled`
  - `rag_embedding_model` (default: `text-embedding-nomic-embed-text-v1.5`)
  - `rag_top_k`
  - status + manual reindex action in Settings

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
