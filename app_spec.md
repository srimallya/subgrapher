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
- Explicit lifecycle:
  - `Start` creates sandboxed iframe runtime from artifact HTML.
  - `Stop` tears down runtime and clears iframe state.
- Full workspace sizing:
  - Preview uses the full artifact workspace area (no centered fixed black viewport).
- Interaction:
  - Click-to-focus and native keyboard/mouse handling inside iframe.
- Edit behavior while running:
  - Saved edits mark runtime stale.
  - User must `Stop` then `Start` to apply changes deterministically.
- `Code` / `Preview` toggle is available for HTML artifacts.

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
- Mail is synced directly into Subgrapher from user-configured IMAP accounts.
- Mailbox accounts are configured in `Settings -> Mail`.
- Mail credentials are stored via the OS keychain; they are not embedded in reference data.
- Active mail flow does not depend on Apple Mail folder scanning or AppleScript automation.
- Sync is read-only.
- Sync targets:
  - the configured mailbox
  - common sent-mail folders where available
- The goal is to reconstruct usable conversation threads from both inbound and outbound mail.
- Reference mail UX uses three stable sections:
  - search/actions
  - thread list
  - content preview
- Search runs against Subgrapher's local mail database, not a live mail client process.
- Adding mail to a reference snapshots selected synced threads into that reference.
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
- `browser:mailSaveAccount`
- `browser:mailDeleteAccount`
- `browser:mailSyncAccount`
- `browser:mailSearchLocalThreads`
- `browser:mailPreviewSource`
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
