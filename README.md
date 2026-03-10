# Subgrapher

> **References are liquid apps. The context graph is the memory. The agent is the runtime.**

Traditional software gives you apps that run programs. Subgrapher gives you a different primitive: the **semantic reference** — a fluid, intentioned knowledge container that accretes web tabs, AI-generated artifacts, highlighted text, tags, and relations to other references. References aren't bookmarks and they aren't apps in the installed-software sense. They're liquid: they flow, reshape, fork, merge, and share.

The **context graph** is the memory. The network of references — root, child, related — with everything embedded in them (what you browsed, what you highlighted, what the agent produced) is the persistent knowledge state. Not a filesystem. Not a database. A living semantic graph that grows with your thinking.

The **agent** is the runtime. It doesn't assist with browsing — it executes the liquid app. It creates artifacts, diffs them, generates visualizations, queries the Hyperweb for what others have referenced, and indexes local folders as context. The agent is what makes a reference computable rather than static.

This is a post-app computing model. The unit is not the program — it's the intentioned context. And the agent is what runs it.

---

Subgrapher is a standalone desktop browser workspace app scaffold built from the architecture in `browser_tab_architecture_+_agent.md`.

## Current build

This implementation delivers:

- 3-panel workspace core: references, browser/artifact surface, and Lumino chat.
- App-level pages for `Workspace`, `Hyperweb`, `Private Shares`, `Settings`, and `History`.
- Native Electron `BrowserView` runtime for web tabs (including marker mode and URL history capture).
- Unified artifact runtime:
  - artifact types: `markdown` and `html`
  - `html` artifacts run in preview by default, with `Code` as a secondary toggle and `Refresh` for manual rerender
  - iframe sandbox: `allow-scripts allow-forms allow-pointer-lock allow-downloads` (no same-origin grant)
- Legacy visualization migration:
  - old `viz` tab payloads are auto-mapped into markdown artifacts on load
  - dedicated viz tabs are deprecated
- Workspace surfaces and tabs:
  - `web`, `artifact`, `files`, `skills`, `mail`
  - URL-bar artifact commands: `/add`, `/create`, `/rename/<name>`, `/rm`
- Reference graph lifecycle:
  - create root
  - fork child
  - commit current page into active reference
- Lumino dual lane runtime:
  - Path A: scoped reference execution
  - Path B: orchestrator lane for reference resolution, web intake, and Path A delegation
- Context ingestion:
  - mount folders as read-only indexed context (recursive with limits)
  - indexed extensions include text/code, common docs (`.pdf`, Office/OpenDocument, `.msg`, `.eml`), and common images (`.png`, `.jpg/.jpeg`, `.gif`, `.webp`, `.bmp`, `.tif/.tiff`, `.heic/.heif`)
  - default folder mount limits: up to 500 files, up to 32MB per file
  - import local context files across the same supported extension set (text/code/docs/images)
  - Files preview renders mounted images directly; docs/binary files show extracted fragments + summaries
  - web crawler commands: `/crawl <url>`, `/crawl status`, `/crawl stop`
- Mail runtime:
  - local mail store backed by `mail_store.sqlite` under app `userData`
  - account setup lives in `Settings -> Mail`
  - supported account modes:
    - generic IMAP/SMTP with password auth
    - Gmail / Google Workspace OAuth bootstrap from Settings
  - credentials and OAuth secrets are stored in the OS keychain, not in reference data
  - sync is IMAP-driven and manual-only right now:
    - `Sync` per account in Settings
    - `Sync Selected` from Settings
    - `Sync` from the global Mail page / reference Mail tab
  - there is no background polling scheduler, IMAP IDLE loop, or OS new-mail notification path yet
  - sync indexes the configured mailbox plus discovered/common sent, drafts, archive, and trash folders where available
  - mail is normalized into local accounts, mailboxes, messages, and reconstructed threads
  - incompatible legacy `mail_store.sqlite` files are discarded on startup; the app rebuilds the local mail store with the current schema and requires a fresh sync
  - both the global `Mail` page and reference `mail` tab search the local Subgrapher mail store, not Apple Mail and not live mailbox APIs
  - available mail actions today:
    - search threads by query/account/folder/smart view
    - preview normalized thread content
    - compose, reply, save draft, send
    - attach local files to outgoing mail
    - mark thread read/unread
    - archive thread when provider capabilities allow it
    - move thread to trash
    - attach selected synced threads into the active reference
    - reference mail attach flow is two-step:
      - select one or more synced threads in the full list
      - `Add Selected` attaches that set to the active reference and switches the tab into an attached-thread review view
      - `Back` returns to the full list while preserving the current attached selection so more threads can be added and attached again
  - mail layouts are fixed and practical:
    - reference mail tab: search/actions, thread list, content preview
    - global Mail page: account/folder nav, thread list, content preview, composer
  - mail preview preserves readable paragraphs, headers, quoted reply blocks, and mailbox/account metadata where possible
- Web research reliability:
  - shared web search path is used across Path A, Path B, and Telegram orchestration
  - fallback chain: DDG API -> DDG HTML -> Bing HTML parser
  - empty search result sets are surfaced as valid no-result responses (not transport failures)
  - explicit web/search intent now requires at least one web-evidence step in agent mode
  - deterministic recovery executes `web_search` (+ top-result `fetch_webpage`) when a model skips tool calls
  - citation gate validates deliverable artifact content (when present), avoiding false failures on concise chat acks
- Skills runtime:
  - save/run local or global skills
  - skills are reference-linkable and manageable from the `skills` tab
- Memory replay mode:
  - periodic + semantic checkpoints
  - lane filters, replay controls, checkpoint diff, and fork-from-checkpoint flow
- Private history page:
  - search and inspect visited URLs
  - semantic map clustering + embedded preview
- Provider/runtime support:
  - providers: `openai`, `cerebras`, `google`, `anthropic`, `lmstudio`
  - per-provider key profiles (multiple keys + primary selection)
  - model fetch per provider/key
  - LM Studio base URL, default model, optional token
  - unified image analysis tool path:
    - tries active provider native image understanding first (when available for selected model)
    - falls back automatically to LM Studio image analysis on native failure/unavailability
    - supports `image_url`, `local_path`, and mounted-file `context_file_id`
  - local OCR for mounted images:
    - `ocr_context_images` runs bundled Python OCR over image context files without using model vision
    - returns raw OCR text plus filename-like candidates for batch extraction workflows
- Local-file abstraction routing:
  - when abstraction is enabled and a non-LM Studio provider is selected, local mounted files are abstracted before remote use
  - image/doc/pdf/binary context files can be analyzed through LM Studio during abstraction copy generation
  - `read_context_file` auto-attempts LM Studio vision summaries for image files with metadata fallback on failure
- Local evidence RAG:
  - hybrid local evidence search uses BM25 + semantic vectors
  - persistent SQLite index per reference (`semantic_references/<ref>/rag/index.sqlite`)
  - primary embeddings via LM Studio (`/v1/embeddings`), with automatic local hash-embedding fallback
  - Settings controls: `rag_enabled`, `rag_embedding_model`, `rag_top_k`, index status, and manual reindex for active workspace
- Key/secret storage:
  - provider keys and secure refs are implemented via OS keychain on macOS
  - mailbox passwords are stored in the OS keychain; they are not written to the reference store
- Data-at-rest protection:
  - core reference/metadata stores are encrypted at rest (AES-256-GCM envelope)
  - plaintext legacy stores are migrated on read to encrypted format
  - Settings includes app-data `Lock/Unlock` controls with system auth:
    - Touch ID when available
    - macOS account password fallback
- Telegram + orchestrator controls:
  - bot token management and test ping
  - user registration tracking
  - job controls (`/jobs`, `/job_create`, `/job_edit`, `/job_pause`, `/job_resume`, `/job_delete`)
- Hyperweb and sharing:
  - public feed posting, semantic reference search/import, and snapshot publishing
  - E2E chat over existing Hyperweb RTC data-channel (`DM`, `room`, delivery/read ack, basic p2p file transfer)
  - private reference sharing with TTC members and shared rooms
- Multi-device sync:
  - optional `Multi-Device Auto Sync` toggle in Settings (`off` by default)
  - requires Trust Commons identity (`trustcommons_identity_id`); without identity, sync cannot be enabled
  - when enabled, local sync bridge is auto-managed by runtime settings
  - note: Hyperweb peer identity is currently device-local; two devices may appear as two peer fingerprints
- First-run onboarding:
  - default search engine selection (`google`, `bing`, `ddg`)
  - Chrome/Safari import
  - default browser setup helper
  - provider key save + model fetch
- Python runtime policy:
  - packaged builds use immutable runtime policy (`pip install` disabled at runtime)
  - if bundled Python is missing in Windows packaging, app falls back to system Python for tool execution where available
  - `requests` is available via bundled dependency and sandbox fallback shim
  - bundled OCR dependencies include `Pillow` and `rapidocr-onnxruntime`
  - pygame compatibility is gated: pygame runtime hooks are enabled only when user code imports/uses `pygame`

## Run

```bash
npm install
npm start
```

## Build installers

Local packaging commands:

```bash
npm run build:release
npm run build:mac
npm run build:win
```

- macOS output: `dist/Subgrapher-<version>-mac.dmg` (unsigned, arm64)
- Windows output: `dist/Subgrapher-<version>-setup.exe` (NSIS installer, x64)

## GitHub release pipeline

- Workflow file: `.github/workflows/release.yml`
- Triggered on tags matching `v*` and manual dispatch.
- Builds DMG on `macos-latest` and NSIS EXE on `windows-latest`.
- Uploads artifacts to GitHub Release when run from a tag.

Windows signing is optional in CI:

- `WIN_CSC_LINK`: certificate path or base64-encoded certificate payload
- `WIN_CSC_KEY_PASSWORD`: certificate password
- `WIN_CSC_TIMESTAMP_SERVER` (optional): timestamp server URL

If signing secrets are missing, CI still builds an unsigned `.exe`.

## Install and invite links

`subgrapher://...` invite links only work after Subgrapher is installed and protocol-registered.

For new users:
1. Share the GitHub Release URL first (`.dmg` / `.exe` download).
2. User installs and launches Subgrapher.
3. Then user opens the invite link.

## Important notes

- This is a clean standalone project, independent of `ttc_webapp` SaaS runtime.
- Context files imported from mounted folders are read-only by default.
- Mail sync is IMAP-based and local to Subgrapher's own database; Apple Mail folder scanning and AppleScript control are not part of the active mail path.
- Multi-device sync is opt-in and identity-gated; users without Trust Commons identity remain local-only.
- Use chat commands for direct workspace mutations:
  - `/artifact title: content`
  - `/viz <title>` (creates an HTML artifact scaffold)
  - `/crawl <url>` / `/crawl status` / `/crawl stop`
  - `/diff artifact <artifactId> <text>`

## File map

- `main.js`: Electron main process + IPC + reference store.
- `preload.js`: secure renderer bridge.
- `browser_view_preload.js`: marker selection capture.
- `renderer/index.html`: 3-panel UI shell.
- `renderer/styles.css`: UI theme/layout.
- `renderer/app.js`: UI behavior and runtime patch application.
- `runtime/agent_runtime.js`: local agentic response engine.
- `runtime/lumino_path_a.js`: scoped Path A execution runtime.
- `runtime/lumino_path_b.js`: orchestrator Path B runtime.
- `runtime/hyperweb_manager.js`: Hyperweb signaling + peer exchange manager.
- `runtime/trustcommons_identity.js`: Trust Commons identity bootstrap helper.
- `runtime/file_indexer.js`: folder ingestion/indexing.
- `runtime/lumino_crawler.js`: crawl and ingestion pipeline.
- `runtime/keychain.js`: macOS keychain provider key storage.
- `runtime/mail_imap.js`: read-only IMAP client used for mailbox sync.
- `runtime/mail_store.js`: local mail database and thread search/export layer.
- `runtime/mail_parser.js`: raw email parsing and body normalization.

## License

This project is licensed under the GNU Affero General Public License, version 3 or later (`AGPL-3.0-or-later`).

- Full license text: `LICENSE`
- If you distribute modified versions (including networked deployments), you must provide corresponding source under AGPL terms.
