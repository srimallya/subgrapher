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
  - `html` artifacts support `Code` / `Preview` plus explicit `Start` / `Stop` lifecycle
  - iframe sandbox: `allow-scripts allow-forms allow-pointer-lock allow-downloads` (no same-origin grant)
- Legacy visualization migration:
  - old `viz` tab payloads are auto-mapped into markdown artifacts on load
  - dedicated viz tabs are deprecated
- Workspace surfaces and tabs:
  - `web`, `artifact`, `files`, `skills`
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
  - import `.txt` / `.md` context files
  - web crawler commands: `/crawl <url>`, `/crawl status`, `/crawl stop`
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
- Key/secret storage:
  - provider keys and secure refs are implemented via OS keychain on macOS
- Telegram + orchestrator controls:
  - bot token management and test ping
  - user registration tracking
  - job controls (`/jobs`, `/job_create`, `/job_edit`, `/job_pause`, `/job_resume`, `/job_delete`)
- Hyperweb and sharing:
  - public feed posting, semantic reference search/import, and snapshot publishing
  - private reference sharing with TTC members and shared rooms
- First-run onboarding:
  - default search engine selection (`google`, `bing`, `ddg`)
  - Chrome/Safari import
  - default browser setup helper
  - provider key save + model fetch
- Python runtime policy:
  - packaged builds use immutable runtime policy (`pip install` disabled at runtime)
  - if bundled Python is missing in Windows packaging, app falls back to system Python for tool execution where available

## Run

```bash
cd /Users/srimallyamaitra/codes/subgrapher
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

- Workflow file: `/Users/srimallyamaitra/codes/subgrapher/.github/workflows/release.yml`
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
- Use chat commands for direct workspace mutations:
  - `/artifact title: content`
  - `/viz <title>` (creates an HTML artifact scaffold)
  - `/crawl <url>` / `/crawl status` / `/crawl stop`
  - `/diff artifact <artifactId> <text>`

## File map

- `/Users/srimallyamaitra/codes/subgrapher/main.js`: Electron main process + IPC + reference store.
- `/Users/srimallyamaitra/codes/subgrapher/preload.js`: secure renderer bridge.
- `/Users/srimallyamaitra/codes/subgrapher/browser_view_preload.js`: marker selection capture.
- `/Users/srimallyamaitra/codes/subgrapher/renderer/index.html`: 3-panel UI shell.
- `/Users/srimallyamaitra/codes/subgrapher/renderer/styles.css`: UI theme/layout.
- `/Users/srimallyamaitra/codes/subgrapher/renderer/app.js`: UI behavior and runtime patch application.
- `/Users/srimallyamaitra/codes/subgrapher/runtime/agent_runtime.js`: local agentic response engine.
- `/Users/srimallyamaitra/codes/subgrapher/runtime/lumino_path_a.js`: scoped Path A execution runtime.
- `/Users/srimallyamaitra/codes/subgrapher/runtime/lumino_path_b.js`: orchestrator Path B runtime.
- `/Users/srimallyamaitra/codes/subgrapher/runtime/hyperweb_manager.js`: Hyperweb signaling + peer exchange manager.
- `/Users/srimallyamaitra/codes/subgrapher/runtime/trustcommons_identity.js`: Trust Commons identity bootstrap helper.
- `/Users/srimallyamaitra/codes/subgrapher/runtime/file_indexer.js`: folder ingestion/indexing.
- `/Users/srimallyamaitra/codes/subgrapher/runtime/lumino_crawler.js`: crawl and ingestion pipeline.
- `/Users/srimallyamaitra/codes/subgrapher/runtime/keychain.js`: macOS keychain provider key storage.

## License

This project is licensed under the GNU Affero General Public License, version 3 or later (`AGPL-3.0-or-later`).

- Full license text: `/Users/srimallyamaitra/codes/subgrapher/LICENSE`
- If you distribute modified versions (including networked deployments), you must provide corresponding source under AGPL terms.
