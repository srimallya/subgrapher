# Subgrapher App Spec (Unified Artifact Runtime)

## Summary
Subgrapher uses one artifact runtime for authored outputs:
- `markdown` artifacts for text/image docs
- `html` artifacts for interactive visualizations and games

Legacy pygame/viz tabs are removed from active runtime behavior.
Image analysis is unified: active provider native vision is attempted first, then LM Studio fallback is used when needed.

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

## Local Context Ingestion + Abstraction
- Folder mounts are read-only indexed context with recursive traversal and extension filtering.
- Indexed types include text/code plus binary document formats:
  - `.pdf`, `.doc/.docx`, `.xls/.xlsx`, `.ppt/.pptx`, `.rtf`
  - `.odt/.ods/.odp`, `.msg`, `.eml`
- Default ingest caps:
  - up to 500 files
  - up to 4MB per file
- With abstraction routing enabled for non-local providers, local files are summarized into an abstraction copy.
- Non-text local files (image/doc/pdf/binary) may be analyzed via LM Studio during abstraction construction.

## IPC / Renderer Contract
Removed from renderer preload usage:
- `browser:vizStart`
- `browser:vizStop`
- `browser:vizInput`
- `browser:vizFrame`

`browser:srOpenVizTab` remains deprecated guidance only (no new viz tab creation).

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
