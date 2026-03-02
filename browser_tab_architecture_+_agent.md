# Browser Tab Architecture (Lumino Runtime) - Rebuild Reference

## Onboarding: Metaphor Mapping Table

| Platform Concept | Product Metaphor | What It Means in Practice |
| --- | --- | --- |
| Semantic reference (`sr_*`) | App | One self-contained workspace unit with its own tabs, files, and runtime history. |
| `tabs[]` + `artifacts[]` + `context_files[]` + `youtube_transcripts{}` | App data/filesystem | The app's working files and surfaces (web, editable docs, imported context, transcript assets). |
| `reference_graph` + `agent_weights` + `decision_trace` + `highlights` + `chat_thread` | App memory state | The persistent memory backing behavior, reasoning context, and continuity. |
| Lumino harness agent | App runtime | The active execution loop that reads memory state, calls tools, and emits state mutations. |
| `pending_*` response payloads (`pending_artifacts`, `pending_diff_ops`, etc.) | Runtime state patches | Structured deltas applied by renderer/main process to mutate app memory safely. |
| Lumino scope (`active + parent + direct children`) | Runtime sandbox boundary | Hard execution boundary limiting which apps are visible/mutable in a given run. |

This document is updated to reflect the current implementation and the correct system metaphor:

- **Reference = App**
- **All tab/file types + context graph = App memory state**
- **Lumino agent = App runtime**

The goal is rebuild parity: a new browser that behaves the same way.

## 1) Interface

The browser workspace is a desktop Electron feature with 3 panels in `templates/index.html`.

- Left panel: **My References (App List)**
  - Tree of reference nodes (parent/child lineage).
  - Search, rename, fork, delete-with-succession, pin top-level root.
  - In this metaphor, each node is an **app instance**.

- Center panel: **Unified Data Browser**
  - Native `BrowserView` in `#browser-view-container` (not iframe).
  - URL bar, reload, marker mode, commit/new-root/fork-child actions.
  - Unified workspace tabs:
    - `web` tab = live page surface
    - `artifact` tab = editable file surface (markdown/text)
    - `pygame` tab = runtime visualization surface
  - Add/close/switch behavior is synchronized between semantic tabs and actual Electron tabs.

- Right panel: **Chat Interface (Lumino Runtime Console)**
  - Reference-scoped thread.
  - Context file import (`.txt`, `.md`).
  - Clear chat + auto-fork.
  - Diff review queue (apply/reject pending selective diffs).

### Native BrowserView behavior

- Main process owns browser runtime tabs (`browserTabs` map, max 10).
- Renderer computes bounds and pushes `browser:updateBounds`.
- Zoom factor is applied when mapping CSS pixels to native bounds.
- BrowserView is hidden for modal overlays and for non-web active surfaces (artifact/pygame).
- Audio policy enforces active-tab-only playback.

## 2) Reference System (App Model)

### Core model

A semantic reference (`sr_*`) is treated as an **app container**.

- App identity: `id`, `title`, `intent`, `tags`
- App topology: `parent_id`, `children`, `lineage`, `relation_type`
- App files/tabs:
  - `tabs[]` (`web` / `pygame`)
  - `artifacts[]` (markdown/text/external context descriptors)
  - `context_files[]`
  - `youtube_transcripts{}`
- App memory state:
  - `reference_graph` (nodes/edges)
  - `agent_weights`
  - `decision_trace`
  - `highlights` (web + artifact marks)
  - `chat_thread`
- App lifecycle metadata: `created_at`, `updated_at`, `last_used_at`, `pinned_root`

### Persistence

- Primary persistence is Electron store key:
  - `embeddedBrowser.semanticReferences`
- Legacy key migration:
  - `embeddedBrowser.references` -> migrated root app
- Imported context files are persisted in:
  - `app.getPath('userData')/semantic_references/<srId>/context_files`

### App lifecycle operations

- `browser:srSaveInActive`:
  - append to active app, or create child app, or create root app based on similarity/classification.
- `browser:srFork` / `browser:srAddChild`:
  - clone app state into child lineage.
- `browser:srClearChatAndAutoFork`:
  - forks app and resets runtime chat thread.
- `browser:srDeleteWithSuccession`:
  - promotes successor child and cleans runtime tabs/sessions.

### Search/classification behavior

- `browser:srSearch` is hybrid (keyword + semantic API when available).
- `/api/sr/similarity` resolves semantic match:
  - preferred: `all-MiniLM-L6-v2`
  - fallback: token similarity

### Marker system as app memory write

- Web selection events from `browser_view_preload.js` emit `browser:marker:web-selection`.
- Main process toggles and stores highlights under active app memory state.
- Artifact editor selection writes via `browser:srToggleArtifactHighlight`.

## 3) Unified Data Browser and Chat Interface (Lumino Runtime)

This section defines the runtime contract between UI state, app memory state, and Lumino runtime.

### Lumino scope rule (hard boundary)

Lumino scope is restricted to:

- active app (reference)
- direct parent app
- direct child apps

This scope is enforced in both frontend payload building and backend filtering.

### Runtime request/response loop

1. Browser panel chat emits `lumino:chat` with:
   - `source: "browser_panel"`
   - `sr_id`
   - `sr_artifacts`
   - `sr_context_files`
   - `sr_all_refs` (Lumino-scoped refs with app memory summaries)
2. Backend resolves pipeline using `resolve_lumino_pipeline(...)` -> `agentic_harness`.
3. Backend builds `SRContext` (tool callbacks + allowed reference IDs).
4. Lumino runs `process_interaction_agentic_harness(...)`.
5. Response is emitted as `lumino:response` with runtime deltas:
   - `pending_artifacts`
   - `pending_weight_updates`
   - `pending_decision_traces`
   - `pending_workspace_tabs`
   - `pending_diff_ops`
6. Renderer applies these deltas into app memory state using Electron IPC handlers.

### Agent-as-runtime semantics

Lumino tools are runtime instructions against app memory state:

- Create/update files: notes, todos, markdown artifacts
- Read files/context: context file access, reference reads
- Graph memory updates: weights, decision traces, graph nodes/edges
- Runtime surface control: open pygame tab, patch pygame viz request
- Controlled mutation: `apply_selective_diff`

### Selective diff system

Supported mutation targets:

- `artifact`
- `context_file`
- `pygame_viz_request`

Flow:

- Runtime queues diff in `pending_diff_ops`.
- Renderer safety-checks auto-apply.
- Safe operations auto-apply through `browser:srApplyDiffOp`.
- Unsafe/failed operations are queued in Diff Review panel for manual apply/reject.

### Runtime tab outputs

- `pending_workspace_tabs` can open:
  - artifact tabs
  - pygame tabs
- Pygame runtime is managed in main process and streams:
  - `browser:python-frame`
  - `browser:python-status`

## Rebuild parity checklist

1. Keep 3-panel structure: app list, unified data browser, runtime chat.
2. Keep native host-managed browser surface (`BrowserView`-style embedding).
3. Preserve reference-as-app schema and lifecycle semantics.
4. Preserve app memory state fields (`reference_graph`, `decision_trace`, `agent_weights`, highlights, chat thread).
5. Preserve Lumino scope boundary (active + parent + children only).
6. Preserve runtime delta contract (`pending_*` payloads) and renderer-side patch application.
7. Preserve selective diff safety model (auto-apply + review queue fallback).
8. Preserve unified tabs across web/artifact/pygame.

## Source map

- Interface and renderer logic: `templates/index.html`
- Renderer IPC bridge: `electron/preload.js`
- Main-process browser/reference runtime: `electron/main.js`
- Browser selection capture preload: `electron/browser_view_preload.js`
- Lumino routing and socket payload contract: `app.py`, `util/lumino_mode.py`
- Lumino runtime/tool loop: `lumino_agent.py`
