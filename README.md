# khonliang-store

Bus-native store agent. Target eventual owner of the artifact backend
(currently served by the bus's `bus_artifact_*` MCP surface) and host
of a browser-based viewer mode for rendered artifacts.

## Status — Phase 4a writes + Phase 2 reads + Phase 3 viewer landed; bus deprecation follows

Registered bus agent. Skill surface today:

- **Read API** — `artifact_list`, `artifact_metadata`,
  `artifact_get`, `artifact_head`, `artifact_tail`,
  `artifact_grep`, `artifact_excerpt`. All seven route through
  an `ArtifactBackend` abstraction.
- **Write API** — `artifact_create(kind, title, content, ...)`
  — persists a new artifact via the configured backend. Returns
  the new artifact's metadata (id, sha256, size_bytes, …) on
  success or `{error: ...}` on validation failure.
- **`display(artifacts, layout='tabs')`** — lazy in-process HTTP
  viewer that pre-fetches via the same `ArtifactBackend`
  (in-process call, no bus round-trip), and returns a browser
  URL. Renderers cover markdown (marked.js CDN), JSON
  (click-to-collapse), graphviz (local `dot -Tsvg` rendered via
  base64-data `<img>`), prism.js code highlighting, and a `<pre>`
  fallback. Extensible via `@register_renderer("type/x")`.

Backends are config-driven via `[artifacts] backend: bus | local`
(see `config.example.yaml`). The default `bus` keeps Phase-2
behavior — proxy reads to the bus's REST surface, reject writes
with a clear error envelope. Switch to `local` to use the new
SQLite-backed `LocalArtifactStore`; writes persist locally and
all reads come from the local DB.

Tracked under `fr_store_73e5a6f4` (writes), `fr_store_08c1c6b2`
(reads), `fr_store_d22556bb` (viewer). Phase-1 scaffold under
`fr_store_4ea7d48b` is complete.

## Phase status

- **Phase 1 — scaffold** ✅ shipped (PR #1).
- **Phase 2 — artifact read skills** ✅ shipped (PR #3).
  `ArtifactBackend` ABC + `BusBackedArtifactStore` HTTP backend;
  viewer fetch path goes through the same backend in-process.
- **Phase 3 — viewer mode** ✅ shipped (PR #2).
- **Phase 4a — local write surface** ✅ shipped.
  `LocalArtifactStore` (SQLite) + `artifact_create`; backend
  selection via config; default unchanged.
- **Phase 4b** — `CompositeArtifactBackend(local, bus)` for
  union reads + bus → local migration tooling. _Open._
- **Phase 4c** — deprecate bus's `bus_artifact_*` HTTP routes
  once 4b's migration has run. _Open._
- **Phase 5 — researcher integration** — _open_. Wires
  `fr_researcher_000ad07c`'s `stage_payload` / `ingest_from_artifact`
  through the store once writes land.

## Architecture boundary

- **khonliang-bus-lib** — agent SDK. `BaseAgent` + `@handler` +
  `AgentTestHarness` come from here.
- **khonliang-bus** — the bus service. Store registers as a
  participant; it does not run the bus itself.
- **khonliang-developer** / **khonliang-reviewer** — sibling agents.
  They consume artifacts today; once store owns the backend, they'll
  route through the store agent's skills instead of
  `bus_artifact_*`.

Store intentionally has **no** dependencies on khonliang-developer or
khonliang-reviewer. If a cross-agent interaction is needed, it goes
through the bus like every other agent-to-agent call.

## Running

```bash
# Install into the bus
python -m store.agent install --id store-primary --bus http://localhost:8788 --config config.yaml

# Start (normally done by the bus on boot)
python -m store.agent --id store-primary --bus http://localhost:8788 --config config.yaml

# Uninstall
python -m store.agent uninstall --id store-primary --bus http://localhost:8788 --config config.yaml
```

Copy `config.example.yaml` to `config.yaml` and edit for your local
bus URL. The config file is git-ignored.

## Development

```bash
pip install -e ".[test]"
pytest -q
python -m compileall store
```

Per the khonliang convention:
- Each change goes through a PR with Copilot review.
- Local `config.yaml` + `data/` + `logs/` are git-ignored.
- Commit messages reference the FR id where applicable.
