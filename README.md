# khonliang-store

Bus-native store agent. Target eventual owner of the artifact backend
(currently served by the bus's `bus_artifact_*` MCP surface) and host
of a browser-based viewer mode for rendered artifacts.

## Status — Phase 2 reads + Phase 3 viewer landed; write-ownership migration follows

Registered bus agent. Skill surface today:

- **Read API** — `artifact_list / metadata / get / head / tail /
  grep / excerpt`. All seven route through an `ArtifactBackend`
  abstraction. The shipped backend is `BusBackedArtifactStore`,
  an HTTP client against the bus's REST routes (where data still
  lives). Phase 4 swaps in a local SQLite backend without
  changing the skill surface.
- **`display(artifacts, layout='tabs')`** — lazy in-process HTTP
  viewer that pre-fetches via the same `ArtifactBackend`
  (in-process call, no bus round-trip), and returns a browser
  URL. Renderers cover markdown (marked.js CDN), JSON
  (click-to-collapse), graphviz (local `dot -Tsvg` rendered via
  base64-data `<img>`), prism.js code highlighting, and a `<pre>`
  fallback. Extensible via `@register_renderer("type/x")`.

Tracked under `fr_store_08c1c6b2` (reads) and `fr_store_d22556bb`
(viewer). Phase-1 scaffold under `fr_store_4ea7d48b` is complete.

## Phase status

- **Phase 1 — scaffold** ✅ shipped (PR #1).
- **Phase 2 — artifact read skills** ✅ shipped. `ArtifactBackend`
  ABC + `BusBackedArtifactStore` HTTP backend; viewer fetch path
  goes through the same backend in-process.
- **Phase 3 — viewer mode** ✅ shipped (PR #2).
- **Phase 4 — artifact write skills + migration**
  (`stage_payload`, `replace`, `delete`) — _open_. Local SQLite
  backend replaces `BusBackedArtifactStore`; bus's artifact REST
  surface becomes a read-proxy or is removed.
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
