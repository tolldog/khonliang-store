# khonliang-store

Bus-native store agent. Target eventual owner of the artifact backend
(currently served by the bus's `bus_artifact_*` MCP surface) and host
of a browser-based viewer mode for rendered artifacts.

## Status — Phase 4b composite + migration landed; bus deprecation follows

Registered bus agent. Skill surface today:

- **Read API** — `artifact_list`, `artifact_metadata`,
  `artifact_get`, `artifact_head`, `artifact_tail`,
  `artifact_grep`, `artifact_excerpt`. All seven route through
  an `ArtifactBackend` abstraction.
- **Write API** — `artifact_create(kind, title, content, ...)`
  — persists a new artifact via the configured backend.
- **Migration** — `artifact_migrate_from_bus(limit, dry_run)`
  pages through the bus's REST list and copies each artifact
  into the local SQLite store, preserving ids. Idempotent;
  returns `{copied, skipped, errors, scanned}`.
- **`display(artifacts, layout='tabs')`** — lazy in-process HTTP
  viewer that pre-fetches via the same `ArtifactBackend` and
  returns a browser URL. Renderer registry extensible via
  `@register_renderer("type/x")`.

Backends are config-driven via `[artifacts] backend: bus |
local | composite` (see `config.example.yaml`). The default
`bus` keeps the Phase-2 read-only proxy behavior; `local`
switches to the SQLite write path with no bus fallback;
`composite` (Phase 4b, recommended for migrating deployments)
writes locally and reads check local-first with bus fallback.

Tracked under `fr_store_ef668d56` (composite + migration),
`fr_store_73e5a6f4` (writes), `fr_store_08c1c6b2` (reads),
`fr_store_d22556bb` (viewer). Phase-1 scaffold under
`fr_store_4ea7d48b` is complete.

## Phase status

- **Phase 1 — scaffold** ✅ shipped (PR #1).
- **Phase 2 — artifact read skills** ✅ shipped (PR #3).
- **Phase 3 — viewer mode** ✅ shipped (PR #2).
- **Phase 4a — local write surface** ✅ shipped (PR #4).
- **Phase 4b — composite backend + migration** ✅ shipped.
- **Phase 4c** — deprecate bus's `bus_artifact_*` HTTP routes
  once operators have run the migration. _Open._
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
