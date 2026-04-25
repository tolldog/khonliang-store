# khonliang-store

Bus-native store agent. Target eventual owner of the artifact backend
(currently served by the bus's `bus_artifact_*` MCP surface) and host
of a browser-based viewer mode for rendered artifacts.

## Status — Phase 3 viewer landed; artifact-ownership migration follows

Registered bus agent with one real skill: `display(artifacts,
layout='tabs')`. Lazily starts an in-process HTTP viewer on first
call, pre-fetches the listed artifacts via the bus, and returns a
URL the caller can open in a browser. The viewer renders by
content_type — markdown via marked.js (CDN), JSON pretty-printed
with click-to-collapse, graphviz via local `dot -Tsvg`, common code
MIME types via prism.js (CDN), and a `<pre>` fallback for unknown
types. The renderer registry is the explicit extension hook: new
file types extend via `@register_renderer("type/x")`.

Tracked under `fr_store_d22556bb`. The Phase-1 scaffold under
`fr_store_4ea7d48b` is also complete.

## Phase status

- **Phase 1 — scaffold** ✅ shipped (PR #1).
- **Phase 2 — artifact read skills** (`get`, `list`, `metadata`,
  `head`, `tail`, `grep`, `excerpt`) — _open_. The viewer reads
  artifacts via the bus today; this phase moves ownership of reads
  into the store.
- **Phase 3 — viewer mode** ✅ shipped (PR #2).
- **Phase 4 — artifact write skills + migration**
  (`stage_payload`, `replace`, `delete`) — _open_. Once writes move,
  the bus artifact surface becomes a read-proxy or is removed.
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
