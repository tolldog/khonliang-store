# khonliang-store

Bus-native store agent. Eventual owner of the artifact backend and
host of a browser-based viewer mode. The phased-landing convention
is preserved — **don't bundle Phase 2 (artifact read skills) into a
Phase 4 (write ownership) PR or vice versa**. Each phase is its own
FR with its own PR.

## Status

Phase 2 reads (`fr_store_08c1c6b2`): the store agent now owns the
artifact read surface — `artifact_list / metadata / get / head /
tail / grep / excerpt`. All seven route through an
`ArtifactBackend` abstraction; today the shipped backend is
`BusBackedArtifactStore`, an HTTP client against the bus's REST
routes (where data still lives). Phase 4 swaps in a local SQLite
backend without changing the skill surface or the viewer's
fetch path.

Phase 3 viewer (`fr_store_d22556bb`): `display(artifacts,
layout='tabs')` lazily starts an in-process HTTP viewer,
pre-fetches artifacts via the same `ArtifactBackend` (in-process
call, no bus round-trip), and returns a browser URL. Renderers
are extensible via `@register_renderer("type/x")`.

Phase 4 (write ownership + bus surface deprecation) remains open.

## Stack

- Python, async throughout
- SQLite-backed store (planned — not yet in scope)
- Native khonliang-bus agent via `khonliang-bus-lib`

## Ecosystem position

```
INFRASTRUCTURE (services)
├─ khonliang-scheduler  — LLM inference scheduling
└─ khonliang-bus        — agent bus service, service registry,
                          artifacts today (store agent takes over
                          artifacts in a future phase), MCP adapter

LIBRARIES (Python)
├─ khonliang            — agent primitives, stores, MCP transport
├─ khonliang-bus-lib    — agent base/client for bus registration
└─ researcher-lib       — evaluation primitives

AGENTS/APPS
├─ researcher  — ingest world: papers, OSS, RSS → corpus
├─ developer   — dev lifecycle: FRs, specs, work units, git/PRs
├─ reviewer    — code review across models and vendors
└─ store       — artifact backend + viewer  ← THIS REPO
```

## Architecture boundary

- **khonliang-bus-lib** = library. Agent primitives. Don't reimplement.
- **khonliang-bus** = service. Store registers with it via bus-lib.
  Store does **not** run the bus itself.
- **khonliang-developer / khonliang-reviewer / khonliang-researcher**
  = sibling agents. No direct imports across these repos — any
  cross-agent interaction goes through the bus like every other
  agent-to-agent call.

When in doubt: if it's about *storing, reading, rendering, or
displaying an artifact*, it belongs here eventually. Today, nothing
belongs here yet.

## Phase roadmap

Each phase is its own FR. Do not stack them into a single PR — the
smaller-PR convention is what kept the scaffold separate from the
viewer skill.

1. **Phase 1** ✅ shipped — scaffold, health_check, tests, CLI
   (`fr_store_4ea7d48b`).
2. **Phase 2** ✅ shipped — artifact read skills (get, list,
   metadata, head, tail, grep, excerpt) (`fr_store_08c1c6b2`).
   Proxy to the bus artifact backend via
   `BusBackedArtifactStore`; the `ArtifactBackend` ABC is the
   swap point for Phase 4.
3. **Phase 3** ✅ shipped — viewer mode (`fr_store_d22556bb`).
   Browser URL for tabbed rendering. Graphviz, markdown, JSON
   tree, code highlighting; renderer registry extensible via
   `@register_renderer`.
4. **Phase 4** — artifact write skills + ownership migration. The
   store agent becomes the write path with a local SQLite
   backend; the bus artifact surface becomes a read-proxy or is
   removed. _Open._
5. **Phase 5** — cross-reference from fr_researcher_000ad07c
   (`stage_payload` / `ingest_from_artifact`) once store owns the
   write path. _Open._

## Running

Preferred bus-native agent:

```bash
.venv/bin/python -m store.agent --id store-primary --bus http://localhost:8788 --config /abs/path/config.yaml
```

For dogfooding, start and restart store through khonliang-bus
lifecycle tools when the bus is running. Config paths must be
absolute for cross-session launches.

## MCP tool response convention

Same as researcher / developer / reviewer: token-efficient, no
preamble, data-only, default to brief.

## Claude's role

Pure code + code review. When this repo grows real functionality,
each addition goes through a PR with Copilot review; Claude-authored
commits get a cross-vendor review before merge (per the user-level
convention in `~/.claude/CLAUDE.md`).
