# khonliang-store

Bus-native store agent. Eventual owner of the artifact backend and
host of a browser-based viewer mode. **Phase 1 is scaffold-only** —
registered-but-empty shell. Don't add artifact or viewer functionality
to this repo without a specific FR scoping the addition; the
phased-landing convention is the reason the scaffold PR exists.

## Status

Alive as a bus agent. Inherits `health_check` from `BaseAgent`; has
no skills of its own yet. See `fr_store_4ea7d48b` (Phase 1 scaffold)
and `fr_store_d22556bb` (viewer mode follow-up).

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

## Phase roadmap (not yet in scope)

Each phase is its own FR. Do not stack them into a single PR — the
smaller-PR convention is the reason the scaffold is empty.

1. **Phase 1 (this PR)** — scaffold, health_check, tests, CLI.
2. **Phase 2** — artifact read skills (get, list, metadata, head,
   tail, grep, excerpt). Proxy to the bus artifact backend initially.
3. **Phase 3** — viewer mode (`fr_store_d22556bb`). Browser URL for
   tabbed/split rendering. Graphviz, markdown, JSON tree, code
   highlighting.
4. **Phase 4** — artifact write skills + ownership migration. The
   store agent becomes the write path; the bus artifact surface
   becomes a read-proxy or is removed.
5. **Phase 5** — cross-reference from fr_researcher_000ad07c
   (`stage_payload` / `ingest_from_artifact`) once store owns the
   write path.

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
