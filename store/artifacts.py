"""Artifact backend abstraction.

The store agent's read skills (``artifact_get`` and friends) all
route through an :class:`ArtifactBackend`. Phase 2 ships
:class:`BusBackedArtifactStore` — an HTTP client that talks to the
bus's existing REST surface (where artifact data lives today).
Phase 4 will add a local SQLite-backed implementation that owns
the data; the skill surface and the viewer's fetch path don't
change when the swap happens.

Why HTTPX-to-bus-REST and not a websocket-routed
``request(agent_type='bus')`` call: the bus does not register
itself as an agent in the dispatch table today — that's separate
architectural debt Phase 4 inherits. The MCP adapter has
talked HTTP to ``/v1/artifacts/...`` since the bus shipped, so
HTTP is the established transport for the bus-as-data-source
relationship; preserving that shape keeps Phase 2 self-contained.
"""

from __future__ import annotations

import abc
from typing import Any, Optional

import httpx


class ArtifactBackend(abc.ABC):
    """Source-of-truth for artifact reads.

    All methods are async so the store agent's skill handlers can
    await them on the agent event loop without blocking. The
    return shape mirrors what the bus emits today; consumers can
    target either backend without a translation layer.
    """

    @abc.abstractmethod
    async def list(
        self,
        *,
        session_id: str = "",
        kind: str = "",
        producer: str = "",
        limit: int = 20,
    ) -> list[dict[str, Any]]: ...

    @abc.abstractmethod
    async def metadata(self, artifact_id: str) -> dict[str, Any]: ...

    @abc.abstractmethod
    async def get(
        self,
        artifact_id: str,
        *,
        offset: int = 0,
        max_chars: int = 4000,
    ) -> dict[str, Any]: ...

    @abc.abstractmethod
    async def head(
        self,
        artifact_id: str,
        *,
        lines: int = 80,
        max_chars: int = 4000,
    ) -> dict[str, Any]: ...

    @abc.abstractmethod
    async def tail(
        self,
        artifact_id: str,
        *,
        lines: int = 80,
        max_chars: int = 4000,
    ) -> dict[str, Any]: ...

    @abc.abstractmethod
    async def grep(
        self,
        artifact_id: str,
        *,
        pattern: str,
        context_lines: int = 10,
        max_matches: int = 10,
        max_chars: int = 4000,
    ) -> dict[str, Any]: ...

    @abc.abstractmethod
    async def excerpt(
        self,
        artifact_id: str,
        *,
        start_line: int,
        end_line: int,
        max_chars: int = 4000,
    ) -> dict[str, Any]: ...


class BusBackedArtifactStore(ArtifactBackend):
    """Read artifacts from the bus's REST surface over HTTP.

    Each call hits ``GET /v1/artifacts[/...]`` on the bus URL the
    store agent was configured with. A single :class:`httpx.AsyncClient`
    is reused for connection pooling; the caller owns lifecycle via
    :meth:`close`.
    """

    # 30s is generous for a single-artifact read against a local
    # bus; tune downward if a remote bus is ever in scope.
    DEFAULT_TIMEOUT = 30.0

    def __init__(
        self,
        bus_url: str,
        *,
        client: Optional[httpx.AsyncClient] = None,
        timeout: float = DEFAULT_TIMEOUT,
    ) -> None:
        # Strip trailing slash so f"{bus_url}/v1/..." doesn't double up.
        self._bus_url = bus_url.rstrip("/")
        self._owns_client = client is None
        self._client = client or httpx.AsyncClient(timeout=timeout)

    async def close(self) -> None:
        if self._owns_client:
            await self._client.aclose()

    # -- ArtifactBackend ----------------------------------------------------

    async def list(
        self,
        *,
        session_id: str = "",
        kind: str = "",
        producer: str = "",
        limit: int = 20,
    ) -> list[dict[str, Any]]:
        params = {
            "session_id": session_id,
            "kind": kind,
            "producer": producer,
            "limit": limit,
        }
        result = await self._get_json("/v1/artifacts", params=params)
        return result if isinstance(result, list) else []

    async def metadata(self, artifact_id: str) -> dict[str, Any]:
        return await self._get_json(f"/v1/artifacts/{artifact_id}")

    async def get(
        self,
        artifact_id: str,
        *,
        offset: int = 0,
        max_chars: int = 4000,
    ) -> dict[str, Any]:
        return await self._get_json(
            f"/v1/artifacts/{artifact_id}/content",
            params={"offset": offset, "max_chars": max_chars},
        )

    async def head(
        self,
        artifact_id: str,
        *,
        lines: int = 80,
        max_chars: int = 4000,
    ) -> dict[str, Any]:
        return await self._get_json(
            f"/v1/artifacts/{artifact_id}/head",
            params={"lines": lines, "max_chars": max_chars},
        )

    async def tail(
        self,
        artifact_id: str,
        *,
        lines: int = 80,
        max_chars: int = 4000,
    ) -> dict[str, Any]:
        return await self._get_json(
            f"/v1/artifacts/{artifact_id}/tail",
            params={"lines": lines, "max_chars": max_chars},
        )

    async def grep(
        self,
        artifact_id: str,
        *,
        pattern: str,
        context_lines: int = 10,
        max_matches: int = 10,
        max_chars: int = 4000,
    ) -> dict[str, Any]:
        return await self._get_json(
            f"/v1/artifacts/{artifact_id}/grep",
            params={
                "pattern": pattern,
                "context_lines": context_lines,
                "max_matches": max_matches,
                "max_chars": max_chars,
            },
        )

    async def excerpt(
        self,
        artifact_id: str,
        *,
        start_line: int,
        end_line: int,
        max_chars: int = 4000,
    ) -> dict[str, Any]:
        return await self._get_json(
            f"/v1/artifacts/{artifact_id}/excerpt",
            params={
                "start_line": start_line,
                "end_line": end_line,
                "max_chars": max_chars,
            },
        )

    # -- internals ----------------------------------------------------------

    async def _get_json(
        self,
        path: str,
        *,
        params: Optional[dict[str, Any]] = None,
    ) -> Any:
        url = f"{self._bus_url}{path}"
        try:
            resp = await self._client.get(url, params=params or {})
        except httpx.HTTPError as exc:
            return {"error": f"bus unreachable: {type(exc).__name__}: {exc}"}
        if resp.status_code == 404:
            return {"error": "artifact not found"}
        if resp.status_code >= 400:
            return {"error": f"bus returned HTTP {resp.status_code}"}
        try:
            return resp.json()
        except ValueError:
            return {"error": "bus returned non-JSON body"}
