"""WebSocket connection manager for real-time dashboard broadcasting."""

import asyncio
import json
import logging

from fastapi import WebSocket

logger = logging.getLogger(__name__)


class ConnectionManager:
    """Tracks active WebSocket connections and broadcasts JSON events to all of them."""

    def __init__(self) -> None:
        self._connections: set[WebSocket] = set()

    async def connect(self, ws: WebSocket) -> None:
        await ws.accept()
        self._connections.add(ws)
        logger.debug("WebSocket client connected; total=%d", len(self._connections))

    def disconnect(self, ws: WebSocket) -> None:
        self._connections.discard(ws)
        logger.debug("WebSocket client disconnected; total=%d", len(self._connections))

    async def broadcast(self, message: dict) -> None:
        """Send *message* as JSON text to every connected client.

        Silently removes connections that have been closed or errored.
        """
        if not self._connections:
            return
        data = json.dumps(message)
        dead: set[WebSocket] = set()
        for ws in list(self._connections):
            try:
                await ws.send_text(data)
            except Exception:
                dead.add(ws)
        for ws in dead:
            self._connections.discard(ws)


# Module-level singleton shared by the app and all service modules.
_manager = ConnectionManager()


def get_manager() -> ConnectionManager:
    """Return the shared ConnectionManager instance."""
    return _manager


def broadcast_event(message: dict) -> None:
    """Schedule a broadcast from any context (sync or async).

    When called from within a running event loop (e.g. from an async service
    or from a sync function that itself runs inside an async coroutine), this
    schedules the broadcast as a background task so it does not block the caller.

    When called outside any event loop (e.g. during unit tests that don't use
    an async runner) the broadcast is silently skipped.
    """
    try:
        loop = asyncio.get_running_loop()
        loop.create_task(_manager.broadcast(message))
    except RuntimeError:
        pass  # No running event loop — skip (e.g. sync unit tests)
