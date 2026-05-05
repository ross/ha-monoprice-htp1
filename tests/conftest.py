"""Shared test fixtures and fake aiohttp infrastructure."""
import asyncio
import sys
from json import dumps
from pathlib import Path

# aiohtp1.py has no HA dependencies; import it directly to avoid the HA
# imports in monoprice_htp1/__init__.py which aren't available in tests.
sys.path.insert(0, str(Path(__file__).parent.parent / "monoprice_htp1"))

import aiohttp
import pytest
from aiohttp import WSMessage, WSMsgType

from aiohtp1 import Htp1  # noqa: E402


class FakeWebSocket:
    """Scriptable WebSocket replacement. Tests push inbound messages; outbound sends are captured."""

    def __init__(self):
        self._inbox: asyncio.Queue = asyncio.Queue()
        self.sent: list[str] = []
        self.closed: bool = False
        self.send_exception: Exception | None = None

    async def receive(self) -> WSMessage:
        return await self._inbox.get()

    async def send_str(self, s: str) -> None:
        if self.send_exception is not None:
            raise self.send_exception
        self.sent.append(s)

    async def close(self) -> None:
        self.closed = True
        self._inbox.put_nowait(WSMessage(WSMsgType.CLOSED, None, ""))

    def push_text(self, data: str) -> None:
        self._inbox.put_nowait(WSMessage(WSMsgType.TEXT, data, ""))

    def push_mso(self, payload: dict) -> None:
        self.push_text(f"mso {dumps(payload)}")

    def push_msoupdate(self, ops) -> None:
        self.push_text(f"msoupdate {dumps(ops)}")

    def push_close(self) -> None:
        self._inbox.put_nowait(WSMessage(WSMsgType.CLOSE, b"", ""))

    def push_closed(self) -> None:
        self._inbox.put_nowait(WSMessage(WSMsgType.CLOSED, None, ""))

    def push_closing(self) -> None:
        self._inbox.put_nowait(WSMessage(WSMsgType.CLOSING, None, ""))

    def push_error(self) -> None:
        self._inbox.put_nowait(WSMessage(WSMsgType.ERROR, None, ""))


class FakeSession:
    """Scriptable aiohttp.ClientSession replacement. Queue ws instances or exceptions to serve."""

    def __init__(self):
        self._queue: asyncio.Queue = asyncio.Queue()
        self.connect_count: int = 0
        self.last_url: str | None = None

    def queue_ws(self, ws: FakeWebSocket) -> None:
        self._queue.put_nowait(ws)

    def queue_failure(self, exc: Exception) -> None:
        self._queue.put_nowait(exc)

    async def ws_connect(self, url: str, **kwargs) -> FakeWebSocket:
        self.connect_count += 1
        self.last_url = url
        item = await self._queue.get()
        if isinstance(item, BaseException):
            raise item
        return item


@pytest.fixture
def fake_ws() -> FakeWebSocket:
    return FakeWebSocket()


@pytest.fixture
def fake_session() -> FakeSession:
    return FakeSession()


@pytest.fixture
def mso_payload() -> dict:
    return {
        "powerIsOn": True,
        "muted": False,
        "volume": -25.0,
        "input": "0",
        "inputs": {
            "0": {"label": "HDMI 1", "visible": True},
            "1": {"label": "HDMI 2", "visible": False},
            "2": {"label": "HDMI 3", "visible": True},
        },
        "upmix": {
            "select": "off",
            "off": {"homevis": True},
            "auto": {"homevis": True},
            "dts": {"homevis": False},
        },
        "cal": {"vph": 0.0, "vpl": -90.0},
        "versions": {"SerialNumber": "SN123456"},
    }


@pytest.fixture
def htp1(fake_session: FakeSession) -> Htp1:
    client = Htp1("dev.local", fake_session)
    client.RECONNECT_DELAY_INITIAL = 0
    client.RECONNECT_DELAY_MAX = 0
    client.MSO_WAIT_TIMEOUT = 0.1
    return client


@pytest.fixture
async def connected_htp1(
    htp1: Htp1, fake_ws: FakeWebSocket, mso_payload: dict
) -> tuple[Htp1, FakeWebSocket]:
    """A connected Htp1 plus its live FakeWebSocket for pushing further messages."""
    fake_ws.push_mso(mso_payload)
    htp1.session.queue_ws(fake_ws)
    await htp1.connect()
    yield htp1, fake_ws
    await htp1.stop()


async def drain(n: int = 3) -> None:
    """Yield to the event loop n times to let background receive tasks process messages."""
    for _ in range(n):
        await asyncio.sleep(0)


async def wait_until(condition, timeout: float = 1.0) -> None:
    """Poll condition() until truthy or timeout."""
    async with asyncio.timeout(timeout):
        while not condition():
            await asyncio.sleep(0.01)
