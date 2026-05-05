"""The aiohttp Monoprice HTP-1 client library."""

import asyncio
from collections.abc import Awaitable, Callable
from contextlib import suppress
from json import dumps, loads
from logging import getLogger
from typing import Any

import aiodns
import aiohttp


class AioHtp1Exception(Exception):
    """Base error for aiohtp1."""


class ConnectionException(AioHtp1Exception):
    """Error connecting to HTP-1."""


class Htp1:
    """Connect to and manage a Monoprice HTP-1."""

    RECONNECT_DELAY_INITIAL = 5
    RECONNECT_DELAY_MAX = 300
    MSO_WAIT_TIMEOUT = 5

    log = getLogger("aiohtp1")

    def __init__(
        self,
        host: str,
        session: aiohttp.ClientSession,
    ) -> None:
        """Initialize."""

        self.host: str = host
        self.session: aiohttp.ClientSession = session

        # socket
        self._websocket: aiohttp.ClientWebSocketResponse | None = None
        # tasks
        self._recveive_task: Awaitable[None] | None = None
        self._try_connect_task: Awaitable[None] | None = None
        # subscribers
        self._subscriptions: dict[str, list[Callable]] = {}
        # state
        self._state: dict[str, Any] | None = None
        self._state_ready: asyncio.Event = asyncio.Event()
        self._tx: dict[str, Any] | None = None
        self._trying_to_connect: bool = False

        self.reset()

    def reset(self):
        """Reset the Htp1 object's state."""
        self._state = None
        self._tx = None
        self._state_ready.clear()

    @property
    def connected(self):
        """Returns True if the Htp1 device is connected and data is ready."""
        return self._state_ready.is_set()

    async def connect(self):
        """Connect to the HTP-1 device and open the control websocket."""
        self.reset()

        url = f"ws://{self.host}/ws/controller"
        self.log.debug("connect: url=%s", url)

        try:
            self._websocket = await self.session.ws_connect(url)

            # we have a connection, start our receiving handler
            self._recveive_task = asyncio.create_task(self._recveive())

            # request the initial state
            self.log.debug("connect:   requesting mso")
            await self._websocket.send_str("getmso")

            # wait until we receive the initial state
            async with asyncio.timeout(self.MSO_WAIT_TIMEOUT):
                await self._state_ready.wait()
        except (
            TimeoutError,
            aiodns.error.DNSError,
            aiohttp.client_exceptions.ClientError,
            asyncio.CancelledError,
        ) as err:
            self.log.warning("connect: failed to connect and retrieve mso")
            await self._disconnect()
            raise ConnectionException from err

        self.log.debug("connect:   received mso, ready")
        await self._notify("#connection")

    async def _disconnect(self):
        self.log.debug("_disconnect:")
        if self._websocket is not None:
            # if we have an open connection, close it, this should exit and
            # clean up the _recveive_task as well
            await self._websocket.close()
        if self._recveive_task is not None:
            # if our receiving handler is running, stop it
            self._recveive_task.cancel()
            with suppress(asyncio.CancelledError):
                await self._recveive_task
            self._recveive_task = None
        # websocket is no longer valid
        self._websocket = None
        self.log.debug("_disconnect: done")

    async def try_connect(self):
        """Start the process of persistently trying to connect to the HTP-1 device."""
        if self._try_connect_task is not None and not self._try_connect_task.done():
            return
        self._try_connect_task = asyncio.create_task(self._try_connect())

    async def _try_connect(self):
        self.log.debug("_try_connect:")
        self._trying_to_connect = True
        sleep_time = self.RECONNECT_DELAY_INITIAL
        try:
            while self._trying_to_connect:
                try:
                    await self.connect()
                except ConnectionException:
                    self.log.debug("_try_connect:   failed")
                    await asyncio.sleep(sleep_time)
                    sleep_time *= 2
                    sleep_time = min(sleep_time, self.RECONNECT_DELAY_MAX)
                else:
                    self.log.debug("_try_connect:   connected")
                    return
        finally:
            self._try_connect_task = None
            self.log.debug("_try_connect:   exited loop")

    async def _stop_connect(self):
        self.log.debug("_stop_connect:")
        self._trying_to_connect = False
        if self._try_connect_task is not None:
            self._try_connect_task.cancel()
            with suppress(asyncio.CancelledError):
                await self._try_connect_task
        self.log.debug("_stop_connect: done")

    async def _recveive(self):
        self.log.debug("_recveive:")
        try:
            while True:
                msg = await self._websocket.receive()
                if msg.type in (
                    aiohttp.WSMsgType.CLOSE,
                    aiohttp.WSMsgType.CLOSING,
                    aiohttp.WSMsgType.CLOSED,
                    aiohttp.WSMsgType.ERROR,
                ):
                    await self.try_connect()
                    await self._notify("#connection")
                    break
                if msg.type != aiohttp.WSMsgType.TEXT:
                    # not interested
                    continue
                msg = msg.data
                self.log.debug("_recveive:   msg=%s", msg[:100])
                cmd, payload = msg.split(" ", 1)
                handler = getattr(self, f"_cmd_{cmd}", None)
                if handler:
                    # parse the (json) payload
                    payload = loads(payload)
                    try:
                        await handler(payload)
                    except Exception:
                        # don't exit if a handler has a problem, just log it
                        self.log.exception("_recveive: handler=%s, threw an exception")
        finally:
            # self._recveive_task = None
            self.log.debug("_recveive:   exited loop")

    async def stop(self):
        """Disconnect from the HTP-1 device and shut down any running background tasks."""
        self.log.debug("stop:")
        await self._stop_connect()
        await self._disconnect()
        self.reset()

    ## Handlers

    async def _cmd_mso(self, payload):
        self.log.debug("_cmd_mso: payload=***")
        self._state = payload
        self._state_ready.set()

    async def _cmd_msoupdate(self, payload):
        if not isinstance(payload, list):
            payload = [payload]
        self.log.debug("_cmd_msoupdate: len(payload)=%d", len(payload))
        for piece in payload:
            op = piece["op"]
            path = piece["path"][1:].split("/")
            d = self._state
            last = path.pop()
            if op in ("add", "replace"):
                # for now we're assuming adds are a mistake as we don't expect
                # new nodes to show up, same for deletes/removes.
                for node in path:
                    if isinstance(d, list):
                        node = int(node)
                    d = d[node]
            else:
                raise NotImplementedError

            value = piece["value"]
            self.log.debug(
                "_cmd_msoupdate:   op=%s, path=%s, value=%s", op, path, value
            )

            # make the change
            d[last] = value

            await self._notify(piece["path"], value)

    ## Subscriptions

    def subscribe(self, subject, callback):
        """Subscribe to notifications.

        - /config/path: be notified when changes occur to the specified path
        - #connection: be notified of changes to the specified topic
        """
        self.log.debug("subscribe: subject=%s, callback=%s", subject, subject)
        if subject not in self._subscriptions:
            self._subscriptions[subject] = []

        self._subscriptions[subject].append(callback)

    async def _notify(self, subject, value=None):
        self.log.debug("notify: subject=%s, value=%s", subject, value)
        subscribers = self._subscriptions.get(subject) or []
        self.log.debug("notify:   subscribers=%s", subscribers)
        for subscriber in subscribers:
            await subscriber(value)

    ## Async ContextManager

    async def __aenter__(self):
        """Start a transaction of grouped changes to the HTP-1's state."""
        if self._tx is not None:
            raise AioHtp1Exception("transaction already in progress")
        self._tx = {}
        return self

    async def __aexit__(self, exc_type=None, exc_val=None, exc_tb=None):
        """End a transaction, any uncommitted changes will be abandoned."""
        self._tx = None

    async def commit(self):
        """Commit any pending changes to the HTP-1 device."""
        self.log.debug("commit: _tx=%s", self._tx)
        if not self._tx:
            return False

        ops = [
            {
                "op": "replace",
                "path": k,
                "value": v,
            }
            for k, v in self._tx.items()
        ]
        payload = dumps(ops, separators=(",", ":"))
        try:
            await self._websocket.send_str(f"changemso {payload}")
        except aiohttp.ClientConnectionResetError:
            self.log.warning("commit: connection reset while sending")
            return False

        self._tx = {}
        return True

    ## Operations

    @property
    def serial_number(self):
        """Retrieve the HTP-1 device's serial number."""
        return self._state["versions"]["SerialNumber"]

    @property
    def cal_vph(self):
        """Retrieve the HTP-1 device's calibration max volume."""
        return self._state["cal"]["vph"]

    @property
    def cal_vpl(self):
        """Retrieve the HTP-1 device's calibration min volume."""
        return self._state["cal"]["vpl"]

    @property
    def muted(self):
        """Retrieve the HTP-1 device's muted value."""
        try:
            return self._tx["/muted"]
        except (TypeError, KeyError):
            pass
        return self._state["muted"]

    @muted.setter
    def muted(self, value):
        """Set the HTP-1 device's muted value."""
        if self._tx is None:
            raise AioHtp1Exception("no transaction in progress")
        self._tx["/muted"] = value

    @property
    def volume(self):
        """Retrieve the HTP-1 device's volume."""
        try:
            return self._tx["/volume"]
        except (TypeError, KeyError):
            pass
        return self._state["volume"]

    @volume.setter
    def volume(self, value):
        """Set the HTP-1 device's volume."""
        if self._tx is None:
            raise AioHtp1Exception("no transaction in progress")
        self._tx["/volume"] = value

    @property
    def power(self):
        """Retrieve the HTP-1 device's power state."""
        try:
            return self._tx["/powerIsOn"]
        except (TypeError, KeyError):
            pass
        try:
            return self._state["powerIsOn"]
        except (TypeError, KeyError):
            pass
        return None

    @power.setter
    def power(self, value):
        """Set the HTP-1 device's power state."""
        if self._tx is None:
            raise AioHtp1Exception("no transaction in progress")
        self._tx["/powerIsOn"] = value

    @property
    def input(self):
        """Retrieve the HTP-1 device's input."""
        try:
            _id = self._tx["/input"]
        except (TypeError, KeyError):
            _id = self._state["input"]
        return self._state["inputs"][_id]["label"]

    @input.setter
    def input(self, value):
        """Set the HTP-1 device's input."""
        if self._tx is None:
            raise AioHtp1Exception("no transaction in progress")
        for _id, info in self._state["inputs"].items():
            if value == info["label"]:
                self._tx["/input"] = _id
                return
        raise AioHtp1Exception("input '{value}' not found")

    @property
    def inputs(self):
        """List the HTP-1 device's visible inputs."""
        if not self._state:
            return []
        return [
            i["label"] for i in self._state["inputs"].values() if i["visible"]
        ]

    @property
    def upmix(self):
        """Retrieve the HTP-1 device's upmix."""
        try:
            return self._tx["/upmix/select"]
        except (TypeError, KeyError):
            pass
        return self._state["upmix"]["select"]

    @upmix.setter
    def upmix(self, value):
        """Set the HTP-1 device's upmix."""
        if self._tx is None:
            raise AioHtp1Exception("no transaction in progress")
        self._tx["/upmix/select"] = value

    @property
    def upmixes(self):
        """List the HTP-1 device's visible upmixes."""
        if not self._state:
            return []
        return [
            k for k, i in self._state["upmix"].items() if k != "select" and i["homevis"]
        ]
