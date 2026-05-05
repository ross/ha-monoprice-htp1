"""Behavioral tests for the Htp1 WebSocket client.

All tests drive the public API as Home Assistant would: connect, subscribe,
read state, issue commands. No private attribute access.
"""
import asyncio
import logging
from json import dumps, loads

import aiodns
import aiohttp
import pytest

from aiohtp1 import AioHtp1Exception, ConnectionException, Htp1
from tests.conftest import FakeWebSocket, drain, wait_until


# ── Initial connect ───────────────────────────────────────────────────────────

async def test_connect_sends_getmso(htp1, fake_ws, mso_payload):
    fake_ws.push_mso(mso_payload)
    htp1.session.queue_ws(fake_ws)
    await htp1.connect()
    assert fake_ws.sent[0] == "getmso"


async def test_connect_opens_correct_url(htp1, fake_ws, mso_payload):
    fake_ws.push_mso(mso_payload)
    htp1.session.queue_ws(fake_ws)
    await htp1.connect()
    assert htp1.session.last_url == "ws://dev.local/ws/controller"


async def test_connected_false_before_connect(htp1):
    assert not htp1.connected


async def test_connected_true_after_mso(htp1, fake_ws, mso_payload):
    fake_ws.push_mso(mso_payload)
    htp1.session.queue_ws(fake_ws)
    await htp1.connect()
    assert htp1.connected


async def test_connect_fires_connection_subscriber(htp1, fake_ws, mso_payload):
    calls = []
    async def on_connection(value):
        calls.append(value)
    htp1.subscribe("#connection", on_connection)
    fake_ws.push_mso(mso_payload)
    htp1.session.queue_ws(fake_ws)
    await htp1.connect()
    assert len(calls) == 1


async def test_connect_raises_on_timeout(htp1, fake_ws):
    htp1.session.queue_ws(fake_ws)  # ws connected but mso never arrives
    with pytest.raises(ConnectionException):
        await htp1.connect()
    assert not htp1.connected


async def test_connect_raises_on_client_error(htp1):
    htp1.session.queue_failure(aiohttp.ClientConnectionError("refused"))
    with pytest.raises(ConnectionException):
        await htp1.connect()
    assert not htp1.connected


async def test_connect_raises_on_dns_error(htp1):
    htp1.session.queue_failure(aiodns.error.DNSError(1, "not found"))
    with pytest.raises(ConnectionException):
        await htp1.connect()
    assert not htp1.connected


async def test_connect_raises_on_session_closed(htp1):
    """Regression: RuntimeError('Session is closed') during HA shutdown is caught as ConnectionException."""
    htp1.session.queue_failure(RuntimeError("Session is closed"))
    with pytest.raises(ConnectionException):
        await htp1.connect()
    assert not htp1.connected


# ── State properties ──────────────────────────────────────────────────────────

async def test_state_properties_after_mso(connected_htp1, mso_payload):
    htp1, ws = connected_htp1
    assert htp1.serial_number == "SN123456"
    assert htp1.cal_vph == 0.0
    assert htp1.cal_vpl == -90.0
    assert htp1.volume == -25.0
    assert htp1.muted is False
    assert htp1.power is True
    assert htp1.input == "HDMI 1"
    assert htp1.inputs == ["HDMI 1", "HDMI 3"]  # only visible=True
    assert htp1.upmix == "off"
    assert set(htp1.upmixes) == {"off", "auto"}  # homevis=True; "dts" excluded


async def test_power_none_when_powerIsOn_missing(htp1, fake_ws, mso_payload):
    payload = {k: v for k, v in mso_payload.items() if k != "powerIsOn"}
    fake_ws.push_mso(payload)
    htp1.session.queue_ws(fake_ws)
    await htp1.connect()
    assert htp1.power is None


async def test_inputs_empty_before_connect(htp1):
    assert htp1.inputs == []


async def test_upmixes_empty_before_connect(htp1):
    assert htp1.upmixes == []


async def test_msoupdate_single_op_as_dict(connected_htp1):
    htp1, ws = connected_htp1
    ws.push_msoupdate({"op": "replace", "path": "/volume", "value": -10.0})
    await drain()
    assert htp1.volume == -10.0


async def test_msoupdate_list_applies_all_ops(connected_htp1):
    htp1, ws = connected_htp1
    ws.push_msoupdate([
        {"op": "replace", "path": "/volume", "value": -15.0},
        {"op": "replace", "path": "/muted", "value": True},
    ])
    await drain()
    assert htp1.volume == -15.0
    assert htp1.muted is True


async def test_msoupdate_nested_path(connected_htp1):
    htp1, ws = connected_htp1
    ws.push_msoupdate({"op": "replace", "path": "/inputs/0/label", "value": "Blu-ray"})
    await drain()
    assert htp1.input == "Blu-ray"


async def test_subscriber_exception_does_not_kill_receive_loop(connected_htp1):
    htp1, ws = connected_htp1

    async def bad_subscriber(value):
        raise RuntimeError("subscriber blew up")

    htp1.subscribe("/volume", bad_subscriber)
    ws.push_msoupdate({"op": "replace", "path": "/volume", "value": -5.0})
    await drain()
    # If receive loop died, this second update would not be applied
    ws.push_msoupdate({"op": "replace", "path": "/volume", "value": -3.0})
    await drain()
    assert htp1.volume == -3.0


# ── Subscriptions ─────────────────────────────────────────────────────────────

async def test_subscribe_notified_on_matching_update(connected_htp1):
    htp1, ws = connected_htp1
    received = []
    async def on_volume(value):
        received.append(value)
    htp1.subscribe("/volume", on_volume)
    ws.push_msoupdate({"op": "replace", "path": "/volume", "value": -20.0})
    await drain()
    assert received == [-20.0]


async def test_multiple_subscribers_all_called(connected_htp1):
    htp1, ws = connected_htp1
    calls_a, calls_b = [], []
    async def cb_a(v): calls_a.append(v)
    async def cb_b(v): calls_b.append(v)
    htp1.subscribe("/muted", cb_a)
    htp1.subscribe("/muted", cb_b)
    ws.push_msoupdate({"op": "replace", "path": "/muted", "value": True})
    await drain()
    assert calls_a == [True]
    assert calls_b == [True]


async def test_subscriber_for_unrelated_path_not_called(connected_htp1):
    htp1, ws = connected_htp1
    calls = []
    async def cb(v): calls.append(v)
    htp1.subscribe("/muted", cb)
    ws.push_msoupdate({"op": "replace", "path": "/volume", "value": -10.0})
    await drain()
    assert calls == []


async def test_connection_subscriber_fires_on_disconnect(connected_htp1, mso_payload):
    htp1, ws = connected_htp1
    calls = []
    async def on_connection(v): calls.append(v)
    htp1.subscribe("#connection", on_connection)

    new_ws = FakeWebSocket()
    new_ws.push_mso(mso_payload)
    htp1.session.queue_ws(new_ws)

    ws.push_close()
    await wait_until(lambda: len(calls) >= 1)
    assert len(calls) >= 1


# ── Transactional writes ──────────────────────────────────────────────────────

async def test_commit_sends_changemso(connected_htp1):
    htp1, ws = connected_htp1
    async with htp1 as tx:
        tx.volume = -20.0
        await tx.commit()
    msg = ws.sent[-1]
    assert msg.startswith("changemso ")
    ops = loads(msg[len("changemso "):])
    assert ops == [{"op": "replace", "path": "/volume", "value": -20.0}]


async def test_staged_value_visible_inside_transaction(connected_htp1):
    htp1, ws = connected_htp1
    async with htp1 as tx:
        tx.volume = -10.0
        assert tx.volume == -10.0  # staged value, not committed state


async def test_exit_without_commit_discards_changes(connected_htp1):
    htp1, ws = connected_htp1
    original = htp1.volume
    async with htp1 as tx:
        tx.volume = 0.0
    assert htp1.volume == original


async def test_set_outside_transaction_raises(connected_htp1):
    htp1, ws = connected_htp1
    with pytest.raises(AioHtp1Exception):
        htp1.volume = -5.0


async def test_nested_transaction_raises(connected_htp1):
    htp1, ws = connected_htp1
    async with htp1 as tx:
        with pytest.raises(AioHtp1Exception):
            async with htp1:
                pass


async def test_input_valid_label_sends_correct_id(connected_htp1):
    htp1, ws = connected_htp1
    async with htp1 as tx:
        tx.input = "HDMI 3"
        await tx.commit()
    ops = loads(ws.sent[-1][len("changemso "):])
    assert ops == [{"op": "replace", "path": "/input", "value": "2"}]


async def test_input_unknown_label_raises(connected_htp1):
    htp1, ws = connected_htp1
    with pytest.raises(AioHtp1Exception):
        async with htp1 as tx:
            tx.input = "Does Not Exist"


async def test_commit_no_changes_returns_false(connected_htp1):
    htp1, ws = connected_htp1
    sent_before = len(ws.sent)
    async with htp1 as tx:
        result = await tx.commit()
    assert result is False
    assert len(ws.sent) == sent_before


async def test_commit_batches_multiple_ops_into_one_message(connected_htp1):
    htp1, ws = connected_htp1
    async with htp1 as tx:
        tx.volume = -18.0
        tx.muted = True
        await tx.commit()
    # exactly one changemso message sent, containing both ops
    changemso_msgs = [s for s in ws.sent if s.startswith("changemso ")]
    assert len(changemso_msgs) == 1
    ops = loads(changemso_msgs[0][len("changemso "):])
    assert len(ops) == 2
    assert {op["path"] for op in ops} == {"/volume", "/muted"}


async def test_commit_connection_reset_returns_false(connected_htp1):
    """Regression: ClientConnectionResetError during send returns False, not an exception."""
    htp1, ws = connected_htp1
    ws.send_exception = aiohttp.ClientConnectionResetError()
    async with htp1 as tx:
        tx.volume = -5.0
        result = await tx.commit()
    assert result is False


# ── Disconnect / reconnect ────────────────────────────────────────────────────

@pytest.mark.parametrize("disconnect_fn", ["push_close", "push_closed", "push_closing", "push_error"])
async def test_all_disconnect_types_trigger_reconnect(connected_htp1, mso_payload, disconnect_fn):
    """Regression: CLOSING/CLOSED/ERROR no longer spin the CPU — all trigger reconnect."""
    htp1, ws = connected_htp1

    new_ws = FakeWebSocket()
    new_ws.push_mso(mso_payload)
    htp1.session.queue_ws(new_ws)

    getattr(ws, disconnect_fn)()
    await wait_until(lambda: htp1.connected)


async def test_stale_ws_messages_ignored_after_disconnect(connected_htp1, mso_payload):
    htp1, ws = connected_htp1
    original_volume = htp1.volume

    new_ws = FakeWebSocket()
    new_ws.push_mso(mso_payload)
    htp1.session.queue_ws(new_ws)

    ws.push_close()
    await wait_until(lambda: htp1.connected)

    # Any message pushed on the old (dead) ws should be silently ignored
    ws.push_msoupdate({"op": "replace", "path": "/volume", "value": 0.0})
    await drain()
    assert htp1.volume == original_volume


async def test_reconnect_backoff_doubles_and_caps(fake_session, mso_payload, monkeypatch):
    """Exponential backoff doubles on each failure and is capped at RECONNECT_DELAY_MAX."""
    import aiohtp1 as module

    sleep_calls = []
    real_sleep = asyncio.sleep

    async def recording_sleep(t):
        sleep_calls.append(t)
        await real_sleep(0)  # yield without the actual delay

    monkeypatch.setattr(module.asyncio, "sleep", recording_sleep)

    client = Htp1("dev.local", fake_session)
    client.RECONNECT_DELAY_INITIAL = 1
    client.RECONNECT_DELAY_MAX = 5
    client.MSO_WAIT_TIMEOUT = 0.05

    connected_event = asyncio.Event()
    async def on_connection(v):
        if client.connected:
            connected_event.set()
    client.subscribe("#connection", on_connection)

    # 4 failures → sleeps of 1, 2, 4, 5 (capped) → then success
    for _ in range(4):
        fake_session.queue_failure(aiohttp.ClientConnectionError("refused"))
    success_ws = FakeWebSocket()
    success_ws.push_mso(mso_payload)
    fake_session.queue_ws(success_ws)

    await client.try_connect()
    async with asyncio.timeout(2.0):
        await connected_event.wait()
    await client.stop()

    assert sleep_calls == [1, 2, 4, 5]


async def test_try_connect_no_duplicate_tasks(htp1, mso_payload):
    """Regression: a second try_connect() call while one is running is a no-op."""
    # Queue two websockets; with the guard only one should be consumed
    for _ in range(2):
        ws = FakeWebSocket()
        ws.push_mso(mso_payload)
        htp1.session.queue_ws(ws)

    await htp1.try_connect()
    await htp1.try_connect()  # guard: second call returns immediately

    await wait_until(lambda: htp1.connected)
    await htp1.stop()
    assert htp1.session.connect_count == 1


# ── Shutdown ──────────────────────────────────────────────────────────────────

async def test_stop_after_direct_connect_only(htp1, fake_ws, mso_payload):
    """Regression: stop() works when only connect() was used (path taken by config_flow)."""
    fake_ws.push_mso(mso_payload)
    htp1.session.queue_ws(fake_ws)
    await htp1.connect()
    await htp1.stop()  # must not raise AttributeError from _stop_connect None-guard
    assert not htp1.connected


async def test_stop_during_reconnect_cancels_cleanly(htp1):
    htp1.session.queue_failure(aiohttp.ClientConnectionError("refused"))
    await htp1.try_connect()
    await asyncio.sleep(0)
    await asyncio.sleep(0)  # let the task process the failure and reach the next ws_connect
    await htp1.stop()
    assert not htp1.connected


async def test_stop_clears_connected(connected_htp1):
    htp1, ws = connected_htp1
    assert htp1.connected
    await htp1.stop()
    assert not htp1.connected


async def test_stop_is_idempotent(connected_htp1):
    htp1, ws = connected_htp1
    await htp1.stop()
    await htp1.stop()  # must not raise


# ── Logging regression ────────────────────────────────────────────────────────

async def test_handler_exception_logs_cmd_name(connected_htp1, caplog):
    """Regression: exception log includes the command name, not a bare %%s placeholder."""
    htp1, ws = connected_htp1
    # "delete" op is unimplemented → raises NotImplementedError inside the handler
    ws.push_msoupdate({"op": "delete", "path": "/volume"})
    with caplog.at_level(logging.ERROR, logger="aiohtp1"):
        await drain()
    assert any("msoupdate" in r.message for r in caplog.records)
