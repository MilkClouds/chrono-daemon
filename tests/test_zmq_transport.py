"""ZMQ transport behavior.

These tests are asyncio-only because ``runlet.transports.zmq`` is implemented
on top of ``pyzmq.asyncio``.
"""

from __future__ import annotations

from multiprocessing.connection import Connection
import multiprocessing as mp
from typing import Any

import anyio
import msgspec
import pytest

from runlet import Channel, ChannelClosed, ChannelInUse, Context, EndOfStream, Supervisor, WouldBlock, daemon
from runlet.transports.zmq import MsgpackSerde, open_zmq_channel, open_zmq_receive, open_zmq_send

pytestmark = pytest.mark.anyio


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


class WireItem(msgspec.Struct):
    value: int
    label: str = "item"


async def test_zmq_channel_round_trips_msgspec_struct(free_tcp_port: int) -> None:
    address = f"tcp://127.0.0.1:{free_tcp_port}"
    serde = MsgpackSerde(WireItem)
    ch: Channel[WireItem] = open_zmq_channel(address, maxsize=4, serde=serde)

    await ch.send.send(WireItem(1, "one"))
    await ch.send.send(WireItem(2, "two"))
    await ch.send.aclose()

    assert await ch.recv.receive() == WireItem(1, "one")
    assert await ch.recv.receive() == WireItem(2, "two")
    with pytest.raises(EndOfStream):
        await ch.recv.receive()


async def test_zmq_receive_close_is_eventually_reported_to_sender(free_tcp_port: int) -> None:
    address = f"tcp://127.0.0.1:{free_tcp_port}"
    ch: Channel[WireItem] = open_zmq_channel(address, maxsize=1, serde=MsgpackSerde(WireItem))

    await ch.send.send(WireItem(0))
    assert await ch.recv.receive() == WireItem(0)
    await ch.recv.aclose()

    for _ in range(20):
        try:
            await ch.send.send(WireItem(1))
        except ChannelClosed:
            break
        await anyio.sleep(0)
    else:
        pytest.fail("sender did not observe receiver close")


async def test_zmq_nowait_receive_reports_empty_buffer(free_tcp_port: int) -> None:
    address = f"tcp://127.0.0.1:{free_tcp_port}"
    ch: Channel[WireItem] = open_zmq_channel(address, maxsize=1, serde=MsgpackSerde(WireItem))

    with pytest.raises(WouldBlock):
        ch.recv.receive_nowait()

    await ch.send.aclose()
    await ch.recv.aclose()


async def test_zmq_send_nowait_delivers_when_socket_is_ready(free_tcp_port: int) -> None:
    address = f"tcp://127.0.0.1:{free_tcp_port}"
    ch: Channel[WireItem] = open_zmq_channel(address, maxsize=2, serde=MsgpackSerde(WireItem))

    ch.send.send_nowait(WireItem(7))
    assert await ch.recv.receive() == WireItem(7)

    await ch.send.aclose()
    await ch.recv.aclose()


async def test_zmq_send_close_after_full_buffer_delivers_end_of_stream(free_tcp_port: int) -> None:
    address = f"tcp://127.0.0.1:{free_tcp_port}"
    ch: Channel[WireItem] = open_zmq_channel(address, maxsize=1, serde=MsgpackSerde(WireItem))

    await ch.send.send(WireItem(1))
    await ch.send.aclose()

    assert await ch.recv.receive() == WireItem(1)
    with anyio.fail_after(1):
        with pytest.raises(EndOfStream):
            await ch.recv.receive()

    await ch.recv.aclose()


async def test_zmq_send_close_without_peer_does_not_hang(free_tcp_port: int) -> None:
    address = f"tcp://127.0.0.1:{free_tcp_port}"
    send = open_zmq_send(address, bind=True, serde=MsgpackSerde(WireItem), linger_ms=0)

    with anyio.fail_after(1):
        await send.aclose()


async def test_zmq_config_rejects_negative_maxsize(free_tcp_port: int) -> None:
    address = f"tcp://127.0.0.1:{free_tcp_port}"

    with pytest.raises(ValueError, match="maxsize"):
        open_zmq_channel(address, maxsize=-1)


async def test_zmq_concurrent_receive_raises_channel_in_use(free_tcp_port: int) -> None:
    address = f"tcp://127.0.0.1:{free_tcp_port}"
    ch: Channel[WireItem] = open_zmq_channel(address, maxsize=1, serde=MsgpackSerde(WireItem))
    started = anyio.Event()

    async def waiting_receive() -> None:
        started.set()
        await ch.recv.receive()

    async with anyio.create_task_group() as tg:
        tg.start_soon(waiting_receive)
        await started.wait()
        await anyio.sleep(0)
        with pytest.raises(ChannelInUse):
            await ch.recv.receive()
        tg.cancel_scope.cancel()

    await ch.send.aclose()
    await ch.recv.aclose()


async def test_zmq_channel_runs_between_supervised_daemons(free_tcp_port: int) -> None:
    address = f"tcp://127.0.0.1:{free_tcp_port}"
    ch: Channel[WireItem] = open_zmq_channel(address, maxsize=8, serde=MsgpackSerde(WireItem))
    received: list[int] = []
    done = anyio.Event()

    @daemon
    async def producer(ctx: Context) -> None:
        for i in range(5):
            await ch.send.send(WireItem(i))
        await ch.send.aclose()

    @daemon
    async def consumer(ctx: Context) -> None:
        async for item in ch.recv:
            received.append(item.value)
        done.set()

    async with Supervisor() as sup:
        sup.add(producer())
        sup.add(consumer())
        await done.wait()

    assert received == [0, 1, 2, 3, 4]


async def test_zmq_channel_connects_supervised_daemons_across_processes(free_tcp_port: int) -> None:
    address = f"tcp://127.0.0.1:{free_tcp_port}"
    ctx = mp.get_context("spawn")
    parent_conn, child_conn = ctx.Pipe(duplex=True)
    proc = ctx.Process(target=_receiver_process_main, args=(address, child_conn))
    proc.start()
    child_conn.close()

    try:
        ready = await anyio.to_thread.run_sync(_recv_with_timeout, parent_conn, 5.0)
        assert ready == ("ready", None)

        send = open_zmq_send(address, serde=MsgpackSerde(WireItem), maxsize=8)
        produced = anyio.Event()

        @daemon
        async def producer(ctx: Context) -> None:
            for i in range(6):
                await send.send(WireItem(i))
            await send.aclose()
            produced.set()

        async with Supervisor() as sup:
            sup.add(producer())
            await produced.wait()

        result = await anyio.to_thread.run_sync(_recv_with_timeout, parent_conn, 5.0)
        assert result == ("ok", [0, 1, 2, 3, 4, 5])
    finally:
        parent_conn.close()
        proc.join(timeout=5)
        if proc.is_alive():
            proc.terminate()
            proc.join(timeout=5)

    assert proc.exitcode == 0


def _recv_with_timeout(conn: Connection, timeout: float) -> object:
    if not conn.poll(timeout):
        raise TimeoutError(f"timed out waiting for child process message after {timeout}s")
    return conn.recv()


def _receiver_process_main(address: str, conn: Connection) -> None:
    try:
        anyio.run(_receiver_process_async, address, conn, backend="asyncio")
    except BaseException as exc:  # pragma: no cover - parent reports the failure.
        conn.send(("error", f"{type(exc).__name__}: {exc}"))
        raise
    finally:
        conn.close()


async def _receiver_process_async(address: str, conn: Connection) -> None:
    recv = open_zmq_receive(address, bind=True, serde=MsgpackSerde(WireItem), maxsize=8)
    received: list[int] = []
    done = anyio.Event()

    @daemon
    async def consumer(ctx: Context) -> None:
        async for item in recv:
            received.append(item.value)
        done.set()

    async with Supervisor() as sup:
        sup.add(consumer())
        conn.send(("ready", None))
        await done.wait()

    conn.send(("ok", received))


def test_msgpack_serde_validates_wire_type() -> None:
    serde: MsgpackSerde[WireItem] = MsgpackSerde(WireItem)
    payload = MsgpackSerde[dict[str, Any]]().encode({"value": "wrong"})

    with pytest.raises(msgspec.ValidationError):
        serde.decode(payload)
