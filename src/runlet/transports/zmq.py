"""ZeroMQ-backed SPSC channel endpoints.

Install with ``runlet[zmq]``. This adapter uses ``pyzmq.asyncio`` and is
therefore intended for asyncio-backed runlet deployments. The core in-process
channel remains backend-agnostic.

Wire format: every message is a single ZMQ frame whose first byte tags it as
data (``_DATA``), end-of-stream (``_EOF``), or receiver-close (``_CLOSE``).
A single frame is delivered atomically, so a send cancelled mid-message cannot
leave a half-written multipart on the wire.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from dataclasses import dataclass
from typing import Any, Generic, Protocol, TypeVar, cast

from anyio.lowlevel import checkpoint

try:
    import msgspec
    import zmq
    import zmq.asyncio
except ModuleNotFoundError as exc:  # pragma: no cover - exercised by users without the extra.
    raise ImportError("runlet.transports.zmq requires the 'zmq' extra: pip install runlet[zmq]") from exc

from runlet import Channel, ChannelStats, ReceiveStream, SendStream
from runlet._types import ChannelClosed, ChannelInUse, EndOfStream, WouldBlock

T = TypeVar("T")

_DATA = b"D"
_EOF = b"E"
_CLOSE = b"C"


class Serde(Protocol[T]):
    """Serializer used by ZMQ endpoints."""

    def encode(self, item: T) -> bytes: ...

    def decode(self, data: bytes) -> T: ...


class MsgpackSerde(Generic[T]):
    """MessagePack serializer backed by msgspec.

    ``message_type`` is passed to ``msgspec.msgpack.Decoder``. Leave it as
    ``Any`` for untyped payloads, or pass a msgspec Struct/dataclass/list type
    when the receiver should validate the wire payload.
    """

    def __init__(self, message_type: Any = Any) -> None:
        self._encoder = msgspec.msgpack.Encoder()
        self._decoder = msgspec.msgpack.Decoder(type=message_type)

    def encode(self, item: T) -> bytes:
        return self._encoder.encode(item)

    def decode(self, data: bytes) -> T:
        return self._decoder.decode(data)


@dataclass(frozen=True)
class ZmqConfig:
    """Socket settings shared by ZMQ send and receive endpoints."""

    maxsize: int = 0
    linger_ms: int = 1000

    def __post_init__(self) -> None:
        if self.maxsize < 0:
            raise ValueError(f"maxsize must be >= 0, got {self.maxsize}")
        if self.linger_ms < -1:
            raise ValueError(f"linger_ms must be >= -1, got {self.linger_ms}")

    @property
    def hwm(self) -> int:
        # ZMQ treats HWM=0 as unlimited. runlet's maxsize=0 means rendezvous,
        # which ZMQ cannot represent exactly. Reserve one extra slot so close
        # control frames can follow a full data buffer.
        return max(2, self.maxsize + 1)


def open_zmq_channel(
    address: str,
    *,
    bind_receive: bool = True,
    maxsize: int = 0,
    serde: Serde[T] | None = None,
    context: Any | None = None,
    linger_ms: int = 1000,
) -> Channel[T]:
    """Open both endpoints for one ZMQ-backed SPSC channel.

    By default the receive side binds and the send side connects. Use the
    separate ``open_zmq_send`` and ``open_zmq_receive`` helpers when the two
    endpoints live in different processes.
    """
    recv = open_zmq_receive(
        address,
        bind=bind_receive,
        maxsize=maxsize,
        serde=serde,
        context=context,
        linger_ms=linger_ms,
    )
    send = open_zmq_send(
        address,
        bind=not bind_receive,
        maxsize=maxsize,
        serde=serde,
        context=context,
        linger_ms=linger_ms,
    )
    return Channel(send=send, recv=recv)


def open_zmq_send(
    address: str,
    *,
    bind: bool = False,
    maxsize: int = 0,
    serde: Serde[T] | None = None,
    context: Any | None = None,
    linger_ms: int = 1000,
) -> SendStream[T]:
    """Open a ZMQ send endpoint."""
    cfg = ZmqConfig(maxsize=maxsize, linger_ms=linger_ms)
    socket = _open_pair_socket(address, bind=bind, cfg=cfg, context=context)
    return _ZmqSend(socket=socket, serde=serde or MsgpackSerde(), cfg=cfg)


def open_zmq_receive(
    address: str,
    *,
    bind: bool = True,
    maxsize: int = 0,
    serde: Serde[T] | None = None,
    context: Any | None = None,
    linger_ms: int = 1000,
) -> ReceiveStream[T]:
    """Open a ZMQ receive endpoint."""
    cfg = ZmqConfig(maxsize=maxsize, linger_ms=linger_ms)
    socket = _open_pair_socket(address, bind=bind, cfg=cfg, context=context)
    return _ZmqReceive(socket=socket, serde=serde or MsgpackSerde(), cfg=cfg)


def _open_pair_socket(address: str, *, bind: bool, cfg: ZmqConfig, context: Any | None) -> Any:
    ctx = context or zmq.asyncio.Context.instance()
    socket = ctx.socket(zmq.PAIR)
    socket.setsockopt(zmq.SNDHWM, cfg.hwm)
    socket.setsockopt(zmq.RCVHWM, cfg.hwm)
    socket.setsockopt(zmq.LINGER, cfg.linger_ms)
    if bind:
        socket.bind(address)
    else:
        socket.connect(address)
    return socket


class _ZmqSend(Generic[T]):
    def __init__(self, *, socket: Any, serde: Serde[T], cfg: ZmqConfig) -> None:
        self._socket = socket
        self._serde = serde
        self._cfg = cfg
        self._busy = False
        self._closed = False
        self._peer_closed = False

    async def send(self, item: T) -> None:
        if self._busy:
            raise ChannelInUse("send endpoint already has an active sender")
        if self._closed:
            raise ChannelClosed("send side already closed")
        self._busy = True
        try:
            await checkpoint()
            self._drain_control_nowait()
            if self._peer_closed:
                raise ChannelClosed("receive side closed")
            await self._socket.send(_DATA + self._serde.encode(item))
        except zmq.ZMQError as exc:
            raise ChannelClosed("zmq send failed") from exc
        finally:
            self._busy = False

    def send_nowait(self, item: T) -> None:
        if self._busy:
            raise ChannelInUse("send endpoint already has an active sender")
        if self._closed:
            raise ChannelClosed("send side already closed")
        self._drain_control_nowait()
        if self._peer_closed:
            raise ChannelClosed("receive side closed")
        try:
            # Synchronous C-level send on the async socket: no Future churn.
            zmq.Socket.send(self._socket, _DATA + self._serde.encode(item), flags=zmq.DONTWAIT)
        except zmq.Again as exc:
            raise WouldBlock("zmq send would block") from exc
        except zmq.ZMQError as exc:
            raise ChannelClosed("zmq send failed") from exc

    async def aclose(self) -> None:
        if self._closed:
            return
        self._closed = True
        try:
            self._drain_control_nowait()
            if not self._peer_closed:
                # Best-effort: teardown must never block on a full buffer.
                _send_control_nowait(self._socket, _EOF)
                await checkpoint()
        finally:
            self._socket.close(linger=self._cfg.linger_ms)

    def statistics(self) -> ChannelStats:
        return ChannelStats(
            current_buffer_used=-1,
            max_buffer_size=self._cfg.maxsize,
            open_send_streams=0 if self._closed else 1,
            open_receive_streams=0 if self._peer_closed else -1,
            waiters_send=1 if self._busy else 0,
            waiters_receive=-1,
        )

    def _drain_control_nowait(self) -> None:
        while True:
            try:
                frame = _recv_nowait(self._socket)
            except WouldBlock:
                return
            if frame[:1] in (_CLOSE, _EOF):
                self._peer_closed = True


class _ZmqReceive(Generic[T]):
    def __init__(self, *, socket: Any, serde: Serde[T], cfg: ZmqConfig) -> None:
        self._socket = socket
        self._serde = serde
        self._cfg = cfg
        self._busy = False
        self._closed = False
        self._peer_closed = False

    async def receive(self) -> T:
        if self._busy:
            raise ChannelInUse("receive endpoint already has an active receiver")
        if self._closed or self._peer_closed:
            raise EndOfStream
        self._busy = True
        try:
            while True:
                frame = await self._socket.recv()
                item = self._decode_frame(frame)
                if item is not _SKIP:
                    return cast(T, item)
        except zmq.ZMQError as exc:
            raise EndOfStream from exc
        finally:
            self._busy = False

    def receive_nowait(self) -> T:
        if self._busy:
            raise ChannelInUse("receive endpoint already has an active receiver")
        if self._closed or self._peer_closed:
            raise EndOfStream
        try:
            while True:
                item = self._decode_frame(_recv_nowait(self._socket))
                if item is not _SKIP:
                    return cast(T, item)
        except WouldBlock:
            raise
        except zmq.ZMQError as exc:
            raise EndOfStream from exc

    async def __aiter__(self) -> AsyncIterator[T]:  # type: ignore[override]
        while True:
            try:
                yield await self.receive()
            except EndOfStream:
                return

    async def aclose(self) -> None:
        if self._closed:
            return
        self._closed = True
        try:
            # Best-effort: teardown must never block on a full buffer.
            _send_control_nowait(self._socket, _CLOSE)
            await checkpoint()
        finally:
            self._socket.close(linger=self._cfg.linger_ms)

    def statistics(self) -> ChannelStats:
        return ChannelStats(
            current_buffer_used=-1,
            max_buffer_size=self._cfg.maxsize,
            open_send_streams=0 if self._peer_closed else -1,
            open_receive_streams=0 if self._closed else 1,
            waiters_send=-1,
            waiters_receive=1 if self._busy else 0,
        )

    def _decode_frame(self, frame: bytes) -> T | object:
        tag = frame[:1]
        if tag == _DATA:
            return self._serde.decode(frame[1:])
        if tag == _EOF:
            self._peer_closed = True
            raise EndOfStream
        if tag == _CLOSE:
            return _SKIP
        raise ChannelClosed(f"invalid zmq channel frame: {frame!r}")


_SKIP = object()


def _recv_nowait(socket: Any) -> bytes:
    try:
        # Synchronous C-level recv on the async socket: no Future churn.
        return zmq.Socket.recv(socket, flags=zmq.DONTWAIT)
    except zmq.Again as exc:
        raise WouldBlock("zmq receive would block") from exc


def _send_control_nowait(socket: Any, frame: bytes) -> None:
    """Best-effort control send for teardown; never blocks, never raises."""
    try:
        zmq.Socket.send(socket, frame, flags=zmq.DONTWAIT)
    except zmq.ZMQError:
        pass


__all__ = [
    "MsgpackSerde",
    "Serde",
    "ZmqConfig",
    "open_zmq_channel",
    "open_zmq_receive",
    "open_zmq_send",
]
