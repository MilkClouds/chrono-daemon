# ADR 0011: ZMQ transport ships as an optional extra

Status: Accepted (2026-06-25)

## Context

ADR 0006 reserved a transport-adapter slot by making `SendStream` and
`ReceiveStream` Protocols. ADR 0007 kept the core runtime dependency set to
`anyio` only, and explicitly deferred the first serialization choice until a
cross-process transport needed one.

The first useful remote transport should prove that daemons can communicate
without changing their call sites: producer code still calls
`await out.send(item)`, consumer code still calls `await recv.receive()` or
`async for item in recv`, and non-SPSC topologies still live in recipes.

## Decision

Ship a first remote adapter under `runlet.transports.zmq`, installed with the
`zmq` extra:

```bash
pip install "runlet[zmq]"
```

The extra depends on `pyzmq` for ZeroMQ sockets and `msgspec` for MessagePack
serialization. The core package still depends only on `anyio`.

The adapter is deliberately SPSC:

- one `open_zmq_send(address, ...)` endpoint;
- one `open_zmq_receive(address, ...)` endpoint;
- `open_zmq_channel(address, ...)` convenience for same-process loopback and
  tests;
- `MsgpackSerde(message_type)` for typed payload validation on receive.

It uses `pyzmq.asyncio`, so it is an asyncio-backend transport. The in-process
channel remains backend-agnostic and is still the default `open_channel()`
implementation.

## Consequences

+ Daemon code can move from in-process channels to TCP-backed ZMQ endpoints
  without changing the daemon interface.
+ Serialization is explicit and typed at the transport boundary.
+ Optional dependencies stay out of the default install.
+ ADR 0010's single-active-endpoint guard is preserved for blocking
  `send()` and `receive()` calls.
- ZMQ queue semantics are not identical to in-process rendezvous semantics.
  `maxsize` maps to socket high-water marks with one extra control-frame slot;
  `maxsize=0` uses a small bounded ZMQ queue rather than a true zero-slot
  rendezvous.
- Close propagation is cooperative over control frames. Once a peer close is
  observed, later sends or receives fail with the runlet exceptions, but a
  network peer cannot observe a close before the control frame arrives.
- The first remote adapter is asyncio-only because `pyzmq.asyncio` exposes
  asyncio Futures. Trio users should keep using in-process channels or add a
  separate transport adapter.

## Related

- ADR 0006: Transport adapter slot.
- ADR 0007: Optional deps belong outside the core runtime dependency set.
- ADR 0010: Channel endpoints are single-owner.
