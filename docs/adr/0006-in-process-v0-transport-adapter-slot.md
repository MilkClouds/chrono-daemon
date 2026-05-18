# ADR 0006 — Ship in-process only at first, with a reserved transport-adapter slot

Status: Accepted (2026-05-18)

## Context

Concurrency libraries that try to be transport-agnostic from day one (dora-rs,
HORUS, anything DDS-flavored) pay a permanent complexity tax: every API has
to consider serialization, addressing, discovery, and partial failure. The
core abstractions blur because they have to fit both same-process and
cross-network use cases.

Libraries that ship in-process only and add transports as an afterthought
(historical asyncio-queue wrappers, in-process actor libraries) often end up
unable to add cross-process support without breaking changes to user code —
because `Queue.put` has a different cost profile from "send over a network",
or because the same-process API assumes Python object identity.

We want neither failure mode. The plan is to ship in-process only at first,
but to shape the `Channel` API now so multi-process and network transports
can land in a later release without renaming or re-typing.

## Decision

`SendStream` and `ReceiveStream` are Protocols. The in-process
implementation (`_Send`, `_Recv`) wraps `anyio.MemoryObjectStream`s and is
the only one shipped initially. `open_channel()` is the factory; it
returns the in-process implementation.

The Protocol surface is deliberately drawn at:
- `async send(item) -> None`
- `async receive() -> T` (raising `EndOfStream`)
- `__aiter__()`
- `async aclose()`
- `statistics() -> ChannelStats` (ADR 0008)

Notably absent: any operation that assumes in-process Python-object
identity (no "peek by reference," no "in-place mutation of buffered items"),
any operation that requires synchronous semantics, and any operation that
returns a non-typed item.

When a later release adds e.g. a `ZenohChannel`, the user-facing call site
stays `Channel.send.send(item)`; only the factory changes.

## Consequences

+ The initial release ships small (zero serialization concerns) and fast
  (in-process buffer, no network).
+ The user-facing API is stable across transport choices. Daemon code is
  transport-agnostic.
+ `ChannelStats` already includes counts that make sense for any transport
  (buffer used / max / open producers / open consumers / waiters on each
  side), so introspection scales when transports are added.
- The Protocol surface is *committed* now. Future transports cannot, for
  example, require synchronous send semantics — they must adapt to async.
- A transport that wants to expose backend-specific features (e.g. a
  Zenoh-specific QoS knob) has to do it through extra factory kwargs, not
  through the Channel API itself. That's a deliberate restriction.
- Serialization is a separate, deferred decision. The first cross-process
  transport will force it; whichever choice we make then will likely be
  argued in its own ADR.

## Related

- ADR 0001 — `Channel` being the sole comm primitive means this one
  decision covers all communication patterns; we don't need parallel
  protocols for service-style or topic-style transports.
- ADR 0007 — refusing extra runtime deps means the in-process
  implementation has no serialization cost to pay; that bill comes due
  when a network transport lands.
