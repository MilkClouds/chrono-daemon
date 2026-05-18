# ADR 0001 — Channel is the sole communication primitive

Status: Accepted (2026-05-18)

## Context

ROS-style frameworks expose pub/sub `Topic`, point-to-point queues, services,
parameters, and broadcast events as distinct primitives. Each adds API
surface, QoS knobs, and edge cases — most notably the *slow-consumer policy*
problem: when a topic has N subscribers and one is slow, the publisher has to
choose between blocking everyone, dropping for everyone, or dropping
per-subscriber, and that choice has to live somewhere in the configuration
matrix. ROS2's QoS profile sprawl is the load-bearing evidence that this is
not a small cost.

The target workloads for runlet — robotics control loops, ML eval harnesses,
agent orchestration — are dominated by **statically wired** dataflow: the
producer and the consumer of any given message are known at supervisor
construction. Where dynamic subscription happens at all (log taps,
visualizers), it is rare enough that handling it as an explicit, in-code
fanout rather than a runtime-discovered subscription is acceptable.

A 1:N broadcast can always be expressed as N 1:1 channels plus a fanout
daemon. The reverse direction — taking a Topic and recovering "exactly one
consumer gets each message" — requires fighting the primitive.

## Decision

`Channel[T]` is the sole communication primitive in runlet. It is a typed,
bounded queue with two endpoints, `send` and `recv`. Multiple producers and
multiple consumers may share an endpoint; each item is delivered to **exactly
one** waiting receiver (competing-consumers semantics). Closing the send side
propagates `EndOfStream` to all receivers after the buffer drains.

There is no `Topic`, no broadcast primitive, no service/RPC primitive, no
parameter system, no discovery on the core surface. Users that need 1:N
broadcast import `runlet.recipes.fanout.tee` — recipes live under a
sibling namespace (`runlet.recipes`) with weaker stability guarantees
than the core (see `docs/recipes.md`), so they're available without
copy-paste but are signaled as best-effort rather than part of the
load-bearing API.

## Consequences

+ One API to learn; static type checking on a single `Channel[T]`.
+ Backpressure has one meaning: `send` blocks while the receiver is slow.
  No slow-consumer policy matrix, no QoS profile to negotiate.
+ Replay determinism is straightforward — there is no multi-subscriber
  ordering ambiguity to specify.
+ Wiring is visible in code (every channel is a named local variable), so a
  reader can statically trace dataflow.
- 1:N broadcast costs the user a one-line `from runlet.recipes.fanout
  import tee` plus the wiring. For workloads with pervasive broadcast
  (e.g. a single `on_tick` driving multiple inference loops), the wiring
  is still explicit, just import-able.
- Migration from ROS code that relies on runtime subscriber discovery is
  not a mechanical port — every dynamic subscription has to become an
  explicit channel handed in at construction.
- Lifecycle-event streams that ROS users would express as a topic
  ("session created/destroyed broadcasts") have to be modeled as either
  N pre-allocated channels or a single channel with the `tee` recipe.

## Related

- ADR 0006 (transport adapter slot): `Channel` is a Protocol so multi-process
  and network backends can be added later without reopening this decision.
- `runlet.recipes.fanout.tee`: the canonical 1:N broadcast helper.
- `runlet.recipes.batcher`: shows how request/response is built from
  channels alone (no service primitive needed).
