# ADR 0010 — Channel endpoints are single-owner

Status: Accepted (2026-06-25)

Supersedes: the MPMC endpoint-sharing sentence in ADR 0001.

## Context

ADR 0001 correctly chose `Channel[T]` as the only core communication
primitive, with fanout, batching, and request/response expressed as recipes.
It also allowed multiple producers and multiple consumers to share one
endpoint with competing-consumer semantics.

That sharing is the wrong default for runlet's target shape. Most runlet
pipelines are statically wired dataflow graphs: one daemon owns the producer
side of a connection and one daemon owns the consumer side. If two daemons
accidentally receive from the same endpoint, the bug is subtle: messages are
silently partitioned rather than broadcast. If two producers share a send
endpoint, close ownership becomes ambiguous.

The recipe namespace already covers deliberate non-SPSC patterns:
`fanout.tee` for 1:N broadcast, `batcher` for fan-in request/response, and
lossy wrappers for explicit drop policies. Keeping core channels SPSC makes
the common case safer and keeps non-trivial routing visible.

## Decision

`Channel[T]` remains the sole core communication primitive, but its endpoint
ownership model is single producer / single consumer:

- one active task owns `Channel.send`;
- one active task owns `Channel.recv`;
- concurrent blocking `send()` or `receive()` calls on the same endpoint are
  a wiring error and raise `ChannelInUse`.

The in-process implementation enforces this as a concurrent-use guard. It
does not try to implement a global ownership registry: `send_nowait()` and
`receive_nowait()` remain low-level synchronous operations used by recipes,
and sequential use from different setup/helper tasks is not rejected.

## Consequences

+ Accidental competing-consumer bugs fail fast instead of silently splitting
  messages.
+ Close ownership is conceptually simple: the single producer closes the send
  side; the single consumer may close the receive side to reject future sends.
+ Core stays small. MPSC, SPMC, fanout, batching, and lossy backpressure stay
  explicit recipes or future factories rather than becoming channel modes.
- Users that want worker-pool competing consumers must build that pattern
  deliberately, likely as a recipe with a clear close/error policy.
- The runtime guard is conservative rather than complete. It catches
  concurrent blocking use, not every possible sequential handoff of an
  endpoint between tasks.
- Future transport adapters must preserve the same fail-fast concurrent-use
  semantic or document why they cannot.

## Related

- ADR 0001 — Channel is the sole communication primitive.
- ADR 0006 — Transport adapters share the `SendStream` / `ReceiveStream`
  protocol surface.
- `runlet.recipes.fanout.tee` — the canonical 1:N broadcast helper.
- `runlet.recipes.batcher` — deliberate fan-in request/response.
