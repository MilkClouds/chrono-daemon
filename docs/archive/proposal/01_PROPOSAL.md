# chrono-daemon

## Problem

비동기 환경에서 **여러 데몬을 띄우고, 그것들 사이에 간단한 통신을 흘리는 패턴**은 robotics control loop, ML evaluation harness, agent orchestration 등 도메인을 가리지 않고 반복적으로 등장한다. 그러나 현실은 다음과 같다:

- 매번 손으로 짠다. `simple_env` 같은 prototype 형태로 같은 코드가 프로젝트마다 다시 작성된다.
- ROS / ROS2의 `Node + Clock + Publisher + Subscriber + Executor` 추상화가 형태상 정답에 가깝지만, ROS runtime 의존 비용이 너무 크다 (`colcon` build system, DDS, 설치 복잡도, Python 호환성 문제).
- ROS를 대체하려는 모던 프로젝트들(dora-rs, HORUS, Apollo CyberRT, Drake, Holoscan)은 모두 무겁거나 분산 dataflow에 특화되어 있고, **sim-time deterministic replay가 first-class가 아니다**.
- 순수 Python 구현체는 `RedisROS` 정도가 거의 유일한데 Redis 의존이 있고 sim-time 지원이 없다.
- 결과적으로 "추론 동안 환경 시간이 멈춘 채로 burst replay 가능한 multi-daemon scenario"를 깔끔하게 표현할 수 있는 작은 라이브러리가 없다.

## Goal

**`anyio` 위에 얹힌 작고 범용적인 동시성 라이브러리. 4개의 primitive로 robotics control loop, ML eval rollout, agent orchestration을 모두 표현할 수 있어야 한다.**

- 핵심 추상화: `Channel`, `Clock`, `Daemon`, `Supervisor` — 그게 전부.
- **`SimClock`으로 deterministic burst replay를 first-class로 지원**한다. `await clock.advance(10.0)` 한 줄로 10초 분량의 multi-daemon scenario를 microseconds wall-time에 결정론적으로 재생한다.
- asyncio와 trio 두 backend를 모두 지원한다.
- 런타임 의존은 `anyio>=4` 하나뿐이다.

## Non-Goal

- **ROS API와의 1:1 호환성.** 형태는 비슷할 수 있어도, 우리는 ROS 호환 layer가 아니다.
- **`Topic` / pub-sub broadcast** ([ADR 0001](../../adr/0001-channel-is-the-sole-comm-primitive.md)). 모든 통신은 1:1 `Channel`. 1:N broadcast가 필요하면 `recipes.fanout.tee`로 명시적으로 wiring한다.
- **Services / RPC / parameter system / discovery.** 필요하면 `Channel` 위에 사용자가 직접 짠다 (`recipes.batcher` 참조).
- **`on_start`/`run`/`on_stop` 너머의 lifecycle 상태머신** ([ADR 0005](../../adr/0005-no-lifecycle-states-beyond-start-run-stop.md)). ROS2 managed node의 5상태(`unconfigured/inactive/active/...`)는 도입하지 않는다.
- **Multi-process / network transport** (v0 한정. `Channel` Protocol slot은 [ADR 0006](../../adr/0006-in-process-v0-transport-adapter-slot.md)에서 reserved).
- **`anyio` 외 runtime dependency** ([ADR 0007](../../adr/0007-anyio-only-runtime-dependency.md)). `msgspec`, `structlog`, `pydantic`은 들이지 않는다.
- **CLI / launcher.** chrono-daemon은 라이브러리다. `chrono-daemon run` 같은 명령은 없다.

## Architecture

### 4 Primitives

| Primitive | 역할 |
|-----------|------|
| **`Channel[T]`** | 유일한 inter-daemon 통신 primitive. SPSC bounded queue. 여러 producer/consumer가 필요하면 recipe로 명시적으로 표현한다. |
| **`Clock`** | `WallClock` (production, monotonic real time)과 `SimClock` (deterministic, `advance(dt)`/`advance_to(t)`로 시간을 일괄 진행). |
| **`Daemon`** | Long-running async unit. `on_start` / `run` / `on_stop` lifecycle hook. class 형태 또는 `@daemon` decorator. |
| **`Supervisor`** | `async with Supervisor(...) as sup:` 구조적 동시성 root. `add(daemon)` / `spawn(fn)` / `signal_stop()` / `await stop(grace=...)` 으로 lifecycle 관리. 에러 정책: `shutdown` (default) / `restart` / `ignore`. |

런타임 객체 그래프는 항상 다음 형태다:

```
Supervisor                       (typically one per process)
├── Clock                        (one, shared)
├── Daemon A ──send──▶ Channel X ──recv──▶ Daemon B
├── Daemon C ──send──▶ Channel Y ──recv──▶ Daemon A
└── ...
```

Wiring은 코드에 명시적으로 드러난다 — 모든 channel과 consumer는 변수로 잡혀 있고, 런타임에 발견되는 topic 이름이 아니다. 이 명시성은 의도된 trade-off이며 [ADR 0001](../../adr/0001-channel-is-the-sole-comm-primitive.md)에 기록되어 있다.

### SimClock — 차별점

다른 ROS-like 라이브러리들은 wall-clock에서만 동작하거나, sim-time을 가지더라도 후행 add-on이다. chrono-daemon은 그 반대다:

- `Clock` Protocol이 모든 시간 wait의 단일 통로다. Daemon은 `ctx.clock.sleep(...)`만 호출하고 `anyio.sleep(...)`은 부르지 않는다.
- `SimClock`은 sleeper의 deadline heap을 유지하고, driver task가 `await clock.advance(dt)`를 부르면 deadline 순서로 일괄 wake한다. 매 wake 사이에 settle round (anyio fairness checkpoint × N)를 끼워 trio backend의 task scheduling 차이를 흡수한다 ([ADR 0002](../../adr/0002-wall-and-sim-clocks-as-pluggable-protocol.md)).
- 같은 daemon 코드가 production에서는 `WallClock`, 테스트와 replay에서는 `SimClock`을 받는다. 코드 변경 없이 백엔드를 바꿔 끼운다.

### Structured concurrency

`Supervisor`는 `anyio.create_task_group` 위의 얇은 lifecycle layer다. 모든 daemon은 supervisor의 자식으로 시작되며, supervisor가 종료될 때 cancellation propagation이 자동으로 전체 그래프를 정리한다. ROS2 multi-threaded executor의 callback group 복잡도 같은 것은 없다.

### Cooperative shutdown

`Supervisor.signal_stop()`은 협력적 종료 신호를 보내고, `await Supervisor.stop(grace=N)`은 grace window 후 강제 cancellation까지 보장한다. `on_stop`은 정상 경로, 모든 Exception 경로 (shutdown/restart/ignore), 그리고 강제 cancel 경로에서도 shielded scope 안에서 보장 실행된다 ([ADR 0009](../../adr/0009-cooperative-stop-signaling.md)).

상세 설계는 [docs/concepts.md](../../concepts.md) 및 [ADRs](../../adr/) 참조.

## Real-Time vs Sim-Time Replay — 차별점

`SimClock` 기반 deterministic burst replay는 chrono-daemon의 핵심 차별점이다.

- ROS2 / rclpy의 `/clock` topic은 sim-time을 다루지만, 다른 노드와의 sync는 wall-clock-driven publisher에 의존한다. 즉 sim-time은 있어도 burst replay는 아니다.
- dora-rs는 분산 dataflow로 wall-clock에서만 동작한다. 노드들이 각자 별도 OS process에서 돌아 sim-time intercept point가 없다.
- Apollo CyberRT의 `Time` 추상은 sim/replay를 지원하지만 차량 자율주행 도메인에 강하게 묶여 있고 C++ 빌드 시스템이 무겁다.
- Drake systems framework는 sim-time first-class이지만 dynamical-systems modeling 전용이고 in-process port만 있어 distributed 시나리오에 부적합하다.

chrono-daemon의 niche는 "pure Python + anyio + SimClock burst replay first-class + 4 primitive minimal core"의 교집합이다. 이 조합을 만족하는 기존 프로젝트는 0개다 ([02_COMPETITIVE_LANDSCAPE.md](./02_COMPETITIVE_LANDSCAPE.md) 참조).

## Use Cases

다음 패턴들이 모두 chrono-daemon의 4 primitive로 자연스럽게 표현된다:

| Use case | chrono-daemon 매핑 |
|----------|------------|
| **System 2/1/0 inference pipeline** | `S2Loop`/`S1Loop`/`S0Loop` = `Daemon` 3개, per-session clock = `Clock` 인스턴스, 각 stage 간 데이터 흐름 = `Channel`, session `register/unregister` = `Supervisor` add/cooperative stop. `examples/system_stack_mock.py` 및 `examples/system_stack_multi_session.py`에서 실제 mock 구현. |
| **Eval rollout workers** | 각 worker = `Daemon`, per-session `SimClock`으로 결정론적 replay, 결과 수집 = `Channel`. |
| **Agent orchestration** | Planner/executor/tool = `Daemon`, 메시지 흐름 = `Channel`, 외부 sync ABC와의 bridge = `recipes.sync_bridge.host_async_dispatcher`. |
| **Multi-rate reactive control** | 각 rate loop = `Daemon` with `ctx.clock.every(period)`, shared state = `recipes.latest.Latest[T]` cache, command stream = `Channel`. |

`examples/`의 두 mock pipeline은 ergonomic notes까지 함께 제공한다. 어디가 깔끔했고, 어디가 boilerplate였고, 어디서 v0가 부족했는지는 [examples/README.md](../../../examples/README.md)에 정리한다.

## Supported Backends

| Backend | 상태 | 비고 |
|---------|------|------|
| **asyncio** | Full | Byte-deterministic cross-run replay 가능. |
| **trio** | Full | 모든 테스트 통과. 단, trio default scheduler가 task-spawn 순서를 randomize하여 byte-deterministic cross-run replay는 보장되지 않는다 (length/monotone time/distribution은 일치). `ready_gate` recipe 후보. |

## References

설계 시 깊이 분석한 기존 프로젝트. 각 행의 링크는 상세 분석 문서로 연결된다.

| 프로젝트 | 참고 포인트 |
|----------|------------|
| [dora-rs](./03_REFERENCE_SURVEY/dora_rs.md) | Dataflow-oriented robotic architecture. 분산 first 디자인의 한계 (sim-replay 부재)를 명확히 했다. |
| [Apollo CyberRT](./03_REFERENCE_SURVEY/apollo_cyberrt.md) | `Node`/`Channel`/`Time` 추상과 record/replay 모델. sim-time first-class의 production 예시. |
| [Drake systems framework](./03_REFERENCE_SURVEY/drake.md) | `System` / `Context` / `Simulator` / `Diagram` — sim-time 중심 in-process composition의 reference. |
| [NVIDIA Holoscan SDK](./03_REFERENCE_SURVEY/holoscan.md) | `Operator` / `Scheduler` / `Clock` 헤더 구조의 깔끔함. multi-fragment 분산 모델. |
| [Orocos RTT](./03_REFERENCE_SURVEY/orocos_rtt.md) | `TaskContext` / `Activity` / `Port` — hard real-time component 모델의 성숙도. |
| [HORUS](./03_REFERENCE_SURVEY/horus.md) | Rust 신생 framework의 macro-based daemon DSL, scheduler 5종 execution class. |
| [ERDOS](./03_REFERENCE_SURVEY/erdos.md) | Watermark 기반 deterministic ordering. AV pipeline 도메인. |
| [RedisROS](./03_REFERENCE_SURVEY/redisros.md) | Pure Python ROS2 클론. Redis 의존의 trade-off. |
| [`simple_env`](./03_REFERENCE_SURVEY/simple_env.md) | 저자 본인 prototype. Sync-only burst replay의 출발점. `Env.step(dt_ns)` 모델이 `SimClock.advance(dt)`의 직접 조상. |

상세 분석은 [03_REFERENCE_SURVEY/](./03_REFERENCE_SURVEY/) 참조.

## Competitive Landscape

본 프로젝트와 동일한 4 axes (pure Python + anyio + SimClock burst replay first-class + 4 primitive minimal core)를 모두 만족하는 기존 프로젝트는 **0개**다. 7개 카테고리, 20+ 후보 프로젝트의 README + 핵심 헤더 정독으로 검증했다. 상세는 [02_COMPETITIVE_LANDSCAPE.md](./02_COMPETITIVE_LANDSCAPE.md) 참조.

## Summary

1. **4 primitive minimal core.** `Channel` / `Clock` / `Daemon` / `Supervisor` — 더 늘리지 않는다.
2. **`SimClock` first-class.** Deterministic burst replay가 add-on이 아니라 핵심 가치다.
3. **`anyio` 위 얇게.** asyncio + trio 둘 다, dependency는 `anyio` 하나.
4. **시장 공백 확인.** 동일 axes의 기존 프로젝트 부재를 전수 조사로 검증.
