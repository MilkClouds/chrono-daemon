# simple_env

- 출처: <https://github.com/MilkClouds/simple_env>
- 언어: Python (pure)
- 카테고리: Pure Python prototype (저자 본인)
- 첫 release: 2024 (저자 본인 prototype, 비공개 / 소규모 공개)

## 한 줄 요약

> "A minimal library with virtually no dependencies, designed to create ROS-like Nodes that perform full-synchronous message passing (publisher/subscriber) and fully synchronous time processing(Timer). Its original purpose was to support 'burst' mode processing of ROS-like messages."

**runlet의 직접 조상.** 저자가 robotics evaluation 작업 중 ROS2의 무게가 부담스러워 손으로 짠 ~150 LOC prototype. **`Env.step(dt_ns)` 모델이 `SimClock.advance(dt)`의 직접 영감원**이다.

## 핵심 추상화

| simple_env 컴포넌트 | 의미 | runlet 대응 |
|---------------------|------|-------------|
| **`Node`** | 컴포지션 단위. `create_subscription` / `create_publisher` / `create_timer` / `get_clock`. ROS2 형태. | `Daemon` |
| **`Env`** | 모든 Node + Subscription + Timer + Clock의 컨테이너. `step(dt_ns)` 로 advance. | `Supervisor` + `SimClock` |
| **`Publisher` / `Subscription`** | Topic-based pub/sub. Untyped (msg type 강제 안 함). | `Channel.send` / `Channel.recv` |
| **`Timer`** | Period-based callback. `step()`에서 deadline 체크. | `Clock.every(period)` |
| **`Clock` (`SIM_TIME` / `SYSTEM_TIME`)** | 두 모드. SIM_TIME은 `Env.step(time_ns)` 으로만 advance. | `Clock` (`WallClock` / `SimClock`) |
| **`Time` / `Duration`** | ns precision time value. ROS2 형태. | (runlet은 `float seconds`) |

simple_env는 **fully synchronous**다 — 모든 callback이 `Env.step()` 안에서 직접 호출, async 없음. async 환경에서는 사용 불가.

## SimClock과의 관계

**`Env.step(dt_ns)`가 runlet `SimClock.advance(dt)`의 직접 조상.**

```python
# simple_env:
env = Env(use_sim_time=True)
env.register_node(MyNode())
env.step(dt_ns=int(0.1 * S_TO_NS))   # advance 0.1s, fire all timers due

# runlet (async 버전):
clock = SimClock()
async with Supervisor(clock=clock) as sup:
    sup.add(MyDaemon())
    await clock.advance(0.1)
```

`Env.step`은 sync, `SimClock.advance`는 async. 그 외 의미는 동일.

## 가치 제안 비교

| 어필 포인트 | simple_env | runlet |
|------------|-----------|--------|
| Pure Python | ✓ | ✓ |
| Sim-time burst replay | ✓ | ✓ |
| 작은 dependency | ✓ (loguru only) | ✓ (anyio only) |
| ROS2 형태 API | ✓ | ✓ (형태 유사, 1:1 호환 아님) |
| Sync (no async) | ✓ | ✗ |
| asyncio / trio | ✗ | ✓ |
| Structured concurrency | ✗ | ✓ |
| Cooperative shutdown | ✗ | ✓ |
| Logger sim-time aware | ✗ | ✓ ([ADR 0008](../../adr/0008-sim-aware-logging-and-supervisor-diagnostics.md)) |
| Daemon `on_stop` 보장 | ✗ | ✓ ([ADR 0009](../../adr/0009-cooperative-shutdown-and-lifecycle-guarantees.md)) |
| Channel introspection | ✗ | ✓ (`ChannelStats`) |
| Type-checked Channel | ✗ (untyped msg) | ✓ (`Channel[T]`) |
| 예외 supervision | ✗ | ✓ (3종 정책) |
| Pytest test 커버리지 | ✗ (테스트 없음) | ✓ (68+ tests, 2 backend) |

simple_env는 sync prototype, runlet은 async production-shape 라이브러리. simple_env의 핵심 가치 (sim-time burst replay first-class + minimal core)는 보존하면서, async + lifecycle 보장 + test 인프라를 추가했다.

## 채택한 디자인 결정

- **`step(dt)` / `advance(dt)` burst step API.** simple_env의 핵심 패턴.
- **Sim-time vs system-time 두 모드.** simple_env의 `SIM_TIME` / `SYSTEM_TIME` enum이 runlet의 `SimClock` / `WallClock` 분리에 영감.
- **Untyped → typed로 발전.** simple_env는 의도적으로 message type을 검증하지 않음 ("sacrifice efficiency for flexibility"). runlet은 type checker 통과 위해 typed (`Channel[T]`) 로 강화.
- **Pure Python + minimal dependency.** simple_env는 loguru 하나, runlet은 anyio 하나.

## 거부한 디자인 결정 (또는 진화)

- **Fully synchronous 모델.** simple_env의 가장 큰 제약. async/await 환경 (모던 Python ML 코드, dispatcher, eval harness) 에 끼우기 어려움. runlet은 anyio async로 전환.
- **`Env.step()` 안에서 callback 직접 호출.** simple_env는 callback이 throw하면 `Env.step()` 자체가 throw — 다른 daemon 영향. runlet은 `Supervisor.on_error` policy로 격리.
- **Topic broadcast.** simple_env는 ROS2 형태로 1:N broadcast 지원. runlet은 [ADR 0001](../../adr/0001-channel-is-the-sole-comm-primitive.md) 으로 1:1 환원.
- **Untyped message.** simple_env는 의도적 untyped. runlet은 `Channel[T]` Generic.
- **threading.RLock in Clock.** simple_env가 thread safety 위해 lock을 둠 — 그러나 실제 parallelism은 없어 vestigial. runlet은 async라 lock 불필요.

## 관찰

simple_env는 **runlet의 의도된 evolution path**다. 다음 순서:

1. **simple_env (2024)** — sync ROS-shaped prototype. 빠르게 prototyping용. 한계 (async 환경 부적합) 인지.
2. **runlet v0 (2026)** — async ROS-shaped 라이브러리. simple_env의 가치 (sim-time burst replay) 를 보존하면서 anyio + lifecycle 보장 추가.

runlet v0의 integration test (`test_three_daemon_pipeline_under_simclock`)는 명시적으로 "would runlet replace simple_env" 시나리오를 cover한다 — sensor → controller → motor 파이프라인을 SimClock burst로 replay.

simple_env는 GitHub에서 거의 비공개 prototype 형태로 머물러 있어 외부 채택 사례는 없다. runlet은 그 prototype을 외부 사용 가능한 라이브러리로 일반화한 것이며, 본 프로젝트 (reflex repo)의 evaluation harness, inference dispatcher, agent orchestration 등에 backbone으로 활용될 예정.

## Links

- 메인 레포: <https://github.com/MilkClouds/simple_env>
- runlet의 simple_env-등가 integration test:
  `projects/runlet/tests/test_integration.py::test_three_daemon_pipeline_under_simclock`
