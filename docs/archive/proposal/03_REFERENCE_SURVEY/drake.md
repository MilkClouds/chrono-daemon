# Drake systems framework

- 출처: <https://github.com/RobotLocomotion/drake>
- 언어: C++ 코어 + Python bindings (`pydrake`)
- ★: 4,033 (2026-05 기준)
- 카테고리: System composition (in-process modeling framework)
- 첫 release: 2014 (MIT Robot Locomotion Group), 현재 Toyota Research Institute (TRI) 주관

## 한 줄 요약

Drake는 dynamical-systems modeling, optimization, controls를 위한 modeling framework다. `systems/framework` 디렉터리에 있는 **`System` / `Context` / `Simulator` / `Diagram` 추상이 chrono-daemon의 핵심 아이디어와 가장 가깝다** — sim-time이 first-class이고, 모든 시스템이 in-process로 deterministic하게 advance된다.

## 핵심 추상화

| Drake 컴포넌트 | 의미 | chrono-daemon 대응 |
|----------------|------|-------------|
| **`drake::systems::System<T>`** | Composable dynamical block. Input/output port + continuous/discrete state. | `Daemon` (단, 함수형 — `Calc` callback 중심) |
| **`drake::systems::Diagram<T>`** | Multiple `System`을 묶은 composite system. | `Supervisor` (담는 컨테이너) |
| **`drake::systems::Context<T>`** | 한 instant의 system state (time, continuous state, discrete state, parameters). | chrono-daemon `Context` (concept만 유사 — Drake는 더 풍부) |
| **`drake::systems::Simulator<T>`** | `Diagram`을 시간 축에서 advance하는 executor. `AdvanceTo(t)` / `Initialize()`. | `Supervisor` + `SimClock` |
| **`InputPort` / `OutputPort`** | System 간 데이터 연결 (in-process Eigen value, not networked). | `Channel[T]` (단, chrono-daemon은 queue, Drake는 instantaneous value) |
| **`PeriodicEvent` / `DiscreteUpdate`** | 주기적 event handler. | `Clock.every(period)` async iterator |
| **`AbstractValue`** | Type-erased port 값. | Python typed object (runtime check 없음) |

`Simulator::AdvanceTo(t)`는 정확히 chrono-daemon의 `SimClock.advance_to(t)` 패턴이다 — 가상 시간을 일괄 진행하면서 deadline에 도달한 모든 system을 결정론적으로 fire한다.

## SimClock과의 관계

**Drake는 sim-time first-class의 가장 깔끔한 reference다.** `Context::get_time()`이 system 상의 유일한 시간 source이며, `Simulator`만이 이를 advance할 수 있다. 모든 system은 `Context`를 통해 시간을 읽기에 wall-clock에 의존하지 않는다.

chrono-daemon의 디자인 결정 중 다음이 Drake에서 직접 영감받았다:

- **시계가 plug-in이고 daemon이 그것을 통해서만 시간을 읽는다.** Drake의 `Context::get_time()` ↔ chrono-daemon의 `ctx.clock.now()`.
- **외부 driver가 시간을 advance한다.** Drake의 `Simulator::AdvanceTo(t)` ↔ chrono-daemon의 `SimClock.advance_to(t)`.
- **In-process composition이 first-class.** Drake는 distributed가 아니라 단일 process 내에서 system들을 묶는다. chrono-daemon도 v0에서 동일.

## 가치 제안 비교

| 어필 포인트 | Drake | chrono-daemon |
|------------|-------|--------|
| Sim-time first-class | ✓ | ✓ |
| In-process composition | ✓ | ✓ |
| Continuous-time integration (ODE) | ✓ | ✗ (discrete event only) |
| Convex optimization / MathematicalProgram | ✓ | ✗ |
| URDF / SDF parsing + multibody dynamics | ✓ | ✗ |
| Network pub/sub | ✗ (in-process port only) | ✗ (v0) / 예정 |
| Asyncio / trio backend | ✗ (C++ synchronous) | ✓ |
| Pure Python | ✗ (C++ core) | ✓ |
| 작은 의존성 | ✗ (Eigen, SNOPT, IPOPT, MOSEK, Bazel) | ✓ |
| Robotics-domain dynamic 도메인 묶임 | ✓ | ✗ |

Drake는 dynamics + control modeling framework이며 모든 가치 제안이 그 도메인에 묶여 있다. chrono-daemon은 그 일부 ("composition + sim-time advance") 만을 추출해 robotics 외 도메인 (ML eval, agent orchestration)에서도 쓸 수 있게 만든 것.

## 채택한 디자인 결정

- **`Clock` plug-in.** Drake의 `Context::get_time` 패턴.
- **`Simulator::AdvanceTo(t)` shape.** chrono-daemon의 `SimClock.advance_to(t)`가 같은 의미. burst step 가능.
- **In-process composition first-class.** v0에서 distributed를 의식적으로 미룬 것 ([ADR 0006](../../../adr/0006-in-process-v0-transport-adapter-slot.md)).
- **`System` = composable unit.** chrono-daemon의 `Daemon`도 동일 — composition unit이 단일 추상.

## 거부한 디자인 결정

- **Continuous-time ODE integration.** Drake는 RungeKutta integrator 등으로 continuous state를 advance. chrono-daemon은 discrete event only.
- **`InputPort` / `OutputPort` = instantaneous value.** Drake는 port가 "현재 시점의 값"이지 queue가 아니다. system이 다른 system의 output을 읽으면 그 instant의 값을 계산한다. chrono-daemon은 그 반대 — `Channel`은 명시적 queue.
- **`Context`가 풍부한 state 모델.** Drake `Context`는 continuous state, discrete state, abstract state, parameters를 모두 들고 있다. chrono-daemon `Context`는 daemon 1개에 대한 lifecycle metadata (clock, cancel_scope, logger, name)만.
- **Type-erased `AbstractValue`.** chrono-daemon은 `Channel[T]` Generic으로 정적 type check.

## 관찰

Drake systems framework는 chrono-daemon의 **sim-time + composition 측면에서 가장 가까운 reference**다. 다만 Drake는 dynamical-systems modeling을 위해 만들어졌고, 그 도메인의 추가 추상 (port = instantaneous value, Context = state vector, Simulator = ODE integrator)이 모두 들어 있다.

chrono-daemon은 그 핵심 골격 (composition + sim-time advance + in-process)만 떼어내, robotics dynamics가 아닌 일반 동시성 / async daemon 컨텍스트로 옮긴 것이다. 이는 simple_env가 이미 sync 환경에서 시도했던 작업의 async 버전이다.

`pydrake`로 Python에서 Drake를 쓸 수는 있으나, Bazel build + Eigen / SNOPT / IPOPT 등 의존성이 무거워 "작은 라이브러리 끼워 쓰기" use case에 부적합하다. chrono-daemon은 이 단점을 정확히 보완한다.

## Links

- Drake 메인 사이트: <https://drake.mit.edu/>
- `systems/framework` 디렉터리: <https://github.com/RobotLocomotion/drake/tree/master/systems/framework>
- Drake systems framework 튜토리얼: <https://drake.mit.edu/doxygen_cxx/group__systems.html>
