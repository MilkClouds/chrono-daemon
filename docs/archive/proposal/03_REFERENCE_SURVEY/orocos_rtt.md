# Orocos RTT (Real-Time Toolkit)

- 출처: <https://github.com/orocos-toolchain/rtt>
- 언어: C++
- ★: 88 (2026-05 기준)
- 카테고리: Mature robotics middleware (real-time component model)
- 첫 release: 2006 (벨기에 KU Leuven), 현재 orocos-toolchain 조직 유지

## 한 줄 요약

ROS보다 오래된 robotics component framework. **Hard real-time을 의식한 component 모델 (TaskContext + Activity + Port)이 매우 성숙**하며, 산업용 manipulator 컨트롤 등 엄격한 timing이 필요한 도메인에서 채택되어 왔다.

## 핵심 추상화

| Orocos RTT 컴포넌트 | 의미 | runlet 대응 |
|---------------------|------|-------------|
| **`RTT::TaskContext`** | 핵심 component 단위. State machine (`Init`/`PreOperational`/`Stopped`/`Running`). | `Daemon` (lifecycle 더 축소) |
| **`RTT::DataPort<T>` / `BufferPort<T>` / `EventPort<T>`** | Pub/Sub 위한 typed port. Connection policy (data, buffer, lock-free 등) 다양. | `Channel[T]` (단, runlet은 buffer 하나) |
| **`RTT::Activity`** | TaskContext 실행 정책. `PeriodicActivity` / `NonPeriodicActivity` / `SlaveActivity`. | (runlet은 `anyio.TaskGroup`에 위임) |
| **`RTT::os::TimeService`** | 모노토닉 / wall clock 추상. ns precision. | `Clock` (`WallClock` / `SimClock`) |
| **`RTT::os::Timer`** | Periodic / one-shot timer. | `Clock.every(period)` |
| **`RTT::Operation` / `Method` / `Command`** | 서비스 호출 RPC primitive. | (없음 — runlet은 채널 위에 사용자가 짠다) |
| **`RTT::ConnPolicy`** | Port connection 설정 (buffer size, lock-free, pull/push). | `open_channel(maxsize=N)` |

State machine은 ROS2 lifecycle node와 비슷하지만 Orocos가 시간상 먼저 (2006). ROS2의 lifecycle 추상이 Orocos에서 영감받았다는 게 일반적 인식.

## SimClock과의 관계

**Orocos RTT는 sim-time 추상이 약하다.** `RTT::os::TimeService`는 monotonic vs wall clock 정도만 구분하고, 일괄 advance 같은 sim-clock semantics는 제공하지 않는다. Real-time 도메인에서는 wall clock이 절대적이라 sim-clock의 의미가 약하다.

이는 Orocos가 의도한 use case (산업용 controller, 실차 / 실 manipulator 정밀 timing)와 일치한다 — sim에서 fast-forward할 일이 없는 도메인.

## 가치 제안 비교

| 어필 포인트 | Orocos RTT | runlet |
|------------|------------|--------|
| Hard real-time | ✓ (Xenomai / RTAI integration) | ✗ |
| Component state machine | ✓ (4-state) | ✗ ([ADR 0005](../../../adr/0005-no-lifecycle-states-beyond-start-run-stop.md)) |
| 다양한 Port 타입 (data/buffer/event) | ✓ | ✗ (Channel 하나로 환원) |
| Activity 정책 다양성 | ✓ (periodic/non-periodic/slave) | ✗ (anyio TaskGroup에 위임) |
| Lua / XML scripting | ✓ | ✗ |
| Sim-time first-class | ✗ | ✓ |
| asyncio / trio | ✗ (C++ thread model) | ✓ |
| 작은 dependency | ✗ (Boost, ACE, ...) | ✓ |

Orocos는 hard real-time controller 도메인을 정밀하게 cover한다. runlet은 그 정밀도가 필요 없는 다른 domain (Python async, eval, agents)에서 작고 쓰기 쉬운 추상을 제공한다.

## 채택한 디자인 결정

- **Component (TaskContext)가 lifecycle 단위.** runlet `Daemon`이 같은 의미. 단 Orocos는 4-state, runlet은 3-hook.
- **Port가 typed.** runlet `Channel[T]` Generic.
- **Connection policy를 사용자가 명시.** runlet `open_channel(maxsize=N)`.

## 거부한 디자인 결정

- **4-state lifecycle machine.** ROS2 lifecycle node와 동일 이유로 거부 ([ADR 0005](../../../adr/0005-no-lifecycle-states-beyond-start-run-stop.md)) — 99% 사용 패턴은 `on_start` / `on_stop` 두 hook이면 충분.
- **다양한 Port 종류.** Orocos는 `DataPort` (latest value, 비결정적), `BufferPort` (queue, 결정적), `EventPort` 등 분리. runlet은 모두 `Channel`로 환원하고 latest-value 패턴은 [recipes/latest.py](../../../recipes.md)로 분리.
- **Activity 정책 노출.** runlet은 `anyio` task group에 위임.
- **자체 scripting language.** runlet은 Python 코드로 wiring.

## 관찰

Orocos RTT는 **real-time controller domain의 성숙한 reference**다. 20년 가까이 산업용 robotics에서 검증된 component 모델. runlet은 Orocos의 핵심 패턴 (typed port + composition unit + 명시적 connection policy)을 채택하면서, Orocos가 강조한 real-time precision과 풍부한 port semantics는 의식적으로 버렸다.

다른 axes:

- 코드베이스가 매우 dated하다 (Boost 1.x, ACE library, 2006-style C++).
- ROS2가 등장하면서 사용자가 줄었다. 현재 active development는 minor maintenance 수준.
- Python binding (`pyrtt`)이 있으나 만족스러운 형태는 아니다.

runlet의 use case가 Orocos의 도메인 (산업용 hard real-time)이 아니라 다른 도메인 (Python async ML eval / robotics control prototype)이라, Orocos를 직접 가져다 쓸 수는 없다. 디자인 reference로만 가치.

## Links

- 메인 레포: <https://github.com/orocos-toolchain/rtt>
- Orocos toolchain 문서: <https://orocos-toolchain.github.io/rtt/>
- KU Leuven 사이트: <https://www.orocos.org/>
