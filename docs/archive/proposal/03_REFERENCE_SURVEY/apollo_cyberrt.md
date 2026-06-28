# Apollo CyberRT

- 출처: <https://github.com/ApolloAuto/apollo/tree/master/cyber>
- 언어: C++ 코어 + Python tooling
- 카테고리: ROS-clone alternative (자율주행 production-grade)
- 첫 release: 2018 (Apollo 3.5와 함께 도입)

## 한 줄 요약

Baidu의 자율주행 스택 Apollo가 ROS1을 대체하기 위해 만든 in-house middleware. **sim-time과 record/replay가 first-class인 ROS-shaped 추상화의 production 예시.**

## 핵심 추상화

| CyberRT 컴포넌트 | 의미 | chrono-daemon 대응 |
|------------------|------|-------------|
| **`apollo::cyber::Node`** | Component composition unit. `CreateWriter` / `CreateReader` / `CreateClient` / `CreateService`. | `Daemon` |
| **`apollo::cyber::Component`** | Node의 wrapper. `Init()` / `Proc(msg)` lifecycle. DAG로 조립. | `Daemon` (class form) |
| **`apollo::cyber::Writer<T>` / `Reader<T>`** | Pub/Sub. Channel 위에 bound. | `Channel[T].send` / `Channel[T].recv` (단, 1:N broadcast가 first-class) |
| **`apollo::cyber::Clock` / `Time`** | System time과 simulation time을 일관된 API로 추상화. `Clock::Now()` / `Clock::Mode()`. | `Clock` (`WallClock` / `SimClock`) |
| **`apollo::cyber::Timer`** | Periodic callback. | `Clock.every(period)` |
| **`cyber::scheduler::Scheduler`** | Component DAG를 thread/core에 매핑. `classic` / `choreography` 두 정책. | `Supervisor` (정책은 명시 안 함, anyio task group 위임) |
| **Channel (topic)** | 메시지 라우팅 단위. | `Channel[T]` |
| **Record / Replay** | `cyber_recorder` / `cyber_record` — bag 형식으로 wall-clock 또는 sim-time replay. | (없음 — chrono-daemon은 라이브러리 layer만) |

## SimClock과의 관계

**CyberRT는 sim-time과 record/replay를 production-quality로 제공한다.** Apollo의 시뮬레이션 환경 (LGSVL, Carla integration)과 실차 운영 환경이 동일한 CyberRT API를 공유하며, 시계 모드만 바뀐다.

- `apollo::cyber::Clock::Mode()` 가 `CYBER_LAUNCH_MODE_MOCK_TIME`이면 모든 `Time::Now()` / `Sleep()` 호출이 mock clock에서 시간을 가져온다.
- `cyber_record` bag을 replay하면 publish timestamp가 mock clock으로 흐른다.

chrono-daemon의 `Clock` plug-in (`WallClock` vs `SimClock`)은 이 패턴의 직접 영감원이다. 다만 chrono-daemon은 단일 process in-memory, CyberRT는 분산 + record/replay 인프라까지.

## 가치 제안 비교

| 어필 포인트 | Apollo CyberRT | chrono-daemon |
|------------|---------------|--------|
| 자율주행 production 검증 | ✓ (Baidu Apollo 차량 fleet) | ✗ |
| Sim-time first-class | ✓ | ✓ |
| Record / replay 인프라 | ✓ (`cyber_record`) | ✗ |
| Shared-memory transport | ✓ | ✓ (anyio in-process) |
| Static DAG composition | ✓ (Component DAG) | ✗ (코드로 동적) |
| 무거운 build system | ✓ (Bazel + Apollo monorepo) | ✗ (uv + pyproject) |
| 자율주행 도메인 묶임 | ✓ (frame_id, perception/planning/control 의존) | ✗ |
| 다른 도메인에서 떼어 쓰기 | ✗ (Apollo runtime 의존) | ✓ |

CyberRT는 axes 중 **sim-time first-class**라는 가장 중요한 axis를 만족하지만, "작고 떼어 쓰기 쉬운 Python 라이브러리"라는 axis를 만족하지 않는다.

## 채택한 디자인 결정

- **`Clock`이 plug-in이다.** 같은 daemon 코드가 production (`WallClock`)과 test (`SimClock`)에서 동작. CyberRT의 `Clock::Mode()` 패턴을 따른다.
- **Time abstraction이 ns 단위.** `simple_env`도 ns 단위였고, CyberRT의 `apollo::cyber::Time`도 ns. chrono-daemon은 그러나 `float seconds`를 채택 — Python idiom (`asyncio.sleep(0.1)`)과 일관성. ns precision은 사용자가 필요시 wrap.
- **`Timer` = Clock 위의 sugar.** CyberRT의 `Timer`도 별도 primitive지만 사실상 `Clock` + callback. chrono-daemon은 `Clock.every(period)` async iterator로 통합.

## 거부한 디자인 결정

- **Component DAG composition.** CyberRT는 static DAG (`Component` + `dag_xml`)로 시스템을 조립. chrono-daemon은 코드로 동적 wiring. trade-off는 dora-rs YAML과 동일.
- **자체 record/replay 인프라.** chrono-daemon의 v0 scope 밖. 사용자가 필요시 daemon 하나로 `Channel`을 tap하면 됨.
- **Service / RPC primitive.** CyberRT는 `Client` / `Service`도 first-class. chrono-daemon은 채널 위에 사용자가 짠다 ([ADR 0001](../../../adr/0001-channel-is-the-sole-comm-primitive.md), [recipes/batcher.py](../../../recipes.md)).
- **Topic broadcast.** CyberRT의 `Writer<T>` / `Reader<T>`는 1:N broadcast가 first-class. chrono-daemon은 1:1 + 명시적 fanout ([ADR 0001](../../../adr/0001-channel-is-the-sole-comm-primitive.md)).

## 관찰

Apollo CyberRT는 **chrono-daemon의 가치 제안 (sim-time first-class) 이 production에서 정당화된다는 증명**이다. Baidu가 ROS1을 버리고 CyberRT를 만든 이유 중 핵심이 "sim과 production에서 같은 코드가 도는가"였고, 이를 위해 sim-time API를 first-class로 가져갔다.

chrono-daemon은 이 가치를 작은 Python 라이브러리 형태로 추출했다. 차량 자율주행 도메인에 묶이지 않고, Apollo monorepo / Bazel build 없이, anyio 위에 얇게.

CyberRT의 코드는 Apollo 전체 레포 (~5 GB) 안에 있어 standalone build가 까다롭다. 별도 fork `yiakwy-mapping-team/cybertron`이 standalone 빌드를 시도하나 잘 maintained 되지 않는다. 이는 CyberRT의 코어 가치 (sim-time first-class)와 production 의존성이 분리되지 않은 결과 — chrono-daemon은 이 분리를 의도적으로 강제한다.

## Links

- Apollo 메인 레포: <https://github.com/ApolloAuto/apollo>
- CyberRT 디렉터리: <https://github.com/ApolloAuto/apollo/tree/master/cyber>
- Standalone fork: <https://github.com/yiakwy-mapping-team/cybertron>
- CyberRT 디자인 문서 (Apollo wiki): <https://apollo.baidu.com/community/article/1130>
