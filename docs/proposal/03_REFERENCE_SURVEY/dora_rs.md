# dora-rs

- 출처: <https://github.com/dora-rs/dora>
- 언어: Rust 코어 + Python / C / C++ 노드 binding
- ★: 3,760 (2026-05 기준)
- 카테고리: Dataflow framework (분산 first)
- 첫 release: 2022-02, 활발한 개발 중

## 한 줄 요약

> "If we were to rewrite ROS today, what would it look like?" — Xavier Tao, dora-rs 저자 (2022)

dora-rs는 ROS의 분산 dataflow 모델을 모던 Rust + Zenoh shared-memory transport로 재설계한 프로젝트다. ROS2 대비 10–17× 빠른 IPC, YAML 기반 declarative graph, multi-language 노드 지원이 핵심 셀링 포인트.

본 프로젝트의 시작점에서 "가장 가까운 reference"로 사용자가 명시했으나, **runlet과는 의도된 axes가 다르다** (분산 production vs in-process test/replay).

## 핵심 추상화

| dora-rs 컴포넌트 | 의미 | runlet 대응 |
|------------------|------|-------------|
| **Node** | 별도 OS process. 입력/출력 ID로 식별. | `Daemon` (in-process task) |
| **Operator** | 같은 process 안의 lighter-weight unit. **second-class.** | `Daemon` (decorated 형태) |
| **Dataflow YAML** | 노드 간 wiring을 declarative하게 기술 | 코드로 명시 (`sup.add` + Channel 변수) |
| **Coordinator** | 분산 노드 lifecycle 관할 | `Supervisor` |
| **Daemon** | (dora 용어) 각 머신에서 노드를 띄우는 supervisor | (없음 — runlet은 multi-process 없음) |
| **Zenoh transport** | Shared-memory 우선 + network fallback | `anyio.create_memory_object_stream` (in-process only) |
| **Inputs / Outputs** | YAML로 wiring. 노드는 ID로 받음. | typed `Channel[T]` 변수 |
| **Tick events** | `dora/timer/millis/100` 같은 가상 ID로 timer 입력 받음 | `Clock.every(period)` |
| **Apache Arrow data format** | 노드 간 메시지 포맷 | typed Python object (in-process pickle 불필요) |

dora-rs는 노드를 별도 OS process로 분리하는 게 디폴트이며, in-process operator는 second-class 사용법이다.

## SimClock과의 관계

**dora-rs는 sim-time / burst replay 추상이 없다.**

- 각 노드가 별도 OS process라 시계가 OS-level. 외부에서 일괄 advance할 intercept point가 없다.
- Tick event (`dora/timer/millis/100`)는 wall-clock의 dora daemon이 생성한다. Simulation에서 시계를 일괄 진행하는 API가 없다.
- ROS bag 같은 record/replay tool이 별도로 있지만, "test에서 사용 시 코드 동일 + sim 시계로 burst" 패턴은 의도된 use case가 아니다.

dora-rs README, FOSDEM 2024 슬라이드, 첫 release blog에서 sim-clock / burst-replay 관련 키워드 검색했으나 0건. dora-rs는 production deployment를 가정한 설계다.

## 가치 제안 비교

| 어필 포인트 | dora-rs | runlet |
|------------|---------|--------|
| 분산 multi-machine | ✓ (Zenoh + SSH cluster scheduling) | ✗ (v0 in-process only, [ADR 0006](../../adr/0006-in-process-v0-transport-adapter-slot.md) slot 보유) |
| Multi-language | ✓ (Python / Rust / C / C++) | ✗ (Python only) |
| 빠른 IPC | ✓ (10–17× faster than ROS2) | ✓ (anyio in-memory) |
| YAML graph | ✓ | ✗ (코드로 명시) |
| ROS2 bridge | ✓ (DDS) | ✗ |
| Hot reload (Python) | ✓ | ✗ |
| Sim-time burst replay | ✗ | ✓ |
| asyncio + trio backend | ✗ (Rust runtime) | ✓ |
| Pure Python | ✗ (Rust core + binding) | ✓ |
| Structured concurrency | ✗ | ✓ (anyio TaskGroup) |
| Daemon `on_stop` lifecycle 보장 | ✗ (process 종료에 의존) | ✓ ([ADR 0009](../../adr/0009-cooperative-shutdown-and-lifecycle-guarantees.md)) |

dora-rs는 **production distributed dataflow**, runlet은 **in-process test/replay + minimal daemon supervisor**다. 이름 (`Node`, `Channel`, ...)이 겹치는 부분이 많지만 의도된 use case가 다르다.

## 채택한 디자인 결정

- **노드 = lifecycle unit이라는 개념.** dora-rs의 노드 추상이 runlet의 `Daemon`과 형태상 같다. 단 dora-rs는 OS process, runlet은 async task.
- **명시적 dataflow wiring.** runlet은 YAML 대신 코드 변수로 wiring하지만, "consumer가 producer를 명시적으로 참조"라는 핵심 원칙은 공유한다.
- **Channel과 transport의 분리.** dora-rs가 Zenoh를 transport adapter로 둔 것처럼, runlet도 `Channel` Protocol slot을 [ADR 0006](../../adr/0006-in-process-v0-transport-adapter-slot.md)으로 reserved.

## 거부한 디자인 결정

- **YAML graph declaration.** runlet은 코드로 wiring한다. YAML는 graph diff 가독성 / hot reload 측면에서 이점이 있으나, type checker 통과 / refactor 친화성 / IDE jumping에서 손해. Use case가 distributed deployment가 아니라 in-process test/replay라 trade-off가 다르다.
- **노드 = 별도 OS process.** runlet은 v0에서 in-process만. 분산은 v0.x로 미뤘다 ([ADR 0006](../../adr/0006-in-process-v0-transport-adapter-slot.md)).
- **Apache Arrow message format.** in-process에서 직렬화 cost는 0이어야. 사용자가 Python object를 그대로 전달한다.

## 관찰

dora-rs는 매우 active한 프로젝트이며 사용자 layer (Python binding) 도 깔끔하다. 만약 우리 use case가 "multi-machine robotics deployment with VLA inference"였다면 dora-rs가 자연스러운 선택지다. runlet의 use case는 그렇지 않다:

- **#191의 `LocalS{1,2}Service`** 같은 시나리오는 같은 process에서 inference + 시뮬레이션을 결정론적으로 burst-replay하는 게 valuable. dora-rs는 거의 정의상 inference를 별도 process로 분리하므로 이 패턴이 안 맞다.
- **eval rollout worker 100개 띄우기** 시나리오에서는 각 worker가 별도 SimClock에서 결정론적으로 돌아야. dora-rs의 process-per-node 모델로는 sim-clock 동기화가 안 된다.

따라서 dora-rs는 runlet의 **경쟁 프로젝트가 아니라 인접 프로젝트**다. v0.x에서 multi-process transport adapter를 만들 때 dora-rs의 Zenoh 사용 패턴을 참고할 수 있다.

## Links

- README: <https://github.com/dora-rs/dora/blob/main/README.md>
- FOSDEM 2024 slides: <https://archive.fosdem.org/2024/events/attachments/fosdem-2024-3225-dora-rs-simplifying-robotics-stack-for-next-gen-robots/slides/22303/dora-fosdem_05S0HAi.pdf>
- DORA paper (arXiv): <https://arxiv.org/pdf/2602.13252.pdf>
- User guide: <https://dora-rs.ai/dora/>
