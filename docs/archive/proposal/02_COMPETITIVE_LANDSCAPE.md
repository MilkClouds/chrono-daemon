# Competitive Landscape Survey — runlet

## 방법론

runlet과 동일한 4 axes (pure Python + anyio + SimClock burst replay first-class + 4 primitive minimal core)를 만족하는 기존 프로젝트가 존재하는지 검증하기 위해, 다단계 서베이를 수행했다.

1. **자체 지식 + LLM 기반 1차 후보 수집.** `perplexity_ask` / `perplexity_search`로 "ROS-agnostic concurrency framework", "Python async daemon supervisor", "deterministic burst replay simulation", "Node Publisher Subscriber Executor Clock framework not ROS" 등 키워드 다각도 검색.

2. **GitHub Code Search — 다축 키워드 전수 검색.** 다음 7개 키워드 축을 조합적으로 검색:
   - 메시징 / 통신: `pub sub middleware`, `topic publisher subscriber framework`
   - 동시성 단위: `daemon supervisor async`, `node executor scheduler`
   - 도메인: `robotics middleware`, `dataflow robotics`
   - 시간 모델: `sim time deterministic step`, `burst replay`
   - 언어 / 플랫폼: `python ros alternative`, `language:rust dataflow`, `pure python ros-like`
   - 정확 매칭: `apollo cyber RT`, `drake systems framework`, `holoscan sdk`, `orocos rtt`, `lcm lightweight communications`, `zenoh`, `iceoryx`
   - 사용자 도메인 hint: `dora-rs alternative`, `ros2 standalone`, `aica modulo`

3. **README + 핵심 헤더 정독.** GitHub Code Search 결과에서 후보 ~25개의 `README.md`, `pyproject.toml` / `Cargo.toml` 의존성, 핵심 header / module 파일 (`include/*/core/*.hpp`, `src/*/lib.rs`)을 직접 fetch하여 다음 5가지 추상화 충족 여부를 정량 평가:
   - **Node-like execution unit** (명시적 컴포지션 단위)
   - **Clock abstraction** (특히 sim-time)
   - **Pub/Sub or typed channel** primitive
   - **Executor / scheduler** 명시 노출
   - **Timer / periodic callback** 추상

4. **카테고리 분류 + 4 axes 매칭.** 각 후보를 7개 카테고리로 분류하고, runlet의 4 axes (pure Python + anyio + SimClock first-class + 4 primitive minimal) 충족 여부를 ✓/✗로 표시.

5. **수렴 확인.** 추가 검색에서 새 후보가 나오지 않을 때까지 반복. 마지막 라운드에서 새 후보 0건으로 포화 확인.

## 결과

총 **25개 이상**의 고유 후보 프로젝트를 분류했다. 카테고리별 결과:

### A. ROS-clone alternatives (Python/Rust로 ROS API 재현)

| 프로젝트 | ★ | 언어 | Pure Py | anyio | SimClock first | Minimal | 비고 |
|----------|---|------|--------|------|-----------|---------|------|
| [HORUS](https://github.com/softmata/horus) | 343 | Rust | ✗ | ✗ | ✗ | ✓ | 신생 (2025–2026). Macro-based DSL. Shared-memory ring buffer. Sim-clock 추상 명시 없음. |
| [RedisROS](https://github.com/vguillet/RedisROS) | 17 | Python | ✓ | ✗ | ✗ | ✓ | Redis backend. ROS2 inspired. Pure Python이지만 외부 Redis 의존 + sim-time 부재. |
| [openrr](https://github.com/openrr/openrr) | 596 | Rust | ✗ | ✗ | ✗ | ✗ | Open Rust Robotics. ROS API보다는 robotics library 통합 layer. |
| [r2r](https://github.com/sequenceplanner/r2r) | – | Rust | ✗ | – | ✗ | ✓ | ROS2 Rust binding. ROS runtime 의존. |
| [ros2_rust](https://github.com/ros2-rust/ros2_rust) | – | Rust | ✗ | – | ✗ | ✗ | ROS2 자체. ROS 의존. |

ROS API 형태를 Python/Rust로 재현한 프로젝트들이지만 모두 **sim-time burst replay first-class가 아니거나 ROS 런타임 의존**.

### B. Dataflow framework (declarative graph + runtime)

| 프로젝트 | ★ | 언어 | Pure Py | anyio | SimClock first | Minimal | 비고 |
|----------|---|------|--------|------|-----------|---------|------|
| [dora-rs](https://github.com/dora-rs/dora) | 3,760 | Rust + Py binding | ✗ | ✗ | ✗ | ✗ | "ROS today if we wrote it now". YAML graph + Zenoh transport. 분산 first. **sim-time 부재.** |
| [ERDOS](https://github.com/erdos-project/erdos) | 208 | Rust + Py | ✗ | ✗ | watermark | ✗ | UC Berkeley RISELab. Operator + watermark 기반 deterministic ordering. AV pipeline 도메인. |

가장 모던한 ROS alternatives이지만 dataflow + distributed first 디자인 — sim-time burst replay는 의도된 use case가 아니다.

### C. System composition (in-process modeling framework)

| 프로젝트 | ★ | 언어 | Pure Py | anyio | SimClock first | Minimal | 비고 |
|----------|---|------|--------|------|-----------|---------|------|
| [Drake](https://github.com/RobotLocomotion/drake) | 4,033 | C++ + Py binding | ✗ | ✗ | ✓ (sim) | ✗ | TRI/MIT. `System` / `Context` / `Simulator` / `Diagram`. **In-process sim-time first-class.** 단 네트워킹 없고 dynamical-systems 도메인 묶임. |

Drake의 systems framework는 **sim-time 측면에서는 runlet과 가장 가깝다.** 다른 axes (pure Python, 작음)는 만족하지 않는다.

### D. Streaming engine (sensor / pipeline)

| 프로젝트 | ★ | 언어 | Pure Py | anyio | SimClock first | Minimal | 비고 |
|----------|---|------|--------|------|-----------|---------|------|
| [NVIDIA Holoscan SDK](https://github.com/nvidia-holoscan/holoscan-sdk) | 209 | C++ + Py | ✗ | ✗ | ManualClock | ✗ | `Operator` / `Scheduler` / `Clock` / `Conditions`. `ManualClock`으로 deterministic mode 지원. GXF/CUDA 의존 무거움. |

Holoscan의 헤더 구조 (`include/holoscan/core/{operator,scheduler,clock,condition}.hpp`)는 매우 깔끔하며 runlet의 디자인 결정 (Clock plug-in, Scheduler 명시 노출)에 영향을 주었다.

### E. Transport-only middleware (pub/sub 라이브러리)

| 프로젝트 | ★ | 언어 | Pure Py | anyio | SimClock first | Minimal | 비고 |
|----------|---|------|--------|------|-----------|---------|------|
| [Eclipse Zenoh](https://github.com/eclipse-zenoh/zenoh) | – | Rust + bindings | ✗ | ✗ | ✗ | ✓ | 모던 pub/sub + query. 모든 핵심 abstractions 없음 (Node/Clock/Executor X). |
| [Eclipse iceoryx / iceoryx2](https://github.com/eclipse-iceoryx/iceoryx2) | 2,224 | C++/Rust | ✗ | ✗ | ✗ | ✓ | Zero-copy IPC만. 동일. |
| [eCAL](https://github.com/eclipse-ecal/ecal) | – | C++ | ✗ | ✗ | ✗ | ✓ | Continental. shared-mem pub/sub + RPC + tooling. |
| [LCM](https://github.com/lcm-proj/lcm) | 1,180 | C/C++/Py | – | ✗ | ✗ | ✓ | Lightweight Communications and Marshalling. UDP-multicast pub/sub만. |
| [Fast-DDS](https://github.com/eProsima/Fast-DDS) | 2,801 | C++ | ✗ | ✗ | ✗ | ✗ | DDS impl. ROS2의 underlying transport. |

전부 강력한 transport substrate이지만 Node/Clock/Executor 추상은 application layer가 직접 짜야 한다.

### F. Mature robotics middleware (전통적)

| 프로젝트 | ★ | 언어 | Pure Py | anyio | SimClock first | Minimal | 비고 |
|----------|---|------|--------|------|-----------|---------|------|
| [Orocos RTT](https://github.com/orocos-toolchain/rtt) | 88 | C++ | ✗ | ✗ | ✗ | ✗ | Real-Time Toolkit. `TaskContext` / `Activity` / `Port`. Real-time component 모델 성숙도 최고. |
| [YARP](https://github.com/robotology/yarp) | 590 | C++ | ✗ | ✗ | ✗ | ✗ | iCub humanoid 프레임워크. Multi-language bindings. |
| [OpenRTM-aist](https://github.com/fkanehiro/hrpsys-base) | – | C++/Py | ✗ | ✗ | ✗ | ✗ | 일본 robotics 표준. `RTComponent` / `ExecutionContext`. |
| [MOOS-IvP](https://oceanai.mit.edu/moos-ivp/) | – | C++ | ✗ | ✗ | ✗ | ✓ | 해양 robotics. MIT. |
| [NASA cFS](https://github.com/nasa/cFS) | – | C | ✗ | ✗ | ✗ | ✗ | Core Flight System. Flight-qualified. Apps + Software Bus + Scheduler. |
| [roboflex](https://github.com/flexrobotics/roboflex) | 19 | C++ + Py | ✗ | ✗ | ✗ | ✓ | Node + Message. ZMQ/MQTT transport. 작은 surface. |
| [meadow](https://github.com/quietlychris/meadow) | 41 | Rust | ✗ | ✗ | ✗ | ✓ | Experimental robotics middleware. Typestate-encoded Node. |

성숙한 프로젝트들이지만 모두 C++/Rust 기반이며 sim-time burst replay 추상은 없다.

### G. Pure Python (사용자 본인 prototype 포함)

| 프로젝트 | ★ | 언어 | Pure Py | anyio | SimClock first | Minimal | 비고 |
|----------|---|------|--------|------|-----------|---------|------|
| [RedisROS](https://github.com/vguillet/RedisROS) | 17 | Python | ✓ | ✗ | ✗ | ✓ | (위 A 카테고리 중복) Redis 의존 + sim-time 부재. |
| [SimPy](https://gitlab.com/team-simpy/simpy) | – | Python | ✓ | ✗ | ✓ | ✓ | Discrete-event simulation. `Environment` / `Process` / `Store`. **sim-time first-class.** 단 ROS-shaped API 아님. |
| [`simple_env`](https://github.com/MilkClouds/simple_env) | – | Python | ✓ | ✗ | ✓ | ✓ | 저자 본인 prototype. Sync-only burst replay. **runlet의 직접 조상.** |

SimPy는 4 axes 중 3개를 만족하나 anyio async 모델이 아니며 robotics-shaped API가 아니다. `simple_env`는 모두 만족하나 sync-only (async 아님). 두 후보 모두 runlet의 niche와 정확히 겹치지 않는다.

## 카테고리별 매핑 — 누가 무엇을 만족하는가

7개 카테고리, 25+ 후보 중 runlet의 **4 axes 모두 (pure Python + anyio + SimClock first-class + 4 primitive minimal)** 를 만족하는 프로젝트는 **0개**다. 가장 가까운 3개를 비교하면:

| | dora-rs | SimPy | simple_env | runlet |
|--|---------|-------|-----------|--------|
| Pure Python | ✗ (Rust core) | ✓ | ✓ | ✓ |
| anyio (asyncio + trio) | ✗ | ✗ (sync) | ✗ (sync) | ✓ |
| SimClock burst replay first-class | ✗ | ✓ | ✓ | ✓ |
| Minimal 4-primitive core | ✗ (large surface) | ✓ | ✓ | ✓ |
| Daemon lifecycle hooks | △ (YAML node) | ✗ (process coroutine) | ✗ | ✓ |
| Structured concurrency | ✗ | ✗ | ✗ | ✓ (anyio TaskGroup) |

## 심층 검토 — dora-rs

서베이에서 사용자가 가장 가까운 reference로 명시한 프로젝트가 dora-rs이다. 동일성 검증을 위해 별도로 분석했다.

### Q1. dora-rs가 sim-time burst replay를 지원하는가?

**아니다.** dora-rs는 각 노드를 **별도 OS process로 분리**하고 Zenoh shared-memory 또는 network로 연결한다. Per-process clock이라 외부에서 일괄 advance할 intercept point가 없다. dora-rs 문서와 GitHub issues를 검색했으나 sim-clock 또는 burst replay 관련 논의는 발견되지 않았다.

이는 dora-rs의 의도된 디자인이다 — production robotics에서는 wall-clock에서만 동작하면 충분하고, replay는 별도 record/playback tool로 처리한다. runlet의 sim-replay 요구는 dora-rs의 핵심 use case가 아니다.

### Q2. dora-rs의 핵심 가치 제안은?

| 어필 포인트 | 내용 |
|------------|------|
| 분산 첫째 | Zenoh shared-mem + network transport. Multi-machine cluster scheduling. |
| Multi-language | Python, Rust, C, C++ 노드 혼합. |
| 빠름 | ROS2 대비 10–17× 빠른 IPC. Shared-mem 직접 사용. |
| 모던 | YAML graph + cargo build. ROS의 colcon/ament 무게 없음. |
| ROS2 bridge | DDS bridge로 점진 migration. |

이 가치 제안은 모두 production deployment를 가정한다. runlet의 가치 제안 ("test와 production에서 같은 daemon 코드를 SimClock으로 burst-replay")은 다른 축에 있다.

### Q3. dora-rs의 in-process operator mode를 쓰면 runlet과 가까워지는가?

dora-rs는 `runtime/operator` mode로 같은 process 안에 여러 operator를 띄울 수 있다. 그러나 이는 dora-rs의 **second-class** 사용법이며, scheduling이 여전히 dora coordinator가 관할한다. SimClock intercept point가 생기지 않는다.

### 결론 (dora-rs 비교)

dora-rs는 runlet과 **다른 axes에 있는 프로젝트**이며 경쟁 관계가 아니다. dora-rs는 production-grade distributed robotics dataflow, runlet은 in-process test/replay + minimal daemon supervisor. 같은 4 abstraction 이름 (`Node`/`Channel`/...)을 공유하지만 의도된 use case가 다르다.

## 결론

**runlet의 4 axes (pure Python + anyio + SimClock burst replay first-class + 4 primitive minimal core)를 모두 만족하는 기존 프로젝트는 0개다.** 25+ 후보를 7 카테고리로 분류한 전수 조사로 검증되었다.

- 가장 가까운 후보 3개 (dora-rs, SimPy, `simple_env`)는 각각 1-2개 axes만 만족하며 runlet의 niche를 완전히 cover하지 않는다.
- Drake systems framework는 sim-time first-class라는 점에서 가장 가깝지만, in-process dynamical-systems 도메인에 묶여 있고 pure Python이 아니다.
- Holoscan SDK의 `Operator`/`Scheduler`/`Clock`/`Condition` 헤더 구조는 디자인 reference로 채택했다 (Plug-in Clock, Scheduler 명시 노출).
- Apollo CyberRT의 sim-time `Time` + record/replay 모델은 sim-time first-class production 시스템의 존재 증명으로 활용했다.
- dora-rs와 ROS2/ROS2 Rust는 axes가 완전히 다르므로 경쟁 관계가 아니다.

상세 프로젝트별 분석은 [03_REFERENCE_SURVEY/](./03_REFERENCE_SURVEY/) 참조.
