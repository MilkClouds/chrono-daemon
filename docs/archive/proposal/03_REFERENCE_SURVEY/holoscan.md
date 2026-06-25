# NVIDIA Holoscan SDK

- 출처: <https://github.com/nvidia-holoscan/holoscan-sdk>
- 언어: C++ 코어 + Python binding
- ★: 209 (2026-05 기준)
- 카테고리: Streaming engine (sensor / pipeline)
- 첫 release: 2022-06, NVIDIA가 의료영상 / 엣지 AI 도메인으로 시작

## 한 줄 요약

NVIDIA의 sensor AI streaming framework. `Operator` / `Scheduler` / `Clock` / `Condition` / `Fragment` 5종 추상을 깔끔하게 분리한 헤더 구조가 인상적. **runlet의 모듈 분리 패턴은 Holoscan 헤더 구조에서 직접 영감받았다.**

## 핵심 추상화

| Holoscan 컴포넌트 | 의미 | runlet 대응 |
|-------------------|------|-------------|
| **`Operator`** | 처리 단위 (node). `setup(spec)` / `compute(op_input, op_output, context)`. | `Daemon` |
| **`Fragment`** | Operator들을 묶은 application 또는 sub-graph. Multi-fragment는 별도 process로 분산. | `Supervisor` (단, runlet은 in-process only) |
| **`Scheduler`** | Operator 실행 정책. `GreedyScheduler` / `MultiThreadScheduler` / `EventBasedScheduler`. | (runlet은 `anyio.create_task_group`에 위임. 정책은 별도 노출 없음) |
| **`Clock`** | `RealtimeClock` / `ManualClock`. ManualClock으로 deterministic mode. | `WallClock` / `SimClock` |
| **`Condition`** | Operator 실행 트리거. `MessageAvailableCondition` / `PeriodicCondition` / `DownstreamMessageAffordableCondition` 등. | (없음 — runlet은 사용자가 `Channel.receive()`로 직접) |
| **`IOSpec`** | Operator의 input/output 정의. | `Channel[T]` 변수 |
| **`Resource`** | 공유 자원 (CUDA stream, allocator, etc.) | (없음 — Python global 또는 ctx 첨부) |
| **`Executor`** | Scheduler 위의 실행 driver. | `Supervisor.__aenter__` |

`include/holoscan/core/` 디렉터리의 헤더 구조가 매우 깔끔하다:

```
include/holoscan/core/
├── clock.hpp          ← Clock interface
├── condition.hpp      ← Condition base + trigger types
├── executor.hpp       ← Executor base
├── operator.hpp       ← Operator base
├── scheduler.hpp      ← Scheduler base
├── fragment.hpp       ← Fragment (application)
├── io_context.hpp     ← per-operator IO
├── io_spec.hpp        ← input/output spec
├── ...
```

이 명시적 분리 (clock과 scheduler가 별도 헤더, condition이 별도 개념)는 runlet의 모듈 분리 결정에 영향을 주었다.

## SimClock과의 관계

**Holoscan은 `ManualClock`을 명시적으로 노출하여 deterministic test 모드를 지원한다.** 단 이는 production 모드 (`RealtimeClock`)의 add-on이지 first-class는 아니다 — 대부분의 예제는 `RealtimeClock`을 가정한다.

runlet의 `Clock` plug-in 디자인은 Holoscan의 그것을 거의 그대로 따른다 — 추상 인터페이스 + 두 구현체 + Context 통한 노출. 다만 runlet은 `SimClock`을 더 강하게 강조한다 (test/replay first-class).

## 가치 제안 비교

| 어필 포인트 | Holoscan | runlet |
|------------|----------|--------|
| GPU streaming pipeline | ✓ (CUDA-aware) | ✗ |
| Sensor AI 도메인 | ✓ (의료, 엣지) | ✗ |
| Scheduler 정책 다양성 | ✓ (3종 + 사용자 정의) | ✗ (anyio TaskGroup에 위임) |
| Condition 기반 트리거 | ✓ (별도 추상) | ✗ (`Channel.receive()`로 직접) |
| Multi-fragment 분산 | ✓ | ✗ (v0) |
| Manual clock | ✓ | ✓ (`SimClock` first-class) |
| C++ + Python | ✓ | Python only |
| GXF runtime 의존 | ✓ (NVIDIA Graph eXecution Framework) | ✗ (anyio only) |
| 무거운 build (CMake + CUDA + GXF) | ✓ | ✗ |

Holoscan은 NVIDIA의 ecosystem (CUDA, GXF, Clara) 안에서 의료 / 엣지 sensor AI를 타게팅한다. runlet은 그것과 다른 도메인 (Python async, robotics + eval + agents).

## 채택한 디자인 결정

- **`Clock` 인터페이스 + 구현 분리.** `RealtimeClock` / `ManualClock` 패턴을 `WallClock` / `SimClock`으로 답습.
- **모듈별 헤더/파일 분리.** runlet의 `clock.py` / `channel.py` / `daemon.py` / `supervisor.py` 분리는 Holoscan의 `clock.hpp` / `operator.hpp` / `scheduler.hpp` / `executor.hpp` 패턴을 모방.
- **Scheduler를 명시적 컴포넌트로.** runlet은 Holoscan만큼 복잡한 scheduler 정책을 노출하지 않지만, `Supervisor`가 "scheduler 역할" 임을 인식.

## 거부한 디자인 결정

- **`Condition` 추상.** Holoscan은 "operator가 언제 fire할지"를 별도 `Condition` 객체로 모델. runlet은 사용자가 `await Channel.receive()` 또는 `async for tick in clock.every(...)` 로 직접 표현. 1차 사용자가 추상을 의식적으로 학습해야 하는 비용 vs. 표현력 trade-off에서 표현력 쪽을 선택.
- **Scheduler 정책 다양성.** runlet은 `anyio.create_task_group`의 fair scheduling에 위임. 정책은 노출 안 함.
- **CUDA / GPU integration.** runlet은 도메인 중립.
- **`Resource` 공유 자원 추상.** runlet은 Python global state 또는 ctx 첨부로 사용자가 처리.

## 관찰

Holoscan은 **모던 streaming SDK의 정갈한 헤더 구조를 보여주는 reference**다. NVIDIA가 의료 / 엣지 AI를 위해 GXF runtime 위에 만든 layer라 stack은 무겁지만, 헤더 구조 자체는 매우 잘 정리되어 있다.

runlet의 모듈 분리 결정은 Holoscan의 그것을 vector 압축했다 — Holoscan의 13개 핵심 헤더 (`clock`, `condition`, `executor`, `operator`, `scheduler`, `fragment`, `io_context`, `io_spec`, ...)를 runlet의 4개 모듈 (`clock`, `channel`, `daemon`, `supervisor`)로 합쳤다. 합치는 과정에서 `Condition`은 `Channel.receive()`로, `Scheduler`는 `anyio.TaskGroup`으로, `IOSpec`은 Python type annotation으로 환원되었다.

이는 같은 도메인이 아니므로 1:1 매핑은 아니지만, "헤더로 추상을 분리한다" 라는 원칙 자체가 가치 있는 reference였다.

## Links

- 메인 레포: <https://github.com/nvidia-holoscan/holoscan-sdk>
- User guide: <https://docs.nvidia.com/holoscan/sdk-user-guide/>
- `include/holoscan/core/`: <https://github.com/nvidia-holoscan/holoscan-sdk/tree/main/include/holoscan/core>
