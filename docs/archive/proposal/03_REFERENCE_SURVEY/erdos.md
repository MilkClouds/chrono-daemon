# ERDOS

- 출처: <https://github.com/erdos-project/erdos>
- 언어: Rust 코어 + Python binding
- ★: 208 (2026-05 기준)
- 카테고리: Dataflow framework (academic — AV pipeline)
- 첫 release: 2018, UC Berkeley RISELab. EuroSys 2022 paper "D3: A Dynamic Deadline-Driven Approach for Building Autonomous Vehicles"

## 한 줄 요약

UC Berkeley의 self-driving car / robotics 용 dataflow system. **Watermark 기반 deterministic ordering**이 differentiator — operator가 이전 watermark보다 오래된 메시지를 무시하므로 out-of-order 메시지에서 결정적 처리가 가능하다.

## 핵심 추상화

| ERDOS 컴포넌트 | 의미 | chrono-daemon 대응 |
|----------------|------|-------------|
| **Operator** | 처리 단위 (node). | `Daemon` |
| **Stream** | Operator 간 메시지 흐름. | `Channel[T]` |
| **Watermark** | 메시지 timestamp의 lower bound. Operator는 watermark보다 오래된 메시지를 무시. | (chrono-daemon은 in-process queue라 ordering 자동 — watermark 불필요) |
| **Driver** | Dataflow graph 정의 + 실행 entry point. | `Supervisor.__aenter__` |
| **Application** | 전체 dataflow graph. | (chrono-daemon은 코드로 동적) |
| **Sender / Receiver** | Stream endpoint. | `Channel.send` / `Channel.recv` |

ERDOS는 자율주행 use case에 강하게 맞춰져 있다 — Pylot이라는 AV stack이 ERDOS 위에 빌드되어 있다.

## SimClock과의 관계

**ERDOS는 watermark 기반 deterministic ordering을 제공하지만 sim-clock advance API는 없다.**

- Watermark는 메시지 처리 순서를 deterministic하게 보장하므로 cross-run reproducibility가 좋다.
- 그러나 시간을 일괄 advance하는 patterns (`SimClock.advance(10.0)`) 와는 다르다 — ERDOS는 wall-clock으로 동작하면서 message ordering만 결정성 보장.

chrono-daemon의 `SimClock`은 메시지 ordering이 아니라 **virtual time advancement**가 첫째 의도다. 두 접근법은 직교한다.

## 가치 제안 비교

| 어필 포인트 | ERDOS | chrono-daemon |
|------------|-------|--------|
| AV pipeline 도메인 | ✓ (Pylot AV stack) | ✗ |
| Watermark deterministic ordering | ✓ | ✗ (in-process queue 자동 보장) |
| End-to-end deadline | ✓ (D3 paper) | ✗ |
| Multi-language (Rust + Python) | ✓ | Python only |
| Sim-time burst advance | ✗ | ✓ |
| asyncio / trio | ✗ (Rust runtime) | ✓ |
| Pure Python | ✗ | ✓ |
| 작은 의존성 | ✓ (Rust crate + maturin Python wheel) | ✓ |

ERDOS와 chrono-daemon은 deterministic semantic 추구라는 점에서 motivation을 일부 공유하지만, 결정성의 종류가 다르다.

## 채택한 디자인 결정

- **Operator = lifecycle unit.** chrono-daemon `Daemon`과 동일 의미.
- **Stream = typed message flow.** chrono-daemon `Channel[T]`.
- **Driver / Application split.** chrono-daemon은 supervisor entry point가 같은 역할.

## 거부한 디자인 결정

- **Watermark.** chrono-daemon은 in-process queue로 ordering 자동 보장 — watermark가 필요한 use case (out-of-order network message 처리)가 v0 scope 밖.
- **End-to-end deadline tracking.** ERDOS의 D3 paper는 AV pipeline의 end-to-end deadline을 dynamic하게 enforce. chrono-daemon은 deadline 추상 없음.
- **Application = static graph.** chrono-daemon은 코드로 동적 wiring (dora-rs YAML과 비슷한 이유로 거부).

## 관찰

ERDOS는 **학술 출발의 모던 dataflow** reference다. Pylot이라는 actual AV stack을 빌드하여 ERDOS 디자인을 검증했고, EuroSys 2022 paper "D3"가 watermark + deadline의 가치 제안을 정량화했다.

chrono-daemon과 ERDOS는 시간 / 결정성 추상에서 다른 axes를 잡았다:

- ERDOS: "메시지 ordering이 deterministic" → cross-run reproducibility는 ordering 일치를 의미.
- chrono-daemon: "시간 자체가 deterministic하게 advance" → cross-run reproducibility는 동일 시점 동일 상태를 의미.

ERDOS 모델은 production AV에서 wall-clock으로 도는 시스템에 적합하고, chrono-daemon 모델은 sim/test에서 fast-forward하는 시스템에 적합하다. **두 모델은 합쳐질 수 있다** — sim clock + watermark — 하지만 chrono-daemon v0에서는 watermark가 의식적 no-goal (in-process queue로 자동 처리).

ERDOS의 활동은 D3 paper 이후 감소세다. 2024–2026 동안 commit이 적고 Pylot 위주의 maintenance. 단 디자인 reference로는 여전히 가치.

## Links

- 메인 레포: <https://github.com/erdos-project/erdos>
- Pylot AV stack: <https://github.com/erdos-project/pylot>
- D3 paper (EuroSys 2022): <https://dl.acm.org/doi/10.1145/3492321.3519576>
- Docs: <https://erdos.readthedocs.io/>
