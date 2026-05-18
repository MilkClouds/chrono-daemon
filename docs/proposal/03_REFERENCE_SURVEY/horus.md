# HORUS

- 출처: <https://github.com/softmata/horus>
- 언어: Rust 코어 + Python / C++ binding
- ★: 343 (2026-05 기준)
- 카테고리: ROS-clone alternative (모던 신생)
- 첫 release: 2025-10, 활발한 개발 중 (v0.2.x)

## 한 줄 요약

> "Real-time distributed middleware for Rust, Python, and C++. 575x faster than ROS2."

HORUS는 **2025년 후반 등장한 모던 ROS alternative**로 자칭. Macro-based Rust DSL로 `node!` / `message!` / `service!` / `action!` 정의를 단순화하고, shared-memory ring buffer 기반 IPC + 5종 execution class (RT/compute/event/async-io/best-effort)로 ROS2 대비 큰 polynomial 성능 차를 주장한다.

runlet과 같은 시기 (2025–2026)에 등장한 후보로, **MPSC channel + supervisor + clock 모델이 runlet과 가장 형태상 가깝다.**

## 핵심 추상화

| HORUS 컴포넌트 | 의미 | runlet 대응 |
|----------------|------|-------------|
| **`node!` macro** | Rust DSL로 node 선언. `pub` / `sub` / `data` / `tick` 필드. | `@daemon` decorator |
| **`Topic<T>`** | typed pub/sub channel (shared memory). | `Channel[T]` (단, runlet은 1:1) |
| **`Scheduler`** | 모든 노드 실행. 5종 execution class. | `Supervisor` |
| **`message! / service! / action!`** | 메시지/서비스/액션 DSL. | (서비스/액션 없음, 채널만) |
| **`tick_rate(N.hz())`** | scheduler tick rate 설정. | `Clock.every(period)` |
| **BlackBox** | ring-buffer flight recorder. | (없음) |
| **`enter_safe_state()`** | node가 safety-critical failure 시 호출되는 hook. | `Daemon.on_stop` (단, safety semantic은 없음) |
| **`Miss::SafeMode` / `Miss::Stop`** | deadline miss 정책. | (runlet은 deadline 정책 없음) |

HORUS의 가장 두드러진 특징은 **safety-aware scheduler** — 각 노드에 deadline / budget / on_miss 정책을 명시할 수 있다.

## SimClock과의 관계

**HORUS는 sim-time / deterministic replay 추상이 README에 명시되지 않았다.** 강조는 production real-time performance와 safety. Sim-clock 키워드 검색 결과 0건.

향후 추가될 가능성은 있으나, 2026-05 기준 first-class 가치는 아니다.

## 가치 제안 비교

| 어필 포인트 | HORUS | runlet |
|------------|-------|--------|
| Shared-mem IPC latency | ✓ (11–196 ns 주장) | ✓ (anyio in-memory) |
| 5종 execution class | ✓ (RT / compute / event / async-io / best-effort) | ✗ (anyio TaskGroup 단일 정책) |
| Safety-aware scheduler | ✓ (watchdog / safe-state / BlackBox) | ✗ |
| 3-language (Rust/Python/C++) | ✓ | Python only |
| Deadline / budget 명시 | ✓ | ✗ |
| Sim-time first-class | ✗ | ✓ |
| asyncio / trio | ✗ (Rust runtime) | ✓ |
| Pure Python | ✗ (Rust core + PyO3) | ✓ |
| CLI 풍부 (40+ command) | ✓ (`horus new`/`run`/`deploy`/...) | ✗ |
| 작은 dependency | ✗ (causal-conv1d, fla, terra HAL, ...) | ✓ (anyio only) |

HORUS는 **safety + production real-time** 축에 강하게 투자한다. runlet은 그 축이 아닌 **test/replay + simplicity** 축이다.

## 채택한 디자인 결정

- **Node = 작은 데몬 lifecycle unit.** HORUS `node!` macro의 lifecycle hook 형태가 runlet `Daemon` ABC와 거의 동일 (`tick` / `enter_safe_state` ↔ `run` / `on_stop`).
- **Scheduler 명시 노출.** HORUS의 `Scheduler::new().tick_rate(...).add(node)` 패턴이 runlet `Supervisor` 구조와 비슷.
- **Pub/Sub primitive를 typed로.** HORUS `Topic<T>`, runlet `Channel[T]`. 단 HORUS는 broadcast, runlet은 1:1.

## 거부한 디자인 결정

- **5종 execution class.** HORUS는 사용자가 노드마다 (`order(0).rate(1000.hz()).on_miss(Miss::SafeMode)` ) 명시해 RT class 선택. runlet은 anyio fair scheduling에 위임 — 사용자가 정책을 학습하지 않아도 된다.
- **Safety-critical 추상 (BlackBox, watchdog, safe-state).** HORUS는 industrial / safety domain을 의식. runlet의 use case (ML eval, agents)에서는 overkill.
- **Macro DSL.** HORUS는 Rust `macro_rules!` 로 `node! { pub { ... } sub { ... } data { ... } tick { ... } }` 형태. runlet은 Python class / decorator로 표현.
- **Deadline miss 정책.** runlet은 daemon이 알아서.
- **Custom message DSL.** HORUS는 `message! { Foo { x: f64 } }` 형태로 message 정의. runlet은 Python dataclass / msgspec / 그냥 object.

## 관찰

HORUS는 **runlet과 가장 시간상 가까운 모던 후보**다. 같은 2025–2026 시기에 등장했고, ROS-shaped abstraction을 모던하게 재현한다는 motivation도 공유한다.

다만 다음 axes에서 큰 차이:

- **언어**: HORUS는 Rust first, runlet은 Python only.
- **타겟 use case**: HORUS는 production real-time safety, runlet은 in-process test/replay + minimal supervisor.
- **Sim-time**: HORUS는 future, runlet은 first-class.
- **Safety**: HORUS는 first-class, runlet은 도메인 중립.

HORUS는 신생이라 사용자 base / production 검증이 아직 약하다 (2026-05 기준 ~340 star, 활발한 활동이지만 외부 채택 사례는 적음). 그러나 디자인 의도와 형태가 runlet과 가까우므로 향후 (a) HORUS가 sim-time 추가하거나 (b) runlet이 Rust core로 가지 않는 한 둘은 다른 axes에 머무를 것.

## Links

- 메인 레포: <https://github.com/softmata/horus>
- Docs: <https://docs.horusrobotics.dev/>
- "Coming from ROS2?" 가이드: <https://docs.horusrobotics.dev/learn/coming-from-ros2>
