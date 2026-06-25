# RedisROS

- 출처: <https://github.com/vguillet/RedisROS>
- 언어: Python (pure)
- ★: 17 (2026-05 기준)
- 카테고리: Pure Python ROS-clone
- 첫 release: 2022-10. Last update: 2023.

## 한 줄 요약

> "A Redis-based pure python alternative to ROS2."

순수 Python으로 ROS2 API를 흉내내는 작은 라이브러리. **runlet 서베이 단계에서 "pure Python ROS-like" 키워드의 거의 유일한 매칭**이었다. Redis backend에 의존하지만, ROS2 colcon / DDS 의존을 끊으려는 시도라는 점에서 motivation을 일부 공유한다.

## 핵심 추상화

RedisROS의 README와 코드 inspection 기반:

| RedisROS 컴포넌트 | 의미 | runlet 대응 |
|-------------------|------|-------------|
| **Node** | 컴포지션 단위. ROS2 형태. | `Daemon` |
| **Publisher / Subscriber** | Redis pub/sub topic. | `Channel.send` / `Channel.recv` (단, runlet은 1:1) |
| **Topic** | Redis key. | (없음 — runlet은 [ADR 0001](../../../adr/0001-channel-is-the-sole-comm-primitive.md) Channel-only) |
| **Service / Client** | Redis-based request/reply. | (없음, 채널 위에 사용자가 짠다) |
| **Parameter server** | Redis hash. | (없음) |
| **Spin** | Event loop drainer. | `Supervisor.__aenter__` |

## SimClock과의 관계

**없다.** RedisROS는 wall-clock에서만 동작. README, code, examples 모두 sim-clock 또는 deterministic replay 언급 없음.

## 가치 제안 비교

| 어필 포인트 | RedisROS | runlet |
|------------|----------|--------|
| Pure Python | ✓ | ✓ |
| 작은 학습 곡선 | ✓ | ✓ |
| asyncio / trio | ✗ (Redis-py blocking) | ✓ (anyio) |
| Sim-time first-class | ✗ | ✓ |
| ROS2 API 호환 | ~ (이름만 비슷) | ✗ |
| Redis 의존 | ✓ | ✗ |
| Multi-machine 분산 | ✓ (Redis 통해) | ✗ (v0 in-process) |
| Topic / Service / Parameter 다 있음 | ✓ | ✗ (Channel only) |
| 활발한 development | ✗ (2023 이후 dormant) | ✓ |

RedisROS는 "Redis 위에 ROS2 API"라는 단일 axis에 집중한 작은 프로젝트. runlet과는 axes가 다르다 (anyio + sim-time + minimal).

## 채택한 디자인 결정

- (없음 — RedisROS의 디자인이 runlet에 직접 영감을 준 부분은 없다. 다만 "pure Python ROS-clone이라는 niche가 거의 비어 있다" 라는 시장 신호를 확인.)

## 거부한 디자인 결정

- **Redis 의존.** runlet은 anyio 외 0 dependency ([ADR 0007](../../../adr/0007-anyio-only-runtime-dependency.md)).
- **ROS2 API 그대로 따라가기.** runlet은 형태만 비슷하고 명시적으로 비-호환 (`SendStream` / `ReceiveStream` 분리, `Supervisor` 명시 등).
- **Topic / Service / Parameter 풀 세트.** runlet은 [ADR 0001](../../../adr/0001-channel-is-the-sole-comm-primitive.md) (Channel only) 와 [ADR 0005](../../../adr/0005-no-lifecycle-states-beyond-start-run-stop.md) (lifecycle 최소화) 으로 surface 축소.

## 관찰

RedisROS는 **pure Python ROS-clone의 거의 유일한 reference**다. 17 star라는 사이즈, 2023년 이후 dormant 상태가 두 가지를 시사한다:

1. "Pure Python ROS-like"는 needs는 있지만 그 needs를 RedisROS의 형태로 충족하는 사용자가 많지 않았다.
2. 가능한 이유: Redis 의존이 ROS2 의존을 줄이는 효과가 제한적 (둘 다 외부 service 설치 필요), API가 ROS2 1:1 모방이라 ROS2 사용자에게는 ROS2 자체를 쓸 이유가 더 크다.

runlet은 이 두 lesson을 흡수한다:

- **외부 service 의존 없음.** runlet은 anyio in-memory.
- **ROS2 API 1:1 모방하지 않음.** sim-time first-class라는 별도 가치 제안을 핵심으로 둠.

RedisROS의 존재 자체가 "pure Python concurrency framework with sim-time"이라는 runlet의 niche에 가장 가까운 후보였다는 점에서, 시장 공백 검증의 reference로 활용했다.

## Links

- 메인 레포: <https://github.com/vguillet/RedisROS>
