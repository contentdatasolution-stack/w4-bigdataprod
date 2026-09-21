# Redis Streams 시연 — 오늘 만든 부품을 '진짜 제품'에서 다시 보기 (교수자용)

슬라이드 43(오늘 만든 부품의 진짜 이름) 직후에 하는 단계별 시연. 학생 실습이 아니라 **교수자가 한 단계씩 실행하며 설명**하는 용도.
파이썬으로 직접 만든 장부 · 위치 · 그룹 · '위치를 언제 적느냐'가 제품 안에 그대로 들어 있다는 것을 보여 준다.

**슬라이드: `Redis_Streams_Demo_KR.pptx`** (19장, 발표자 노트 포함) — 본 강의 덱과 같은 디자인. 구성:

| 장 | 내용 |
|---|---|
| 2 | 큰 그림 — 명령 다섯 개 ↔ 오늘 만든 부품, 새것은 XPENDING 하나 |
| 3~5 | **시연 방법** — 준비(설치·서버), 터미널 네 개 배치와 12분 시간표, 화면 읽는 법(Enter · `redis>` 줄 · `--auto`) |
| 6~7 | 1단계 XADD, 2단계 XREAD — 실제 출력 + 짚을 곳 · 말할 것 · 되물을 것 |
| 8 · 11 | 질문 슬라이드 (답은 발표자 노트에만) |
| 9~10 | 3단계 XREADGROUP — 본 강의 슬라이드 39~40 과 같은 장부 그림 |
| 12~13 | 4단계 XPENDING · XACK · NOACK — 중복 vs 유실 |
| 14~16 | 5단계 생산자(바뀐 한 줄), 6단계 소비자(네 줄 대응표 + 시연 시나리오 ①~⑤) |
| 17~18 | 같은 그룹 vs 다른 그룹, 이름 대응표(직접 만든 것 · Redis · Kafka) |
| 19 | **시연 방법** — 끝내기와 문제 해결표 |

## 준비 (한 번)

```bash
brew install redis                        # 이미 설치되어 있으면 생략
source ../../.venv/bin/activate
pip install -r requirements.txt           # redis (파이썬 클라이언트)
```

서버는 **시연 전용 포트 6390 · 메모리 전용**으로 띄운다 → 기본 포트(6379)의 다른 Redis 와 섞이지 않고, 끄면 디스크에 아무것도 남지 않는다.

## 단계

| 단계 | 파일 | 보여 주는 것 | 슬라이드 | 직접 만든 코드에서는 |
|---|---|---|---|---|
| 0 | `sh step0_server.sh` | 서버 켜기 (터미널 하나를 차지) | — | — |
| 1 | `step1_append.py` | 장부에 덧붙이기 `XADD`. ID = 위치 | 34 | `log.append(msg)` · jsonl 에 한 줄 쓰기 |
| 2 | `step2_position.py` | 위치를 **내가** 기억하며 읽기 `XREAD`. 2건 → 1건 → 0건, 장부는 그대로 | 25~28 | `f.seek(pos)` · `state/my_pos.txt` |
| 3 | `step3_groups.py` | 위치를 **Redis 가** 그룹별로 기억 `XREADGROUP`. A 2건 → B 1건 → A 이어서 → 그룹 C 는 처음부터 | 35 · 39 · 40 | `offsets = {"A": 0, "B": 0}` · `consume("B", max_lines=5)` |
| 4 | `step4_pending.py` | 위치를 언제 적느냐 `XPENDING` · `XACK` · `NOACK`. 중복 vs 유실 | 41 | `my_consumer.py` TODO ④ · `--crash` |
| 5 | `step5_upbit_producer.py` | 진짜 데이터: `upbit_producer.py` 에서 **한 줄만** 바뀐 생산자 | 33 | `f.write(...)` → `r.xadd(...)` |
| 6 | `step6_upbit_consumer.py` | 그룹 · 처리 후 ACK · lag. 죽였다 살리기, 그룹 B, `--crash`, `--slow`, 소비자 추가 | 35 · 37 · 41 | `my_consumer.py` 전체 |
| 9 | `step9_cleanup.py` | 장부 지우기 | — | — |

1~4단계는 Enter 를 누를 때마다 한 걸음씩 나간다 (`--auto` 를 붙이면 멈추지 않는다).
화면에 `redis>` 로 찍히는 줄은 **redis-cli 에 그대로 쳐도 되는 명령** — 직접 쳐 보이고 싶으면 `redis-cli -p 6390`.
1단계가 장부를 지우고 새로 만들므로, 꼬이면 언제든 1단계부터 다시 하면 된다.

## 진행 대본 (약 12분)

**터미널 1** — `sh step0_server.sh` (켜 두기)

**터미널 2** — 장난감 데이터로 부품 확인 (6분)

```bash
python step1_append.py      # "덧붙이기밖에 못 한다. 돌아온 ID 가 자리 번호다"
python step2_position.py    # "my_consumer 와 같다 — 위치를 내가 든다. 다 읽어도 XLEN 은 3"
python step3_groups.py      # 슬라이드 39~40 을 옆에 띄워 놓고 한 줄씩 대조. 마지막에 'ACK 대기' 열을 가리키며 4단계로
python step4_pending.py     # "읽었다 ≠ 처리했다. 죽었다 살아나면 다시 온다(중복). NOACK 이면 안 온다(유실)"
```

**터미널 2·3·4** — 진짜 데이터 (6분)

```bash
# 터미널 2: 생산자. student/upbit_producer.py 와 나란히 띄워 '바뀐 한 줄'을 먼저 보여 준다
python step5_upbit_producer.py

# 터미널 3: 소비자 A — ① 따라잡고 새 것만  ② Ctrl+C 후 다시 켜기: 위치 파일이 없는데도 이어 읽는다
python step6_upbit_consumer.py A

# 터미널 4: 그룹 B — 처음부터 따로. 터미널 3 의 A 는 아무 영향이 없다
python step6_upbit_consumer.py B

# 터미널 3: ③ 크래시 → 다시 켜면 [재처리] 가 먼저 나온다. 건수·금액이 죽기 직전 출력과 같다
python step6_upbit_consumer.py A c1 --crash
python step6_upbit_consumer.py A

# 터미널 3: ④ 느린 소비자 — '밀린 수'가 5 → 9 → 12 … 로 늘어난다 (슬라이드 37: 기울기 = 생산 − 소비)
python step6_upbit_consumer.py A c1 --slow
# 터미널 4: 같은 그룹에 소비자를 하나 더 → 밀린 수가 줄기 시작한다. A 의 일을 c1·c2 가 '나눠' 받는다
python step6_upbit_consumer.py A c2
```

끝나면 `python step9_cleanup.py`, 터미널 1 에서 Ctrl+C.

## 되물을 것

- (2 → 3단계) "위치를 내가 들 때와 Redis 가 들 때, 무엇이 편해지고 무엇을 잃나?" → 파일 관리가 없어지고 여러 대가 같은 위치를 공유한다 / 대신 그 Redis 가 죽으면 위치도 같이 위험하다.
- (3단계) "그룹 C 가 공짜인 이유는?" → 장부는 읽어도 줄지 않고, '읽었다'는 기록은 그룹 쪽에만 있다.
- (4단계) "전달 횟수가 2 인 메시지를 받으면 소비자는 무엇을 해야 하나?" → 이미 처리했는지 확인(`sequential_id`)하거나, 두 번 해도 결과가 같게(같은 id 면 덮어쓰기) 만든다.
- (6단계 ④) "같은 그룹에 붙인 c2 와 다른 그룹 B 의 차이는?" → 같은 그룹 = 일을 **나눠** 받는다(처리량 ↑) / 다른 그룹 = 같은 것을 **각자** 받는다(용도가 다름: 집계·저장). 슬라이드 34 의 파티션은 '나눠 받기'를 순서를 지키며 하려는 장치.
- (5단계 `maxlen`) "소비자가 장부 한도보다 더 오래 밀리면?" → 못 읽은 채 사라진다. 완충 지대에도 한도가 있다 (슬라이드 32: 쌓이거나, 버리거나, 멈추거나).

## 이름 대응

| 오늘 만든 것 | Redis Streams | Kafka (6~7주차) |
|---|---|---|
| 장부 `raw/upbit_trades.jsonl` | stream `upbit-trades` | topic (+ partition) |
| 자리 번호 · 바이트 위치 | entry ID `1789…-0` | offset |
| 덧붙이기 | `XADD` | produce |
| `state/upbit_pos_A.txt` | 그룹의 last-delivered-id | committed offset |
| 그룹 A · B | consumer group | consumer group |
| 처리 후 위치 기록 | `XACK` | commit |
| (없음 — 직접 만든 코드에는 '읽는 중' 상태가 없다) | `XPENDING` | — (커밋 안 된 구간이 같은 역할) |
| 밀린 양 = 장부 크기 − 위치 | `XINFO GROUPS` 의 lag | consumer lag |
| 장부 한도 (없음 — 파일은 계속 자란다) | `MAXLEN` | retention |

Redis Streams 와 Kafka 의 가장 큰 차이: Redis 는 **메시지 단위로** ACK 하고 대기 목록을 보여 주지만, Kafka 는 **"여기까지 끝났다"는 위치 하나만** 커밋한다 (우리가 만든 `state/*.txt` 방식에 더 가깝다).

## 검증 기록

2026-09-20, Redis 8.8.0 · redis-py 8.1.0 · 실제 업비트 체결로 전 단계 실행 확인.
크래시 후 재실행 시 같은 묶음이 `[재처리]` 로 다시 옴, `--slow` 에서 밀린 수 5 → 9 → 12 증가, 이후 보통 소비자가 즉시 따라잡음.
장이 한산하면(초당 2~3건) 숫자가 작게 나온다 — `--slow` 는 그래도 밀리도록 5건씩·묶음당 4초로 맞춰 두었다.
