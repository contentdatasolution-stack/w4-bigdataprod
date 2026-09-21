"""3단계 — 소비자 그룹: 위치를 Redis 가 대신 기억한다.  슬라이드 35 · 39 · 40 을 그대로 재현

    python step3_groups.py [--auto]        (먼저 step1 을 실행해 둘 것)

보여 줄 것 — 슬라이드 39~40 의 출력과 한 줄씩 대조된다
  - 그룹 A 가 2건 읽는다 → v41, v42.   A 의 위치만 움직인다 (슬라이드 39: offsets = {A: 2, B: 0})
  - 그룹 B 가 1건 읽는다 → v41.        A 가 이미 읽은 것을 B 는 처음부터 다시 (슬라이드 40)
  - 그룹 A 가 2건 더    → v43 하나뿐.  이어서 읽는다
  - 새 메시지 v44 가 들어오면 두 그룹의 '밀린 수(lag)'가 같이 1 늘어난다
  - 내일 온 그룹 C 는 처음부터 전부 — 생산자도, 장부도, A·B 도 고칠 것이 없다

2단계와 달라진 점: 위치를 내 변수가 아니라 Redis 가 그룹 이름별로 들고 있다.
명령의 '>' 는 "이 그룹이 아직 안 받아 간 것부터"라는 뜻 — 내가 위치를 말해 줄 필요가 없다.
"""
import redis

from common import check, cli, note, pause, r, show_entries, show_groups, title

STREAM = "views"


def make_group(group):
    cli(f"XGROUP CREATE {STREAM} {group} 0")
    try:
        r.xgroup_create(STREAM, group, id="0")          # 0 = 장부의 맨 처음부터 읽는 그룹
    except redis.exceptions.ResponseError:              # 이미 있으면 지우고 다시 (몇 번을 돌려도 같은 결과)
        r.xgroup_destroy(STREAM, group)
        r.xgroup_create(STREAM, group, id="0")
    print("   → OK")


def read(group, count):
    cli(f"XREADGROUP GROUP {group} c1 COUNT {count} STREAMS {STREAM} >")
    resp = r.xreadgroup(group, "c1", {STREAM: ">"}, count=count)
    show_entries(resp[0][1] if resp else [])


check()
if not r.exists(STREAM):
    raise SystemExit("장부 views 가 없음 — step1_append.py 를 먼저 실행하세요")
title("3단계 — 소비자 그룹: 위치를 Redis 가 기억한다 (XREADGROUP)")

make_group("A")
make_group("B")
show_groups(STREAM)
note("두 그룹 모두 아직 아무것도 안 읽었다 — 밀린 수 3")
pause()

read("A", 2)
note("A 가 2건 → v41, v42")
show_groups(STREAM)
note("움직인 것은 A 의 위치뿐. B 는 그대로, 장부도 그대로")
pause()

read("B", 1)
note("B 는 A 가 읽은 v41 을 다시 받는다 — 같은 장부를 각자 읽는다")
pause()

read("A", 2)
note("A 는 이어서 — 2건을 달라고 했지만 남은 것은 v43 하나")
show_groups(STREAM)
pause()

cli(f"XADD {STREAM} * name v44")
print(f"   → {r.xadd(STREAM, {'name': 'v44'})}")
show_groups(STREAM)
note("생산자가 하나 덧붙였을 뿐인데 두 그룹의 밀린 수가 같이 늘었다 — lag = 장부 끝 − 그룹 위치")
pause()

make_group("C")
read("C", 10)
note("내일 온 그룹 C 는 v41 부터 전부. 새 소비자를 붙이는 데 아무것도 고치지 않았다 (슬라이드 35)")
show_groups(STREAM)
note("맨 오른쪽 'ACK 대기' 열을 눈여겨보세요 — A 는 3건을 읽었는데 3건이 '대기'다. 이것이 4단계")
