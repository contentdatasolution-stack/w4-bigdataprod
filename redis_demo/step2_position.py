"""2단계 — 위치를 '내가' 기억하며 읽기.  슬라이드 25~28 (seek/tell) · my_consumer.py 와 같은 구조

    python step2_position.py [--auto]        (먼저 step1 을 실행해 둘 것)

보여 줄 것
  - XREAD 는 "이 위치 '다음'부터 달라"는 명령. 위치는 읽는 쪽이 들고 있다 (여기서는 변수 pos).
    my_consumer.py 의 state/my_pos.txt 에 적던 바이트 위치가 여기서는 마지막으로 읽은 ID.
  - 세 번 읽는다: 2건 → 1건 → 0건.  슬라이드 26~28 의 1회차·2회차·3회차와 같은 모습.
  - 다 읽어도 XLEN 은 그대로 — 읽는다고 장부가 줄지 않는다 (queue.Queue 와의 차이, 슬라이드 39).
"""
from common import check, cli, note, pause, r, show_entries, title

STREAM = "views"

check()
title("2단계 — 위치를 내가 기억하며 읽기 (XREAD)")

pos = "0"                                               # 내가 기억하는 값. 0 = 장부의 맨 처음
for round_no, count in [(1, 2), (2, 2), (3, 2)]:
    cli(f"XREAD COUNT {count} STREAMS {STREAM} {pos}")
    resp = r.xread({STREAM: pos}, count=count)          # pos '다음'부터 최대 count 건
    entries = resp[0][1] if resp else []
    show_entries(entries)
    if entries:
        pos = entries[-1][0]                            # 마지막으로 읽은 ID 가 새 위치
    note(f"{round_no}회차: {len(entries)}건 읽음 → 내 위치 = {pos}")
    pause()

cli(f"XLEN {STREAM}")
print(f"   → {r.xlen(STREAM)}")
note("다 읽었는데도 3건 그대로. '읽었다'는 사실은 장부가 아니라 내 변수 pos 에만 있다")
note("이 프로그램이 죽으면 pos 도 사라진다 → 그래서 my_consumer.py 는 파일에 적었다. 3단계는 그것을 Redis 에게 맡긴다")
