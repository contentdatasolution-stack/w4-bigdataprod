"""1단계 — 장부에 덧붙이기.  슬라이드 34 '중개자 안에는 이름 붙은 장부가 있고, 장부는 덧붙이기만 한다'

    python step1_append.py          # Enter 를 누를 때마다 한 걸음
    python step1_append.py --auto   # 멈추지 않고 끝까지

보여 줄 것
  - XADD 는 '맨 뒤에 덧붙이기'밖에 못 한다. 중간에 끼우거나 고치는 명령이 없다.
  - 덧붙일 때마다 돌아오는 ID(시각-순번)가 그 메시지의 자리 번호 = 위치(offset).
    슬라이드의 0, 1, 2… 대신 Redis 는 '1789899471688-0' 같은 값을 쓴다. 항상 커지기만 한다는 점이 같다.
  - 메시지 이름(v41)과 위치(ID)는 다른 것 — 슬라이드 34 의 '헷갈리지 마세요'.

이 파일은 장부 views 를 지우고 새로 만든다 → 언제든 1단계부터 다시 시작할 수 있다.
"""
from common import check, cli, note, pause, r, show_entries, title

STREAM = "views"

check()
title("1단계 — 장부(stream)에 덧붙이기")

cli(f"DEL {STREAM}")
r.delete(STREAM)
note("깨끗한 상태에서 시작")
pause()

for name in ["v41", "v42", "v43"]:
    cli(f"XADD {STREAM} * name {name}")
    entry_id = r.xadd(STREAM, {"name": name})          # * = ID 는 서버가 정한다 (지금 시각-순번)
    print(f"   → {entry_id}")
    note(f"'{name}' 은 메시지 내용, '{entry_id}' 는 장부에서의 자리 번호")
    pause()

cli(f"XLEN {STREAM}")
print(f"   → {r.xlen(STREAM)}")

cli(f"XRANGE {STREAM} - +")
show_entries(r.xrange(STREAM, "-", "+"))
note("처음(-)부터 끝(+)까지. 생산자는 여기까지만 안다 — 누가 읽는지, 읽는 사람이 있는지조차 모른다")
