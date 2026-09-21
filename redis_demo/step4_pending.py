"""4단계 — 위치를 언제 적느냐: 유실 vs 중복.  슬라이드 41 · my_consumer_answer.py --crash 와 같은 이야기

    python step4_pending.py [--auto]        (먼저 step1 → step3 을 실행해 둘 것)

보여 줄 것
  - 3단계에서 A 는 v41·v42·v43 을 '받아 갔을' 뿐 '처리를 끝냈다'고 말한 적이 없다.
    Redis 는 그 3건을 ACK 대기 목록(XPENDING)에 적어 두고 있다 = "읽었지만 아직 끝났다는 보고가 없는 것"
  - XACK = 처리가 끝난 '후'에 하는 보고.  my_consumer.py 의 TODO ④ 에 해당.
  - 처리 도중 죽었다면?  다시 켜서 '0'(= 내 ACK 대기분)부터 읽으면 같은 것이 '또' 온다 → 중복.  잃어버리지는 않는다.
  - NOACK 으로 읽으면 받는 순간 끝난 것으로 친다 = '처리 전에 표시'.  처리 도중 죽으면 그 메시지는 영영 없다 → 유실.

  실무 기본값은 '처리 후 ACK + 두 번 처리해도 괜찮게'.  XPENDING 의 '전달 횟수'가 2 이상이면 중복 가능성이 있다는 신호.
"""
from common import check, cli, note, pause, r, show_entries, show_groups, title

STREAM, GROUP = "views", "A"


def show_pending():
    cli(f"XPENDING {STREAM} {GROUP} - + 10")
    rows = r.xpending_range(STREAM, GROUP, "-", "+", 10)
    if not rows:
        print("   (없음 — 받아 간 것은 전부 처리 완료 보고가 끝났다)")
    for p in rows:
        name = r.xrange(STREAM, p["message_id"], p["message_id"])[0][1]["name"]
        print(f"   {p['message_id']}   {name}   받아 간 소비자={p['consumer']}   전달 횟수={p['times_delivered']}")
    return rows


check()
if not r.exists(STREAM) or not any(g["name"] == GROUP for g in r.xinfo_groups(STREAM)):
    raise SystemExit("그룹 A 가 없음 — step1_append.py → step3_groups.py 를 먼저 실행하세요")
title("4단계 — 위치를 언제 적느냐: ACK 대기 목록 (XPENDING · XACK)")

rows = show_pending()
note("3단계에서 A 가 받아 간 것들. 처리를 끝냈다고 보고(ACK)한 적이 없어서 그대로 남아 있다")
pause()

if rows:
    first = rows[0]["message_id"]
    cli(f"XACK {STREAM} {GROUP} {first}")
    print(f"   → {r.xack(STREAM, GROUP, first)}")
    note("첫 번째 것만 '처리 끝'이라고 보고")
    show_pending()
    pause()

title("… 나머지를 처리하던 중에 소비자가 죽었다.  다시 켰다")
cli(f"XREADGROUP GROUP {GROUP} c1 STREAMS {STREAM} 0")
resp = r.xreadgroup(GROUP, "c1", {STREAM: "0"})             # '>' 가 아니라 '0' = 내가 받아 가고 ACK 안 한 것부터
again = resp[0][1] if resp else []
show_entries(again)
note("'>' 대신 '0' 으로 읽으면 새 메시지가 아니라 '내 ACK 대기분'이 다시 온다 → 잃어버리지 않는다")
show_pending()
note("대신 전달 횟수가 2 가 됐다 — 죽기 전에 이미 처리했던 것이라면 두 번 처리되는 것 (중복)")
pause()

if again:
    ids = [entry_id for entry_id, _ in again]
    cli(f"XACK {STREAM} {GROUP} " + " ".join(ids))
    print(f"   → {r.xack(STREAM, GROUP, *ids)}")
    show_pending()
    pause()

title("반대로 — 받는 순간 끝난 것으로 치면? (NOACK = 처리 '전'에 표시)")
cli(f"XADD {STREAM} * name v45")
print(f"   → {r.xadd(STREAM, {'name': 'v45'})}")
cli(f"XREADGROUP GROUP {GROUP} c1 NOACK STREAMS {STREAM} >")
resp = r.xreadgroup(GROUP, "c1", {STREAM: ">"}, noack=True)
show_entries(resp[0][1] if resp else [])
show_pending()
note("ACK 대기 목록에 오르지 않는다. 이것들을 처리하다 죽으면? 다시 켜도 '0' 에도 '>' 에도 안 나온다 → 유실")
show_groups(STREAM)
note("정리: 처리 후 ACK → 중복 가능·유실 없음 / 처리 전 표시(NOACK) → 중복 없음·유실 가능.  실무 기본값은 앞쪽")
