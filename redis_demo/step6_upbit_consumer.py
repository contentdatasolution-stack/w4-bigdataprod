"""6단계 — 진짜 데이터의 소비자: 그룹 · 처리 후 ACK · 밀린 수(lag).  my_consumer.py 의 Redis 판

    python step6_upbit_consumer.py                  # 그룹 A, 소비자 c1
    python step6_upbit_consumer.py B                # 그룹 B — 같은 장부를 처음부터 따로 읽는다
    python step6_upbit_consumer.py A c2             # 그룹 A 에 소비자를 하나 더 — A 의 일을 c1 과 '나눠' 받는다
    python step6_upbit_consumer.py A c1 --crash     # 한 묶음을 처리하고 ACK 직전에 죽는다
    python step6_upbit_consumer.py A c1 --slow      # 5건씩 받아 묶음마다 3초 걸리는 느린 소비자 → lag 이 늘어난다

my_consumer.py 와 한 줄씩 대조
    pos = int(POS_FILE.read_text())          →  없다. 위치는 Redis 가 그룹 이름으로 기억한다
    f.seek(pos) · MAX_BYTES 만큼 읽기        →  XREADGROUP ... COUNT 100 ... >
    POS_FILE.write_text(str(pos))  (TODO ④)  →  XACK  (처리가 끝난 '후')
    장부 크기 − 내 위치 = 밀린 양             →  XINFO GROUPS 의 lag

시연 순서 (터미널을 늘려 가며)
    1. 생산자(step5)를 켜 둔 채 이 파일 실행 → 밀린 것을 100건씩 따라잡고, 그 뒤로는 새로 온 것만
    2. Ctrl+C 로 죽였다 다시 켠다 → 이어서 읽는다. 위치 파일이 어디에도 없는데도.
    3. 다른 터미널에서 B 실행 → 처음부터 따로. A 는 영향 없음 (슬라이드 35)
    4. --crash 로 실행 → 다시 켜면 "지난번에 ACK 못 한 N건부터" (슬라이드 41: 중복은 있어도 유실은 없다)
    5. --slow 로 실행 → lag 이 계속 늘어난다 (슬라이드 37: 기울기 = 생산 − 소비).
       같은 그룹에 c2 를 하나 더 붙이면 lag 이 줄기 시작한다 — '소비자를 더 붙이기'
"""
import sys
import time

import redis

from common import check, r

args = [a for a in sys.argv[1:] if not a.startswith("--")]
GROUP = args[0] if len(args) > 0 else "A"
CONSUMER = args[1] if len(args) > 1 else "c1"
CRASH = "--crash" in sys.argv                     # 시연용: 처리는 끝났는데 ACK 직전에 죽는다
SLOW = "--slow" in sys.argv                       # 시연용: 묶음마다 3초 걸리는 소비자
STREAM = "upbit-trades"
BATCH = 5 if SLOW else 100                        # 한 번에 받아 올 최대 건수 (my_consumer 의 MAX_BYTES). 느린 소비자는 5건씩
                                                  # → 5건에 4초 = 초당 1.25건. 한산한 장(초당 2~3건)에서도 확실히 밀린다


def process(entries):
    """처리: 종목별 체결 수와 체결 금액. (무엇이든 될 수 있다 — 집계 · 저장 · 알림)"""
    stats = {}
    for _, m in entries:
        s = stats.setdefault(m["code"], [0, 0.0])
        s[0] += 1
        s[1] += float(m["trade_price"]) * float(m["trade_volume"])     # Redis 는 값을 전부 문자열로 돌려준다
    if SLOW:
        time.sleep(3)
    return "  ".join(f"{code[4:]} {n}건/{amount / 1e6:,.1f}백만" for code, (n, amount) in sorted(stats.items()))


def lag_and_pending():
    g = next(g for g in r.xinfo_groups(STREAM) if g["name"] == GROUP)
    return g.get("lag"), g["pending"]


def handle(entries, label):
    summary = process(entries)
    if CRASH:
        print(f"[{label}] {len(entries)}건 처리함: {summary}")
        print(f"\n! 죽었다 — 처리는 끝났는데 ACK 를 못 했다. 다시 켜면 이 {len(entries)}건이 또 온다")
        sys.exit(1)
    r.xack(STREAM, GROUP, *[entry_id for entry_id, _ in entries])      # 처리가 끝난 '후'에 보고
    lag, pending = lag_and_pending()
    print(f"[{label}] {len(entries):>3}건 · 밀린 수 {lag} · ACK 대기 {pending} · {summary}")


def run():
    check()
    try:
        r.xgroup_create(STREAM, GROUP, id="0", mkstream=True)          # 새 그룹은 장부의 처음부터
        print(f"그룹 {GROUP} 을 새로 만들었다 — 장부의 처음부터 읽는다")
    except redis.exceptions.ResponseError:
        print(f"그룹 {GROUP} 은 이미 있다 — Redis 가 기억하는 위치부터 이어서 읽는다")
    print(f"소비자 {GROUP}/{CONSUMER} · 한 번에 {BATCH}건 (Ctrl+C 종료)\n")

    try:
        # ① 지난번에 받아 가고 ACK 못 한 것부터 ('0' = 내 ACK 대기분)
        while True:
            resp = r.xreadgroup(GROUP, CONSUMER, {STREAM: "0"}, count=BATCH)
            entries = resp[0][1] if resp else []
            if not entries:
                break
            handle(entries, "재처리")

        # ② 그다음 새 메시지 ('>' = 이 그룹이 아직 안 받아 간 것). 없으면 2초까지 기다린다
        while True:                                                     # 끝없이 받기
            resp = r.xreadgroup(GROUP, CONSUMER, {STREAM: ">"}, count=BATCH, block=2000)
            if not resp:
                print("  (새 메시지 없음 — 기다리는 중)")
                continue
            handle(resp[0][1], "새 것 ")
            time.sleep(1)                                               # 화면을 읽을 수 있게 — 1초치를 모아서 받는다
    except KeyboardInterrupt:
        lag, pending = lag_and_pending()
        print(f"\n종료 — 밀린 수 {lag} · ACK 대기 {pending}. 다시 켜면 여기서부터 이어진다 (위치는 Redis 안에 있다)")


if __name__ == "__main__":
    run()
