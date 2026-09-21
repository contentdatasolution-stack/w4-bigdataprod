"""5단계 — 진짜 데이터로: 업비트 실시간 체결 → Redis 장부.  student/upbit_producer.py 에서 '한 줄'만 바뀐 버전

    python step5_upbit_producer.py                  # 기본 5종목, Ctrl+C 로 종료
    python step5_upbit_producer.py KRW-BTC KRW-ETH

보여 줄 것 — 두 파일을 나란히 띄워 놓고
    upbit_producer.py        f.write(json.dumps(m) + "\\n")            파일에 한 줄 덧붙이기
    이 파일                   r.xadd(STREAM, {...}, maxlen=...)         Redis 장부에 한 건 덧붙이기
  받는 쪽(웹소켓)은 한 글자도 안 바뀌었다. 생산자에게 중개자는 '덧붙이는 곳'일 뿐이다.

  maxlen = 장부를 무한정 키우지 않겠다는 약속(오래된 것부터 버린다). Kafka 의 retention 에 해당.
  → 되물을 것: "소비자가 그보다 더 오래 밀려 있으면?" → 못 읽은 채로 사라진다. 완충 지대에도 한도가 있다 (슬라이드 32).
"""
import json
import sys
import time
import uuid

import certifi
import websocket  # websocket-client

from common import check, r

WS_URL = "wss://api.upbit.com/websocket/v1"
CODES = [a for a in sys.argv[1:] if not a.startswith("--")] or ["KRW-BTC", "KRW-ETH", "KRW-XRP", "KRW-SOL", "KRW-DOGE"]
STREAM = "upbit-trades"
MAXLEN = 100_000                                  # 장부에 남겨 둘 최대 건수 (대략)
FIELDS = ["code", "trade_price", "trade_volume", "ask_bid", "trade_timestamp", "sequential_id"]


def run():
    check()
    count = 0
    while True:                                           # 끝이 없다
        try:
            ws = websocket.create_connection(WS_URL, timeout=30, sslopt={"ca_certs": certifi.where()})
            ws.send(json.dumps([{"ticket": str(uuid.uuid4())},
                                {"type": "trade", "codes": CODES, "is_only_realtime": True},
                                {"format": "DEFAULT"}]))
            print(f"연결됨: {CODES} → Redis 장부 '{STREAM}'")
            while True:
                m = json.loads(ws.recv())
                entry_id = r.xadd(STREAM, {k: m[k] for k in FIELDS},       # ← 바뀐 한 줄
                                  maxlen=MAXLEN, approximate=True)
                count += 1
                if count % 50 == 0:
                    print(f"{count}건 기록 · 장부 {r.xlen(STREAM):,}건 · 마지막 위치 {entry_id}")
        except KeyboardInterrupt:
            print(f"\n종료 — 이번 실행에서 {count}건 기록")
            return
        except Exception as e:                            # 끊김/타임아웃 → 재연결
            print(f"연결 끊김 ({type(e).__name__}: {e}) — 3초 뒤 재연결")
            time.sleep(3)


if __name__ == "__main__":
    run()
