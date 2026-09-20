"""생산자 — 업비트 실시간 체결을 받아 장부(raw/upbit_trades.jsonl)에 덧붙이기만 한다.

    python upbit_producer.py                 # 기본 5종목, Ctrl+C 로 종료
    python upbit_producer.py KRW-BTC KRW-ETH # 종목 지정

연결이 끊기면 3초 뒤 다시 붙는다. 소비자가 있는지조차 모른다.
"""
import json
import sys
import time
import uuid
from pathlib import Path

import certifi
import websocket  # websocket-client

WS_URL = "wss://api.upbit.com/websocket/v1"
CODES = sys.argv[1:] or ["KRW-BTC", "KRW-ETH", "KRW-XRP", "KRW-SOL", "KRW-DOGE"]
LEDGER = Path("raw/upbit_trades.jsonl")


def run():
    LEDGER.parent.mkdir(parents=True, exist_ok=True)
    count = 0
    with LEDGER.open("a", encoding="utf-8") as f:
        while True:                                           # 끝이 없다
            try:
                ws = websocket.create_connection(
                    WS_URL, timeout=30, sslopt={"ca_certs": certifi.where()})
                ws.send(json.dumps([{"ticket": str(uuid.uuid4())},
                                    {"type": "trade", "codes": CODES, "is_only_realtime": True},
                                    {"format": "DEFAULT"}]))
                print(f"연결됨: {CODES}")
                while True:
                    m = json.loads(ws.recv())
                    f.write(json.dumps(m, ensure_ascii=False) + "\n")   # 한 줄 = 한 메시지
                    f.flush()
                    count += 1
                    if count % 50 == 0:
                        print(f"{count}건 기록 · 장부 {LEDGER.stat().st_size:,} 바이트")
            except KeyboardInterrupt:
                print(f"\n종료 — 이번 실행에서 {count}건 기록")
                return
            except Exception as e:                             # 끊김/타임아웃 → 재연결
                print(f"연결 끊김 ({type(e).__name__}: {e}) — 3초 뒤 재연결")
                time.sleep(3)


if __name__ == "__main__":
    run()
