"""생산자 — 업비트 실시간 체결을 받아 장부(raw/upbit_trades.jsonl)에 덧붙이기만 한다.

    python upbit_producer.py                 # 기본 5종목, Ctrl+C 로 종료
    python upbit_producer.py KRW-BTC KRW-ETH # 종목 지정

연결이 끊기면 3초 뒤 다시 붙는다. 소비자가 있는지조차 모른다.

■ 이 코드가 하는 일
  1. 업비트 WebSocket(wss://api.upbit.com/websocket/v1)에 연결한다
  2. "이 종목들의 체결(trade)을 실시간으로 보내 달라"는 구독 요청을 한 번 보낸다
  3. 그 뒤로는 묻지 않아도 체결이 일어날 때마다 서버가 메시지를 보내 준다 (끝없이 받기)
  4. 받은 메시지를 고치지 않고 '그대로' 장부 파일 끝에 한 줄로 덧붙인다
  집계·필터 같은 처리는 하지 않는다. 그건 소비자(upbit_consumer.py, my_consumer.py)의 일이다.

■ 장부(ledger)란?
  받은 체결 메시지를 '도착한 순서대로' 쌓아 두는 파일 raw/upbit_trades.jsonl 을 이 실습에서 장부라고 부른다.
  가게의 거래 장부처럼 규칙은 하나 — 덧붙이기만 한다(append-only). 이미 적은 줄은 고치지도 지우지도 않는다.
    - 형식은 JSONL: 한 줄 = 체결 메시지 하나(JSON) + 줄바꿈. 그래서 open("a") 로 끝에 붙이기만 하면 된다
    - 생산자는 '쓰기만', 소비자는 '읽기만' 한다 → 둘은 서로를 모르고, 이 파일 하나로만 이어진다
    - 한 번 적힌 줄의 위치(바이트)는 변하지 않는다 → 소비자가 "나는 몇 바이트까지 읽었다"만 기억하면
      죽었다 다시 켜도 이어서 읽을 수 있고, 여러 소비자(그룹 A, B …)가 각자 속도로 같은 장부를 읽을 수 있다
    - 생산자가 잠깐 꺼져도 장부는 남아 있다 → 소비자는 밀린 것부터 처리하면 된다
  (Kafka 같은 메시지 큐의 '로그(log)'와 같은 생각이다. 거기서는 읽은 위치를 offset 이라고 부른다.)

■ 장부 한 줄의 속성 (업비트 체결 메시지, DEFAULT 포맷)
  {"type": "trade", "code": "KRW-BTC", "timestamp": 1789874437622, "trade_date": "2026-09-20",
   "trade_time": "03:20:37", "trade_timestamp": 1789874437574, "trade_price": 109649000.0,
   "trade_volume": 0.0009, "ask_bid": "BID", "prev_closing_price": 110917000.0, "change": "FALL",
   "change_price": 1268000.0, "sequential_id": 17898744375740000, "best_ask_price": 109649000,
   "best_ask_size": 0.02228361, "best_bid_price": 109602000, "best_bid_size": 0.88702525,
   "stream_type": "REALTIME"}

  실습에서 쓰는 것 (★)
  ★ code               종목 코드. "KRW-BTC" = 원화(KRW)로 거래하는 비트코인(BTC)
  ★ trade_price        체결 가격 (원) — 코인 1개당 가격
  ★ trade_volume       체결량 (코인 개수).  체결 금액(원) = trade_price × trade_volume
  ★ ask_bid            "ASK" = 매도 체결, "BID" = 매수 체결
  ★ trade_timestamp    체결이 일어난 시각 (밀리초 단위 유닉스 시간) — 창(window)을 나누는 기준
  ★ sequential_id      체결 고유번호 — 같은 체결이 두 번 읽혔을 때 중복을 걸러 내는 데 쓴다

  그 밖의 속성
    type               메시지 종류. 체결은 항상 "trade"
    timestamp          업비트 서버가 이 메시지를 보낸 시각 (ms). trade_timestamp 보다 조금 늦다
    trade_date         체결 일자 "yyyy-MM-dd" — UTC 기준 (한국 시간 = UTC + 9시간)
    trade_time         체결 시각 "HH:mm:ss" — UTC 기준
    prev_closing_price 전일 종가
    change             전일 종가 대비: "RISE" 상승 / "EVEN" 보합 / "FALL" 하락
    change_price       전일 종가와의 차이 (부호 없는 절댓값)
    best_ask_price     체결 시점의 최우선 매도 호가 (가장 싸게 팔겠다는 가격)
    best_ask_size      그 매도 호가에 걸린 잔량
    best_bid_price     체결 시점의 최우선 매수 호가 (가장 비싸게 사겠다는 가격)
    best_bid_size      그 매수 호가에 걸린 잔량
    stream_type        "SNAPSHOT" = 연결 직후 보내 주는 최근 값, "REALTIME" = 실시간.
                       이 코드는 is_only_realtime=True 로 구독해서 REALTIME 만 받는다
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
