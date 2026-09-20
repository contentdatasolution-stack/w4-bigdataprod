"""실습 — 나만의 미니 소비자. 장부(raw/upbit_trades.jsonl)에서 '새로 쌓인 것'을 '정해진 크기만큼만' 읽고,
pandas로 그중 일부를 골라(query) 보여 준다.

    python my_consumer.py            # 몇 초 간격으로 여러 번 실행해 보세요

준비: 다른 터미널에서 생산자가 돌고 있어야 한다 →  python upbit_producer.py

지금 상태로도 실행은 된다. 다만 실행할 때마다 장부를 '처음부터 · 전부' 다시 읽고, 고르지 않고 그대로 보여 준다.
TODO ①~④ 네 줄을 채우는 것이 실습.

    ① 지난번 위치로 점프                  ② MAX_BYTES 만큼 읽었으면 멈추기
    ③ pandas로 조건에 맞는 행만 고르기      ④ 처리가 끝난 '후'에 위치 기록

채운 뒤 해 볼 것 (실행 전에 결과를 먼저 예측!)
  1. 연달아 여러 번 실행                  → '밀린 양'은 어떻게 변하나? 몇 번 만에 따라잡나?
  2. MAX_BYTES 를 5_000 으로 줄이고 실행   → 생산자보다 느린 소비자. '밀린 양'은 줄어드나, 늘어나나?
  3. state/my_pos.txt 를 지우고 실행       → 어디서부터 읽나? 장부는 어떻게 됐나?
  4. ③ 의 조건을 바꿔 보기                → 예: XRP 매도(ASK)만 / 체결량(trade_volume) 상위 / 1천만 원 이상
"""
import json
from pathlib import Path

import pandas as pd

LEDGER = Path("raw/upbit_trades.jsonl")      # 생산자가 덧붙이기만 하는 장부
POS_FILE = Path("state/my_pos.txt")          # 내가 기억하는 값: 어디까지 읽었나 (바이트)
MAX_BYTES = 50_000                           # 한 번 실행에 읽을 최대 크기 (약 100건)

pd.set_option("display.width", 140)
pd.set_option("display.float_format", lambda v: f"{v:,.4f}".rstrip("0").rstrip("."))


def run():
    if not LEDGER.exists():
        print("장부가 아직 없음 — 다른 터미널에서 python upbit_producer.py 를 먼저 켜세요")
        return
    POS_FILE.parent.mkdir(parents=True, exist_ok=True)

    pos = int(POS_FILE.read_text()) if POS_FILE.exists() else 0     # 지난번 위치, 없으면 0
    start = pos
    rows = []

    # ── 1. 수집: 지난번 위치부터, MAX_BYTES 만큼만 ─────────────────────────────
    with LEDGER.open("rb") as f:
        # TODO ① 지난번 위치(pos)로 점프한다 — 한 줄
        # (힌트: 슬라이드 25, f.____(pos))

        while True:
            line = f.readline()
            if not line.endswith(b"\n"):          # 새 줄이 없거나, 생산자가 쓰는 중인 반쪽 줄 → 다음 실행에서
                break
            rows.append(json.loads(line))
            pos = f.tell()                        # 여기까지 읽었다

            # TODO ② 이번 실행에서 읽은 크기가 MAX_BYTES 이상이면 멈춘다 — 두 줄 (if ... : break)
            # (힌트: 이번에 읽은 크기 = 지금 위치 - 시작 위치)

    size = LEDGER.stat().st_size
    print(f"위치 {start:,} → {pos:,} 바이트 · 이번에 {pos - start:,} 바이트 / {len(rows)}건 읽음")
    print(f"장부 크기 {size:,} · 밀린 양 {size - pos:,} 바이트")

    # ── 2. 처리: 표로 만들고, 골라서 보여 준다 ─────────────────────────────────
    if rows:
        df = pd.DataFrame(rows)
        df["time"] = (pd.to_datetime(df["trade_timestamp"], unit="ms", utc=True)
                        .dt.tz_convert("Asia/Seoul").dt.strftime("%H:%M:%S"))
        df["amount"] = (df["trade_price"] * df["trade_volume"]).round(0)      # 체결 금액(원)

        # TODO ③ 체결 금액이 100만 원 이상인 '매수(BID)' 체결만 고른다 — 아래 한 줄을 고친다
        # (힌트: df.query("... and ...") · 열 이름: amount, ask_bid · 문자열 값은 작은따옴표로 'BID')
        result = df                               # 지금은 고르지 않고 전부

        cols = ["time", "code", "ask_bid", "trade_price", "trade_volume", "amount"]
        print(f"\n고른 결과: {len(result)}건 / {len(df)}건 (최대 10건 표시)")
        print(result[cols].head(10).to_string(index=False) if len(result) else "  (조건에 맞는 체결 없음)")

        print("\n종목별 요약 (이번에 읽은 것 전체)")
        print(df.groupby("code").agg(trades=("amount", "size"), amount=("amount", "sum"))
                .sort_values("amount", ascending=False).to_string())

    # TODO ④ 처리가 끝난 '후'에 위치(pos)를 POS_FILE 에 적는다 — 한 줄
    # (힌트: POS_FILE.write_text(...) 는 문자열만 받는다)


if __name__ == "__main__":
    run()
