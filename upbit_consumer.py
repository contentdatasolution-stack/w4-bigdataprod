"""소비자 — 장부(raw/upbit_trades.jsonl)를 자기 위치부터 끝없이 읽고, 창(window)마다 집계한다.

    python upbit_consumer.py              # 그룹 A, 10초 창
    python upbit_consumer.py B 60         # 그룹 B, 60초 창

- 위치(바이트)는 state/upbit_pos_<그룹>.txt 에 '처리 후' 기록 → 죽였다 다시 켜도 이어서 읽는다
- 같은 sequential_id 는 한 번만 센다 (처리 후 기록이라 두 번 읽힐 수 있으므로)
- 닫힌 창의 집계 결과는 raw/upbit_windows_<그룹>.jsonl 에 덧붙인다

■ 실행 순서 — 생산자가 먼저, 소비자는 그다음 (터미널 두 개)
  이 코드는 업비트에 직접 연결하지 않는다. 생산자가 만든 장부 파일만 읽는다.

    터미널 1   python upbit_producer.py     →  장부 raw/upbit_trades.jsonl 이 생기고 계속 자란다
    터미널 2   python upbit_consumer.py     →  그 장부를 읽어 창마다 집계한다

    업비트 ──WebSocket──▶ [생산자] ──덧붙이기──▶ raw/upbit_trades.jsonl ──읽기──▶ [소비자] ──▶ 화면 + 결과 파일
                                                  (장부)

  - 생산자는 켜 둔 채로 둔다. 둘은 동시에 돌고, 장부에 새 줄이 생기는 대로 소비자가 따라 읽는다
  - 소비자를 먼저 켜도 된다: "장부가 아직 없음 — 생산자를 먼저 켜세요"를 찍으며 장부가 생길 때까지 기다린다
  - 생산자를 꺼도 소비자는 죽지 않고 새 줄을 기다린다. 장부는 남아 있으므로, 생산자 없이
    예전에 쌓인 장부만 가지고 소비자를 돌려 볼 수도 있다 (밀린 것을 한꺼번에 처리하는 모습을 볼 수 있다)
  - 두 코드 모두 이 폴더에서 실행한다 (raw/, state/ 를 상대 경로로 쓴다)

■ 이 코드가 하는 일
  1. state/upbit_pos_<그룹>.txt 에서 지난번 위치를 읽는다 (없으면 0 = 장부 처음부터)
  2. 장부를 열고 그 위치로 점프한다 — f.seek(pos)
  3. 한 줄씩 읽는다. 줄바꿈으로 끝나지 않으면 '새 줄이 없거나 생산자가 쓰는 중' → 0.2초 쉬고 다시 시도
  4. 읽은 체결을 창(window)에 넣어 종목별로 더한다. 이미 본 sequential_id 는 건너뛴다
  5. 다음 창에 속하는 체결이 처음 도착하면 이전 창을 닫고(emit) 화면과 결과 파일에 내보낸다
  6. 한 줄을 처리한 '후'에 위치를 파일에 적는다 → 죽었다 다시 켜도 읽던 자리를 잃지 않는다

■ 창(window)이란?
  끝없는 흐름은 '전체 평균'을 낼 수 없다 — 끝이 없으니까. 그래서 시간을 일정한 길이로 잘라 구간마다 집계한다.
  10초 창이면 12:22:40~12:22:50, 12:22:50~12:23:00 … 처럼 나뉜다.
    창 시작 시각 = trade_timestamp(ms) // 1000 // 창길이 * 창길이
  기준은 '체결이 일어난 시각'이다 (소비자가 읽은 시각이 아니다). 그래서 밀린 장부를 나중에 읽어도 결과가 같다.
  창은 '다음 창의 체결이 도착해야' 닫힌다 → 마지막 창은 Ctrl+C 로 끄면 출력되지 않고 버려진다.

■ 만들어지는 것
  (1) 화면 출력 — 창이 닫힐 때마다 표 하나

      [12:22:40 ~ +10s] 창 닫힘
        종목           체결수             거래량            VWAP            마지막가
        KRW-BTC       23          0.2945     109,717,027     109,718,000
        KRW-ETH       41         52.0708       3,529,560       3,530,000

  (2) raw/upbit_windows_<그룹>.jsonl — 집계 결과. 한 줄 = (창 하나, 종목 하나). 장부처럼 덧붙이기만 한다

      {"group": "A", "window_start": 1789874560, "code": "KRW-BTC", "trades": 23,
       "volume": 0.29449874, "vwap": 109717027.28, "last": 109718000.0}

        group          소비자 그룹 이름 (실행할 때 준 첫 번째 인자)
        window_start   창이 시작한 시각 (초 단위 유닉스 시간). 위 예는 한국 시간 12:22:40
        code           종목 코드
        trades         그 창에서 일어난 체결 건수
        volume         체결량 합계 (코인 개수) = Σ trade_volume
        vwap           거래량 가중 평균가 = Σ(trade_price × trade_volume) / Σ trade_volume
                       단순 평균과 달리 '많이 거래된 가격'일수록 크게 반영된다
        last           그 창의 마지막 체결 가격

      원본 체결 수백 줄이 종목당 한 줄로 줄어든다 — 이것이 다음 단계(분석·저장)로 넘어가는 데이터다.

  (3) state/upbit_pos_<그룹>.txt — 기억하는 값. 장부를 몇 바이트까지 처리했는지 숫자 하나 (예: 149397)
      - 이 파일이 있어서 죽였다 다시 켜도 이어서 읽는다. 지우면 장부 처음부터 다시 읽는다
      - 그룹마다 파일이 따로다 → A 와 B 를 동시에 켜면 같은 장부를 각자 위치에서, 서로 상관없이 읽는다

  장부(raw/upbit_trades.jsonl)는 읽기만 한다. 소비자는 장부를 고치지도 지우지도 않는다.
"""
import json
import sys
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path

GROUP = sys.argv[1] if len(sys.argv) > 1 else "A"
WINDOW_SEC = int(sys.argv[2]) if len(sys.argv) > 2 else 10
LEDGER = Path("raw/upbit_trades.jsonl")
POS_FILE = Path(f"state/upbit_pos_{GROUP}.txt")
OUT = Path(f"raw/upbit_windows_{GROUP}.jsonl")
KST = timezone(timedelta(hours=9))


def emit(start, stats):
    """창이 닫혔다 → 화면에 찍고 파일에 덧붙인다."""
    label = datetime.fromtimestamp(start, KST).strftime("%H:%M:%S")
    print(f"\n[{label} ~ +{WINDOW_SEC}s] 창 닫힘")
    print(f"  {'종목':<10}{'체결수':>6}{'거래량':>16}{'VWAP':>16}{'마지막가':>16}")
    with OUT.open("a", encoding="utf-8") as f:
        for code, s in sorted(stats.items()):
            vwap = s["amount"] / s["volume"] if s["volume"] else 0
            print(f"  {code:<10}{s['trades']:>6}{s['volume']:>16,.4f}{vwap:>16,.0f}{s['last']:>16,.0f}")
            f.write(json.dumps({"group": GROUP, "window_start": start, "code": code,
                                "trades": s["trades"], "volume": s["volume"],
                                "vwap": vwap, "last": s["last"]}) + "\n")


def run():
    POS_FILE.parent.mkdir(parents=True, exist_ok=True)
    pos = int(POS_FILE.read_text()) if POS_FILE.exists() else 0
    print(f"그룹 {GROUP} · 위치 {pos} 바이트부터 · 창 {WINDOW_SEC}초 (Ctrl+C 종료)")
    window, cur, seen = {}, None, set()

    while not LEDGER.exists():
        print("장부가 아직 없음 — 생산자를 먼저 켜세요"); time.sleep(2)

    with LEDGER.open("rb") as f:
        f.seek(pos)                                          # 지난번 자리로 점프
        try:
            while True:                                      # 끝없이 받기
                line = f.readline()
                if not line.endswith(b"\n"):                 # 새 줄이 없거나 쓰는 중
                    f.seek(pos)
                    time.sleep(0.2)
                    continue
                m = json.loads(line)
                pos = f.tell()

                if m["sequential_id"] not in seen:
                    seen.add(m["sequential_id"])
                    w = m["trade_timestamp"] // 1000 // WINDOW_SEC * WINDOW_SEC
                    if cur is not None and w > cur:           # 다음 창이 시작 → 이전 창 닫기
                        for done in sorted(k for k in window if k < w):
                            emit(done, window.pop(done))
                    cur = max(cur or w, w)
                    s = window.setdefault(w, {}).setdefault(
                        m["code"], {"trades": 0, "volume": 0.0, "amount": 0.0, "last": 0.0})
                    s["trades"] += 1
                    s["volume"] += m["trade_volume"]
                    s["amount"] += m["trade_price"] * m["trade_volume"]
                    s["last"] = m["trade_price"]

                POS_FILE.write_text(str(pos))                # 처리 '후' 위치 기록
        except KeyboardInterrupt:
            print(f"\n종료 — 위치 {pos} 저장됨. 다시 켜면 여기서부터 이어서 읽는다.")


if __name__ == "__main__":
    run()
