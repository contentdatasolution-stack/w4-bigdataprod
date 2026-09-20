"""소비자 — 장부(raw/upbit_trades.jsonl)를 자기 위치부터 끝없이 읽고, 창(window)마다 집계한다.

    python upbit_consumer.py              # 그룹 A, 10초 창
    python upbit_consumer.py B 60         # 그룹 B, 60초 창

- 위치(바이트)는 state/upbit_pos_<그룹>.txt 에 '처리 후' 기록 → 죽였다 다시 켜도 이어서 읽는다
- 같은 sequential_id 는 한 번만 센다 (처리 후 기록이라 두 번 읽힐 수 있으므로)
- 닫힌 창의 집계 결과는 raw/upbit_windows_<그룹>.jsonl 에 덧붙인다
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
