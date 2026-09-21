"""9단계 — 정리. 시연용 장부 두 개를 지운다 (그룹·위치·ACK 대기 목록도 장부와 함께 사라진다).

    python step9_cleanup.py

서버까지 끄려면:  step0 터미널에서 Ctrl+C   (또는  redis-cli -p 6390 shutdown nosave)
서버를 메모리 전용으로 띄웠으므로 끄면 디스크에 남는 것이 없다.
"""
from common import check, cli, r

check()
for stream in ["views", "upbit-trades"]:
    cli(f"DEL {stream}")
    print(f"   → {r.delete(stream)}")
print("\n남은 키:", r.keys("*") or "없음")
