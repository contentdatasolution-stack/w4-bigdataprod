"""공통 도우미 — 연결 · 'redis-cli 에 칠 명령'을 보여 주기 · 단계 사이 멈춤 · 보기 좋게 출력."""
import sys

import redis

PORT = 6390                                   # 기본 포트(6379)와 섞이지 않게 시연 전용 포트
r = redis.Redis(port=PORT, decode_responses=True)


def check():
    try:
        r.ping()
    except redis.exceptions.ConnectionError:
        sys.exit("Redis 서버가 꺼져 있음 — 다른 터미널에서  sh step0_server.sh  를 먼저 실행하세요")


def title(text):
    print(f"\n{'═' * 78}\n{text}\n{'═' * 78}")


def cli(command):
    """지금 실행하는 것과 같은 redis-cli 명령. (redis-cli -p 6390 에서 그대로 쳐도 된다)"""
    print(f"\nredis> {command}")


def note(text):
    print(f"   # {text}")


def pause():
    """설명할 틈. 터미널에서는 Enter 를 기다리고, --auto 를 주거나 파이프로 돌리면 그냥 지나간다."""
    if sys.stdin.isatty() and "--auto" not in sys.argv:
        input("   ⏎ ")


def show_entries(entries):
    """[(id, {필드: 값}), ...] 를 한 줄에 하나씩."""
    if not entries:
        print("   (없음)")
    for entry_id, fields in entries:
        print(f"   {entry_id}   " + "  ".join(f"{k}={v}" for k, v in fields.items()))


def show_groups(stream):
    """XINFO GROUPS — 그룹마다: 어디까지 받아 갔나 · 몇 건 밀렸나 · ACK 안 한 것이 몇 건인가."""
    cli(f"XINFO GROUPS {stream}")
    print(f"   {'그룹':<6}{'last-delivered-id':<22}{'읽은 수':>8}{'밀린 수(lag)':>14}{'ACK 대기(pending)':>20}")
    for g in r.xinfo_groups(stream):
        lag = "-" if g.get("lag") is None else g["lag"]
        print(f"   {g['name']:<8}{g['last-delivered-id']:<22}{g.get('entries-read') or 0:>8}{lag:>14}{g['pending']:>20}")
