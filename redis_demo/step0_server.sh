#!/bin/sh
# 0단계 — 시연 전용 Redis 서버를 켠다. 이 터미널은 서버가 차지한다 (끄려면 Ctrl+C).
#
#   --port 6390        기본 포트(6379)를 쓰는 다른 Redis 와 섞이지 않게
#   --save "" --appendonly no   디스크에 아무것도 쓰지 않는다 → 끄면 전부 사라진다 (시연 뒤 정리할 것이 없다)
#
# 설치: brew install redis
exec redis-server --port 6390 --save "" --appendonly no
