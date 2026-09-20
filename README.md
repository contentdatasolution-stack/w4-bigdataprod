# Week 4 실습 — 수집 코드의 세 가지 모양

| 모양 | 원천 | 파일 |
|---|---|---|
| 한 번 읽기 | 파일 · DB (`yellow_tripdata_2023-01.parquet` → SQLite) | `week4_practice.ipynb` PART 1-A, 1-B |
| 반복해서 묻기 | API (국토교통부 수단통행량) | `week4_practice.ipynb` PART 1-C |
| 끝없이 받기 | 스트림 (업비트 가상자산 실시간 체결) | `week4_practice.ipynb` PART 3, `upbit_producer.py`, `upbit_consumer.py`, `my_consumer.py` |

강의 슬라이드는 `Week4_Data_Collection_KR.pdf`, API 명세는 `OPENAPI 활용자가이드_수단통행량_v1.2.pdf`.

## 준비 (이 폴더에서, 한 번만)

```bash
# 1. 가상환경
python3 -m venv .venv && source .venv/bin/activate      # Windows: .venv\Scripts\activate
pip install -r requirements.txt

# 2. 원본 데이터 (45 MB)
curl -O https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet

# 3. API 인증키 — 코드에 쓰지 않는다
cp .env.example .env        # .env 를 열어 DATA_GO_KR_KEY= 뒤에 자기 키를 넣는다
```

노트북을 VS Code 또는 Jupyter에서 열고 `.venv` 커널로 **위에서부터 순서대로** 실행.
인증키는 환경변수 `DATA_GO_KR_KEY` → 없으면 `.env` 파일 순서로 읽는다. 둘 다 없으면 셀 9가 멈춘다.
키 발급: 공공데이터포털(data.go.kr)에서 '국토교통부_수단통행량' 활용 신청.

> 모든 명령은 **이 폴더에서** 실행한다. 코드가 `raw/`, `state/`, `data/` 를 상대 경로로 쓴다.

## 끝없이 받기 — 터미널 두 개로

```bash
# 터미널 1: 생산자 — 장부 raw/upbit_trades.jsonl 에 덧붙이기만
python upbit_producer.py

# 터미널 2: 소비자 — 그룹 A, 10초 창
python upbit_consumer.py A 10
```

- 소비자를 Ctrl+C로 죽였다 다시 켜면 `state/upbit_pos_A.txt`의 위치부터 이어서 읽는다
- `python upbit_consumer.py B 60` 을 하나 더 켜면 같은 장부를 처음부터 따로 읽는다
- 생산자를 잠깐 꺼도 소비자는 기다리고, 다시 켜면 이어서 받는다

## 실습 — my_consumer.py

생산자를 켜 둔 채 `python my_consumer.py` 를 몇 초 간격으로 여러 번 실행한다.
지금은 매번 장부를 처음부터 전부 읽고, 고르지 않고 그대로 보여 준다. 파일 안의 TODO ①~④ 네 곳을 채워서
**지난번 위치부터 · 정해진 크기(50 KB)만큼만 읽고 · pandas로 100만 원 이상 매수 체결만** 보여 주게 고친다.
채운 뒤 해 볼 실험은 파일 맨 위 설명에 있다.

## 실행하면 생기는 폴더

- `data/taxi.db` — parquet를 적재한 SQLite (노트북 셀 3, 한 번만, 약 430 MB)
- `raw/` — 받은 그대로 저장 (증분 CSV, API 페이지 JSON, 체결 장부 jsonl)
- `state/` — 기억하는 값 (마지막 시각, 소비자 위치). 지우면 처음부터 다시
