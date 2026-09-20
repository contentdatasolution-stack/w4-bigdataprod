# Week 4 실습 — 수집 코드의 세 가지 모양

빅데이터 수업 4주차 **학생 실습용 저장소**입니다. 데이터를 수집하는 코드는 원천에 따라 세 가지 모양을 가집니다.
이 저장소의 노트북과 스크립트를 직접 실행하고, 빈칸(TODO)을 채우면서 세 가지를 모두 경험합니다.

| 모양 | 원천 | 어디서 실습하나 |
|---|---|---|
| 한 번 읽기 | 파일 · DB (뉴욕 택시 parquet → SQLite) | `week4_practice.ipynb` PART 1-A, 1-B |
| 반복해서 묻기 | API (국토교통부 수단통행량) | `week4_practice.ipynb` PART 1-C |
| 끝없이 받기 | 스트림 (업비트 가상자산 실시간 체결) | `week4_practice.ipynb` PART 3, `upbit_producer.py`, `upbit_consumer.py`, `my_consumer.py` |

## 폴더 구조

```
w4-bigdataprod/
├── README.md                 이 문서
├── requirements.txt          필요한 파이썬 패키지 목록
├── .env.example              API 인증키 파일의 견본 (.env 로 복사해서 사용)
├── .gitignore                깃에 올리지 않을 파일 목록 (.env, 데이터, 실행 결과 등)
│
├── week4_practice.ipynb      ★ 메인 실습 노트북
├── upbit_producer.py         생산자 — 실시간 체결을 받아 장부에 기록
├── upbit_consumer.py         소비자 — 장부를 읽어 창(window)마다 집계
├── my_consumer.py            ★ 과제 — TODO ①~④를 채우는 미니 소비자
│
├── Parquet.png               노트북에 들어가는 그림 (행 기반 vs 열 기반 저장)
├── docs/                     참고 문서
│   └── OPENAPI 활용자가이드_수단통행량_v1.2.pdf
│
│   ── 아래는 실행하면 생기는 것들 (깃에 올라가지 않음) ──
├── yellow_tripdata_2023-01.parquet   원본 데이터, 직접 내려받는다 (45 MB)
├── .env                      내 API 인증키
├── data/                     가공해서 만든 DB
├── raw/                      받은 그대로 저장한 원본
└── state/                    "어디까지 했나"를 기억하는 값
```

## 파일 설명

### `week4_practice.ipynb` — 메인 실습 노트북

위에서부터 순서대로 실행합니다. 셀마다 `# 셀 N — 설명` 주석이 달려 있습니다.

| 구간 | 셀 | 내용 |
|---|---|---|
| 공통 준비 | 0 | import, 폴더 생성 |
| PART 1-A · 한 번 읽기 — 파일 | 1 ~ 2 | parquet 파일을 통째로 읽기, 열 이름·타입·빈 값 확인, 파일 이름(2023-01)과 실제 데이터가 맞는지 검사 |
| PART 1-B · 한 번 읽기 — DB | 3 ~ 8 | parquet → SQLite 적재, `read_sql`로 쿼리 보내기, WHERE / GROUP BY가 돌아오는 양을 정한다, 직접 SQL 날려 보기, **증분(incremental) 수집** |
| PART 1-C · 반복해서 묻기 — API | 9 ~ 13 | 인증키 읽기와 `fetch` 함수, 응답 검사(HTTP 200이어도 에러일 수 있다), `json_normalize`, 여러 페이지 받기(끝은 서버의 `totalCount`에게 묻는다), 간단한 집계 |
| PART 3 · 끝없이 받기 — 스트림 | 14 ~ 20 | 업비트 WebSocket 첫 메시지 구조, 생산자 → 큐 → 소비자, 장부에 덧붙이기, 소비자 그룹별 위치(`seek` / `tell`), 창(window) 집계, 정리 |

### `upbit_producer.py` — 생산자

업비트 WebSocket에 연결해 실시간 체결을 받고, 장부 `raw/upbit_trades.jsonl`에 **한 줄 = 한 메시지**로 덧붙이기만 합니다.
누가 읽는지(소비자가 있는지)는 전혀 모릅니다. 연결이 끊기면 3초 뒤 다시 붙습니다.

```bash
python upbit_producer.py                   # 기본 5종목 (BTC, ETH, XRP, SOL, DOGE), Ctrl+C 로 종료
python upbit_producer.py KRW-BTC KRW-ETH   # 종목 지정
```

### `upbit_consumer.py` — 소비자 (완성본, 참고용)

장부를 **자기 위치부터** 끝없이 읽고, 창(window)마다 종목별 체결수 · 거래량 · VWAP · 마지막가를 집계합니다.

```bash
python upbit_consumer.py          # 그룹 A, 10초 창
python upbit_consumer.py B 60     # 그룹 B, 60초 창
```

- 읽은 위치(바이트)를 `state/upbit_pos_<그룹>.txt`에 **처리 후** 기록 → 죽였다 다시 켜도 이어서 읽는다
- 같은 `sequential_id`는 한 번만 센다 (처리 후 기록이라 두 번 읽힐 수 있으므로)
- 닫힌 창의 집계 결과는 `raw/upbit_windows_<그룹>.jsonl`에 덧붙인다

### `my_consumer.py` — 과제: 나만의 미니 소비자

`upbit_consumer.py`의 핵심만 남긴 뼈대입니다. 지금도 실행은 되지만, 실행할 때마다 장부를 **처음부터 · 전부** 다시 읽고 고르지 않은 채 그대로 보여 줍니다.
파일 안의 TODO 네 곳을 채워서 **지난번 위치부터 · 정해진 크기(50 KB)만큼만 읽고 · pandas로 100만 원 이상 매수 체결만** 보여 주게 고칩니다.

| TODO | 할 일 |
|---|---|
| ① | 지난번 위치로 점프 |
| ② | `MAX_BYTES`만큼 읽었으면 멈추기 |
| ③ | pandas로 조건에 맞는 행만 고르기 |
| ④ | 처리가 끝난 **후**에 위치 기록 |

채운 뒤 해 볼 실험 네 가지는 파일 맨 위 설명에 있습니다. 실행하기 전에 결과를 먼저 예측해 보세요.

### 그 밖의 파일

- `requirements.txt` — pandas, pyarrow(parquet 읽기), requests(API), websocket-client · certifi(업비트 WebSocket), ipykernel(노트북 커널)
- `.env.example` — 인증키 파일의 견본. `.env`로 복사한 뒤 `DATA_GO_KR_KEY=` 뒤에 자기 키를 넣는다
- `Parquet.png` — 노트북 PART 1-A에 들어가는 그림 (행 기반 저장 vs 열 기반 저장)
- `docs/OPENAPI 활용자가이드_수단통행량_v1.2.pdf` — PART 1-C에서 쓰는 국토교통부 수단통행량 API 명세 (요청 변수, 응답 항목, 코드표)

## 실행하면 생기는 폴더

세 폴더 모두 코드가 자동으로 만들고, 깃에는 올라가지 않습니다.

| 폴더 | 역할 | 들어가는 것 |
|---|---|---|
| `data/` | 가공해서 만든 DB | `taxi.db` — parquet를 적재한 SQLite (노트북 셀 3, 한 번만, 약 430 MB) |
| `raw/` | **받은 그대로** 저장한 원본 | `trips_after_*.csv` (증분 수집 결과), `api/*.json` (API 응답 페이지), `upbit_trades.jsonl` (체결 장부), `upbit_windows*.jsonl` (창 집계 결과) |
| `state/` | **기억하는 값** — 어디까지 했나 | `taxi_last_ts.txt` (증분 수집의 마지막 시각), `upbit_pos_<그룹>.txt` · `my_pos.txt` (소비자가 읽은 위치) |

`state/`의 파일을 지우면 그 작업은 처음부터 다시 시작합니다. `raw/`의 장부는 지워지지 않고 그대로 남아 있습니다.

## 준비 (이 폴더에서, 한 번만)

```bash
# 0. 저장소 받기
git clone https://github.com/contentdatasolution-stack/w4-bigdataprod.git
cd w4-bigdataprod

# 1. 가상환경
python3 -m venv .venv && source .venv/bin/activate      # Windows: .venv\Scripts\activate
pip install -r requirements.txt

# 2. 원본 데이터 (45 MB)
curl -O https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet

# 3. API 인증키 — 코드에 쓰지 않는다
cp .env.example .env        # .env 를 열어 DATA_GO_KR_KEY= 뒤에 자기 키를 넣는다
```

노트북을 VS Code 또는 Jupyter에서 열고 `.venv` 커널로 **위에서부터 순서대로** 실행합니다.
인증키는 환경변수 `DATA_GO_KR_KEY` → 없으면 `.env` 파일 순서로 읽습니다. 둘 다 없으면 셀 9가 멈춥니다.
키 발급: 공공데이터포털(data.go.kr)에서 '국토교통부_수단통행량' 활용 신청.

> 모든 명령은 **이 폴더에서** 실행합니다. 코드가 `raw/`, `state/`, `data/`를 상대 경로로 씁니다.
> `.env`는 `.gitignore`에 들어 있어 깃에 올라가지 않습니다. 인증키를 코드나 노트북에 직접 쓰지 마세요.

## 끝없이 받기 — 터미널 두 개로

```bash
# 터미널 1: 생산자 — 장부 raw/upbit_trades.jsonl 에 덧붙이기만
python upbit_producer.py

# 터미널 2: 소비자 — 그룹 A, 10초 창
python upbit_consumer.py A 10
```

- 소비자를 Ctrl+C로 죽였다 다시 켜면 `state/upbit_pos_A.txt`의 위치부터 이어서 읽는다
- `python upbit_consumer.py B 60`을 하나 더 켜면 같은 장부를 처음부터 따로 읽는다
- 생산자를 잠깐 꺼도 소비자는 기다리고, 다시 켜면 이어서 받는다

그다음 생산자를 켜 둔 채 `python my_consumer.py`를 몇 초 간격으로 여러 번 실행하면서 과제(TODO ①~④)를 진행합니다.
