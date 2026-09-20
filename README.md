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
├── week4_practice_en.ipynb   메인 실습 노트북의 영문판 (코드는 같고 설명·주석·출력 문구만 영어)
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

영문판은 `week4_practice_en.ipynb`입니다. 셀 구성과 코드는 한글판과 같고, 설명 · 주석 · `print` 문구만 영어입니다. 두 노트북은 같은 `data/`, `raw/`, `state/`를 쓰므로 하나만 골라서 실행하세요.

### `upbit_producer.py` — 생산자

업비트 WebSocket에 연결해 실시간 체결을 받고, 장부 `raw/upbit_trades.jsonl`에 **한 줄 = 한 메시지**로 덧붙이기만 합니다.
누가 읽는지(소비자가 있는지)는 전혀 모릅니다. 연결이 끊기면 3초 뒤 다시 붙습니다.

```bash
python upbit_producer.py                   # 기본 5종목 (BTC, ETH, XRP, SOL, DOGE), Ctrl+C 로 종료
python upbit_producer.py KRW-BTC KRW-ETH   # 종목 지정
```

**장부(ledger)란?** 받은 체결 메시지를 도착한 순서대로 쌓아 두는 파일 `raw/upbit_trades.jsonl`을 이 실습에서 장부라고 부릅니다.
가게의 거래 장부처럼 **덧붙이기만 하고(append-only), 이미 적은 줄은 고치지도 지우지도 않습니다.**
생산자는 쓰기만, 소비자는 읽기만 하므로 둘은 서로를 모른 채 이 파일 하나로만 이어집니다.
한 번 적힌 줄의 위치(바이트)가 변하지 않기 때문에, 소비자는 "몇 바이트까지 읽었다"만 기억하면 이어서 읽을 수 있고 여러 소비자가 각자 속도로 같은 장부를 읽을 수 있습니다.
(Kafka 같은 메시지 큐의 로그(log)와 같은 생각입니다. 거기서는 읽은 위치를 offset이라고 부릅니다.)

**장부 한 줄의 속성** — 업비트 체결 메시지 하나입니다. ★는 실습 코드에서 실제로 쓰는 속성입니다.

| 속성 | 뜻 | 예 |
|---|---|---|
| ★ `code` | 종목 코드. `KRW-BTC` = 원화로 거래하는 비트코인 | `"KRW-BTC"` |
| ★ `trade_price` | 체결 가격(원), 코인 1개당 | `109649000.0` |
| ★ `trade_volume` | 체결량(코인 개수). 체결 금액 = `trade_price` × `trade_volume` | `0.0009` |
| ★ `ask_bid` | `ASK` = 매도 체결, `BID` = 매수 체결 | `"BID"` |
| ★ `trade_timestamp` | 체결이 일어난 시각(밀리초 단위 유닉스 시간). 창(window)을 나누는 기준 | `1789874437574` |
| ★ `sequential_id` | 체결 고유번호. 중복 제거에 쓴다 | `17898744375740000` |
| `type` | 메시지 종류. 체결은 항상 `trade` | `"trade"` |
| `timestamp` | 서버가 메시지를 보낸 시각(ms) | `1789874437622` |
| `trade_date` · `trade_time` | 체결 일자 · 시각. **UTC 기준** (한국 시간 = UTC + 9시간) | `"2026-09-20"` · `"03:20:37"` |
| `prev_closing_price` | 전일 종가 | `110917000.0` |
| `change` · `change_price` | 전일 종가 대비 `RISE` / `EVEN` / `FALL` · 그 차이(절댓값) | `"FALL"` · `1268000.0` |
| `best_ask_price` · `best_ask_size` | 체결 시점의 최우선 매도 호가 · 잔량 | `109649000` · `0.02228361` |
| `best_bid_price` · `best_bid_size` | 체결 시점의 최우선 매수 호가 · 잔량 | `109602000` · `0.88702525` |
| `stream_type` | `SNAPSHOT` = 연결 직후의 최근 값, `REALTIME` = 실시간. 이 코드는 `REALTIME`만 받는다 | `"REALTIME"` |

같은 설명이 `upbit_producer.py` 파일 맨 위에도 들어 있습니다.

### `upbit_consumer.py` — 소비자 (완성본, 참고용)

장부를 **자기 위치부터** 끝없이 읽고, 창(window)마다 종목별 체결수 · 거래량 · VWAP · 마지막가를 집계합니다.

```bash
python upbit_consumer.py          # 그룹 A, 10초 창
python upbit_consumer.py B 60     # 그룹 B, 60초 창
```

- 읽은 위치(바이트)를 `state/upbit_pos_<그룹>.txt`에 **처리 후** 기록 → 죽였다 다시 켜도 이어서 읽는다
- 같은 `sequential_id`는 한 번만 센다 (처리 후 기록이라 두 번 읽힐 수 있으므로)
- 닫힌 창의 집계 결과는 `raw/upbit_windows_<그룹>.jsonl`에 덧붙인다

**실행 순서 — 생산자가 먼저, 소비자는 그다음.** 소비자는 업비트에 직접 연결하지 않고, 생산자가 만든 장부 파일만 읽습니다.

```
업비트 ──WebSocket──▶ [생산자] ──덧붙이기──▶ raw/upbit_trades.jsonl ──읽기──▶ [소비자] ──▶ 화면 + 결과 파일
                     터미널 1                     (장부)                     터미널 2
```

1. 터미널 1에서 `python upbit_producer.py` → 장부 `raw/upbit_trades.jsonl`이 생기고 계속 자란다. **켜 둔 채로 둔다**
2. 터미널 2에서 `python upbit_consumer.py` → 그 장부를 읽어 창마다 집계한다

소비자를 먼저 켜면 "장부가 아직 없음 — 생산자를 먼저 켜세요"를 찍으며 기다립니다. 생산자를 꺼도 장부는 남아 있으므로, 예전에 쌓인 장부만 가지고 소비자를 돌려 볼 수도 있습니다.

**창(window)이란?** 끝없는 흐름은 끝이 없어서 "전체 평균"을 낼 수 없습니다. 그래서 시간을 일정한 길이(기본 10초)로 잘라 구간마다 집계합니다.
기준은 소비자가 읽은 시각이 아니라 **체결이 일어난 시각**(`trade_timestamp`)이라, 밀린 장부를 나중에 읽어도 결과가 같습니다.
창은 다음 창의 체결이 도착해야 닫히므로, Ctrl+C로 끄는 순간 열려 있던 마지막 창은 출력되지 않습니다.

**만들어지는 것**

| 무엇 | 내용 |
|---|---|
| 화면 출력 | 창이 닫힐 때마다 종목별 표 하나 (체결수 · 거래량 · VWAP · 마지막가) |
| `raw/upbit_windows_<그룹>.jsonl` | 집계 결과. **한 줄 = (창 하나, 종목 하나)**. 장부처럼 덧붙이기만 한다 |
| `state/upbit_pos_<그룹>.txt` | 장부를 몇 바이트까지 처리했는지 숫자 하나 (예: `149397`). 지우면 처음부터 다시 읽는다. 그룹마다 따로 |

```
[12:22:40 ~ +10s] 창 닫힘
  종목           체결수             거래량            VWAP            마지막가
  KRW-BTC       23          0.2945     109,717,027     109,718,000
  KRW-ETH       41         52.0708       3,529,560       3,530,000
```

결과 파일 한 줄의 속성:

| 속성 | 뜻 |
|---|---|
| `group` | 소비자 그룹 이름 (실행할 때 준 첫 번째 인자) |
| `window_start` | 창이 시작한 시각 (초 단위 유닉스 시간) |
| `code` | 종목 코드 |
| `trades` | 그 창에서 일어난 체결 건수 |
| `volume` | 체결량 합계 (코인 개수) |
| `vwap` | 거래량 가중 평균가 = Σ(`trade_price` × `trade_volume`) / Σ `trade_volume`. 많이 거래된 가격일수록 크게 반영된다 |
| `last` | 그 창의 마지막 체결 가격 |

원본 체결 수백 줄이 종목당 한 줄로 줄어듭니다. 장부(`raw/upbit_trades.jsonl`)는 읽기만 하고 고치지 않습니다. 같은 설명이 `upbit_consumer.py` 파일 맨 위에도 들어 있습니다.

### 잠깐 — JSONL이 뭔가요? (JSON과 다른 점)

생산자와 소비자가 주고받는 장부(`upbit_trades.jsonl`)와 창 집계 결과(`upbit_windows_<그룹>.jsonl`)는 모두 **JSONL**(JSON Lines) 형식입니다.
규칙은 하나뿐입니다. **한 줄에 JSON 하나**, 줄과 줄 사이는 줄바꿈(`\n`)으로 구분합니다.

```
{"code": "KRW-BTC", "trade_price": 158200000.0, "trade_volume": 0.0021, "ask_bid": "BID"}
{"code": "KRW-ETH", "trade_price": 5921000.0, "trade_volume": 0.15, "ask_bid": "ASK"}
{"code": "KRW-XRP", "trade_price": 4105.0, "trade_volume": 1200.0, "ask_bid": "BID"}
```

같은 내용을 보통의 JSON 파일로 저장하면 전체를 대괄호로 감싼 **하나의 배열**이 됩니다.

```json
[
  {"code": "KRW-BTC", "trade_price": 158200000.0, "trade_volume": 0.0021, "ask_bid": "BID"},
  {"code": "KRW-ETH", "trade_price": 5921000.0, "trade_volume": 0.15, "ask_bid": "ASK"},
  {"code": "KRW-XRP", "trade_price": 4105.0, "trade_volume": 1200.0, "ask_bid": "BID"}
]
```

| | JSON (`.json`) | JSONL (`.jsonl`) |
|---|---|---|
| 파일 전체 | 값 **하나** (보통 큰 배열이나 객체) | 독립된 JSON이 **줄마다 하나씩** |
| 새 데이터 추가 | 맨 끝의 `]` 앞에 끼워 넣어야 함 → 사실상 파일을 다시 써야 한다 | 파일 끝에 한 줄 덧붙이면 끝 (`open("a")`) |
| 읽기 | 끝까지 다 읽어야 파싱된다 (`json.load`) | 한 줄씩 읽으며 바로 처리 (`readline` → `json.loads`) |
| 쓰는 도중 끊기면 | 닫는 괄호가 없어 **파일 전체**를 못 읽는다 | 마지막 한 줄만 버리면 되고 나머지는 멀쩡하다 |
| 이어서 읽기 | 중간 위치에서 시작할 수 없다 | 줄 경계의 바이트 위치만 기억하면 거기서부터 (`seek`) |
| 사람이 보기 | 들여쓰기로 예쁘게 볼 수 있다 | 한 줄이 길어 보기 불편 (대신 `head`, `tail -f`, `wc -l`이 통한다) |
| 어울리는 곳 | 설정 파일, API 응답 한 건 | 로그, 이벤트 스트림, 끝없이 쌓이는 데이터 |

이 실습이 JSONL을 쓰는 이유가 바로 위 표에 있습니다.

- **생산자**는 체결이 올 때마다 `f.write(json.dumps(m) + "\n")` 한 줄만 덧붙입니다. 파일이 아무리 커져도 기존 내용은 건드리지 않습니다.
- **소비자**는 `f.seek(pos)`로 지난번 위치로 점프해 새로 쌓인 줄만 읽습니다. 끝없이 자라는 파일이라 "다 읽고 파싱"은 애초에 불가능합니다.
- 소비자 코드의 `if not line.endswith(b"\n")`는 **생산자가 아직 쓰는 중인 반쪽 줄**을 걸러 내는 검사입니다. 줄바꿈이 있어야 완성된 한 줄입니다.

주의할 점: JSONL 파일 전체는 올바른 JSON이 **아닙니다**. `json.load(f)`로 통째로 읽으면 에러가 납니다. 한 줄씩 `json.loads(line)`으로 읽거나, pandas라면 `pd.read_json("파일.jsonl", lines=True)`를 씁니다.
한 줄 안에는 줄바꿈이 들어갈 수 없으므로 `json.dumps`에 `indent`를 주면 안 됩니다. (API 응답 한 건을 저장하는 `raw/api/*.json`은 추가할 일이 없는 데이터라 보통의 JSON에 `indent=2`로 저장합니다.)

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
cp .env.example .env        # .env 를 열어 DATA_GO_KR_KEY= 뒤에 자기 키를 넣는다. 수업시간에 제공
```

노트북을 VS Code 또는 Jupyter에서 열고 `.venv` 커널로 **위에서부터 순서대로** 실행합니다.
인증키는 환경변수 `DATA_GO_KR_KEY` → 없으면 `.env` 파일 순서로 읽습니다. 
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
