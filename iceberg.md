# Iceberg 테이블 설계 전환 리포트

> **목적**: ORC/Hive/HDFS → Parquet/Iceberg/S3 전환 시 테이블 구조를 Array에서 Flat으로 변경하는 근거와 기대 효과를 설명한다.
> **대상 독자**: FDC 프로젝트 관리자 및 데이터 엔지니어링 팀
> **작성일**: 2026-03-20
> **상태**: 제안 (리뷰 대기)

---

## 1. 요약 (Executive Summary)

기존 Spark 배치 파이프라인은 ORC 포맷 + Hive 테이블 + HDFS 위에 동작하며, Trace 데이터를 **Array(배열) 구조**와 **Flat(평탄화) 구조** 두 벌로 중복 적재해왔다. 이 설계는 ORC/Hive 환경의 특성에 맞춘 합리적 선택이었다.

Flink 스트리밍 전환과 함께 저장소가 **Parquet 포맷 + Iceberg 테이블 + MinIO(S3)** 로 변경된다. 이 새로운 스택에서는 Array 구조가 불필요할 뿐 아니라 성능상 불리하며, **Flat 테이블 하나만으로 기존 두 테이블의 역할을 모두 대체**할 수 있다.

### 제안 요약

| 구분 | 기존 (ORC + Hive + HDFS) | 제안 (Parquet + Iceberg + S3) |
|------|--------------------------|-------------------------------|
| Trace 테이블 수 | 2개 | **1개** |
| Summary 테이블 수 | 3개 | **1개** |
| 총 테이블 수 | 5개 | **2개** |
| 저장 공간 | 동일 데이터 2~3중 적재 | 1벌 적재 |
| Trino ad-hoc 쿼리 성능 | Array 테이블은 explode 필요, Flat 테이블은 타입 캐스팅 필요 | Predicate pushdown + Data skipping으로 최적화 |

---

## 2. 배경: 기존 설계가 만들어진 이유

### 2.1 기존 환경 구성

```
[Kafka] → [Spark 배치 ETL] → [ORC 파일] → [Hive Metastore + HDFS] → [Trino ad-hoc 쿼리]
```

### 2.2 Array 구조가 선택된 이유

기존 `trx` 테이블은 **1 row = 1 measurement point × N parameters** 구조다:

```sql
-- fdc_trace (Array): Kafka 메시지 1건 = 테이블 1 row
name    ARRAY<STRING>    -- ['Temperature', 'Pressure', 'Flow', ...]
value   ARRAY<STRING>    -- ['350.5', '2.1', '15.3', ...]
target        ARRAY<STRING>    -- ['350.0', '2.0', '15.0', ...]
-- ... (총 10개 Array 컬럼)
```

이 설계에는 다음과 같은 합리적 이유가 있었다:

1. **Kafka 메시지와 1:1 매핑**: 원천 데이터가 하나의 타임스탬프에 N개 파라미터를 묶어 전송하므로, 그대로 저장하면 변환 없이 빠르게 랜딩 가능
2. **ORC의 강점 활용**: ORC는 row 단위 읽기에 최적화되어 있어, "특정 웨이퍼의 모든 파라미터"를 한 번에 가져오는 패턴에 효율적
3. **Spark 배치 처리 편의**: `groupBy(key).agg(collect_list(...))` 등 배열 집계가 Spark에서 자연스러움
4. **Hive 파티셔닝 한계 보완**: Hive는 파티션 컬럼만으로 데이터를 건너뛸 수 있으므로, 파일 내부의 세밀한 필터링보다 "한 row에 많은 데이터를 담는" 전략이 합리적

### 2.3 Flat 테이블이 별도로 필요했던 이유

```sql
-- trx_flat (Flat): 분석용. 1 row = 1 measurement point × 1 parameter
name    STRING    -- 'Temperature'
value   STRING    -- '350.5'
target        STRING    -- '350.0'
```

Array 테이블만으로는 다음 쿼리를 효율적으로 수행할 수 없었다:

```sql
-- "Temperature 파라미터만 조회" → Array에서는 전체 row를 읽고 explode 후 필터
SELECT * FROM trx
CROSS JOIN UNNEST(name, value) AS t(name, value)
WHERE t.name = 'Temperature';

-- Flat에서는 직접 필터 가능
SELECT * FROM trx_flat
WHERE pname = 'Temperature';
```

따라서 **Array(원본 보존)와 Flat(분석 편의)을 동시에 유지**하는 2중 적재가 불가피했다.

---

## 3. 핵심 질문: 새 환경에서도 Array + Flat 2중 적재가 필요한가?

**결론: 아니다.** Parquet + Iceberg 환경에서는 Flat 테이블 하나만으로 Array 테이블의 원본 보존 역할까지 충분히 대체할 수 있다. 그 이유를 파일 포맷 레벨부터 설명한다.

---

## 4. ORC vs Parquet: 파일 포맷 수준의 차이

### 4.1 데이터 레이아웃 비교

ORC와 Parquet 모두 **컬럼형(columnar) 포맷**이지만, 내부 구조와 최적화 전략에 의미 있는 차이가 있다.

https://discuss.systems/@andy_pavlo/112442082673214691

```
┌─────────────────────────────────────────────────────────────────┐
│                        ORC 파일 구조                             │
├─────────────────────────────────────────────────────────────────┤
│  Stripe 1 (default 64MB)                                        │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │ Index Data: min/max per column (스칼라 컬럼에만 유효)      │   │
│  │ Row Data: 컬럼별 인코딩 저장                               │   │
│  │ Stripe Footer                                             │   │
│  └──────────────────────────────────────────────────────────┘   │
│  Stripe 2 ...                                                   │
│  File Footer: 전체 통계, 스키마                                  │
└─────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│                      Parquet 파일 구조                           │
├─────────────────────────────────────────────────────────────────┤
│  Row Group 1 (default 128MB)                                    │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │ Column Chunk: param_name                                  │   │
│  │  ├─ Page 1: [values...] + Page Header (min/max/nullcount) │   │
│  │  ├─ Page 2: [values...] + Page Header                     │   │
│  │  └─ Column Chunk metadata (min/max for entire chunk)      │   │
│  │ Column Chunk: param_value                                 │   │
│  │  ├─ Page 1 ...                                            │   │
│  │  └─ Column Chunk metadata                                 │   │
│  └──────────────────────────────────────────────────────────┘   │
│  Row Group 2 ...                                                │
│  Footer: Row Group별, Column Chunk별 min/max 통계                │
└─────────────────────────────────────────────────────────────────┘
```

### 4.2 Array 컬럼에 대한 통계 처리 차이

**이것이 이 리포트의 핵심이다.** ORC와 Parquet 모두 스칼라(단일값) 컬럼에 대해서는 min/max 통계를 유지하지만, **Array(중첩) 컬럼에 대해서는 두 포맷 모두 원소 레벨의 Predicate pushdown을 지원하지 않는다.**

| 동작 | ORC (스칼라 컬럼) | ORC (Array 컬럼) | Parquet (스칼라 컬럼) | Parquet (Array 컬럼) |
|------|------------------|-----------------|---------------------|---------------------|
| Column-level min/max 통계 | O | **X** (배열 전체를 하나의 값으로 취급) | O | **X** |
| Stripe/Row Group 스킵 | O | **X** | O | **X** |
| Predicate pushdown | O | **X** | O | **X** |

**핵심**: Array 컬럼은 ORC든 Parquet든 내부 원소 기준으로 파일을 건너뛸 수 없다. 따라서 `WHERE param_name = 'Temperature'` 같은 쿼리를 Array 테이블에 실행하면 **모든 파일의 모든 Stripe/Row Group을 읽어야** 한다.

### 4.3 그렇다면 기존 ORC에서는 왜 Array가 괜찮았나?

ORC + Hive 환경에서 Array 테이블이 허용됐던 이유:

1. **Array 테이블은 "분석용"이 아니었다**: 원본 보존 + Spark 내부 처리용이었고, 실제 ad-hoc 분석은 별도 Flat 테이블에서 수행
2. **Hive 파티션이 주요 필터링 수단**: `dt`, `fab` 파티션만으로 충분한 데이터 스킵이 가능했고, 파일 내부 통계에 의존하지 않음
3. **ORC의 Stripe 크기(64MB)가 상대적으로 작아** 전체 스캔 비용이 Parquet(128MB Row Group)보다 낮음
4. **HDFS의 Data locality**: 네트워크 I/O 없이 로컬 디스크에서 읽으므로 전체 스캔의 절대 비용이 낮음

### 4.4 새 환경에서 Flat이 결정적으로 유리한 이유

Parquet + Iceberg + S3 환경에서는 다음이 달라진다:

| 요소 | ORC + Hive + HDFS | Parquet + Iceberg + S3 | 영향 |
|------|-------------------|------------------------|------|
| **파일 통계의 중요도** | 낮음 (Hive 파티션이 주력) | **매우 높음** (Iceberg manifest가 파일별 컬럼 통계를 관리, 파일 단위로 스킵) | Flat 컬럼의 min/max 통계가 쿼리 성능을 결정 |
| **네트워크 I/O** | 없음 (HDFS data locality) | **항상 발생** (S3는 remote storage) | 불필요한 파일 읽기 비용이 HDFS 대비 수십~수백 배 |
| **파일 스킵 메커니즘** | Hive 파티션 pruning만 | Iceberg manifest → Parquet Row Group 통계 → Page 통계의 **3단계** 필터링 | Flat 컬럼이어야 3단계 모두 활용 가능 |
| **Sort Order** | 없음 (Hive에서 미지원) | Iceberg가 테이블 레벨 Sort Order 지원 | 정렬 시 같은 값이 같은 파일/Row Group에 모여 min/max 범위가 좁아짐 → 스킵률 극대화 |

---

## 5. Parquet 인코딩 심화: 딕셔너리 인코딩 + RLE

Flat 전환 시 row 수가 N배(파라미터 수)로 증가하지만, 실제 Parquet 저장 크기는 N배가 되지 않는다. 이는 Parquet의 **딕셔너리 인코딩(Dictionary Encoding)**과 **RLE(Run-Length Encoding)**가 반복 패턴을 극도로 효율적으로 압축하기 때문이다. 이 절에서는 그 메커니즘을 상세히 설명한다.

### 5.1 딕셔너리 인코딩 (Dictionary Encoding)

#### 5.1.1 동작 원리

Parquet는 각 Column Chunk를 쓸 때 **기본적으로 딕셔너리 인코딩을 시도**한다. 컬럼에 나타나는 고유 값(distinct value)들을 딕셔너리(사전)에 등록하고, 실제 데이터는 딕셔너리 인덱스(정수)로 저장한다.

```
[딕셔너리 인코딩 전 — 원본 데이터]
eqp_id 컬럼 (STRING): 'EQP001' 'EQP001' 'EQP001' 'EQP002' 'EQP002' 'EQP001' ...
                        (6 bytes × N rows = 수십 KB ~ 수 MB)

[딕셔너리 인코딩 후]
딕셔너리 (Dictionary Page):
  0 → 'EQP001'
  1 → 'EQP002'

데이터 (Data Page):
  0  0  0  1  1  0  ...    ← 정수 인덱스만 저장 (1~2 bytes per value)
```

#### 5.1.2 Flat 변환에서의 효과

Flat 변환 시 `eqp_id`, `lot_id`, `recipe_id`, `fab`, `module_name` 등 **컨텍스트 필드 ~15개**가 N개 row에 걸쳐 동일한 값으로 반복된다. 이 필드들의 **카디널리티(고유 값 수)가 매우 낮으므로** 딕셔너리가 극도로 작다:

| 컬럼 | 예상 카디널리티 (일일 기준) | 딕셔너리 크기 | 원본 대비 압축 |
|------|--------------------------|-------------|--------------|
| `fb` | 1~5 | 수십 바이트 | **>99%** |
| `eqid` | ~100~500 | 수 KB | **>95%** |
| `ltid` | ~1,000~10,000 | 수십 KB | **>90%** |
| `rcid` | ~50~200 | 수 KB | **>95%** |
| `mdlname` | ~10~50 | 수백 바이트 | **>98%** |
| `pname` | ~30~100 | 수 KB | **>95%** |
| `stid` | ~50~500 | 수 KB | **>95%** |

딕셔너리에 모든 고유 값을 저장하는 비용은 미미하며, 각 row는 **1~4 byte 정수 인덱스**만 저장한다. 이렇게 되면 Array에서 1 row에 들어있던 정보가 N개 row로 펼쳐져도, 실제 저장 바이트는 거의 증가하지 않는다.

#### 5.1.3 딕셔너리 Fallback

카디널리티가 지나치게 높으면(기본 임계값: 딕셔너리 페이지 크기 > 1MB) Parquet는 자동으로 **Plain 인코딩**으로 fallback한다. 이에 해당하는 컬럼:

| 컬럼 | 카디널리티 | 인코딩 | 비고 |
|------|----------|--------|------|
| `param_value` | 매우 높음 (연속 실수값) | Plain + ZSTD | 딕셔너리 비효율 → fallback |
| `ts` | 매우 높음 (마이크로초 타임스탬프) | Plain + DELTA_BINARY_PACKED | 정렬 시 Delta 인코딩 효과적 |

이 컬럼들은 Array든 Flat이든 **동일한 값을 동일한 바이트로 저장**하므로, Flat 전환이 이들의 저장 크기에 미치는 영향은 없다.

### 5.2 RLE (Run-Length Encoding)

#### 5.2.1 동작 원리

딕셔너리 인코딩 후의 정수 인덱스 시퀀스에 **RLE(Run-Length Encoding)**가 추가로 적용된다. 동일한 값이 연속으로 나타나면 `(값, 반복 횟수)` 쌍으로 압축한다.

```
[딕셔너리 인덱스 — RLE 전]
eqid: 0 0 0 0 0 0 0 0 0 0  1 1 1 1 1 1 1 1  0 0 0 0 0 0 ...
         ↑ EQP001 × 10         ↑ EQP002 × 8      ↑ EQP001 × 6

[RLE 후]
(0, 10)  (1, 8)  (0, 6)  ...    ← 3개의 쌍으로 24개 값 표현
```

#### 5.2.2 Hybrid RLE/Bit-Packing

Parquet는 순수 RLE와 **Bit-Packing**을 결합한 하이브리드 방식을 사용한다:

| 패턴 | 적용 인코딩 | 예시 |
|------|-----------|------|
| 동일 값 연속 반복 | **RLE** — `(값, 반복 횟수)` | eqid: 같은 장비의 row가 연속 |
| 값이 자주 바뀌지만 범위 좁음 | **Bit-Packing** — N-bit 정수 묶음 | pname: 30개 파라미터 → 5 bit per value |

```
딕셔너리 크기 = 30 → log₂(30) ≈ 5 bits per index

Bit-Packing 효과:
  원래: 30-char 평균 pname × 1,000 rows = 30,000 bytes
  인코딩 후: 5 bits × 1,000 = 625 bytes (+ 딕셔너리 ~900 bytes)
  결과: 1,525 bytes → **95% 압축**
```

#### 5.2.3 Sort Order와 RLE의 시너지

**Iceberg Sort Order가 RLE 효율을 극적으로 향상**시킨다. 정렬 없이는 동일 값이 파일 전체에 분산되지만, Sort Order로 동일 값이 연속 배치되면 RLE의 run 길이가 길어진다:

```
[정렬 없음] eqid: A B A C B A C A B C A A B ...  → RLE 비효율 (run 길이 1~2)
[eqp_id 정렬] eqid: A A A A A A B B B B C C C ...  → RLE 최적 (run 길이 수천~수만)

Sort Order: (eqid, ltid, stid, pname, ts) 적용 시:
→ eqid: 매우 긴 run  (장비별 수천~수만 row 연속)
→ ltid: 긴 run       (lot별 수백~수천 row 연속)
→ stid: 중간 run    (step별 수십~수백 row 연속)
→ pname: 짧은 run (파라미터별 수~수십 row 연속)
→ ts: 정렬되어 DELTA 인코딩 효율적
```

### 5.3 ZSTD 블록 압축과의 결합

딕셔너리 인코딩 + RLE 이후에도 **ZSTD 블록 압축**이 최종적으로 적용된다. 이 3단계 압축 파이프라인:

```
원본 데이터
  → [1단계] 딕셔너리 인코딩 (고유 값 추출 + 인덱스 치환)
    → [2단계] RLE / Bit-Packing (인덱스 시퀀스 압축)
      → [3단계] ZSTD 블록 압축 (최종 바이트 스트림 압축)
```

각 단계가 데이터의 다른 패턴을 활용하므로, **3단계를 거치면 원본 대비 10~50배 압축**이 가능하다. 특히 Flat 변환으로 증가하는 반복 패턴은 1~2단계에서 거의 완전히 흡수된다.

### 5.4 Flat 변환 시 컬럼별 저장 공간 영향 요약

| 컬럼 유형 | 예시 | 인코딩 | Flat 변환 시 크기 변화 | 이유 |
|-----------|------|--------|----------------------|------|
| 저카디널리티 컨텍스트 | fb, eqid, ltid, rcid | Dict + RLE | **거의 0** | 딕셔너리 크기 불변, 인덱스만 N개 추가 → RLE로 흡수 |
| 파라미터 식별 | pname | Dict + Bit-Pack | **미미** (~5 bits/row) | 고유 값 30~100개 → 5~7 bits per row |
| 값 컬럼 | pvalue (STRING) | Plain + ZSTD | **없음** | Array에서나 Flat에서나 동일 값을 동일 바이트로 저장 |
| 통계 컬럼 (Summary) | mean_val (DOUBLE) | Plain + ZSTD | **없음** | 8 byte 고정, 배열에서 개별로 풀어도 총 바이트 동일 |
| 타임스탬프 | ts | Delta + ZSTD | **없음** | 정렬 시 Delta 인코딩으로 차이값만 저장 |

**핵심**: Flat으로 풀어서 증가하는 것은 반복 컨텍스트 필드뿐이며, 이 필드들은 딕셔너리 + RLE로 거의 완전히 흡수된다. 따라서 **실제 저장 크기 증가는 1.5~2배 수준**이며, 파라미터 수 N배의 row 증가와는 비례하지 않는다.

---

## 6. Iceberg의 3단계 데이터 스킵 메커니즘 (Flat에서만 동작)

Iceberg + Parquet 조합이 쿼리 성능에 결정적인 이유는 **3단계 계층적 데이터 스킵**에 있다. 이 메커니즘은 스칼라(Flat) 컬럼에서만 정상 동작한다.

### 6.1 3단계 스킵 아키텍처

```
쿼리: SELECT * FROM fdc_trace_iceberg
      WHERE fab = 'M16' AND param_name = 'Temperature' AND ts > '2026-03-01'

[1단계] Iceberg Manifest 파일 스킵 (메타데이터만 읽음, 실제 데이터 파일 열지 않음)
  ├─ manifest-1.avro: param_name min='Flow', max='Pressure'     → ❌ SKIP (Temperature 범위 밖)
  ├─ manifest-2.avro: param_name min='Speed', max='Voltage'     → ❌ SKIP
  └─ manifest-3.avro: param_name min='Power', max='Temperature' → ✅ READ (범위 안)
      ├─ file-301.parquet: param_name min='Power', max='RPM'    → ❌ SKIP
      ├─ file-302.parquet: param_name min='Temp', max='Temp'    → ✅ READ
      └─ file-303.parquet: param_name min='Temp', max='Vibr'    → ✅ READ

[2단계] Parquet Row Group 스킵 (파일 Footer만 읽음, 데이터 페이지 열지 않음)
  file-302.parquet:
  ├─ Row Group 0: param_name min='Temperature', max='Temperature', ts max='2026-02-28' → ❌ SKIP (ts 범위 밖)
  ├─ Row Group 1: param_name min='Temperature', max='Temperature', ts min='2026-03-01' → ✅ READ
  └─ Row Group 2: param_name min='Temperature', max='Temperature', ts min='2026-03-15' → ✅ READ

[3단계] Parquet Page 스킵 (Column Index, Parquet 1.11+)
  Row Group 1:
  ├─ Page 0: ts min='2026-03-01', max='2026-03-05' → ✅ READ
  ├─ Page 1: ts min='2026-03-06', max='2026-03-10' → ✅ READ
  └─ Page 2: ts min='2026-03-11', max='2026-03-14' → ✅ READ
```

### 6.2 Array 컬럼에서 3단계 스킵이 불가능한 이유

Array 컬럼(`ARRAY<STRING>`)의 경우:

```
file-001.parquet의 param_name 컬럼 통계:
  min = ['Flow', 'Pressure', 'Temperature']   ← 배열 자체가 하나의 값
  max = ['Flow', 'Pressure', 'Temperature']   ← 비교 불가능

→ 'Temperature'가 이 배열 안에 있는지 min/max로 판단할 수 없음
→ 모든 파일을 열어서 UNNEST 후 필터해야 함
```

**Array 컬럼에 대해서는 1단계(Iceberg manifest), 2단계(Row Group), 3단계(Page) 스킵이 모두 불가능하다.** 이것이 Flat 구조가 결정적으로 유리한 근본 원인이다.

### 6.3 Sort Order와의 시너지

Iceberg의 Sort Order를 `(fb, eqid, pname, ts)` 등으로 설정하면, 같은 `pname` 값을 가진 row들이 같은 파일/Row Group에 모인다. 이렇게 되면:

- `pname`의 min/max 범위가 좁아짐 → **스킵률이 극적으로 향상**
- 예: 정렬 없으면 모든 파일에 'Temperature'가 분산 → 스킵 불가. 정렬 후 'Temperature'는 특정 파일들에만 집중 → 나머지 파일 전부 스킵

Hive에는 이런 테이블 레벨 Sort Order 개념이 없었으므로, 기존 환경에서는 활용할 수 없었던 최적화다.

---

## 7. S3(MinIO)에서 불필요한 I/O가 치명적인 이유

### 7.1 HDFS vs S3 읽기 특성 비교

| 특성 | HDFS | S3 (MinIO) |
|------|------|-----------|
| Data locality | **있음** — 데이터가 있는 노드에서 직접 읽기 | **없음** — 항상 네트워크를 통해 읽기 |
| 읽기 지연(latency) | ~1ms (로컬 디스크) | ~5-20ms (네트워크 round-trip) |
| 처리량(throughput) | 로컬 SSD: ~500MB/s | 네트워크: ~100-200MB/s (대역폭 공유) |
| 파일 열기 비용 | 낮음 (NameNode 조회 + 로컬 읽기) | 높음 (HTTP API 호출, TLS handshake, 인증) |
| 부분 읽기 (Range read) | 지원 (하지만 로컬이라 전체 읽기도 빠름) | **필수** — 불필요한 바이트를 읽으면 네트워크 낭비 |

### 7.2 Array 구조의 S3 환경 비용

Array 구조에서 "Temperature 파라미터만 조회" 시나리오:

```
[HDFS 환경 — Array 테이블]
파티션: date='20260301', fb='A6' → 해당 디렉토리의 파일 목록 조회
→ 파일 100개 × 평균 256MB = 25.6GB 읽기
→ 로컬 디스크에서 읽으므로 ~51초 (500MB/s)
→ UNNEST + 필터로 Temperature만 추출

[S3 환경 — Array 테이블]
파티션: date='20260301', fb='A6' → S3 prefix listing (느림)
→ 파일 100개 × 평균 256MB = 25.6GB 네트워크 전송
→ ~128초 (200MB/s, 이론적 최대치)
→ UNNEST + 필터로 Temperature만 추출
→ 실제 필요한 데이터는 전체의 1/N (파라미터 수 N ≈ 30~100)
→ 97~99%의 네트워크 전송이 낭비
```

```
[S3 환경 — Flat 테이블 + Iceberg Sort Order]
Iceberg manifest 조회: pname='Temperature'가 포함된 파일만 식별
→ 파일 3개 × 256MB = 768MB (Sort Order 덕분에 Temperature가 소수 파일에 집중)
→ Row Group 통계로 추가 필터: 2개 Row Group만 읽기 = ~256MB
→ ~1.3초 (200MB/s)
→ 100배 이상의 성능 차이
```

**핵심**: S3 환경에서는 "읽지 않아도 되는 데이터를 읽지 않는 것"이 HDFS 환경보다 수십~수백 배 더 중요하다. Flat 구조 + Iceberg 3단계 스킵이 이를 가능하게 한다.

---

## 8. 2중 적재 제거: Flat 하나로 충분한 이유

### 8.1 기존에 2중 적재가 필요했던 이유와 해소

| 기존 필요성 | 기존 해결 방법 | 새 환경에서의 상태 |
|------------|---------------|-------------------|
| 원본 보존 (Kafka 메시지 1:1) | Array 테이블에 그대로 저장 | **Kafka retention으로 대체** — 원본 복구 필요 시 Kafka에서 replay. 별도 테이블 불필요 |
| Spark 내부 처리 | Array 테이블에서 직접 읽어 처리 | **Flink이 인메모리에서 flat 변환** — 중간 Array 테이블 불필요 |
| Ad-hoc 분석 | Flat 테이블에서 수행 | **동일** — Flat 테이블 유지 |
| Array → Flat 변환 | Spark 배치 ETL (`explode`) | **Flink이 실시간 변환** — 별도 배치 작업 불필요 |

### 8.2 Flink 파이프라인에서의 데이터 흐름

```
기존 (Spark 배치):
  Kafka → [Spark ETL] → fdc_trace (Array, ORC, HDFS)
                       → [Spark ETL] → fdc_trace_flat (Flat, ORC, HDFS)

신규 (Flink 스트리밍):
  Kafka → [Flink: FdcTraceFlattener] → fdc_trace_iceberg (Flat, Parquet, S3)
                                        ↓
                              (인메모리에서 이미 flat)
```

Flink의 `FdcTraceFlattener`가 Kafka protobuf 메시지를 수신하는 즉시 flat으로 변환한다. Array 형태의 중간 저장은 파이프라인에서 어떤 역할도 하지 않는다.

### 8.3 저장 비용 절감

M16 FAB 기준 일일 데이터량:

| 구성 | 테이블 | 예상 일일 저장량 | 비고 |
|------|--------|----------------|------|
| 기존 | `fdc_trace` (Array) | ~50GB (압축 후) | 원본 보존용 |
| 기존 | `fdc_trace_flat` (Flat) | ~180GB (압축 후) | 분석용 (row 수가 N배) |
| 기존 합계 | | **~230GB/일** | 2중 적재 |
| 제안 | `fdc_trace_iceberg` (Flat) | ~150GB (압축 후) | Parquet + ZSTD 압축 + 적절한 타입 사용 |
| **절감** | | **~80GB/일 (~35%)** | 중복 제거 + 압축 효율 향상 |

> 참고: Flat 테이블의 row 수는 Array 대비 N배(파라미터 수)이지만, 반복되는 컨텍스트 필드(eqp_id, lot_id 등)가 Parquet의 딕셔너리 인코딩 + RLE로 매우 효율적으로 압축된다. Trace의 `param_value`는 문자열 혼재로 STRING을 유지하지만 (§10.3), Summary의 14개 통계 컬럼은 DOUBLE로 저장하여 8byte 고정 크기의 이점을 활용한다.

---

## 9. Trino Ad-hoc 쿼리 성능 비교

사용자들이 Trino로 수행하는 주요 ad-hoc 쿼리 패턴별로 기존 대비 성능 변화를 분석한다.

### 9.1 패턴별 쿼리 성능 비교


### 9.2 Trino 쿼리 엔진의 Iceberg 최적화

Trino는 Iceberg 커넥터에 대해 다음과 같은 전용 최적화를 적용한다:

1. **Manifest-level predicate pushdown**: Trino가 WHERE 조건을 Iceberg manifest의 컬럼 통계와 대조하여 파일 목록 단계에서 불필요한 파일을 제외
2. **Row Group pruning**: Parquet 파일 Footer의 Row Group 통계를 활용한 추가 필터링
3. **Column projection**: 필요한 컬럼만 S3에서 읽기 (Range read). Array 컬럼은 일부 원소만 읽을 수 없지만 스칼라 컬럼은 해당 컬럼의 바이트만 정확히 읽음
4. **Dynamic filtering**: JOIN 시 빌드 측 결과를 프로브 측 스캔에 적용

이 최적화들은 **모두 스칼라(Flat) 컬럼에서만 효과적**이다.

---

## 10. 타입 정확성의 영향

기존 Hive 테이블에서는 대부분의 필드가 `VARCHAR` 또는 `ARRAY<STRING>`으로 정의되어 있다. Iceberg Flat 테이블에서는 적절한 네이티브 타입을 사용한다.

### 10.1 타입 변환 상세


### 10.2 타입이 쿼리 성능에 미치는 영향 (Sy 테이블)

```sql
-- 기존: STRING으로 저장된 통계값 비교 (사전순!)
WHERE CAST(t.mn_val AS DOUBLE) > 100.0
-- '9.5' > '100.0' → TRUE (사전순 비교하면 오류)
-- Parquet min/max 통계도 사전순 → Predicate pushdown 시 잘못된 스킵 가능

-- 제안: DOUBLE로 저장 (Summary 테이블)
WHERE mn_val > 100.0
-- 정확한 숫자 비교, Parquet min/max 통계도 숫자 기준으로 정확
```

STRING 타입으로 저장된 숫자는:
- **Predicate pushdown이 오작동**할 수 있음 (사전순 비교)
- 매 쿼리마다 **CAST 연산** 필요 (CPU 비용)
- **딕셔너리 인코딩 효율 저하** (숫자의 문자열 표현은 카디널리티가 높아 딕셔너리 fallback)
- **저장 공간 낭비** (DOUBLE은 8byte 고정, "350.123456789"는 13byte + 가변 길이 오버헤드)

> **참고**: 위 단점은 **Summary 테이블에서는 DOUBLE 전환으로 완전히 해소**된다. Trace 테이블의 값 컬럼(param_value 등)은 문자열 혼재로 STRING을 유지하지만, Trace ad-hoc 쿼리의 주요 필터 컬럼(`param_name`, `eqp_id`, `ts`)은 모두 스칼라 타입이므로 3단계 스킵 효과에는 영향 없다 (§10.3).

### 10.3 Trx pvalue 값 컬럼의 타입 혼재 대응

#### 10.3.1 현상

기존 `trx_flat`의 `pvalue`, `trg`, `lsl`, `lcl`, `ucl`, `usl` 컬럼에는 숫자뿐 아니라 **순수 문자열 값**이 저장되어 있다. 이 문자열 값은 Summary 통계 계산에는 사용되지 않지만, Trace 테이블의 원본 데이터로 존재한다.

```
예시 — 동일 Trace 테이블 내 혼재:
  row 1: param_name='Temperature', param_value='350.5',  target='350.0'   ← 숫자
  row 2: param_name='GasType',     param_value='N2',     target='N2'      ← 문자열
  row 3: param_name='Pressure',    param_value='2.1',    target='2.0'     ← 숫자
  row 4: param_name='RecipeStep',  param_value='STEP_A', target=''        ← 문자열
```

#### 10.3.2 DOUBLE 변환이 불가능한 이유

| 접근 | 문제 |
|------|------|
| DOUBLE로 강제 변환 (문자열 → NULL) | 데이터 손실. 'N2', 'STEP_A' 같은 원본 값이 유실되어 Trace의 원본 보존 목적에 위배 |
| Dual Column (STRING + DOUBLE nullable) | 6개 컬럼이 12개로 증가. 단, 아래 §10.3.2.1 검증 결과에 따라 `param_value`만 dual이면 6→7로 최소화 가능 |

#### 10.3.2.1 Dual Column 재평가: spec 필드 분리 가능성 검증

Kafka 샘플(`kafka/trace_raw.json`)에서 **문자열 param_value를 가진 row의 spec 필드(target, lsl, lcl, ucl, usl)가 전부 빈값/null**인 패턴이 관찰됨. 이 패턴이 전체 운영 데이터에서도 성립하면:

- `trg`, `lsl`, `lcl`, `ucl`, `usl` → 데이터 손실 없이 **DOUBLE 전환 가능**
- `param_value`만 dual column (STRING + DOUBLE nullable) → 컬럼 수 6→7 (12가 아님)
- spec 5개 컬럼의 Predicate pushdown 활성화, 쿼리에서 CAST 제거

**검증 쿼리 (Trino — flat 기준)**:

> 테이블은 최소 파티션만 스캔하도록 작성.
> 필요 컬럼: `param_value`, `target`, `lsl`, `lcl`, `ucl`, `usl` 6개만 (Column Pruning 활용).

```sql
-- SV1: 문자열 param_value의 spec 동반 여부 (1일 × 1FAB 단위 실행)
-- M16 1일 기준 ~213B rows. ICPNT(소규모)에서 먼저 테스트 권장.
SELECT
    COUNT(*) AS string_param_total,
    COUNT_IF(
        COALESCE(trg, '') != ''
        OR COALESCE(lsl, '') != ''
        OR COALESCE(lcl, '') != ''
        OR COALESCE(ucl, '') != ''
        OR COALESCE(usl, '') != ''
    ) AS string_param_with_spec
FROM flat
WHERE fab = 'fab'
  AND dt = '2026-01-31'
  AND param_value IS NOT NULL
  AND param_value != ''
  AND TRY_CAST(pvalue AS DOUBLE) IS NULL;
```

ICPNT 결과 확인 후, A6으로 확장:



**실행 순서**: IP(SV1) → A6(SV1-M16) → 필요 시 fb추가. SV2는 SV1 결과가 0이 아닐 때만 실행.

**기대 결과**:
- SV1에서 `string_param_with_spec = 0`이면 → spec 5개 DOUBLE 전환 안전, `param_value`만 dual column
- SV2에서 0건이면 → 확정

> **상태**: 미실행. 실행 결과에 따라 §10.3.2 결론 및 §12.1 DDL 수정 여부 결정.

#### 10.3.3 STRING 유지가 trx 성능에 미치는 영향: 미미

Trace 테이블에서 STRING 유지가 성능에 미치는 영향이 미미한 이유:

1. **ad-hoc 쿼리에서 `param_value`로 직접 필터하는 패턴이 없다**: BQ-1~5 벤치마크 쿼리에서 `param_value`는 SELECT 대상이지 WHERE 조건이 아니다. 실제 사용 패턴도 `param_name`으로 파라미터를 특정한 후 `param_value`를 읽는 형태
2. **Sort Order에 `param_value`가 포함되지 않는다**: §12.1의 Sort Order는 `(eqp_id, lot_id, step_id, param_name, ts)`이므로 `param_value`의 타입이 Sort Order 효과에 영향 없음
3. **3단계 스킵의 주력 필터 컬럼은 모두 스칼라 타입**: `param_name`(STRING), `eqp_id`(STRING), `ts`(TIMESTAMP) — 이들이 Iceberg manifest, Row Group, Page 스킵을 결정
4. **Column Pruning은 타입과 무관**: 필요한 컬럼만 읽는 최적화는 STRING이든 DOUBLE이든 동일하게 적용

#### 10.3.4 Summary는 영향 없음

Summary 테이블의 14개 통계값(`max_val`, `min_val`, `mean_val` 등)은 **숫자 파라미터에 대해서만 계산된 결과**이므로 문자열이 들어올 수 없다. 따라서 Summary 통계 컬럼의 DOUBLE 타입은 그대로 유지한다.

| 테이블 | 값 컬럼 | 타입 | 근거 |
|--------|---------|------|------|
| **Trace** | param_value, target, lsl, lcl, ucl, usl | **STRING** | 문자열 혼재, 원본 보존, 필터 컬럼 아님 |
| **Summary** | max_val ~ sumsq_val (14개) | **DOUBLE** | 계산 결과, 항상 숫자, 필터 컬럼 가능 |
| **Summary** | count_val | **BIGINT** | 계산 결과, 항상 정수 |

---

## 11. Iceberg Hidden Partitioning

### 11.1 기존 Hive 파티셔닝의 한계

```sql
-- 기존: dt VARCHAR 파티션 (사용자가 명시적으로 관리)
CREATE TABLE ... PARTITIONED BY (dt STRING, fb STRING);

-- 문제 1: 시간 범위 쿼리 시 파티션 pruning이 문자열 비교
WHERE dt BETWEEN '20260301' AND '20260315'  -- 15개 파티션 개별 조회

-- 문제 2: 파티션 값을 ETL에서 직접 계산하여 삽입
-- 문제 3: 더 세밀한 파티셔닝(시간 단위)으로 변경하려면 전체 재적재 필요
```

### 11.2 Iceberg Hidden Partitioning의 장점

```sql
-- 제안: TIMESTAMP 기반 hidden partition
PARTITIONED BY (fab, days(end_dt_ts))

-- 사용자 쿼리: 파티션 컬럼을 의식할 필요 없음
WHERE end_dt_ts >= TIMESTAMP '2026-03-01' AND end_dt_ts < TIMESTAMP '2026-03-16'
-- → Iceberg가 자동으로 15개 일(day) 파티션만 스캔

-- 파티셔닝 변경도 메타데이터만 수정 (데이터 재적재 불필요)
ALTER TABLE ... SET PARTITION SPEC (fab, hours(end_dt_ts));
```

Hidden Partitioning은 Hive에 없는 Iceberg 고유 기능으로, 사용자가 파티션 구조를 몰라도 TIMESTAMP 범위 쿼리가 자동으로 최적화된다.

---

## 12. 제안하는 최종 테이블 설계



---

## 13. 종합 비교표

| 비교 항목 | 기존: ORC + Hive + HDFS (Array+Flat) | 제안: Parquet + Iceberg + S3 (Flat only) |
|-----------|--------------------------------------|------------------------------------------|
| **테이블 수** | Trace 2개 + Summary 3개 = 5개 | Trace 1개 + Summary 1개 = **2개** |
| **데이터 중복** | Array + Flat 2중 적재 | 1벌만 적재 |
| **일일 저장량 (M16)** | ~230GB (Trace 기준) | ~150GB (**~35% 절감**) |
| **ETL 복잡도** | Spark: Array 적재 → Flat 변환 (2단계) | Flink: 직접 Flat 적재 (1단계) |
| **Trino 파라미터 조회** | Array: UNNEST 필수 + 전체 스캔<br>Flat: Hive 파티션 + ORC 통계 | **3단계 스킵: Manifest → Row Group → Page** |
| **Predicate pushdown** | 스칼라 컬럼만 (Array 내부 불가) | 모든 컬럼 (전부 스칼라) |
| **Data skipping 범위** | Hive 파티션 단위 (일/fab) | **파일 → Row Group → Page** (3단계) |
| **Sort Order** | 미지원 (Hive 한계) | Iceberg 테이블 레벨 Sort Order |
| **파티셔닝** | dt STRING 수동 관리 | **Hidden Partition** (TIMESTAMP 자동) |
| **타입 정확성** | 대부분 STRING/ARRAY\<STRING\> | Summary: 네이티브 타입 (DOUBLE, BIGINT). Trace 값 컬럼: STRING 유지 (§10.3, 문자열 혼재) |
| **통계 기반 스킵 정확도** | 숫자를 STRING으로 저장 → 사전순 비교 | Summary: DOUBLE → **정확한 수치 비교**. Trace: 필터 컬럼(param_name, ts)은 스칼라 타입으로 스킵 유효 |
| **파일 I/O 특성** | HDFS data locality → 전체 스캔 비용 낮음 | S3 remote I/O → **불필요한 읽기 비용이 높음** → 스킵 중요성 증가 |
| **압축** | ORC + ZLIB | Parquet + ZSTD (**더 빠른 압축/해제, 유사한 압축률**) |
| **Format version** | Hive (ACID 제한적) | Iceberg v2 (**Row-level delete/update 지원**) |
| **원본 복구** | Array 테이블에서 복구 | Kafka retention 또는 S3 raw archive |
| **스키마 변경** | ALTER TABLE 후 전체 재적재 | Iceberg **schema evolution** (메타데이터만 변경) |

---

---

## 15. 결론



## 부록 A: 용어 정리

| 용어 | 설명 |
|------|------|
| **Predicate Pushdown** | WHERE 조건을 스토리지 레이어로 내려보내 불필요한 데이터를 읽기 전에 걸러내는 최적화 |
| **Data Skipping** | 파일/블록의 통계(min/max)를 확인하여 조건에 맞지 않는 데이터를 읽지 않고 건너뛰는 기법 |
| **Column Pruning** | SELECT에 명시된 컬럼만 읽는 최적화. 컬럼형 포맷(ORC, Parquet)에서 가능 |
| **Row Group** | Parquet 파일 내의 데이터 블록 단위 (기본 128MB). 각 Row Group은 컬럼별 min/max 통계를 보유 |
| **Stripe** | ORC 파일 내의 데이터 블록 단위 (기본 64MB). Row Group과 유사한 역할 |
| **Manifest** | Iceberg가 관리하는 메타데이터 파일. 데이터 파일 목록 + 파일별 컬럼 통계(min/max, null count)를 포함 |
| **Hidden Partitioning** | Iceberg 고유 기능. 원본 컬럼 값에서 파티션 값을 자동 도출 (예: TIMESTAMP → day) |
| **Sort Order** | Iceberg 테이블 레벨 정렬 설정. 데이터 쓰기 시 지정된 컬럼 순서로 정렬하여 min/max 범위를 좁힘 |
| **ZSTD** | Facebook이 개발한 압축 알고리즘. ZLIB 대비 압축/해제 속도가 빠르고 압축률은 유사 |
| **Data Locality** | HDFS에서 데이터가 저장된 노드에서 직접 읽어 네트워크 전송을 피하는 메커니즘. S3에는 없음 |

---
