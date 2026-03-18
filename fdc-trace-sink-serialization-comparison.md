# FDC Trace Sink 직렬화 방식 비교 및 성능 분석

## 1. Schema Registry 사용 시 FDCTraceBaseJob에서 퍼포먼스 이슈를 발생시키는 포인트

### 1.1 현재 아키텍처 요약

`FDCTraceBaseJob`은 그룹핑토픽(Protobuf)을 읽어 장비별토픽(JSON)으로 변환하는 Flink Job이다.

```
[그룹핑토픽 - Protobuf] → FlatMap 변환 → [장비별토픽 - JSON]
                                            ↑
                                     topicPrefix + element.eqpId
                                     (장비별 동적 토픽 생성)
```

**핵심 특성:** Sink에서 `SinkRecordSerializationSchema.serialize()` 메서드가 장비 ID(`eqpId`)를 기반으로 **동적 토픽을 생성**한다.

```java
// FDCTraceBaseJob.java line 453-454
String topic = topicPrefix + element.eqpId;
topic = topic.toLowerCase();
```

### 1.2 Confluent Wire Format(Schema Registry) 사용 시 성능 병목 발생 메커니즘

Confluent의 `KafkaProtobufSerializer`는 직렬화 시 다음 과정을 거친다:

```
serialize(topic, record)
  → SubjectNameStrategy.subjectName(topic, ...)
  → CachedSchemaRegistryClient.registerWithResponse(subject, schema)
      → outer 캐시 조회: schemaToResponseCache[subject]
      → miss 시: Schema Registry에 HTTP 호출 (POST /subjects/{subject}/versions)
```

#### 병목 포인트 1: Subject 수 폭발

기본 `TopicNameStrategy`에서 subject = `{topic}-value`이므로:

| 장비 수 | 생성되는 subject 수 | 예시 |
|---|---|---|
| 100대 | 100개 | `prefix.eqp001-value`, `prefix.eqp002-value`, ... |
| 1,000대 | 1,000개 | outer 캐시 한계(기본 maxSize=1000)에 도달 |
| 2,000대 | 2,000개 | **outer 캐시 초과 → LRU eviction 발생** |

#### 병목 포인트 2: 캐시 Thrashing

`CachedSchemaRegistryClient`의 outer 캐시(`schemaToResponseCache`)는 Guava `Cache`로 구현되어 있으며, `max.schemas.per.subject` 설정값(기본 1000)을 maxSize로 사용한다.

장비 수 > maxSize일 때:

```
시간 T1: eqp0001 직렬화 → subject "prefix.eqp0001-value" → 캐시 miss → HTTP 호출 → 캐시 저장
시간 T2: eqp0002 직렬화 → subject "prefix.eqp0002-value" → 캐시 miss → HTTP 호출 → 캐시 저장
...
시간 T1000: outer 캐시 maxSize=1000 도달
시간 T1001: eqp1001 → eqp0001 캐시 엔트리 evict → HTTP 호출
...
시간 T2001: eqp0001 재등장 → 이미 evict됨 → 또 HTTP 호출 ← ★ 반복 발생
```

이 **캐시 thrashing**은 일시적 warm-up이 아니라 **지속적으로 발생**하는 성능 저하이다.

#### 병목 포인트 3: synchronized 블록 경합

캐시 miss 시 `CachedSchemaRegistryClient.registerWithResponse()`는 `synchronized(this)` 블록에 진입한다 (`CachedSchemaRegistryClient.java` line 554).

```java
synchronized (this) {
    // HTTP 호출 + 캐시 저장
}
```

Flink의 병렬 태스크가 동시에 캐시 miss를 일으키면, **모든 스레드가 이 synchronized 블록에서 대기**하게 된다. HTTP 호출 지연(수십~수백ms)이 직렬화되어 처리량이 급격히 하락한다.

#### 병목 포인트 4: Flink 태스크 재시작 시 캐시 초기화

체크포인트 복구, TaskManager 장애 복구, 리밸런싱 시 `KafkaProtobufSerializer` 인스턴스가 재생성되면서 **모든 캐시가 초기화**된다. 장비 수만큼의 HTTP warm-up이 다시 발생한다.

### 1.3 성능 이슈 요약

| 포인트 | 원인 | JSON Sink 영향 | Confluent Wire Format Sink 영향 |
|---|---|---|---|
| Subject 수 폭발 | 동적 토픽 × TopicNameStrategy | 없음 | subject 수 = 장비 수 |
| 캐시 Thrashing | outer 캐시 maxSize 초과 | 없음 | 지속적 HTTP 재호출 |
| synchronized 경합 | 캐시 miss 시 lock 대기 | 없음 | 병렬 처리량 저하 |
| 재시작 시 warm-up | 캐시 초기화 | 없음 | 장비 수 × HTTP 호출 |

> **결론:** "Protobuf가 JSON보다 느리다"는 것이 아니라, **"Confluent Wire Format + 동적 토픽 + TopicNameStrategy 조합"이 Schema Registry 캐시 구조와 충돌**하여 성능이 하락하는 것이다.

### 1.4 KafkaProtobufSerializer의 per-message 오버헤드 (캐시 thrashing 해소 후에도 존재)

RecordNameStrategy로 캐시 thrashing을 해소하고 SR HTTP 호출을 0으로 만들어도, `KafkaProtobufSerializer`는 매 `serialize()` 호출마다 다음 로직을 실행한다:

```
KafkaProtobufSerializer.serialize() 매 메시지 실행 경로:

1. schemaCache.getIfPresent(descriptor)        ← Guava 캐시 lookup
2. getSubjectName(topic, isKey, record, schema) ← Strategy 호출
3. serializeImpl() 내부:
   a. resolveDependencies(...)                  ← ★ 의존성 트리 순회 (매번 호출)
   b. getIdWithResponse() (캐시 히트)           ← 캐시 lookup
   c. executeRules(..., RuleMode.WRITE)         ← ★ 규칙 실행 (규칙 없어도 메타데이터 확인)
   d. schema.toMessageIndexes(fullName)         ← ★ 디스크립터 트리 탐색 (캐싱 없음)
   e. object.toByteArray()                      ← 실제 Protobuf 직렬화
   f. executeRules(..., RulePhase.ENCODING)     ← ★ 규칙 실행 (규칙 없어도 호출)
   g. schemaIdSerializer.serialize()            ← wire format 조립
```

★ 표시 항목은 규칙/의존성이 없어도 **매 호출마다 실행**되며, 이 오버헤드가 누적된다.

**참고: Confluent Go 클라이언트에서 보고된 동일 패턴의 성능 이슈:**

| 이슈 | 내용 |
|---|---|
| [confluent-kafka-go #1146](https://github.com/confluentinc/confluent-kafka-go/issues/1146) | 순수 protobuf 대비 **2500배** 느림 (스키마 디스크립터 매번 파싱) |
| [confluent-kafka-go #1317](https://github.com/confluentinc/confluent-kafka-go/issues/1317) | 순수 protobuf 대비 **15배** 느림 (100만 메시지 기준) |
| [schema-registry #1515](https://github.com/confluentinc/schema-registry/issues/1515) | Avro SerDe 유사 패턴, 캐싱 적용 후 **7~8배** 성능 개선 |

### 1.5 FDCTraceFinal 스키마 특성이 미치는 영향

FDCTraceFinal은 **37개 필드가 전부 String** 타입이다. 이 특성이 직렬화 방식별 성능 차이에 결정적 영향을 미친다.

| 데이터 타입 | Protobuf vs JSON 순수 직렬화 속도 차이 |
|---|---|
| 숫자(int, double 등) | Protobuf **3~13배** 빠름 |
| **String 위주 (37개 전부 String)** | Protobuf **1.2~1.85배** 빠름 |

Protobuf의 핵심 이점은 숫자의 varint 인코딩과 필드명 생략이다. 하지만 **String 데이터는 두 포맷 모두 바이트를 그대로 복사**해야 하므로, Protobuf의 이점이 필드 번호(varint) vs 필드명(문자열) 차이로만 제한된다.

**결과:** String 위주 스키마에서 Protobuf의 좁은 이점(1.2~1.85배)을 `KafkaProtobufSerializer`의 per-message 오버헤드(1.4절)가 소진하여, **Confluent Wire Format이 JSON보다 느려지는 역전 현상**이 발생할 수 있다.

---

## 2. Sink 직렬화 방식 비교: JSON vs Native Protobuf vs Confluent Wire Format vs Lightweight Wire Format

### 2.1 직렬화 방식 개요

| | JSON | Native Protobuf | Confluent Wire Format | Lightweight Wire Format |
|---|---|---|---|---|
| 직렬화 방법 | `ObjectMapper.writeValueAsBytes()` | `message.toByteArray()` | `KafkaProtobufSerializer` | 커스텀: wire format 헤더 + `toByteArray()` |
| Schema Registry | 불필요 | 불필요 | 필수 (런타임 조회) | 사전 등록만 (런타임 조회 없음) |
| 메시지 포맷 | JSON 텍스트 바이트 | 순수 Protobuf 바이너리 | `[magic 0x0][schemaId 4B][msg index][protobuf bytes]` | `[magic 0x0][schemaId 4B][msg index][protobuf bytes]` (동일) |
| per-message 오버헤드 | ObjectMapper 내부 캐싱된 직렬화 | 없음 | resolveDeps + executeRules×2 + toMessageIndexes | **없음** (바이트 배열 조립만) |
| 메시지 크기 | 가장 큼 (필드명 포함) | 가장 작음 | Protobuf + 6 bytes 헤더 | Protobuf + 6 bytes 헤더 (동일) |
| 소비자 호환성 | JSON 파서 | `.proto` 파일 필요 | Confluent Deserializer | **Confluent Deserializer 호환** |
| 스키마 진화 관리 | 수동 | 수동 | SR 자동 검증 | SR 사전 등록으로 관리 가능 |

**Lightweight Wire Format**은 `KafkaProtobufSerializer`를 사용하지 않고, wire format 헤더(magic byte + schema ID + message index)를 직접 조립하여 `toByteArray()` 결과와 결합하는 방식이다. 메시지 포맷이 Confluent Wire Format과 동일하므로, 소비자 측에서 `KafkaProtobufDeserializer`로 정상 역직렬화 가능하다.

### 2.2 동적 토픽 환경에서의 비교

이 FDC Trace Job의 핵심 특성인 **장비별 동적 토픽**이 각 방식에 미치는 영향:

| | JSON | Native Protobuf | Confluent Wire Format | Lightweight Wire Format |
|---|---|---|---|---|
| 동적 토픽 영향 | 없음 | 없음 | **심각** | 없음 |
| 토픽 수 증가 시 처리량 | 일정 | 일정 | 하락 (캐시 thrashing) | 일정 |
| Schema Registry 의존성 | 없음 | 없음 | 강한 결합 (런타임) | 약한 결합 (초기화 시 1회) |
| SR 장애 시 영향 | 없음 | 없음 | Sink 전체 중단 | 초기화 후에는 영향 없음 |

### 2.3 소비자 측 역직렬화 영향

현재 Sink가 JSON을 사용하는 이유는 "다양한 사용자가 읽기 용이하도록" 하기 위함이다 (`FDCTraceBaseJob.java` 주석 line 65). 직렬화 방식 변경 시 소비자 측 영향을 반드시 고려해야 한다.

| | JSON | Native Protobuf | Confluent Wire Format | Lightweight Wire Format |
|---|---|---|---|---|
| 소비자 역직렬화 | JSON 파서 (언어 무관) | `.proto` 파일 + Protobuf 라이브러리 | Confluent Deserializer + SR 접근 | **Confluent Deserializer + SR 접근** (동일 wire format) |
| 소비자 변경 범위 | 없음 (현재 방식) | 소비자 코드 수정 + `.proto` 배포 | 소비자 코드 수정 + SR 설정 | 소비자 코드 수정 + SR 설정 (Confluent WF와 동일) |
| 스키마 변경 시 | 소비자가 자체 대응 | `.proto` 파일 재배포 필요 | SR이 호환성 자동 검증 | SR 사전 등록으로 관리 (수동 검증) |
| 소비자 다양성 | 높음 (Python, Spark, 등 모든 도구) | 중간 (Protobuf 지원 도구만) | 낮음 (Confluent 에코시스템 필수) | 낮음 (Confluent 에코시스템 필수) |
| 디버깅 편의성 | 높음 (사람이 읽을 수 있음) | 낮음 (바이너리) | 낮음 (바이너리) | 낮음 (바이너리) |

> **주의:** Native Protobuf, Confluent Wire Format, Lightweight Wire Format으로 전환 시, 기존 JSON 소비자 전체에 대한 마이그레이션 계획이 필요하다. Lightweight Wire Format은 소비자 측에서 Confluent Wire Format과 동일하게 처리되므로, Confluent Wire Format 소비자가 이미 존재한다면 추가 변경 없이 호환된다.

### 2.4 Schema Registry 장애 시 영향

| | JSON | Native Protobuf | Confluent Wire Format | Lightweight Wire Format |
|---|---|---|---|---|
| SR 정상 | 해당 없음 | 해당 없음 | 정상 동작 | 정상 동작 |
| SR 응답 지연 | 영향 없음 | 영향 없음 | 캐시 miss 시 직렬화 지연 → backpressure | **영향 없음** (초기화 완료 후) |
| SR 완전 장애 | 영향 없음 | 영향 없음 | **캐시에 있는 subject만 동작, 캐시 miss 발생 시 SerializationException → Sink 중단** | **영향 없음** (schemaId 초기화 시 캐싱됨, 런타임 SR 호출 없음) |
| SR 장애 복구 후 | 해당 없음 | 해당 없음 | 자동 복구 (재시도 시 캐시 재적재) | 해당 없음 |
| **초기화 시 SR 장애** | 해당 없음 | 해당 없음 | **Sink 시작 실패** | **Sink 시작 실패** (schemaId 조회 불가) |

Confluent Wire Format 사용 시 Schema Registry는 **Sink의 단일 장애점(SPOF)**이 된다. Lightweight Wire Format은 초기화 시점에만 SR에 의존하므로 런타임 SPOF를 제거하지만, 초기화 시 SR이 가용해야 한다.

### 2.5 `auto.register.schemas` 설정 주의사항

`auto.register.schemas`의 기본값은 **`true`**이다. 이 설정에 따라 Serializer 내부의 분기가 달라진다:

| 설정 | 호출 메서드 | HTTP 엔드포인트 | 동작 |
|---|---|---|---|
| `true` (기본값) | `registerWithResponse()` | `POST /subjects/{subject}/versions` | 스키마 등록 시도 (이미 있으면 기존 ID 반환) |
| `false` | `getIdWithResponse()` | `POST /subjects/{subject}` | 스키마 조회만 (없으면 예외 발생) |

**운영 환경에서는 반드시 `auto.register.schemas=false`로 설정해야 한다.**

- `true`일 경우: 코드 변경으로 스키마가 의도치 않게 변경되면 **호환되지 않는 스키마가 자동 등록**될 위험이 있다
- `false`일 경우: 사전에 등록된 스키마만 사용하므로 안전하지만, 스키마가 미등록 상태면 즉시 예외 발생

```java
// 운영 환경 권장 설정
props.put("auto.register.schemas", "false");
```

> **테스트 시 주의:** 성능 테스트에서 `auto.register.schemas` 값을 명시적으로 기록해야 한다. `true`와 `false`는 서로 다른 HTTP 엔드포인트를 호출하므로 SR 서버 측 처리 비용이 다를 수 있다.

### 2.6 Confluent Wire Format 사용 시 완화 방안

동적 토픽 환경에서 Confluent Wire Format을 사용해야 한다면:

#### 방안 A: RecordNameStrategy 적용 (권장)

```java
props.put("value.subject.name.strategy",
    "io.confluent.kafka.serializers.subject.RecordNameStrategy");
```

- subject = 레코드 타입 1개 (예: `com.skhynix.datahub.flink.records.protobuf.FDCTraceFinal`)
- 장비 수와 무관하게 outer 캐시 엔트리 = 1
- **캐시 thrashing 완전 해소**
- 단, 스키마 호환성이 토픽이 아닌 레코드 타입 단위로 적용됨

#### 방안 B: max.schemas.per.subject 증가

```java
props.put("max.schemas.per.subject", "10000");
```

- 장비 수를 충분히 커버하도록 캐시 크기 확대
- warm-up 비용은 여전히 존재
- 장비 추가 시 설정값 재조정 필요 — 운영 부담

#### 방안 비교

| 방안 | 캐시 thrashing | warm-up 비용 | 운영 부담 | 권장도 |
|---|---|---|---|---|
| RecordNameStrategy | 해소 | HTTP 1회 | 낮음 | **권장** |
| max.schemas.per.subject 증가 | 해소 (충분히 크면) | 장비 수 × HTTP | 중간 (재설정 필요) | 차선 |
| TopicNameStrategy 유지 (기본) | 발생 | 장비 수 × HTTP | 없음 | 비권장 |

---

## 3. 성능 테스트 매트릭스 및 수행 방법

### 3.1 테스트 매트릭스

#### 축 1: 직렬화 방식 (4종)

| ID | 직렬화 방식 | 구현 방법 |
|---|---|---|
| S1 | JSON | `ObjectMapper.writeValueAsBytes(element)` (현재 코드) |
| S2 | Native Protobuf | `element.toByteArray()` (Schema Registry 미사용) |
| S3 | Confluent Wire Format | `KafkaProtobufSerializer` + Schema Registry |
| S4 | Lightweight Wire Format | 커스텀: `[0x00][schemaId 4B][0x00][toByteArray()]` (SR 초기화 시 1회 조회) |

#### 축 2: 동적 토픽 수 (4단계)

| ID | 토픽 수 | 설명 |
|---|---|---|
| T1 | 1개 | 단일 토픽 (동적 토픽 비활성) |
| T2 | 100개 | 소규모 장비군 |
| T3 | 1,000개 | 중규모 (기본 캐시 한계) |
| T4 | 2,000개 이상 | 대규모 (캐시 초과) |

#### 축 3: Confluent Wire Format 추가 변수 (S3에만 적용)

| ID | 설정 | 설명 |
|---|---|---|
| C1 | TopicNameStrategy (기본) + `auto.register.schemas=false` | subject = `{topic}-value` |
| C2 | RecordNameStrategy + `auto.register.schemas=false` | subject = `{record.fullName}` |
| C3 | TopicNameStrategy + `max.schemas.per.subject=10000` + `auto.register.schemas=false` | 캐시 확대 |

> **참고:** 모든 S3 테스트는 `auto.register.schemas=false`로 통일한다 (운영 환경 기준). 스키마는 테스트 전 사전 등록한다.

#### 축 4: SR 장애 시나리오 (S3에만 적용, 선택적)

| ID | 시나리오 | 설명 |
|---|---|---|
| F1 | SR 정상 | 기본 테스트 |
| F2 | SR 응답 지연 (500ms+) | SR 앞에 네트워크 지연 주입 |
| F3 | SR 완전 장애 (down) | 캐시 warm-up 완료 후 SR 중단 → 신규 subject 발생 시 동작 확인 |

#### 전체 테스트 매트릭스

```
                 │ T1(1토픽) │ T2(100토픽) │ T3(1000토픽) │ T4(2000+토픽)
─────────────────┼───────────┼─────────────┼──────────────┼──────────────
S1: JSON         │   S1-T1   │   S1-T2     │   S1-T3      │   S1-T4
S2: Native PB    │   S2-T1   │   S2-T2     │   S2-T3      │   S2-T4
S3-C1: WF+Topic  │   S3C1-T1 │   S3C1-T2   │   S3C1-T3    │   S3C1-T4
S3-C2: WF+Record │   S3C2-T1 │   S3C2-T2   │   S3C2-T3    │   S3C2-T4
S3-C3: WF+확대    │   S3C3-T1 │   S3C3-T2   │   S3C3-T3    │   S3C3-T4
S4: Lightweight  │   S4-T1   │   S4-T2     │   S4-T3      │   S4-T4
```

### 3.2 측정 메트릭

| 메트릭 | 측정 방법 | 의미 |
|---|---|---|
| **Throughput (msg/sec)** | Flink 메트릭: `numRecordsOutPerSecond` | Sink 처리량 |
| **Throughput (bytes/sec)** | Flink 메트릭: `numBytesOutPerSecond` | 네트워크 대역폭 사용량 |
| **Latency (ms)** | Flink 메트릭: 소스 ingestion ~ sink emit 시간차 | 메시지 처리 지연 |
| **직렬화 메시지 크기 (bytes)** | 동일 메시지 기준 직렬화 결과 크기 비교 | 디스크/네트워크 효율 |
| **Backpressure** | Flink Web UI: `idleTimeMsPerSecond`, `backPressuredTimeMsPerSecond` | 병목 여부 |
| **GC Pause** | JVM 메트릭: GC pause time, GC count | 메모리 압력 |
| **Schema Registry 호출 수** | Schema Registry 서버 로그 또는 클라이언트 측 카운터 | SR 병목 정도 |

### 3.3 테스트 수행 방법

#### 단계 1: 테스트 환경 구성

```
- Flink 클러스터: 동일 스펙 (TaskManager 수, 메모리, parallelism 고정)
- Kafka 클러스터: 동일 환경
- Schema Registry: 동일 인스턴스
- 소스 토픽: 동일한 테스트 데이터를 사전 적재 (재현성 확보)
- 테스트 데이터: 실제 FDCTrace 메시지 또는 동일 스키마의 합성 데이터
```

#### 단계 2: Sink Serializer 구현 (4종)

**S1: JSON (현재 코드 — 기준선)**
```java
// 현재 SinkRecordSerializationSchema 그대로 사용
return new ProducerRecord<>(topic, null, null,
    element.fdcChambId.getBytes(StandardCharsets.UTF_8),
    objectMapper.writeValueAsBytes(element));
```

**S2: Native Protobuf**
```java
// Schema Registry 미사용, 순수 Protobuf 직렬화
return new ProducerRecord<>(topic, null, null,
    element.fdcChambId.getBytes(StandardCharsets.UTF_8),
    element.toByteArray());
```

**S3: Confluent Wire Format**
```java
// KafkaProtobufSerializer 사용
// KafkaSink에 KafkaRecordSerializationSchema.builder()
//   .setValueSerializationSchema(new KafkaProtobufSerializer<>())
//   + value.subject.name.strategy 설정
```

**S4: Lightweight Wire Format**
```java
// 초기화 시: Schema Registry에서 schemaId를 1회 조회하여 캐싱
// open() 에서:
int schemaId = getSchemaIdFromRegistry(srUrl, subject);
byte[] HEADER = new byte[] { 0x00,
    (byte)(schemaId >> 24), (byte)(schemaId >> 16),
    (byte)(schemaId >> 8),  (byte)(schemaId),
    0x00  // message index = [0] (단일 메시지 타입)
};

// serialize() 에서:
byte[] pbBytes = element.toByteArray();
byte[] result = new byte[HEADER.length + pbBytes.length];
System.arraycopy(HEADER, 0, result, 0, HEADER.length);
System.arraycopy(pbBytes, 0, result, HEADER.length, pbBytes.length);
return new ProducerRecord<>(topic, null, null,
    element.getFdcChambId().getBytes(StandardCharsets.UTF_8), result);
```

#### 단계 3: 토픽 수 변수 제어

- `SinkRecordSerializationSchema`의 토픽 생성 로직을 파라미터화
- T1: 고정 토픽명 사용
- T2~T4: `eqpId` 목록을 제어하여 100/1000/2000+ 토픽 생성

#### 단계 4: 테스트 실행 및 측정

```
1. 소스 토픽에 동일 데이터 적재 (최소 10분 이상 지속 가능한 양)
2. 각 매트릭스 조합별 Flink Job 실행
3. 최소 5분 이상 안정 상태(steady state) 확인 후 메트릭 수집
4. warm-up 구간(최초 1~2분)과 steady state 구간을 분리하여 기록
```

#### 단계 5: 결과 분석 포인트

1. **S1 vs S2 (JSON vs Native PB):** 순수 직렬화 성능 비교 — 토픽 수에 따른 변화 없어야 함
2. **S3-C1의 T1 vs T4:** 동일 Wire Format에서 토픽 수 증가에 따른 성능 변화 — 핵심 증거
3. **S3-C1 vs S3-C2 (TopicName vs RecordName, T4 기준):** RecordNameStrategy가 캐시 thrashing을 해소하는지 확인
4. **S3-C2 vs S1 (RecordName Wire Format vs JSON):** 캐시 문제 해소 후에도 per-message 오버헤드로 S3-C2가 S1보다 느린지 확인
5. **S4 vs S2 (Lightweight WF vs Native PB):** 6바이트 헤더 추가의 실질적 오버헤드 측정 — 거의 동등해야 함
6. **S4 vs S1 (Lightweight WF vs JSON):** S4가 JSON을 확실히 이기는지 확인 — **KafkaProtobufSerializer 오버헤드가 근본 원인임을 증명**
7. **S4 vs S3-C2 (Lightweight WF vs Confluent WF):** 동일 wire format이지만 직렬화 경로 차이로 인한 성능 격차 — KafkaProtobufSerializer per-message 오버헤드의 정량적 증거

### 3.4 예상 결과 패턴

```
Throughput (msg/sec)

높음 ┤ ████  ████  ████  ████    ← S2 (Native PB): 토픽 수 무관, 최고 성능
     │ ████  ████  ████  ████    ← S4 (Lightweight WF): S2와 거의 동등 (헤더 6B 추가만)
     │ ███   ███   ███   ███     ← S1 (JSON): 토픽 수 무관, 안정적
     │ ██    ██    ██    ██      ← S3-C2 (WF+RecordName): per-message 오버헤드로 JSON보다 느림 (String 스키마)
     │ ███   ███   ██    █       ← S3-C3 (WF+캐시확대): T4에서 warm-up 영향
     │ ███   ██    █     ▌       ← S3-C1 (WF+TopicName): 토픽 수 증가에 따라 급락
낮음 ┤
     └──T1────T2────T3────T4──→ 토픽 수
```

> **핵심 포인트:**
> - **S2 ≈ S4 > S1 > S3-C2 > S3-C1** (String 위주 스키마 기준)
> - S4(Lightweight Wire Format)는 S2(Native Protobuf)와 거의 동등한 성능. 차이는 6바이트 헤더 복사뿐.
> - S4는 Confluent Wire Format과 동일한 바이너리 포맷이므로 `KafkaProtobufDeserializer`로 정상 역직렬화 가능.
> - S3-C2가 S1보다 느린 이유: `KafkaProtobufSerializer`의 per-message 오버헤드(resolveDependencies, executeRules×2, toMessageIndexes)가 String 위주 스키마에서 Protobuf의 좁은 이점(1.2~1.85배)을 상쇄.
> - S4는 이 오버헤드를 완전히 제거하므로 Protobuf 본연의 직렬화 성능을 회복한다.

### 3.5 테스트 보고서 작성 시 포함 항목

1. 테스트 환경 스펙 (Flink, Kafka, Schema Registry 버전 및 리소스)
2. 테스트 데이터 특성 (메시지 크기, 파라미터 수, 장비 수)
3. 직렬화 설정값 명시 (`auto.register.schemas`, `value.subject.name.strategy`, `max.schemas.per.subject`)
4. 각 매트릭스 조합별 steady state 메트릭 (표 형태)
5. warm-up 구간 vs steady state 구간 그래프
6. Schema Registry 호출 수 (S3 조합만)
7. Flink Backpressure 스크린샷 (S3-C1-T4에서 backpressure 발생 여부)
8. SR 장애 시나리오 결과 (S3 조합만 — 장애 시 Sink 동작 여부 기록)
9. 소비자 측 영향도 정리 (기존 JSON 소비자의 마이그레이션 필요 여부)
10. 결론 및 권장 직렬화 방식
