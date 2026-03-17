# S3-C2 (Confluent Wire Format + RecordNameStrategy) 테스트 점검 체크리스트

## 1. 기존 FDCTraceBaseJobVerProtoBuf.java 코드 리뷰 결과

기존 Protobuf Sink 테스트 코드(`FDCTraceBaseJobVerProtoBuf.java`)에서 발견된 문제점:

### 문제 1: SubjectNameStrategy 미설정 (기본값 TopicNameStrategy 사용)

```java
// FDCTraceBaseJobVerProtoBuf.java line 268-274
serializer = new KafkaProtobufSerializer<>();
Properties props = new Properties();
props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "...");
props.put(KafkaProtobufSerializerConfig.AUTO_REGISTER_SCHEMAS, false);
props.put(KafkaProtobufSerializerConfig.USE_LATEST_VERSION, true);
serializer.configure((Map) props, false);
// ★ value.subject.name.strategy 미설정 → 기본값 TopicNameStrategy 사용
```

**영향:** 장비별 동적 토픽마다 별도의 subject가 생성된다.
- 장비 1000대 → subject 1000개 → 캐시 한계 도달 → 캐시 thrashing
- **이것이 "Protobuf가 느리다"는 테스트 결과의 근본 원인일 가능성이 높다**

### 문제 2: USE_LATEST_VERSION=true 사용

```java
props.put(KafkaProtobufSerializerConfig.USE_LATEST_VERSION, true);
```

`USE_LATEST_VERSION=true`이면 `serializeImpl()` 내부에서 `lookupLatestVersion()` 분기를 탄다:

```
serializeImpl() → Branch 5 (useLatestVersion=true)
  → lookupLatestVersion(subject, schema, latestCompatStrict)
    → CachedSchemaRegistryClient.getLatestSchemaMetadata(subject)
      → latestVersionCache 조회 (TTL 기반 캐시)
      → miss 시: GET /subjects/{subject}/versions/latest  ← HTTP 호출
```

**문제점:**
- `latestVersionCache`는 **TTL 기반** 캐시이다 (`latest.cache.ttl.sec`, 기본 -1이지만 SerDe에서는 달라질 수 있음)
- TTL이 적용되면 주기적으로 캐시가 만료되어 **반복적 HTTP 호출**이 발생한다
- TopicNameStrategy와 결합되면: 장비 수 × 주기적 만료 = 지속적 HTTP 호출

### 문제 3: Confluent Web UI에서 스키마 인식 불가

상대 개발자가 "RecordNameStrategy를 사용했는데 UI에서 스키마가 인식 안 된다"고 했다면:

**가능한 원인:**
1. `RecordNameStrategy`를 설정했지만 스키마를 해당 subject 이름으로 **사전 등록하지 않았다**
2. Confluent Web UI는 기본적으로 **토픽 기반**으로 subject를 표시한다. RecordNameStrategy의 subject(`com.skhynix.datahub.flink.records.protobuf.FDCTraceFinal`)는 토픽 뷰에 나타나지 않을 수 있다 → UI 표시 문제이지 등록 실패가 아닐 수 있음
3. 스키마를 잘못된 subject 이름으로 등록했다

**확인 방법:**
```bash
# 전체 subject 목록 확인
curl http://<schema-registry>:8081/subjects

# RecordNameStrategy subject 직접 확인
curl http://<schema-registry>:8081/subjects/com.skhynix.datahub.flink.records.protobuf.FDCTraceFinal/versions
```

---

## 2. Flink Parallelism이 Schema Registry에 미치는 영향

### 2.1 Serializer 인스턴스와 Parallelism의 관계

Flink에서 각 Sink 태스크(TaskManager slot)는 **독립적인 `KafkaProtobufSerializer` 인스턴스**를 갖는다.

```
Parallelism = 4인 경우:

TaskSlot 0: KafkaProtobufSerializer#0 → CachedSchemaRegistryClient#0 → 자체 캐시
TaskSlot 1: KafkaProtobufSerializer#1 → CachedSchemaRegistryClient#1 → 자체 캐시
TaskSlot 2: KafkaProtobufSerializer#2 → CachedSchemaRegistryClient#2 → 자체 캐시
TaskSlot 3: KafkaProtobufSerializer#3 → CachedSchemaRegistryClient#3 → 자체 캐시
```

**각 인스턴스가 독립적으로 warm-up 한다:**
- RecordNameStrategy: parallelism × 1 HTTP 호출 = 4회 (무시할 수준)
- TopicNameStrategy: parallelism × 장비 수 HTTP 호출 = 4 × 1000 = 4,000회

### 2.2 태스크 재시작/리밸런싱

| 이벤트 | 영향 |
|---|---|
| Flink 체크포인트 복구 | Serializer 재생성 → 캐시 초기화 → warm-up 재발생 |
| TaskManager 장애 복구 | 해당 슬롯의 Serializer만 재생성 |
| 스케일 아웃 (parallelism 증가) | 새 슬롯의 Serializer만 warm-up |

### 2.3 Parallelism이 JSON보다 느린 원인이 될 수 있는가?

**RecordNameStrategy 사용 시:**
- warm-up: parallelism × 1 = 수 회 HTTP 호출 → 몇 초 내 완료
- steady state: 모든 인스턴스 캐시 히트 → **JSON보다 느릴 수 없음**

**TopicNameStrategy 사용 시 (기존 코드):**
- warm-up: parallelism × 장비 수 = 수천~수만 회 HTTP 호출
- steady state: 장비 수 > 캐시 크기이면 **지속적 thrashing**
- **JSON보다 확실히 느림**

→ Parallelism 자체가 문제가 아니라, **TopicNameStrategy + 동적 토픽 조합이 Parallelism에 비례하여 문제를 증폭**시킨다.

---

## 3. 올바른 S3-C2 테스트를 위한 필수 점검 항목

### 3.1 Serializer 설정 체크리스트

```
□ value.subject.name.strategy = RecordNameStrategy (FQCN으로 설정)
  → "io.confluent.kafka.serializers.subject.RecordNameStrategy"

□ auto.register.schemas = false
  → 운영 환경 기준 테스트

□ USE_LATEST_VERSION = false (★ 중요)
  → true이면 latestVersionCache TTL 기반 캐시를 사용하여 주기적 HTTP 호출 발생
  → false이면 schemaToIdCache 또는 schemaToResponseCache 사용 (TTL 없음, 크기 기반만)

□ normalize.schemas = false (기본값 유지)
  → true이면 매번 스키마 정규화 비용 추가

□ Schema Registry URL 올바르게 설정
```

### 3.2 스키마 사전 등록 체크리스트

```
□ FDCTraceFinal.proto에 대한 스키마를 RecordNameStrategy subject로 등록

□ Subject 이름 확인:
  → RecordNameStrategy는 schema.name()을 사용
  → Protobuf의 경우: {package}.{message_name}
  → FDCTraceFinal.proto의 package = "com.skhynix.datahub.flink.records.protobuf"
  → schema.name() = descriptor.getFullName() = "com.skhynix.datahub.flink.records.protobuf.FDCTraceFinal"
  → 따라서 subject = "com.skhynix.datahub.flink.records.protobuf.FDCTraceFinal"

□ 등록 확인:
  curl http://<SR>/subjects
  curl http://<SR>/subjects/<subject-name>/versions/latest

□ Confluent Web UI에서 보이지 않아도 위 curl로 확인되면 정상
```

### 3.3 직렬화 대상 객체 체크리스트

```
□ Sink에 들어가는 객체가 com.google.protobuf.Message를 상속한 Protobuf 생성 클래스인가?
  → com.skhynix.datahub.flink.records.protobuf.FDCTraceFinal (○ Protobuf)
  → com.skhynix.datahub.flink.records.json.FDCTraceFinal (✗ Java POJO)

□ FlatMap에서 FDCTrace → FDCTraceFinal 변환 시 Protobuf Builder 사용하는가?
  → FDCTraceFinal.newBuilder()...build() (○)
  → new FDCTraceFinal(); traceFinal.field = ... (✗ POJO 방식)
```

### 3.4 캐시 동작 검증 체크리스트

```
□ 테스트 시작 후 SR 서버 로그에서 HTTP 요청 수 모니터링
  → warm-up 구간: parallelism 수만큼의 요청 발생 (정상)
  → steady state: 요청 0이어야 함 (캐시 히트)
  → steady state에서도 요청이 지속되면 캐시 miss → 설정 문제

□ SR 서버 로그에서 요청되는 subject 이름 확인
  → "com.skhynix.datahub.flink.records.protobuf.FDCTraceFinal" 이면 RecordNameStrategy 정상
  → "prefix.eqp001-value" 이면 TopicNameStrategy 사용 중 → 설정 오류
```

### 3.5 Flink Job 설정 체크리스트

```
□ Source와 Sink의 parallelism 동일하게 설정 (변수 통제)
□ 동일한 소스 데이터 사용 (JSON Sink 테스트와 동일한 입력)
□ 동일한 Kafka Producer 설정 (compression, batch size, linger.ms 등)
□ 동일한 Flink 환경 (checkpoint interval, restart strategy 등)
□ warm-up 구간(최초 1~2분) 제외하고 steady state 메트릭 수집
```

---

## 4. S3-C2 테스트에서 JSON보다 느린 경우 디버깅 가이드

### Step 1: SR HTTP 호출 수 확인

SR 서버에서 요청 로그를 카운트한다. steady state에서:
- **0이면:** SR 오버헤드 아님 → Step 2로
- **0이 아니면:** 캐시 miss 발생 → 설정 확인 (subject name strategy, USE_LATEST_VERSION)

### Step 2: 직렬화 객체 타입 확인

`serialize()` 메서드에서 직렬화하는 객체가 `Message` 인스턴스인지 확인:
- **`FDCTraceFinal` (protobuf)이면:** 정상 → Step 3로
- **POJO를 변환하고 있으면:** 변환 비용이 원인

### Step 3: Flink Backpressure 확인

Flink Web UI에서 Sink 오퍼레이터의 backpressure 확인:
- **Sink에 backpressure가 있으면:** Kafka Producer 설정 문제 가능성
- **FlatMap에 backpressure가 있으면:** Protobuf Builder 변환 비용

### Step 4: GC 로그 확인

Protobuf Builder 패턴은 immutable 객체를 생성하므로 GC 압력이 다를 수 있다:
- JSON POJO: mutable, 재사용 가능
- Protobuf: Builder + build() = 2개 객체 생성 per 메시지

대량 처리 시 GC 차이가 있을 수 있으나, 이것이 "엄청나게 느린" 원인이 되지는 않는다.

### Step 5: Wire Format 오버헤드 측정

`serialize()` 메서드 전후에 시간 측정을 추가하여 순수 직렬화 시간 비교:
```java
long start = System.nanoTime();
byte[] bytes = serializer.serialize(topic, element);
long elapsed = System.nanoTime() - start;
```

---

## 5. 참고: 올바른 S3-C2 설정 vs 기존 코드 비교

| 설정 | 기존 코드 (FDCTraceBaseJobVerProtoBuf) | 올바른 S3-C2 |
|---|---|---|
| `value.subject.name.strategy` | **미설정 (TopicNameStrategy)** | `RecordNameStrategy` |
| `auto.register.schemas` | `false` | `false` |
| `USE_LATEST_VERSION` | **`true`** | `false` |
| `normalize.schemas` | 미설정 (`false`) | `false` |
| 캐시 사용 | `latestVersionCache` (TTL 기반) | `schemaToResponseCache` (크기 기반, TTL 없음) |
| 동적 토픽 1000개 시 subject 수 | **1000** | **1** |
| SR HTTP 호출 (steady state) | **지속적 (TTL 만료 + 캐시 thrashing)** | **0** |
