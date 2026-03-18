# Confluent Schema Registry 직렬화 플로우 완전 분석

## 1. 전체 직렬화 호출 체인

```
KafkaProtobufSerializer.serialize(topic, headers, record)
  │
  ├─① schemaCache.getIfPresent(descriptor)        ← Layer 1: 로컬 Descriptor→Schema 캐시
  │     miss → ProtobufSchemaUtils.getSchema(record) + resolveDependencies()
  │
  ├─② getSubjectName(topic, isKey, record, schema)
  │     → SubjectNameStrategy.subjectName(topic, isKey, schema)
  │
  └─③ serializeImpl(subject, topic, isKey, headers, record, schema)
        │
        ├─ autoRegisterSchema=true (기본값):
        │    → CachedSchemaRegistryClient.registerWithResponse(subject, schema)
        │        캐시: schemaToResponseCache[subject][(schema,normalize)]
        │        miss → POST /subjects/{subject}/versions   ← HTTP 호출
        │
        └─ autoRegisterSchema=false:
             → CachedSchemaRegistryClient.getIdWithResponse(subject, schema)
                 캐시: schemaToIdCache[subject][(schema,normalize)]
                 miss → POST /subjects/{subject}            ← HTTP 호출
        │
        └─④ object.toByteArray() + wire format 조립
             → [magic 0x0][schemaId 4bytes][message index][protobuf payload]
```

## 2. 캐시 아키텍처 (3계층)

### Layer 1: KafkaProtobufSerializer 로컬 캐시

**소스:** `KafkaProtobufSerializer.java` line ~34

```java
private static int DEFAULT_CACHE_CAPACITY = 1000;
private Cache<Descriptor, ProtobufSchema> schemaCache;
```

- Protobuf `Descriptor` → `ProtobufSchema` 매핑
- 메시지 타입이 동일하면 즉시 히트
- **subject/topic과 무관** — 순수하게 메시지 타입별 캐시

### Layer 2: CachedSchemaRegistryClient 2단계 캐시

**소스:** `CachedSchemaRegistryClient.java` line 85-101

```java
Cache<String, Cache<SchemaAndNormalize, RegisterSchemaResponse>> schemaToResponseCache;
//     ↑ subject(outer)    ↑ (schema, normalize)(inner)         ↑ 응답(schemaId 포함)
```

**구조:**

```
schemaToResponseCache (outer, Guava Cache, maxSize=cacheCapacity)
  ├─ "topic1-value" → inner Cache (maxSize=cacheCapacity)
  │    └─ (FDCTraceFinal schema, false) → RegisterSchemaResponse(id=1)
  ├─ "topic2-value" → inner Cache (maxSize=cacheCapacity)
  │    └─ (FDCTraceFinal schema, false) → RegisterSchemaResponse(id=1)
  └─ ...
```

**캐시 히트 과정** (`registerWithResponse()` line 535-577):

```java
// 1단계: outer 캐시에서 subject별 inner 캐시를 가져옴 (없으면 생성)
Cache<SchemaAndNormalize, RegisterSchemaResponse> schemaResponseMap =
    schemaToResponseCache.get(subject,
        () -> CacheBuilder.newBuilder().maximumSize(cacheCapacity).build());

// 2단계: inner 캐시에서 schema 조회 (비동기 fast path)
SchemaAndNormalize cacheKey = new SchemaAndNormalize(schema, normalize);
RegisterSchemaResponse cachedResponse = schemaResponseMap.getIfPresent(cacheKey);
if (cachedResponse != null) {
    return cachedResponse;  // ★ CACHE HIT — HTTP 호출 없음
}

// 3단계: synchronized slow path (double-checked locking)
synchronized (this) {
    cachedResponse = schemaResponseMap.getIfPresent(cacheKey);  // 재확인
    if (cachedResponse != null) return cachedResponse;

    // ★ CACHE MISS — Schema Registry HTTP 호출 발생
    final RegisterSchemaResponse retrievedResponse =
        registerAndGetId(subject, schema, ...);
    schemaResponseMap.put(cacheKey, retrievedResponse);  // 캐시 저장
}
```

### Layer 3: Schema Registry 서버

- `_schemas` 카프카 토픽에 영구 저장

## 3. 캐시 설정값 정리

**소스:** `AbstractKafkaSchemaSerDeConfig.java` line 81-83

| 설정 프로퍼티 | 상수명 | 기본값 | 단위/의미 | 상세 설명 |
|---|---|---|---|---|
| `max.schemas.per.subject` | `MAX_SCHEMAS_PER_SUBJECT_CONFIG` | **1000** | **개수** (스키마 객체 수) | `CachedSchemaRegistryClient` 생성자에 `cacheCapacity`로 전달된다. 이 값은 outer 캐시(subject 수 상한)와 inner 캐시(subject당 스키마 수 상한) 모두에 동일하게 적용된다. 즉, **최대 1000개의 서로 다른 subject**를 캐시할 수 있고, 각 subject 안에서 **최대 1000개의 서로 다른 스키마 버전**을 캐시할 수 있다. 스키마의 바이트 크기와는 무관하며, 순수하게 스키마 객체 개수 기준이다. |
| `auto.register.schemas` | `AUTO_REGISTER_SCHEMAS` | **true** | **boolean** | `true`이면 `registerWithResponse()` 호출 (스키마 등록 시도, 이미 존재하면 기존 ID 반환). `false`이면 `getIdWithResponse()` 호출 (조회만, 미등록 시 예외). 운영 환경에서는 `false` 권장. |
| `value.subject.name.strategy` | `VALUE_SUBJECT_NAME_STRATEGY` | `TopicNameStrategy` (실질적 기본값) | **클래스명** (Strategy 구현체의 FQCN) | value 직렬화 시 subject 이름을 생성하는 전략 클래스. 이 subject 이름이 `CachedSchemaRegistryClient`의 outer 캐시 key로 사용되므로, 전략 선택이 캐시 효율에 직접적인 영향을 미친다. |
| `latest.cache.size` | `LATEST_CACHE_SIZE` | **1000** | **개수** (subject 수) | `getLatestSchemaMetadata()` 호출 결과를 캐시하는 `latestVersionCache`의 최대 엔트리 수. **최대 1000개 subject의 latest 버전 정보**를 캐시한다. 스키마 크기나 버전 수와 무관하며, subject당 latest 1건만 저장하므로 엔트리 수 = subject 수이다. `useLatestVersion=true` 설정 시에만 사용된다. |
| `latest.cache.ttl.sec` | `LATEST_CACHE_TTL_CONFIG` | **-1** (무제한) | **초** (seconds) | `latestVersionCache`의 TTL(Time-To-Live). `-1`이면 TTL 없이 크기 기반 eviction만 적용. `0` 이상이면 해당 초 후 캐시 엔트리 만료. 예: `60`이면 latest 조회 결과가 60초 후 자동 만료되어 다음 조회 시 SR에 재요청한다. |

### 설정값 단위 요약

```
max.schemas.per.subject = 1000
  → outer 캐시: 최대 1000개 subject (토픽/레코드 타입별)
  → inner 캐시: subject당 최대 1000개 스키마 객체 (버전별)
  → 단위: 스키마 객체 개수 (바이트 크기 무관)

latest.cache.size = 1000
  → 최대 1000개 subject의 latest 버전 메타데이터 캐시
  → 단위: subject 개수 (스키마 크기/버전 수 무관)
  → subject당 1건(latest)만 저장

latest.cache.ttl.sec = -1
  → 단위: 초 (seconds)
  → -1: TTL 없음 (크기 기반 eviction만)
  → 0 이상: 해당 초 후 만료
```

> **참고:** `schema.registry.cache.max.size`라는 프로퍼티는 `SchemaRegistryClientConfig`에 존재하지 않는다.
> `max.schemas.per.subject`가 `CachedSchemaRegistryClient` 생성자에 `cacheCapacity`로 전달되는 유일한 캐시 크기 설정이다.

## 4. 캐시 Eviction 방식

**모든 캐시는 Guava `Cache`** (과거 버전의 `IdentityHashMap`이 아님)

```java
CacheBuilder.newBuilder().maximumSize(cacheCapacity).build();
```

- **Eviction 방식:** Guava의 approximate LRU (접근 빈도 + 최근성 기반)
- **TTL:** 주요 캐시(`schemaToResponseCache`, `schemaToIdCache`)에는 TTL 없음 — 순수 크기 기반
- **outer 캐시 maxSize = cacheCapacity (1000):** subject 수가 1000개 초과 시 LRU eviction
- **inner 캐시 maxSize = cacheCapacity (1000):** subject당 스키마 수가 1000개 초과 시 eviction (실질적으로 초과할 일 거의 없음)

## 5. SubjectNameStrategy 구현체별 subject 생성 규칙

Schema Registry에서 **subject**는 스키마 호환성 검증의 단위이다. 동일 subject 안에서 스키마를 등록하면 이전 버전과의 호환성(backward, forward 등)이 검증된다. SubjectNameStrategy는 이 subject 이름을 어떻게 생성할지 결정하는 전략 인터페이스이다.

### TopicNameStrategy (기본값)

**소스:** `TopicNameStrategy.java` line 37-42

```java
public String subjectName(String topic, boolean isKey, ParsedSchema schema) {
    return isKey ? topic + "-key" : topic + "-value";
}
```

**설명:** 토픽 이름을 기준으로 subject를 생성한다. 동일 토픽에 발행되는 모든 메시지는 같은 subject에 속하므로, **하나의 토픽에는 하나의 스키마 타입만 허용**된다. 가장 보편적이고 안전한 전략으로, 토픽과 스키마가 1:1로 매핑되는 일반적인 환경에 적합하다.

- subject: `{topic}-value` 또는 `{topic}-key`
- 토픽마다 별도의 subject 생성
- `schema` 파라미터 무시
- **스키마 호환성 범위:** 토픽 단위 — 같은 토픽 내에서만 호환성 검증
- **적합한 환경:** 토픽 수가 고정적이거나 적은 경우
- **부적합한 환경:** 동적 토픽이 대량 생성되는 경우 (subject 수 폭발 → 캐시 문제)

### RecordNameStrategy

**소스:** `RecordNameStrategy.java` line 46-51

```java
public String subjectName(String topic, boolean isKey, ParsedSchema schema) {
    return getRecordName(schema, isKey);  // schema.name() 반환
}
```

**설명:** 스키마의 fully-qualified 레코드 이름을 subject로 사용한다. 토픽 이름을 완전히 무시하므로, **동일 스키마 타입이 여러 토픽에 발행되어도 단 하나의 subject**만 생성된다. 하나의 토픽에 서로 다른 스키마 타입의 메시지를 혼합하여 발행할 수도 있다.

- subject: `{schema의 fully-qualified name}` (예: `com.skhynix.datahub.flink.records.protobuf.FDCTraceFinal`)
- `topic` 파라미터 무시 — 토픽 수와 무관하게 레코드 타입당 1개 subject
- `usesSchema()` → `true` 반환
- **스키마 호환성 범위:** 레코드 타입 단위 (글로벌) — 모든 토픽에서 해당 타입의 호환성이 동일하게 적용
- **적합한 환경:** 동일 스키마가 다수 토픽에 발행되는 경우 (동적 토픽 환경에 최적)
- **주의점:** 특정 토픽에서만 스키마를 다르게 진화시키는 것이 불가능 (글로벌 호환성)

### TopicRecordNameStrategy

**소스:** `TopicRecordNameStrategy.java` line 38-43

```java
public String subjectName(String topic, boolean isKey, ParsedSchema schema) {
    return topic + "-" + getRecordName(schema, isKey);
}
```

**설명:** 토픽 이름과 레코드 이름을 결합하여 subject를 생성한다. 하나의 토픽에 여러 스키마 타입을 혼합할 수 있으면서, 동시에 토픽별로 독립적인 호환성 검증이 가능하다. 다만 subject 수가 (토픽 수 × 레코드 타입 수)로 증가하므로, 동적 토픽 환경에서는 TopicNameStrategy와 동일한 캐시 문제가 발생한다.

- subject: `{topic}-{schema.name()}`
- 토픽 × 레코드 타입 조합만큼 subject 생성
- `usesSchema()` → `true` 반환
- **스키마 호환성 범위:** (토픽, 레코드 타입) 쌍 단위 — 가장 세분화된 호환성 관리
- **적합한 환경:** 하나의 토픽에 여러 이벤트 타입을 발행하면서 토픽별 독립적 호환성이 필요한 경우
- **부적합한 환경:** 동적 토픽이 대량 생성되는 경우 (TopicNameStrategy와 동일한 캐시 문제)

### Strategy 비교 요약

| | TopicNameStrategy | RecordNameStrategy | TopicRecordNameStrategy |
|---|---|---|---|
| subject 결정 기준 | 토픽 | 레코드 타입 | 토픽 + 레코드 타입 |
| 1토픽 1스키마 | 지원 | 지원 | 지원 |
| 1토픽 N스키마 | 불가 | 지원 | 지원 |
| N토픽 1스키마 | subject N개 | **subject 1개** | subject N개 |
| 호환성 범위 | 토픽별 독립 | 글로벌 (모든 토픽 공유) | (토픽, 타입)별 독립 |
| 동적 토픽 적합성 | 부적합 | **최적** | 부적합 |

## 6. SubjectNameStrategy가 캐시에 미치는 영향

`CachedSchemaRegistryClient`의 2단계 캐시에서 **outer 캐시의 key가 subject name**이므로, SubjectNameStrategy 선택이 캐시 효율에 직접적인 영향을 미친다.

### 동적 토픽 환경 (장비별 토픽, 예: 2000대)

| Strategy | 생성되는 subject 수 | outer 캐시 엔트리 | eviction 여부 (기본 maxSize=1000) |
|---|---|---|---|
| TopicNameStrategy | 2,000 | 2,000 | **eviction 발생 → 캐시 thrashing** |
| RecordNameStrategy | 1 | 1 | eviction 없음 |
| TopicRecordNameStrategy | 2,000 | 2,000 | **eviction 발생 → 캐시 thrashing** |

### 캐시 thrashing 시나리오 (TopicNameStrategy + 장비 2000대)

```
1. eqp0001 메시지 → subject "prefix.eqp0001-value" → 캐시 miss → HTTP 호출
2. eqp0002 메시지 → subject "prefix.eqp0002-value" → 캐시 miss → HTTP 호출
...
1000. eqp1000 메시지 → outer 캐시 maxSize=1000 도달
1001. eqp1001 메시지 → eqp0001의 캐시 엔트리 evict → HTTP 호출
...
2001. eqp0001 메시지 → 이미 evict됨 → 또 HTTP 호출  ← ★ 반복
```

## 7. 참고 소스 코드

| 파일 | 위치 |
|---|---|
| `CachedSchemaRegistryClient.java` | `client/src/main/java/io/confluent/kafka/schemaregistry/client/` |
| `SchemaRegistryClientConfig.java` | `client/src/main/java/io/confluent/kafka/schemaregistry/client/` |
| `AbstractKafkaSchemaSerDe.java` | `schema-serializer/src/main/java/io/confluent/kafka/serializers/` |
| `AbstractKafkaSchemaSerDeConfig.java` | `schema-serializer/src/main/java/io/confluent/kafka/serializers/` |
| `AbstractKafkaProtobufSerializer.java` | `protobuf-serializer/src/main/java/io/confluent/kafka/serializers/protobuf/` |
| `KafkaProtobufSerializer.java` | `protobuf-serializer/src/main/java/io/confluent/kafka/serializers/protobuf/` |
| `TopicNameStrategy.java` | `schema-serializer/src/main/java/io/confluent/kafka/serializers/subject/` |
| `RecordNameStrategy.java` | `schema-serializer/src/main/java/io/confluent/kafka/serializers/subject/` |
| `TopicRecordNameStrategy.java` | `schema-serializer/src/main/java/io/confluent/kafka/serializers/subject/` |
| `SchemaRegistryClientFactory.java` | `client/src/main/java/io/confluent/kafka/schemaregistry/client/` |

> GitHub Repository: https://github.com/confluentinc/schema-registry
