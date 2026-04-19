  모델 현황 요약 (2026.04 기준)

  ┌───────────┬──────────────────┬───────────────────────────────────────────────────┬─────────────────────────────────┐
  │   모델    │       최신       │                  주요 코딩 벤치                   │              특징               │
  ├───────────┼──────────────────┼───────────────────────────────────────────────────┼─────────────────────────────────┤
  │ Kimi K2.5 │ Moonshot, 1T MoE │ SWE-Bench Verified 76.8, LiveCodeBench 85         │ 에이전틱·비주얼·프론트엔드 강세 │
  ├───────────┼──────────────────┼───────────────────────────────────────────────────┼─────────────────────────────────┤
  │ GLM-5.1   │ Z.ai, 2026.03.27 │ SWE-Bench Pro 58.4 (1위), Claude Opus 4.6의 94.6% │ 코딩·추론 특화 후속 학습        │
  ├───────────┼──────────────────┼───────────────────────────────────────────────────┼─────────────────────────────────┤
  │ Qwen 3.5  │ Alibaba, 2026.02 │ LiveCodeBench v6 83.6                             │ 에이전트·툴 호출·멀티스텝       │
  └───────────┴──────────────────┴───────────────────────────────────────────────────┴─────────────────────────────────┘

  벤치마크 축이 다 달라서 직접 비교 불가 → 아래 테스트 필요.

  ---
  실험 프로토콜

  공통 규칙

  - 동일 프롬프트를 세 모델에 복붙 (토씨 하나 다르면 안 됨)
  - 각 모델 thinking on / off 2회 → 모델당 4문제×2=8회, 총 24회
  - 새 채팅창에서 시작 (이전 대화 오염 방지)
  - 답변 전문을 그대로 저장해서 저한테 전달

  전달 포맷

  === MODEL: Kimi 2.5 | THINKING: ON | TASK: 1 ===
  <답변 전문>
  === END ===

  ---
  테스트 과제 4종

  Task 1 — Flink (Java) 스트리밍 파이프라인

  다음 요구사항을 충족하는 Apache Flink 1.18 Java 애플리케이션을 작성하세요.
  빌드 도구는 Maven이며, pom.xml 의존성도 포함하세요.

  [소스] Kafka topic "orders" (JSON)
    필드: order_id(string), user_id(long), amount(decimal), event_time(epoch ms), status(string)

  [처리]
    1. event_time 기준 워터마크, 최대 out-of-orderness 10초
    2. status="PAID" 인 이벤트만 필터링
    3. user_id 별 5분 tumbling window로 amount 합계 및 건수 계산
    4. late event는 side output으로 분기

  [싱크] StarRocks "user_sales_5m" 테이블 (Primary Key 모델)에 Stream Load upsert
    StarRocks Flink Connector 1.2.9 사용

  [요구]
    - Checkpoint exactly-once, interval 60s, RocksDB state backend
    - Kafka consumer group "flink-orders-v1"
    - 코드에 한국어 주석 최소화, 과도한 래퍼 클래스 만들지 말 것
    - 설명은 코드 위에 3줄 이내로만

  Task 2 — Spark (Scala) 스큐 조인 최적화

  Apache Spark 3.5 (Scala 2.12) 배치 Job을 작성하세요.
  sbt build.sbt 의존성도 포함하세요.

  [입력]
    - fact_clicks: s3://dw/fact_clicks/dt=2026-04-01/ (parquet, 약 50억 row)
      필드: user_id(long), item_id(long), click_time(timestamp), session_id(string)
    - dim_users: s3://dw/dim_users/ (parquet, 약 2천만 row)
      필드: user_id(long), country(string), tier(string), signup_date(date)

  [문제]
    user_id 분포가 skew되어 있어 상위 100개 user_id가 전체 클릭의 40% 차지
    단순 join 시 특정 executor OOM 발생

  [요구]
    1. user_id 기준 inner join으로 country, tier 붙이기
    2. salting 기법으로 skew 해결 (salt 개수는 상수로 하드코딩 X, 설정 가능하게)
    3. country별 클릭 수 집계 후 상위 10개 country 추출
    4. 결과를 parquet으로 s3://dw/mart/country_clicks_daily/dt=2026-04-01/ 에 쓰기
    5. AQE 활성화 설정 포함, 불필요한 repartition 피할 것

  Task 3 — StarRocks DDL & 쿼리 설계

  StarRocks 3.2 기준으로 아래 요구사항을 만족하는 DDL과 쿼리를 작성하세요.

  [테이블] user_event (일별 약 10억 row, 보관 90일)
    필드:
      event_id bigint, user_id bigint, event_type varchar(32),
      event_time datetime, country varchar(8), revenue decimal(18,4),
      properties json

  [DDL 요구]
    1. 테이블 모델 선택과 이유를 DDL 위 주석 2줄로만 설명
    2. event_time 기준 일자 파티션 (동적 파티션 자동 생성/삭제, 최근 90일 유지)
    3. user_id 기준 버킷팅 (적절한 버킷 수 근거 제시)
    4. country, event_type 컬럼에 적합한 인덱스 제안

  [쿼리 요구]
   다음 쿼리가 10초 내 반환되도록 최적화 (materialized view 포함 가능):
    "최근 7일간 국가별 DAU, 총 revenue, event_type='purchase' 건수"

  답변 형식: DDL 한 블록, MV DDL 한 블록, 최종 SELECT 쿼리 한 블록. 다른 설명 금지.

  Task 4 — 버그 수정 + 리뷰 (판별력 높음)

  아래 Flink Java 코드는 프로덕션에서 "간헐적 데이터 손실"과 "checkpoint 실패"가 발생합니다.
  원인 3가지 이상 찾고, 수정된 전체 코드를 제시하세요.

  원본 코드:
  ---
  public class OrderAggregator {
    public static void main(String[] args) throws Exception {
      StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
      env.enableCheckpointing(5000);

      KafkaSource<String> source = KafkaSource.<String>builder()
        .setBootstrapServers("kafka:9092")
        .setTopics("orders")
        .setGroupId("agg")
        .setStartingOffsets(OffsetsInitializer.latest())
        .setValueOnlyDeserializer(new SimpleStringSchema())
        .build();

      DataStream<Order> stream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "kafka")
        .map(json -> new ObjectMapper().readValue(json, Order.class));

      stream.keyBy(o -> o.userId)
        .window(TumblingProcessingTimeWindows.of(Time.minutes(5)))
        .reduce((a, b) -> new Order(a.userId, a.amount.add(b.amount), a.eventTime))
        .addSink(new FlinkKafkaProducer<>("results", new SimpleStringSchema(), props));

      env.execute();
    }
  }
  ---

  형식: (1) 문제점 번호 매겨 나열, (2) 수정된 전체 코드, (3) 그 외 개선 제안 1-2줄.

  ---
  평가 루브릭 (제가 채점할 기준)

  각 답변을 5축 × 0-5점으로 채점, 과제별 가중치 적용.

  ┌──────────────┬─────────────────────────────────┬─────────────────────────────────────────────────────────────────────┐
  │      축      │              설명               │                              핵심 체크                              │
  ├──────────────┼─────────────────────────────────┼─────────────────────────────────────────────────────────────────────┤
  │ Correctness  │ 컴파일/실행 시 돌아가는가       │ API 시그니처, import, 버전 호환성                                   │
  ├──────────────┼─────────────────────────────────┼─────────────────────────────────────────────────────────────────────┤
  │ Spec 준수    │ 요구사항 항목을 빠뜨리지 않는가 │ 체크포인트, salt 설정화, 형식 지시                                  │
  ├──────────────┼─────────────────────────────────┼─────────────────────────────────────────────────────────────────────┤
  │ Idiomaticity │ 해당 프레임워크 관용구          │ DataStream vs Table API 선택, Dataset API 사용, StarRocks 권장 패턴 │
  ├──────────────┼─────────────────────────────────┼─────────────────────────────────────────────────────────────────────┤
  │ Conciseness  │ 과잉 설계/불필요 추상화 없는가  │ 요청 안 한 helper class, try-catch 남발                             │
  ├──────────────┼─────────────────────────────────┼─────────────────────────────────────────────────────────────────────┤
  │ Currency     │ 최신 API·deprecated 회피        │ FlinkKafkaProducer(old) vs KafkaSink(new) 등                        │
  └──────────────┴─────────────────────────────────┴─────────────────────────────────────────────────────────────────────┘

  과제별 가중치
  - Task 1 (Flink): Correctness×1.5, Currency×1.5 (최신 Kafka/StarRocks 커넥터 API 변화 큼)
  - Task 2 (Spark): Idiomaticity×1.5 (salting 구현 방식이 실력 드러남)
  - Task 3 (StarRocks): Spec 준수×2 (요구 형식 엄격, DE 도메인 지식)
  - Task 4 (Bug fix): Correctness×2 (원인 몇 개 잡았는지가 핵심)

  Thinking 모드 비교 관점
  - thinking on이 정확도↑지만 장황함↑·형식 위반↑이면 감점
  - Task 3처럼 "설명 금지" 지시 어기면 즉시 -2

  ---
  참고 출처

  - https://huggingface.co/moonshotai/Kimi-K2.5
  - https://huggingface.co/zai-org/GLM-5.1
  - https://dataconomy.com/2026/04/08/z-ais-glm-5-1-tops-swe-bench-pro-beating-major-ai-rivals/
  - https://qwen.ai/blog?id=qwen3.5
  - https://venturebeat.com/technology/alibabas-qwen-3-5-397b-a17-beats-its-larger-trillion-parameter-model-at-a

  준비 되셨으면 Task 1부터 한 모델씩 결과 전달해주세요. 4과제 전부 돌리기 부담되면 Task 1, 4만 해도 변별력 충분합니다.
