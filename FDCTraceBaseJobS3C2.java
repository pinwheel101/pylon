// src/main/java/com/skhynix/datahub/flink/FDCTraceBaseJobS3C2.java
package com.skhynix.datahub.flink;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.sink.KafkaSinkBuilder;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.KafkaSourceBuilder;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.flink.util.ParameterTool;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.skhynix.datahub.flink.common.Constants;
import com.skhynix.datahub.flink.common.SettingsReader;
import com.skhynix.datahub.flink.common.SettingsReader.KafkaConfig;
import com.skhynix.datahub.flink.records.protobuf.FDCTrace;
import com.skhynix.datahub.flink.records.protobuf.FDCTraceFinal;
import com.skhynix.datahub.flink.records.protobuf.FDCTraceParamter;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializerConfig;

//@formatter:off
/**
 * Title : FDC Trace S4 Lightweight Wire Format 성능 테스트 Job
 *
 * 목적: Lightweight Wire Format (커스텀 바이트 라이터)의 성능 측정
 *
 * KafkaProtobufSerializer를 사용하지 않고, Confluent Wire Format 헤더를 직접 조립하여
 * toByteArray() 결과와 결합하는 방식. 메시지 포맷이 Confluent Wire Format과 동일하므로
 * 소비자 측에서 KafkaProtobufDeserializer로 정상 역직렬화 가능.
 *
 * 기존 S3-C2 (KafkaProtobufSerializer + RecordNameStrategy)와의 차이:
 * - KafkaProtobufSerializer의 per-message 오버헤드 완전 제거
 *   (resolveDependencies, executeRules×2, toMessageIndexes 등)
 * - Schema Registry는 open() 시 schemaId 1회 조회 후 캐싱
 * - 런타임에는 순수 toByteArray() + 6바이트 헤더 복사만 수행
 *
 * Wire Format 구조: [0x00][schemaId 4B][0x00][protobuf bytes]
 *   - 0x00: magic byte (Confluent wire format identifier)
 *   - schemaId 4B: big-endian schema ID
 *   - 0x00: message index = [0] (단일 메시지 타입, varint로 0 = 1바이트)
 *   - protobuf bytes: message.toByteArray()
 *
 * 사전 조건:
 * - Schema Registry에 FDCTraceFinal 스키마를 사전 등록 필요
 *   subject = "com.skhynix.datahub.flink.records.protobuf.FDCTraceFinal"
 *
 *   curl -X POST "http://<SR>:8081/subjects/com.skhynix.datahub.flink.records.protobuf.FDCTraceFinal/versions" \
 *     -H "Content-Type: application/vnd.schemaregistry.v1+json" \
 *     -d '{"schemaType":"PROTOBUF","schema":"<FDCTraceFinal.proto 내용>"}'
 *
 * 데이터 흐름:
 * [그룹핑토픽 - Protobuf FDCTrace]
 *   → Source (KafkaProtobufDeserializer)
 *   → FlatMap (FDCTrace → FDCTraceFinal Protobuf Builder)
 *   → Sink (Lightweight Wire Format: 헤더 + toByteArray())
 *   → [장비별 동적 토픽 - Confluent Wire Format 호환]
 */
//@formatter:on
public class FDCTraceBaseJobS3C2 {
    private static final Logger logger = LoggerFactory.getLogger(FDCTraceBaseJobS3C2.class);

    public static KafkaSource<FDCTrace> makeKafkaSource(final ParameterTool parameterTool) {
        final String configEnv = parameterTool.get(Constants.CONFIG_ENV, Constants.CONFIG_ENV_DEV);
        final String sourceTopic = parameterTool.get(Constants.SOURCE_TOPIC);
        final String startOffset = parameterTool.get(Constants.START_OFFSET, Constants.DEFAULT_START_OFFSET_EARLIEST);

        KafkaConfig kafkaConfig = Constants.CONFIG_ENV_PRD.equalsIgnoreCase(configEnv) ?
                SettingsReader.getInstance().getKafkaConfigProduction() : SettingsReader.getInstance().getKafkaConfigDevelopment();

        KafkaSourceBuilder<FDCTrace> builder = KafkaSource.<FDCTrace>builder()
                .setBootstrapServers(kafkaConfig.getBootstrapServers())
                .setTopics(sourceTopic)
                .setGroupId(parameterTool.get(Constants.KAFKA_GROUP_ID))
                .setStartingOffsets(
                        !NumberUtils.isNumber(startOffset) ? OffsetsInitializer.committedOffsets(
                                startOffset.equalsIgnoreCase("latest")
                                        ? OffsetResetStrategy.LATEST
                                        : OffsetResetStrategy.EARLIEST)
                                : OffsetsInitializer.timestamp(Long.valueOf(startOffset)))
                .setProperty(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, "1048576")
                .setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "10000")
                .setProperty(ConsumerConfig.RECEIVE_BUFFER_CONFIG, "33554432")
                .setProperty(ConsumerConfig.SEND_BUFFER_CONFIG, "33554432")
                .setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")
                .setValueOnlyDeserializer(new SourceDeserializationSchema())
                .setClientIdPrefix(FDCTraceBaseJobS3C2.class.getName().toLowerCase() + "-client-" + System.currentTimeMillis());

        if (kafkaConfig.isSecurity()) {
            return builder.setProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, kafkaConfig.getSecurityProtocol())
                    .setProperty(Constants.SASL_MECHANISM, kafkaConfig.getSaslMechanism())
                    .setProperty(Constants.SASL_JAAS_CONFIG, kafkaConfig.getSaslJaasConfig())
                    .build();
        } else {
            return builder.build();
        }
    }

    public static KafkaSink<FDCTraceFinal> makeKafkaSink(final ParameterTool parameterTool) {
        final String configEnv = parameterTool.get(Constants.CONFIG_ENV, Constants.CONFIG_ENV_DEV);
        String topicPrefix = parameterTool.get(Constants.SOURCE_TOPIC);
        logger.info("topicPrefix: {}", topicPrefix);

        KafkaConfig kafkaConfig = Constants.CONFIG_ENV_PRD.equalsIgnoreCase(configEnv) ?
                SettingsReader.getInstance().getKafkaConfigProduction() : SettingsReader.getInstance().getKafkaConfigDevelopment();

        if (topicPrefix.endsWith(".NEW")) {
            topicPrefix = topicPrefix.replace(".NEW", ".");
        }

        KafkaSinkBuilder<FDCTraceFinal> builder = KafkaSink.<FDCTraceFinal>builder()
                .setBootstrapServers(kafkaConfig.getBootstrapServers())
                .setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, Constants.DEFAULT_COMPRESSION_TYPE_LZ4)
                .setProperty(ProducerConfig.ACKS_CONFIG, "1")
                .setProperty(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, Constants.DEFAULT_MAX_REQUEST_SIZE)
                .setProperty(ProducerConfig.BATCH_SIZE_CONFIG, "2097152")
                .setProperty(ProducerConfig.SEND_BUFFER_CONFIG, "2097152")
                .setProperty(ProducerConfig.LINGER_MS_CONFIG, "300")
                .setProperty(ProducerConfig.BUFFER_MEMORY_CONFIG, "134217728")
                .setRecordSerializer(
                        new LightweightWireFormatSinkSerializationSchema(topicPrefix.replaceAll("\\.(grp\\d{2,}|dlq)$", ".")))
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .setTransactionalIdPrefix(FDCTraceBaseJobS3C2.class.getName().toLowerCase() + "-tran-" + System.currentTimeMillis());

        if (kafkaConfig.isSecurity()) {
            return builder.setProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, kafkaConfig.getSecurityProtocol())
                    .setProperty(Constants.SASL_MECHANISM, kafkaConfig.getSaslMechanism())
                    .setProperty(Constants.SASL_JAAS_CONFIG, kafkaConfig.getSaslJaasConfig())
                    .build();
        } else {
            return builder.build();
        }
    }

    public static void main(String[] args) throws Exception {
        logger.info("args : {}", String.join(",", args));

        final ParameterTool parameterTool = ParameterTool.fromArgs(args);
        final String configEnv = parameterTool.get(Constants.CONFIG_ENV, Constants.CONFIG_ENV_DEV);
        final String clusterGroup = parameterTool.get(Constants.CLUSTER_GROUP);
        final String jobName = parameterTool.get(Constants.JOB_NAME);
        final String sourceTopic = parameterTool.get(Constants.SOURCE_TOPIC);
        final String kafkaGroupId = parameterTool.get(Constants.KAFKA_GROUP_ID);
        final String startOffset = parameterTool.get(Constants.START_OFFSET, Constants.DEFAULT_START_OFFSET_EARLIEST);
        final int sourceParallelism = parameterTool.getInt(Constants.SOURCE_PARALLELISM, 0);
        final boolean disableOperatorChaining = parameterTool.getBoolean(Constants.DISABLE_OPERATOR_CHAINING, false);
        final boolean enableLogger = parameterTool.getBoolean(Constants.ENABLE_LOGGER, false);

        logger.info("configEnv: {}", configEnv);
        logger.info("clusterGroup : {}", clusterGroup);
        logger.info("jobName : {}", jobName);
        logger.info("sourceTopic : {}", sourceTopic);
        logger.info("kafkaGroupId : {}", kafkaGroupId);
        logger.info("startOffset : {}", startOffset);
        logger.info("sourceParallelism : {}", sourceParallelism);
        logger.info("disableOperatorChaining : {}", disableOperatorChaining);
        logger.info("enableLogger : {}", enableLogger);
        logger.info("serializationStrategy : Lightweight Wire Format (S4)");

        if (StringUtils.isBlank(configEnv) || StringUtils.isBlank(clusterGroup) || StringUtils.isBlank(jobName) ||
                StringUtils.isBlank(sourceTopic) || StringUtils.isBlank(kafkaGroupId)) {
            logger.error(
                    "IllegalArgumentException : Please check the group designation. ex. --env dev --clusterGroup a --jobName basejob --sourceTopic hello_topic1 --kafka.group.id sanghyun_test [--startOffset earliest] [--disableOperatorChaining false]");
            throw new IllegalArgumentException(
                    "Please check the group designation. ex. --env dev --clusterGroup a --jobName basejob --sourceTopic hello_topic1 --kafka.group.id sanghyun_test [--startOffset earliest] [--disableOperatorChaining false]");
        }

        Configuration config = new Configuration();
        config.set(RestartStrategyOptions.RESTART_STRATEGY, "fixed-delay");
        config.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS, 3);
        config.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_DELAY, Duration.ofSeconds(10));
        config.set(ExecutionOptions.BUFFER_TIMEOUT, Duration.ofMillis(10));
        config.set(CheckpointingOptions.ENABLE_UNALIGNED, true);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(config);
        if (disableOperatorChaining) {
            env.disableOperatorChaining();
        }

        env.enableCheckpointing(60 * 60 * 1000);

        DataStream<FDCTrace> sourceDs = env
                .fromSource(makeKafkaSource(parameterTool), WatermarkStrategy.noWatermarks(), "Kafka Source")
                .returns(FDCTrace.class)
                .setParallelism(sourceParallelism == 0 ? env.getParallelism() : sourceParallelism);

        DataStream<FDCTraceFinal> processDs = null;

        if (sourceParallelism == 0 || sourceParallelism == env.getParallelism()) {
            processDs = sourceDs
                    .flatMap(new ConvertingToProtoBufFlatmapFunction(enableLogger))
                    .returns(FDCTraceFinal.class)
                    .name("forward Processing FlatmapFunction");
        } else {
            processDs = sourceDs
                    .keyBy(element -> element.getEqpId())
                    .flatMap(new ConvertingToProtoBufFlatmapFunction(enableLogger))
                    .returns(FDCTraceFinal.class)
                    .name("hash Processing FlatmapFunction");
        }

        processDs.sinkTo(makeKafkaSink(parameterTool))
                .name("Kafka Sink");

        env.execute(String.format("%s Jobs", jobName));
    }

    // =========================================================================
    // Source Deserialization (FDCTraceBaseJob과 동일)
    // =========================================================================
    private static class SourceDeserializationSchema implements DeserializationSchema<FDCTrace> {

        private static final long serialVersionUID = -392805895791097765L;

        private transient KafkaProtobufDeserializer<FDCTrace> deserializer;

        @SuppressWarnings({ "unchecked", "rawtypes" })
        @Override
        public void open(InitializationContext context) throws Exception {
            DeserializationSchema.super.open(context);

            deserializer = new KafkaProtobufDeserializer<>();
            Properties props = new Properties();
            props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                    "http://icahubkafka001.datahub.skhynix.com:8081,http://icahubkafka002.datahub.skhynix.com:8081");
            props.put(KafkaProtobufDeserializerConfig.SPECIFIC_PROTOBUF_VALUE_TYPE, "com.skhynix.datahub.flink.records.protobuf.FDCTrace");
            props.put(KafkaProtobufDeserializerConfig.DERIVE_TYPE_CONFIG, "true");
            props.put(KafkaProtobufDeserializerConfig.AUTO_REGISTER_SCHEMAS, "false");
            deserializer.configure((Map) props, false);
        }

        @Override
        public TypeInformation<FDCTrace> getProducedType() {
            return TypeInformation.of(FDCTrace.class);
        }

        @Override
        public FDCTrace deserialize(byte[] message) throws IOException {
            return deserializer.deserialize(null, message);
        }

        @Override
        public boolean isEndOfStream(FDCTrace nextElement) {
            return false;
        }
    }

    // =========================================================================
    // Sink Serialization: Lightweight Wire Format (S4)
    //
    // KafkaProtobufSerializer를 사용하지 않고 Confluent Wire Format 헤더를 직접 조립.
    // per-message 오버헤드(resolveDependencies, executeRules×2, toMessageIndexes) 완전 제거.
    //
    // Wire Format: [0x00][schemaId 4B][0x00][toByteArray()]
    //   - 0x00: magic byte
    //   - schemaId: SR에서 open() 시 1회 조회하여 캐싱
    //   - 0x00: message index = [0] (varint, 단일 메시지 타입)
    //   - toByteArray(): 순수 Protobuf 직렬화
    //
    // 소비자 측에서 KafkaProtobufDeserializer로 정상 역직렬화 가능.
    // =========================================================================
    private static class LightweightWireFormatSinkSerializationSchema
            implements KafkaRecordSerializationSchema<FDCTraceFinal> {

        private static final long serialVersionUID = 7823456789012345679L;

        private static final String SR_URL =
                "http://icahubkafka001.datahub.skhynix.com:8081,http://icahubkafka002.datahub.skhynix.com:8081";
        private static final String SUBJECT =
                "com.skhynix.datahub.flink.records.protobuf.FDCTraceFinal";

        private String topicPrefix;
        private transient byte[] wireFormatHeader;

        public LightweightWireFormatSinkSerializationSchema(String topicPrefix) {
            this.topicPrefix = topicPrefix;
        }

        @Override
        public void open(SerializationSchema.InitializationContext context, KafkaSinkContext sinkContext)
                throws Exception {
            KafkaRecordSerializationSchema.super.open(context, sinkContext);

            // Schema Registry에서 schemaId를 1회 조회
            int schemaId = lookupSchemaId();

            // wire format 헤더를 사전 조립 (6바이트 고정)
            // [magic 0x00][schemaId 4B big-endian][message index 0x00]
            wireFormatHeader = new byte[] {
                    0x00,                           // magic byte
                    (byte) (schemaId >> 24),         // schemaId byte 1 (MSB)
                    (byte) (schemaId >> 16),         // schemaId byte 2
                    (byte) (schemaId >> 8),          // schemaId byte 3
                    (byte) schemaId,                 // schemaId byte 4 (LSB)
                    0x00                             // message index = [0] (varint)
            };

            logger.info("S4 Lightweight Wire Format Sink initialized: subject={}, schemaId={}, headerSize={}B",
                    SUBJECT, schemaId, wireFormatHeader.length);
        }

        private int lookupSchemaId() throws IOException, RestClientException {
            List<String> srUrls = java.util.Arrays.asList(SR_URL.split(","));
            SchemaRegistryClient srClient = new CachedSchemaRegistryClient(srUrls, 10);

            // 사전 등록된 스키마의 ID를 조회
            ProtobufSchema schema = new ProtobufSchema(
                    FDCTraceFinal.getDescriptor());
            int id = srClient.getId(SUBJECT, schema);

            logger.info("Schema ID resolved: subject={}, id={}", SUBJECT, id);
            return id;
        }

        @Override
        public ProducerRecord<byte[], byte[]> serialize(FDCTraceFinal element,
                KafkaSinkContext context, Long timestamp) {

            // 동적 토픽: 장비별 토픽 (FDCTraceBaseJob과 동일한 토픽 생성 로직)
            String topic = topicPrefix + element.getEqpId();
            topic = topic.toLowerCase();

            // 순수 Protobuf 직렬화
            byte[] pbBytes = element.toByteArray();

            // wire format 헤더 + protobuf bytes 결합
            byte[] value = new byte[wireFormatHeader.length + pbBytes.length];
            System.arraycopy(wireFormatHeader, 0, value, 0, wireFormatHeader.length);
            System.arraycopy(pbBytes, 0, value, wireFormatHeader.length, pbBytes.length);

            return new ProducerRecord<>(
                    topic,
                    null,
                    null,
                    element.getFdcChambId().getBytes(StandardCharsets.UTF_8),
                    value);
        }
    }

    // =========================================================================
    // FlatMap: FDCTrace (Protobuf) → FDCTraceFinal (Protobuf)
    // FDCTraceBaseJobVerProtoBuf.java의 ConvertingFDCTraceFinalTypeToProtoBufFlatmapFunction과 동일
    // =========================================================================
    private static class ConvertingToProtoBufFlatmapFunction
            extends RichFlatMapFunction<FDCTrace, FDCTraceFinal> {

        private static final long serialVersionUID = -5311962414688795125L;

        private static final Logger logger = LoggerFactory.getLogger(ConvertingToProtoBufFlatmapFunction.class);
        private boolean enableLogger = false;

        public ConvertingToProtoBufFlatmapFunction(boolean enableLogger) {
            this.enableLogger = enableLogger;
        }

        @Override
        public void open(OpenContext openContext) throws Exception {
            super.open(openContext);

            if (enableLogger) {
                logger.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>> ConvertingToProtoBufFlatmapFunction open. {}", System.currentTimeMillis());
            }
        }

        @Override
        public void flatMap(FDCTrace trace, Collector<FDCTraceFinal> out) throws Exception {
            long startTime = System.currentTimeMillis();
            int messageCount = 0;

            List<FDCTraceParamter> paramList = trace.getParameterListList();

            if (paramList != null) {
                for (FDCTraceParamter param : paramList) {
                    FDCTraceFinal.Builder finalBuilder = FDCTraceFinal.newBuilder();

                    if (trace.getCollectionInterval() != null) {
                        finalBuilder.setCollectionInterval(trace.getCollectionInterval());
                    }
                    if (trace.getFab() != null) {
                        finalBuilder.setFab(trace.getFab());
                    }
                    if (trace.getEqpId() != null) {
                        finalBuilder.setEqpId(trace.getEqpId());
                    }
                    if (trace.getFdcChambId() != null) {
                        finalBuilder.setFdcChambId(trace.getFdcChambId());
                    }
                    if (param.getAlias() != null) {
                        finalBuilder.setAlias(param.getAlias());
                    }
                    if (param.getName() != null) {
                        finalBuilder.setName(param.getName());
                    }
                    if (param.getParamValue() != null) {
                        finalBuilder.setParamValue(param.getParamValue());
                    }
                    if (trace.getContext() != null) {
                        if (trace.getContext().getOperId() != null) {
                            finalBuilder.setOperId(trace.getContext().getOperId());
                        }
                        if (trace.getContext().getOperDesc() != null) {
                            finalBuilder.setOperDesc(trace.getContext().getOperDesc());
                        }
                        if (trace.getContext().getBatchId() != null) {
                            finalBuilder.setBatchId(trace.getContext().getBatchId());
                        }
                        if (trace.getContext().getBatchTyp() != null) {
                            finalBuilder.setBatchTyp(trace.getContext().getBatchTyp());
                        }
                        if (trace.getContext().getSlotNo() != null) {
                            finalBuilder.setSlotNo(trace.getContext().getSlotNo());
                        }
                        if (trace.getContext().getLotCd() != null) {
                            finalBuilder.setLotCd(trace.getContext().getLotCd());
                        }
                        if (trace.getContext().getLotId() != null) {
                            finalBuilder.setLotId(trace.getContext().getLotId());
                        }
                        if (trace.getContext().getLotTyp() != null) {
                            finalBuilder.setLotTyp(trace.getContext().getLotTyp());
                        }
                        if (trace.getContext().getPortNo() != null) {
                            finalBuilder.setPortNo(trace.getContext().getPortNo());
                        }
                        if (trace.getContext().getMesRecipeId() != null) {
                            finalBuilder.setMesRecipeId(trace.getContext().getMesRecipeId());
                        }
                        if (trace.getContext().getProdId() != null) {
                            finalBuilder.setProdId(trace.getContext().getProdId());
                        }
                        if (trace.getContext().getRecipeId() != null) {
                            finalBuilder.setRecipeId(trace.getContext().getRecipeId());
                        }
                        if (trace.getContext().getReticleId() != null) {
                            finalBuilder.setReticleId(trace.getContext().getReticleId());
                        }
                        if (trace.getContext().getStatCd() != null) {
                            finalBuilder.setStatCd(trace.getContext().getStatCd());
                        }
                        if (trace.getContext().getStepId() != null) {
                            finalBuilder.setStepId(trace.getContext().getStepId());
                        }
                        if (trace.getContext().getStepNm() != null) {
                            finalBuilder.setStepNm(trace.getContext().getStepNm());
                        }
                        if (trace.getContext().getSubstId() != null) {
                            finalBuilder.setSubstId(trace.getContext().getSubstId());
                        }
                        if (trace.getContext().getZone() != null) {
                            finalBuilder.setZone(trace.getContext().getZone());
                        }
                    }
                    if (trace.getEventTime() != null) {
                        finalBuilder.setEventTime(trace.getEventTime());
                    }
                    if (trace.getTxid() != null) {
                        finalBuilder.setTxid(trace.getTxid());
                    }
                    if (finalBuilder.getTxid() != null || finalBuilder.getName() != null) {
                        finalBuilder.setHubTxnId(finalBuilder.getTxid() + "_" + finalBuilder.getName());
                    }
                    if (param.getSvid() != null) {
                        finalBuilder.setSvid(param.getSvid());
                    }
                    if (param.getTarget() != null) {
                        finalBuilder.setTarget(param.getTarget());
                    }
                    if (param.getLsl() != null) {
                        finalBuilder.setLsl(param.getLsl());
                    }
                    if (param.getUsl() != null) {
                        finalBuilder.setUsl(param.getUsl());
                    }
                    if (param.getLcl() != null) {
                        finalBuilder.setLcl(param.getLcl());
                    }
                    if (param.getUcl() != null) {
                        finalBuilder.setUcl(param.getUcl());
                    }
                    if (param.getModelName() != null) {
                        finalBuilder.setModelName(param.getModelName());
                    }
                    if (finalBuilder.getLotId() != null) {
                        finalBuilder.setAliasLotId(StringUtils.substring(finalBuilder.getLotId(), 0, 7));
                    }
                    if (trace.getUuid() != null) {
                        finalBuilder.setUuid(trace.getUuid() + "_" + finalBuilder.getHubTxnId());
                    }

                    ++messageCount;
                    out.collect(finalBuilder.build());
                }
            }

            long endTime = System.currentTimeMillis();

            if (enableLogger) {
                logger.info("FDCTrace -> FDCTraceFinal(Protobuf). startTime : {}ms, endTime : {}ms, executedTime : {}ms, Total MessageCount : {} ",
                        startTime, endTime, endTime - startTime, messageCount);
            }
        }

        @Override
        public void close() throws Exception {
            super.close();

            if (enableLogger) {
                logger.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>> ConvertingToProtoBufFlatmapFunction close. {}",
                        System.currentTimeMillis());
            }
        }
    }
}
