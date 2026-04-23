import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.ConsumerGroupState;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Kafka 소스 연결/인증만 검증하는 경량 Flink Job.
 *
 * <h3>실행 단계</h3>
 * <ol>
 *   <li><b>DNS + TCP</b> — bootstrap server 한 개씩 호스트 해석 + 포트 접속</li>
 *   <li><b>describeCluster</b> — SASL 핸드셰이크 + 자격증명 검증</li>
 *   <li><b>describeTopic</b> — 토픽 존재 + READ/DESCRIBE 권한 검증</li>
 *   <li><b>listConsumerGroupOffsets</b> — group-id 권한 검증</li>
 *   <li><b>Flink KafkaSource 스모크</b> — bounded(latest→latest)로 실제 커넥터 경로까지 실행
 *       (skip 하려면 --check.skip-flink-smoke true)</li>
 * </ol>
 *
 * <p>각 단계에서 실패하면 {@link CheckDiagnostics}가 예외를 분류해
 * "원인 + 다음 조치"를 로그로 남기고 비정상 종료한다 (exit 1).
 * 단계가 독립적이므로 로그만으로 어느 단계에서 깨졌는지 바로 보인다.
 *
 * <h3>CLI 파라미터</h3>
 * <pre>
 * --kafka.bootstrap-servers   host1:9092,host2:9092
 * --kafka.topic               summary
 * --kafka.group-id            flink-source-check
 * --kafka.security-protocol   SASL_PLAINTEXT      (기본값)
 * --kafka.sasl-mechanism      SCRAM-SHA-512       (기본값)
 * --kafka.sasl-jaas-config    'org.apache.kafka.common.security.scram.ScramLoginModule
 *                              required username="..." password="...";'
 * --check.timeout-seconds     30                  (기본값)
 * --check.skip-flink-smoke    false               (기본값)
 * </pre>
 */
public class KafkaSourceCheckJob {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaSourceCheckJob.class);

    public static void main(String[] args) {
        KafkaCheckConfig config;
        try {
            config = new KafkaCheckConfig(Params.parseArgs(args));
        } catch (IllegalArgumentException e) {
            LOG.error("설정 오류: {}", e.getMessage());
            System.exit(2);
            return;
        }

        logConfig(config);

        boolean ok = true;
        ok &= runStep("[1/5] DNS + TCP",
                () -> checkDnsAndTcp(config));
        // AdminClient 단계들은 한 클라이언트로 묶어서 인증 비용을 중복시키지 않는다
        ok &= runAdminSteps(config);
        if (ok && !config.skipFlinkSmoke) {
            ok &= runStep("[5/5] Flink KafkaSource smoke",
                    () -> runFlinkSmoke(config));
        } else if (config.skipFlinkSmoke) {
            LOG.info("[5/5] Flink KafkaSource smoke — SKIPPED (--check.skip-flink-smoke=true)");
        }

        if (!ok) {
            LOG.error("Kafka 소스 연결 검증 실패 — 위 로그의 [원인 예외] / [조치] 참고.");
            System.exit(1);
        }
        LOG.info("✓ 모든 검증 통과 — Flink KafkaSource 로 연결/인증/권한 모두 정상.");
    }

    // ── Step 1: DNS + TCP ────────────────────────────────────────────────

    /**
     * bootstrap server 문자열을 파싱해 각 host:port 에 대해:
     * (a) {@link InetAddress#getAllByName} 로 DNS 해석
     * (b) {@link Socket#connect} 로 L4 연결
     * 을 시도한다.
     *
     * <p>Kafka 클라이언트는 bootstrap 실패 시 {@link TimeoutException}으로만 나타나기 쉽다.
     * 인증 이전 단계(네트워크/DNS)를 먼저 분리해두면, 이후 AdminClient 타임아웃이
     * "네트워크가 문제는 아니다" 라는 보장 아래에서 해석된다.
     */
    private static void checkDnsAndTcp(KafkaCheckConfig config) throws Exception {
        int connectTimeoutMs = Math.max(3000, config.timeoutSeconds * 1000 / 3);
        for (String server : config.bootstrapServers.split(",")) {
            String hostPort = server.trim();
            int colon = hostPort.lastIndexOf(':');
            if (colon < 0) {
                throw new IllegalArgumentException(
                        "bootstrap server는 host:port 형식이어야 합니다: " + hostPort);
            }
            String host = hostPort.substring(0, colon);
            int port = Integer.parseInt(hostPort.substring(colon + 1));

            InetAddress[] resolved;
            try {
                resolved = InetAddress.getAllByName(host);
            } catch (java.net.UnknownHostException e) {
                logDiagnosis("DNS/TCP", CheckDiagnostics.diagnose(e));
                throw e;
            }
            LOG.info("  DNS OK: {} → {}", host, describeAddresses(resolved));

            try (Socket socket = new Socket()) {
                socket.connect(new InetSocketAddress(resolved[0], port), connectTimeoutMs);
                LOG.info("  TCP OK: {}:{} (connectTimeout={}ms)", host, port, connectTimeoutMs);
            } catch (Exception e) {
                logDiagnosis("DNS/TCP", CheckDiagnostics.diagnose(e));
                throw e;
            }
        }
    }

    // ── Steps 2-4: AdminClient ──────────────────────────────────────────

    /**
     * 세 개의 AdminClient 호출을 순서대로 돌린다.
     * 한 단계라도 실패하면 그 단계까지의 실패를 분류해 로그로 남기고 false 를 돌려준다.
     * AdminClient는 단계 결과와 무관하게 try-with-resources 로 반드시 닫는다.
     */
    private static boolean runAdminSteps(KafkaCheckConfig config) {
        try (AdminClient admin = AdminClient.create(config.adminProperties())) {
            boolean ok = true;
            ok &= runStep("[2/5] describeCluster (SASL 인증)",
                    () -> describeCluster(admin, config.timeoutSeconds));
            if (!ok) return false;
            ok &= runStep("[3/5] describeTopic (토픽/ACL)",
                    () -> describeTopic(admin, config.topic, config.timeoutSeconds));
            if (!ok) return false;
            ok &= runStep("[4/5] listConsumerGroupOffsets (group ACL)",
                    () -> describeGroup(admin, config.groupId, config.timeoutSeconds));
            return ok;
        } catch (Exception e) {
            logDiagnosis("AdminClient bootstrap", CheckDiagnostics.diagnose(e));
            return false;
        }
    }

    private static void describeCluster(AdminClient admin, int timeoutSeconds) throws Exception {
        DescribeClusterResult r = admin.describeCluster();
        String clusterId = await(r.clusterId(), timeoutSeconds);
        Node controller = await(r.controller(), timeoutSeconds);
        Iterable<Node> nodes = await(r.nodes(), timeoutSeconds);
        int nodeCount = 0;
        for (Node n : nodes) nodeCount++;
        LOG.info("  cluster-id={}, controller={}:{}, nodes={}",
                clusterId,
                controller == null ? "?" : controller.host(),
                controller == null ? "?" : controller.port(),
                nodeCount);
    }

    private static void describeTopic(AdminClient admin, String topic, int timeoutSeconds) throws Exception {
        Map<String, TopicDescription> map = await(
                admin.describeTopics(Collections.singletonList(topic)).allTopicNames(),
                timeoutSeconds);
        TopicDescription td = map.get(topic);
        if (td == null) {
            throw new IllegalStateException("describeTopics 응답에 '" + topic + "' 없음");
        }
        LOG.info("  topic='{}' partitions={} internal={}",
                td.name(), td.partitions().size(), td.isInternal());
    }

    private static void describeGroup(AdminClient admin, String groupId, int timeoutSeconds) throws Exception {
        // listConsumerGroupOffsets: group이 아직 아무것도 커밋하지 않았어도(=신규 group-id)
        // 빈 맵을 반환하며 성공한다. 즉 "존재 여부"가 아니라 "ACL 검증"이 목적.
        Map<String, ListConsumerGroupOffsetsSpec> spec =
                Collections.singletonMap(groupId, new ListConsumerGroupOffsetsSpec());
        Map<org.apache.kafka.common.TopicPartition, org.apache.kafka.clients.consumer.OffsetAndMetadata> offsets =
                await(admin.listConsumerGroupOffsets(spec)
                        .partitionsToOffsetAndMetadata(groupId), timeoutSeconds);

        // describeConsumerGroups는 존재 여부도 같이 알려준다 (참고용, 실패해도 전체 단계는 이미 통과)
        ConsumerGroupState state = ConsumerGroupState.UNKNOWN;
        try {
            state = await(admin.describeConsumerGroups(Collections.singletonList(groupId))
                    .describedGroups().get(groupId), timeoutSeconds).state();
        } catch (Exception ignore) {
            // describeConsumerGroups 권한이 없어도 소스 자체는 돌 수 있으므로 무시
        }
        LOG.info("  group-id='{}' state={} committed-partitions={}",
                groupId, state, offsets.size());
    }

    /** {@link KafkaFuture#get(long, TimeUnit)} 를 타임아웃과 함께 호출하는 얇은 래퍼. */
    private static <T> T await(KafkaFuture<T> future, int timeoutSeconds)
            throws InterruptedException, ExecutionException, TimeoutException {
        return future.get(timeoutSeconds, TimeUnit.SECONDS);
    }

    // ── Step 5: Flink KafkaSource smoke ─────────────────────────────────

    /**
     * Flink 런타임에서 KafkaSource 경로가 실제로 돌아가는지 최소 비용으로 확인한다.
     *
     * <p>starting=latest, bounded=latest 로 설정하면:
     * <ul>
     *   <li>SplitEnumerator 가 브로커에서 메타데이터를 받아야 시작 오프셋을 계산한다
     *       → 인증/인가가 여기서 한 번 더 검증된다</li>
     *   <li>시작 오프셋 == 종료 오프셋 이므로 reader는 0 레코드를 읽고 즉시 종료한다
     *       → 토픽 데이터량과 무관하게 빠르게 끝난다</li>
     * </ul>
     *
     * <p>즉 "메시지가 들어오는지"가 아니라 "Flink 커넥터가 브로커에 붙는지"를 검증한다.
     * 실제 메시지 흐름까지 보려면 이 Job이 아니라 본 파이프라인에서 확인한다.
     */
    private static void runFlinkSmoke(KafkaCheckConfig config) throws Exception {
        KafkaSource<byte[]> source = KafkaSource.<byte[]>builder()
                .setBootstrapServers(config.bootstrapServers)
                .setTopics(config.topic)
                .setGroupId(config.groupId)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setBounded(OffsetsInitializer.latest())
                .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(new RawBytesDeserializer()))
                .setProperties(config.kafkaSourceProperties())
                .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<byte[]> stream = env.fromSource(
                        source, WatermarkStrategy.noWatermarks(), "Kafka Source Check")
                .setParallelism(1);
        stream.sinkTo(new DiscardingSink<>()).setParallelism(1);

        JobClient client = env.executeAsync("kafka-source-check");
        try {
            client.getJobExecutionResult().get(config.timeoutSeconds, TimeUnit.SECONDS);
            LOG.info("  Flink Job 완료 — KafkaSource 가 bounded latest→latest 로 정상 종료.");
        } catch (TimeoutException e) {
            // 타임아웃은 거의 항상 "메타데이터 응답 지연" = 네트워크/인증 지연
            safeCancel(client);
            throw e;
        }
    }

    private static void safeCancel(JobClient client) {
        try {
            client.cancel().get(30, TimeUnit.SECONDS);
        } catch (Exception e) {
            LOG.warn("  JobClient.cancel() 실패: {}", e.toString());
        }
    }

    // ── 공통 유틸 ────────────────────────────────────────────────────────

    @FunctionalInterface
    private interface CheckedStep {
        void run() throws Exception;
    }

    /**
     * 단계 하나를 실행하고 결과를 로그로 남긴다.
     * 예외는 삼키지 않고 {@link CheckDiagnostics} 로 분류해 원인/조치를 먼저 찍는다.
     *
     * @return 성공하면 true, 실패하면 false
     */
    private static boolean runStep(String label, CheckedStep step) {
        LOG.info("{} 시작", label);
        long t0 = System.nanoTime();
        try {
            step.run();
            long ms = (System.nanoTime() - t0) / 1_000_000;
            LOG.info("{} OK ({} ms)", label, ms);
            return true;
        } catch (Exception e) {
            long ms = (System.nanoTime() - t0) / 1_000_000;
            LOG.error("{} FAIL ({} ms)", label, ms);
            logDiagnosis(label, CheckDiagnostics.diagnose(e));
            return false;
        }
    }

    private static void logDiagnosis(String label, Diagnosis d) {
        LOG.error("▼ {} 진단", label);
        LOG.error("  분류: {}", d.category);
        LOG.error("  원인: {}", d.reason);
        LOG.error("  조치: {}", d.nextAction);
        LOG.error("  원인 예외: {}: {}",
                d.root.getClass().getName(),
                d.root.getMessage() == null ? "(no message)" : d.root.getMessage());
        LOG.debug("  stack:", d.root);
    }

    private static void logConfig(KafkaCheckConfig c) {
        LOG.info("=== Kafka Source Check 설정 ===");
        LOG.info("  bootstrap.servers = {}", c.bootstrapServers);
        LOG.info("  topic             = {}", c.topic);
        LOG.info("  group.id          = {}", c.groupId);
        LOG.info("  security.protocol = {}", c.securityProtocol);
        LOG.info("  sasl.mechanism    = {}", c.saslMechanism);
        LOG.info("  sasl.jaas.config  = {}", c.maskedJaasConfig());
        LOG.info("  timeout           = {} s", c.timeoutSeconds);
        LOG.info("  skip-flink-smoke  = {}", c.skipFlinkSmoke);
    }

    private static String describeAddresses(InetAddress[] addresses) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < addresses.length; i++) {
            if (i > 0) sb.append(',');
            sb.append(addresses[i].getHostAddress());
        }
        return sb.toString();
    }
}