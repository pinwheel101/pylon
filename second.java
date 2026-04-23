import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.config.SaslConfigs;

import java.util.Map;
import java.util.Properties;

/**
 * KafkaSourceCheckJob 전용 파라미터 컨테이너.
 
 * <h3>필수</h3>
 * <ul>
 *   <li>--kafka.bootstrap-servers</li>
 *   <li>--kafka.topic</li>
 *   <li>--kafka.group-id</li>
 * </ul>
 *
 * <h3>선택 (SASL 인증용)</h3>
 * <ul>
 *   <li>--kafka.security-protocol (기본: SASL_PLAINTEXT)</li>
 *   <li>--kafka.sasl-mechanism (기본: SCRAM-SHA-512)</li>
 *   <li>--kafka.sasl-jaas-config (security-protocol이 SASL_로 시작하면 필수)</li>
 * </ul>
 *
 * <h3>선택 (진단 튜닝)</h3>
 * <ul>
 *   <li>--check.timeout-seconds (기본: 30) — AdminClient/Flink Job 단계별 타임아웃</li>
 *   <li>--check.skip-flink-smoke (기본: false) — AdminClient 진단만 수행하고 Flink Job은 제출하지 않음</li>
 * </ul>
 */
final class KafkaCheckConfig {

    final String bootstrapServers;
    final String topic;
    final String groupId;
    final String securityProtocol;
    final String saslMechanism;
    final String saslJaasConfig;
    final int timeoutSeconds;
    final boolean skipFlinkSmoke;

    KafkaCheckConfig(Map<String, String> params) {
        this.bootstrapServers = Params.require(params, "kafka.bootstrap-servers");
        this.topic = Params.require(params, "kafka.topic");
        this.groupId = Params.require(params, "kafka.group-id");
        this.securityProtocol = params.getOrDefault("kafka.security-protocol", "SASL_PLAINTEXT");
        this.saslMechanism = params.getOrDefault("kafka.sasl-mechanism", "SCRAM-SHA-512");
        this.saslJaasConfig = params.get("kafka.sasl-jaas-config");
        this.timeoutSeconds = Integer.parseInt(
                params.getOrDefault("check.timeout-seconds", "30"));
        this.skipFlinkSmoke = Boolean.parseBoolean(
                params.getOrDefault("check.skip-flink-smoke", "false"));

        if (isSasl() && (saslJaasConfig == null || saslJaasConfig.isBlank())) {
            throw new IllegalArgumentException(
                    "security-protocol=" + securityProtocol
                            + " 일 때는 --kafka.sasl-jaas-config 가 필수입니다.");
        }
    }

    boolean isSasl() {
        return securityProtocol != null && securityProtocol.startsWith("SASL_");
    }

    /**
     * AdminClient/Producer/Consumer가 공통으로 쓰는 보안 프로퍼티.
     * Flink KafkaSource용 Properties에도 그대로 주입한다.
     */
    Properties securityProperties() {
        Properties p = new Properties();
        p.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        p.setProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, securityProtocol);
        if (isSasl()) {
            p.setProperty(SaslConfigs.SASL_MECHANISM, saslMechanism);
            p.setProperty(SaslConfigs.SASL_JAAS_CONFIG, saslJaasConfig);
        }
        return p;
    }

    /**
     * AdminClient 전용 타임아웃까지 붙인 프로퍼티.
     * 응답 없는 브로커에 무한 대기하지 않도록 request timeout을 timeoutSeconds로 강제한다.
     */
    Properties adminProperties() {
        Properties p = securityProperties();
        int timeoutMs = timeoutSeconds * 1000;
        p.setProperty(CommonClientConfigs.REQUEST_TIMEOUT_MS_CONFIG, String.valueOf(timeoutMs));
        p.setProperty(CommonClientConfigs.DEFAULT_API_TIMEOUT_MS_CONFIG, String.valueOf(timeoutMs));
        p.setProperty(CommonClientConfigs.CLIENT_ID_CONFIG, "kafka-source-check-admin");
        return p;
    }

    /**
     * Flink KafkaSource의 setProperties()용. consumer 기본 속성 + 보안 속성.
     */
    Properties kafkaSourceProperties() {
        Properties p = securityProperties();
        p.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, "kafka-source-check-consumer");
        return p;
    }

    /**
     * JAAS config의 password가 로그에 그대로 남지 않도록 마스킹한 표현.
     * {@code password="xxx"} 형태 이외에는 raw 값을 통째로 "***" 로 바꾼다.
     */
    String maskedJaasConfig() {
        if (saslJaasConfig == null) return "(none)";
        return saslJaasConfig.replaceAll(
                "(?i)(password\\s*=\\s*)\"[^\"]*\"",
                "$1\"***\"");
    }
}