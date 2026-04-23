import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.GroupAuthorizationException;
import org.apache.kafka.common.errors.SaslAuthenticationException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.errors.UnsupportedSaslMechanismException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.errors.TimeoutException;

import java.net.ConnectException;
import java.net.UnknownHostException;
import java.util.concurrent.ExecutionException;

/**
 * Kafka 클라이언트/Flink 소스에서 올라오는 예외를 "실패 유형 + 원인 + 다음 조치"로 바꿔주는 헬퍼.
 *
 * <p>동일한 인증 실패라도 표면 예외는 {@link ExecutionException} → {@link SaslAuthenticationException}
 * 형태로 여러 겹에 감싸져 있어, 로그에 스택만 찍으면 "왜 실패했는지" 결론이 묻힌다.
 * 이 클래스는 원인 체인을 끝까지 풀어 가장 구체적인 타입을 찾은 뒤, 운영자가 바로
 * 다음 조치를 할 수 있도록 분류된 메시지를 돌려준다.
 */
final class CheckDiagnostics {

    /** 진단 결과. */
    static final class Diagnosis {
        final String category;      // "AUTH", "AUTHZ", "CONFIG", "NETWORK", "TIMEOUT", "UNKNOWN"
        final String reason;        // 사람이 읽을 한국어 원인
        final String nextAction;    // 다음 조치 힌트
        final Throwable root;       // 가장 구체적인 원인 예외

        Diagnosis(String category, String reason, String nextAction, Throwable root) {
            this.category = category;
            this.reason = reason;
            this.nextAction = nextAction;
            this.root = root;
        }

        @Override
        public String toString() {
            return String.format(
                    "[%s] %s%n  → 조치: %s%n  → 원인 예외: %s: %s",
                    category, reason, nextAction,
                    root.getClass().getName(),
                    root.getMessage() == null ? "(no message)" : root.getMessage());
        }
    }

    private CheckDiagnostics() {}

    /**
     * 예외 체인의 가장 구체적인 원인을 타입별로 분류한다.
     * 분류되지 않으면 category=UNKNOWN 으로 원본 메시지를 그대로 노출한다.
     */
    static Diagnosis diagnose(Throwable t) {
        Throwable root = unwrap(t);

        // ── 인증 (자격증명 불일치) ────────────────────────────────────────
        if (root instanceof SaslAuthenticationException) {
            return new Diagnosis(
                    "AUTH",
                    "SASL 인증 실패 — username/password 불일치, 또는 계정 비활성/삭제 가능성.",
                    "1) --kafka.sasl-jaas-config 의 username/password 재확인  "
                            + "2) Confluent에서 해당 user의 SCRAM credential 등록 여부 확인 "
                            + "('confluent kafka acl list' / Confluent CP의 scram-credentials)  "
                            + "3) JAAS config에 ScramLoginModule 클래스를 썼는지 확인 "
                            + "(org.apache.kafka.common.security.scram.ScramLoginModule)",
                    root);
        }
        if (root instanceof UnsupportedSaslMechanismException
                || containsMessage(root, "Unexpected Kafka request", "Unsupported SASL mechanism")) {
            return new Diagnosis(
                    "CONFIG",
                    "SASL mechanism 불일치 — 브로커가 지원하지 않는 mechanism을 클라이언트가 보냄.",
                    "1) --kafka.sasl-mechanism 값이 브로커 설정(sasl.enabled.mechanisms)과 일치하는지 확인  "
                            + "2) SCRAM-SHA-512 / SCRAM-SHA-256 중 브로커가 허용하는 것으로 맞추기",
                    root);
        }
        // Handshake 단계 실패 — security.protocol 자체가 틀린 경우가 흔함
        if (containsMessage(root,
                "Failed authentication",
                "authentication failed",
                "SaslAuthenticationException",
                "Authentication failed during authentication")) {
            return new Diagnosis(
                    "AUTH",
                    "SASL 핸드셰이크 또는 자격증명 검증 실패.",
                    "1) --kafka.security-protocol 이 SASL_PLAINTEXT / SASL_SSL 중 브로커 리스너와 맞는지 확인  "
                            + "2) JAAS config 끝 세미콜론(;) 누락 여부  "
                            + "3) username/password 이스케이프 확인 (쌍따옴표, 특수문자)",
                    root);
        }

        // ── 인가 (ACL) ──────────────────────────────────────────────────
        if (root instanceof TopicAuthorizationException) {
            return new Diagnosis(
                    "AUTHZ",
                    "토픽 READ/DESCRIBE 권한 없음 — user 자체는 인증되지만 ACL 미비.",
                    "Confluent에서 해당 user에게 READ/DESCRIBE 권한 부여: "
                            + "kafka-acls --add --allow-principal User:<name> "
                            + "--operation Read --operation Describe --topic <topic>",
                    root);
        }
        if (root instanceof GroupAuthorizationException) {
            return new Diagnosis(
                    "AUTHZ",
                    "Consumer group 권한 없음 — group-id 에 대한 READ/DESCRIBE ACL 미비.",
                    "kafka-acls --add --allow-principal User:<name> "
                            + "--operation Read --operation Describe --group <group-id>",
                    root);
        }
        if (root instanceof AuthorizationException) {
            return new Diagnosis(
                    "AUTHZ",
                    "인증은 성공했지만 클러스터/리소스 ACL이 부족합니다: " + root.getClass().getSimpleName(),
                    "Confluent 관리자에게 해당 principal의 ACL 점검 요청",
                    root);
        }

        // ── 잘못된 토픽 이름 ──────────────────────────────────────────────
        if (root instanceof UnknownTopicOrPartitionException) {
            return new Diagnosis(
                    "CONFIG",
                    "토픽이 클러스터에 존재하지 않음.",
                    "1) --kafka.topic 오타 확인  "
                            + "2) Confluent에서 토픽 생성 여부 확인 "
                            + "('confluent kafka topic list' 또는 kafka-topics --list)",
                    root);
        }

        // ── 네트워크 ─────────────────────────────────────────────────────
        if (root instanceof UnknownHostException) {
            return new Diagnosis(
                    "NETWORK",
                    "bootstrap server 호스트명 DNS 해석 실패.",
                    "1) --kafka.bootstrap-servers 의 호스트명 오타 확인  "
                            + "2) Flink Pod 내 DNS 설정(coredns) 및 Service/ExternalName 확인",
                    root);
        }
        if (root instanceof ConnectException) {
            return new Diagnosis(
                    "NETWORK",
                    "TCP 연결 거부됨 — 브로커가 해당 포트에서 listen하지 않거나 방화벽/NetworkPolicy 차단.",
                    "1) bootstrap server host:port 정확한지 확인  "
                            + "2) 클러스터 내부에서 `nc -zv <host> <port>` 로 연결 테스트  "
                            + "3) NetworkPolicy / Kafka listener(advertised.listeners) 설정 확인",
                    root);
        }

        // ── 타임아웃 (인증 전에 응답이 안 옴 = 주로 네트워크) ──────────────
        if (root instanceof TimeoutException) {
            return new Diagnosis(
                    "TIMEOUT",
                    "브로커 응답 타임아웃 — 네트워크 경로 문제 또는 브로커 과부하. "
                            + "SASL_PLAINTEXT에서 인증이 아예 시작되지 않은 경우에도 이 예외로 나타난다.",
                    "1) `nc -zv <host> <port>` 로 L4 도달 확인  "
                            + "2) --check.timeout-seconds 값을 늘려 재시도  "
                            + "3) advertised.listeners 가 클라이언트가 접근 가능한 주소인지 확인",
                    root);
        }

        // ── 최상위 인증 추상 타입 (구체 타입이 매치되지 않았을 때 fallback) ──
        if (root instanceof AuthenticationException) {
            return new Diagnosis(
                    "AUTH",
                    "인증 단계에서 실패: " + root.getClass().getSimpleName(),
                    "JAAS config, mechanism, security protocol 조합 재확인",
                    root);
        }

        return new Diagnosis(
                "UNKNOWN",
                "분류되지 않은 실패 — 스택트레이스를 확인하세요.",
                "로그의 원인 예외 타입/메시지를 운영 담당자와 공유",
                root);
    }

    /**
     * 예외 체인을 끝까지 풀되, 진단에 의미 있는 Kafka 도메인 예외를 만나면 거기서 멈춘다.
     * ExecutionException/CompletionException 같은 wrapper에 가려져 진짜 원인 타입이
     * instanceof 체크로 잡히지 않는 문제를 해결하는 것이 목적.
     * self-cause 순환을 대비해 체인 깊이를 16으로 제한.
     */
    static Throwable unwrap(Throwable t) {
        Throwable current = t;
        int depth = 0;
        while (current != null && depth++ < 16) {
            if (isKafkaDomainException(current)) return current;
            Throwable cause = current.getCause();
            if (cause == null || cause == current) return current;
            current = cause;
        }
        return t;
    }

    private static boolean isKafkaDomainException(Throwable t) {
        return t instanceof AuthenticationException
                || t instanceof AuthorizationException
                || t instanceof UnsupportedSaslMechanismException
                || t instanceof UnknownTopicOrPartitionException
                || t instanceof TimeoutException
                || t instanceof UnknownHostException
                || t instanceof ConnectException;
    }

    private static boolean containsMessage(Throwable t, String... needles) {
        String msg = t.getMessage();
        if (msg == null) return false;
        for (String n : needles) {
            if (msg.contains(n)) return true;
        }
        return false;
    }
}