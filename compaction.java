import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Map;

/**
 * Iceberg 테이블 유지보수 배치 잡.
 *
 * <p>스트리밍 파이프라인()이
 * 체크포인트마다 생성하는 스몰 파일을 주기적으로 compaction하고,
 * 오래된 스냅샷과 메타데이터를 정리한다.
 *
 * <h3>실행 순서</h3>
 * <ol>
 *   <li>테이블 속성 설정 (멱등, 매 실행마다 보장)</li>
 *   <li>{@code rewrite_data_files} — 스몰 파일 binpack compaction</li>
 *   <li>{@code expire_snapshots} — 오래된 스냅샷 제거</li>
 *   <li>{@code rewrite_manifests} — 매니페스트 파일 최적화</li>
 * </ol>
 *
 * <h3>필수 파라미터</h3>
 * <pre>
 * --iceberg.catalog           Iceberg catalog 이름 (예: polaris)
 * --iceberg.catalog-type      rest | hive
 * --iceberg.uri               catalog URI
 * --iceberg.warehouse         warehouse 이름
 * --maintenance.tables        대상 테이블 (쉼표 구분, 예: fdc.fdc_summary,fdc.fdc_summary_flat)
 * </pre>
 *
 * <h3>선택 파라미터 (기본값)</h3>
 * <pre>
 * --maintenance.snapshot-max-age-hours   72      만료 기준 시간
 * --maintenance.snapshot-retain-last     100     최소 보존 스냅샷 수
 * --maintenance.target-file-size-bytes   268435456  binpack 목표 파일 크기 (256 MB)
 * --maintenance.min-file-size-bytes      67108864   이보다 작은 파일만 compaction 대상 (64 MB)
 * </pre>
 *
 * <h3>스케줄링</h3>
 * <p>K8s CronJob 또는 Airflow 등으로 1–4시간 주기 실행을 권장한다.
 * {@code k8s/maintenance-job.yaml} 참고.
 */
public class IcebergMaintenanceJob {

    private static final Logger LOG = LoggerFactory.getLogger(IcebergMaintenanceJob.class);

    private static final DateTimeFormatter TS_FMT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(ZoneOffset.UTC);

    private static final long DEFAULT_TARGET_FILE_SIZE = 268_435_456L; // 256 MB
    private static final long DEFAULT_MIN_FILE_SIZE = 67_108_864L;     // 64 MB
    private static final int DEFAULT_SNAPSHOT_MAX_AGE_HOURS = 72;
    private static final int DEFAULT_SNAPSHOT_RETAIN_LAST = 100;

    public static void main(String[] args) {
        Map<String, String> params = Params.parseArgs(args);

        String catalogName = Params.require(params, "iceberg.catalog");
        String tables = Params.require(params, "maintenance.tables");

        long targetFileSize = Long.parseLong(
                params.getOrDefault("maintenance.target-file-size-bytes",
                        String.valueOf(DEFAULT_TARGET_FILE_SIZE)));
        long minFileSize = Long.parseLong(
                params.getOrDefault("maintenance.min-file-size-bytes",
                        String.valueOf(DEFAULT_MIN_FILE_SIZE)));
        int snapshotMaxAgeHours = Integer.parseInt(
                params.getOrDefault("maintenance.snapshot-max-age-hours",
                        String.valueOf(DEFAULT_SNAPSHOT_MAX_AGE_HOURS)));
        int snapshotRetainLast = Integer.parseInt(
                params.getOrDefault("maintenance.snapshot-retain-last",
                        String.valueOf(DEFAULT_SNAPSHOT_RETAIN_LAST)));

        // --- Environment ---
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .withConfiguration(Configuration.fromMap(env.getConfiguration().toMap()))
                .build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        // --- Catalog ---
        IcebergCatalogRegistrar.register(tableEnv, params);

        // --- 테이블별 유지보수 ---
        boolean hasFailure = false;
        for (String rawTable : tables.split(",")) {
            String table = rawTable.trim();
            LOG.info("=== Maintenance start: `{}`.{} ===", catalogName, table);

            hasFailure |= runStep("set-properties",
                    () -> setTableProperties(tableEnv, catalogName, table));
            hasFailure |= runStep("rewrite-data-files",
                    () -> rewriteDataFiles(tableEnv, catalogName, table,
                            targetFileSize, minFileSize));
            hasFailure |= runStep("expire-snapshots",
                    () -> expireSnapshots(tableEnv, catalogName, table,
                            snapshotMaxAgeHours, snapshotRetainLast));
            hasFailure |= runStep("rewrite-manifests",
                    () -> rewriteManifests(tableEnv, catalogName, table));

            LOG.info("=== Maintenance end: `{}`.{} ===", catalogName, table);
        }

        if (hasFailure) {
            LOG.error("Some maintenance steps failed — check logs above for details.");
            System.exit(1);
        }
        LOG.info("All maintenance steps completed successfully.");
    }

    /**
     * 각 단계를 독립 실행하고, 실패해도 다음 단계로 진행한다.
     *
     * @return 실패 시 {@code true}
     */
    private static boolean runStep(String stepName, Runnable step) {
        try {
            LOG.info("[{}] starting", stepName);
            step.run();
            LOG.info("[{}] completed", stepName);
            return false;
        } catch (Exception e) {
            LOG.error("[{}] failed: {}", stepName, e.getMessage(), e);
            return true;
        }
    }

    // ── Table Properties ────────────────────────────────────────────────

    /**
     * 스트리밍 환경에 적합한 테이블 속성을 설정한다 (멱등).
     * <ul>
     *   <li>{@code write.parquet.compression-codec=zstd} — gzip 대비 압축 해제 2-3배 빠름</li>
     *   <li>{@code write.metadata.delete-after-commit.enabled=true} — 체크포인트마다 누적되는 metadata.json 자동 정리</li>
     *   <li>{@code write.metadata.previous-versions-max=100} — 최근 100개 metadata 파일만 보존</li>
     * </ul>
     */
    private static void setTableProperties(StreamTableEnvironment tableEnv,
                                            String catalogName, String table) {
        tableEnv.executeSql(String.format(
                "ALTER TABLE `%s`.%s SET (" +
                        "'write.parquet.compression-codec' = 'zstd', " +
                        "'write.metadata.delete-after-commit.enabled' = 'true', " +
                        "'write.metadata.previous-versions-max' = '100'" +
                        ")", catalogName, table));
    }

    // ── Rewrite Data Files (Compaction) ─────────────────────────────────

    /**
     * binpack 전략으로 스몰 파일을 합친다.
     * {@code min-file-size-bytes} 미만의 파일만 대상이 되며,
     * {@code target-file-size-bytes} 크기로 병합한다.
     */
    private static void rewriteDataFiles(StreamTableEnvironment tableEnv,
                                          String catalogName, String table,
                                          long targetFileSize, long minFileSize) {
        LOG.info("  binpack: target={}B, min={}B", targetFileSize, minFileSize);
        tableEnv.executeSql(String.format(
                "CALL `%s`.`system`.rewrite_data_files(" +
                        "table => '%s', " +
                        "strategy => 'binpack', " +
                        "options => map(" +
                        "'target-file-size-bytes', '%d', " +
                        "'min-file-size-bytes', '%d'))",
                catalogName, table, targetFileSize, minFileSize));
    }

    // ── Expire Snapshots ────────────────────────────────────────────────

    /**
     * 기준 시각보다 오래된 스냅샷을 제거한다.
     * 최소 {@code retainLast}개는 시각과 무관하게 보존한다.
     */
    private static void expireSnapshots(StreamTableEnvironment tableEnv,
                                         String catalogName, String table,
                                         int maxAgeHours, int retainLast) {
        String olderThan = TS_FMT.format(
                Instant.now().minus(maxAgeHours, ChronoUnit.HOURS));
        LOG.info("  olderThan={}, retainLast={}", olderThan, retainLast);
        tableEnv.executeSql(String.format(
                "CALL `%s`.`system`.expire_snapshots(" +
                        "table => '%s', " +
                        "older_than => TIMESTAMP '%s', " +
                        "retain_last => %d)",
                catalogName, table, olderThan, retainLast));
    }

    // ── Rewrite Manifests ───────────────────────────────────────────────

    /**
     * 매니페스트 파일을 병합하여 scan planning 속도를 개선한다.
     */
    private static void rewriteManifests(StreamTableEnvironment tableEnv,
                                          String catalogName, String table) {
        tableEnv.executeSql(String.format(
                "CALL `%s`.`system`.rewrite_manifests(table => '%s')",
                catalogName, table));
    }
}