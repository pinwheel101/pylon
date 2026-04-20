
/**
 * Iceberg 테이블 유지보수 배치 잡 — Flink 네이티브 구현.

 * <h3>필수 파라미터</h3>
 * <pre>
 * --iceberg.catalog           Iceberg catalog 이름
 * --iceberg.catalog-type      rest | hive
 * --iceberg.uri               catalog URI
 * --iceberg.warehouse         warehouse 이름
 * --maintenance.tables        대상 테이블 (쉼표 구분 가능)
 * </pre>
 *
 * <h3>선택 파라미터 (기본값)</h3>
 * <pre>
 * --maintenance.snapshot-max-age-hours   72
 * --maintenance.snapshot-retain-last     100
 * --maintenance.target-file-size-bytes   536870912   (512 MB)
 * --maintenance.min-file-size-bytes      134217728   (128 MB, 현재 Flink 모드에서 미사용)
 * </pre>
 *
 * <h3>실행</h3>
 * <p>Flink Application 모드로 실행하며 entryClass를 이 클래스로 지정한다.
 * K8s CronJob으로 1–4시간 주기 호출을 권장한다.
 */
public class IcebergFlinkMaintenanceJob {

    private static final Logger LOG = LoggerFactory.getLogger(IcebergFlinkMaintenanceJob.class);

    private static final long DEFAULT_TARGET_FILE_SIZE = 536_870_912L;
    private static final long DEFAULT_MIN_FILE_SIZE = 134_217_728L;
    private static final int DEFAULT_SNAPSHOT_MAX_AGE_HOURS = 72;
    private static final int DEFAULT_SNAPSHOT_RETAIN_LAST = 100;

    public static void main(String[] args) throws Exception {
        Map<String, String> params = Params.parseArgs(args);

        String catalogName = Params.require(params, "iceberg.catalog");
        String catalogType = Params.require(params, "iceberg.catalog-type");
        String uri = Params.require(params, "iceberg.uri");
        String warehouse = Params.require(params, "iceberg.warehouse");
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

        LOG.info("Maintenance params: target={}B, min(ignored in Flink mode)={}B, "
                        + "snapshotMaxAgeHours={}, snapshotRetainLast={}",
                targetFileSize, minFileSize, snapshotMaxAgeHours, snapshotRetainLast);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);

        Catalog catalog = buildCatalog(catalogName, catalogType, uri, warehouse).loadCatalog();

        boolean hasFailure = false;
        for (String rawTable : tables.split(",")) {
            String tableName = rawTable.trim();
            if (tableName.isEmpty()) continue;

            TableIdentifier id = TableIdentifier.parse(tableName);
            LOG.info("=== Maintenance start: `{}`.{} ===", catalogName, id);

            Table table;
            try {
                table = catalog.loadTable(id);
            } catch (Exception e) {
                LOG.error("Failed to load table `{}`.{}: {}", catalogName, id, e.getMessage(), e);
                hasFailure = true;
                continue;
            }

            hasFailure |= runStep("set-properties",
                    () -> setTableProperties(table));
            hasFailure |= runStep("rewrite-data-files",
                    () -> rewriteDataFiles(env, table, targetFileSize));
            hasFailure |= runStep("expire-snapshots",
                    () -> expireSnapshots(table, snapshotMaxAgeHours, snapshotRetainLast));
            hasFailure |= runStep("rewrite-manifests",
                    () -> rewriteManifests(table));

            LOG.info("=== Maintenance end: `{}`.{} ===", catalogName, id);
        }

        if (hasFailure) {
            LOG.error("Some maintenance steps failed — check logs above for details.");
            System.exit(1);
        }
        LOG.info("All maintenance steps completed successfully.");
    }

    private static boolean runStep(String stepName, ThrowingRunnable step) {
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

    @FunctionalInterface
    private interface ThrowingRunnable {
        void run() throws Exception;
    }

    // ── Catalog ──────────────────────────────────────────────────────────

    private static CatalogLoader buildCatalog(String name, String type,
                                              String uri, String warehouse) {
        Configuration hadoopConf = new Configuration();
        Map<String, String> props = new HashMap<>();
        props.put(CatalogProperties.URI, uri);
        props.put(CatalogProperties.WAREHOUSE_LOCATION, warehouse);

        if ("rest".equalsIgnoreCase(type)) {
            return CatalogLoader.rest(name, hadoopConf, props);
        }
        if ("hive".equalsIgnoreCase(type)) {
            return CatalogLoader.hive(name, hadoopConf, props);
        }
        throw new IllegalArgumentException(
                "Unsupported --iceberg.catalog-type: " + type + " (rest|hive)");
    }

    // ── Table Properties ─────────────────────────────────────────────────

    private static void setTableProperties(Table table) {
        table.updateProperties()
                .set("write.parquet.compression-codec", "zstd")
                .set("write.metadata.delete-after-commit.enabled", "true")
                .set("write.metadata.previous-versions-max", "100")
                .commit();
    }

    // ── Rewrite Data Files (Compaction) ──────────────────────────────────

    private static void rewriteDataFiles(StreamExecutionEnvironment env,
                                         Table table,
                                         long targetFileSize) {
        LOG.info("  binpack: targetSizeInBytes={}B", targetFileSize);
        RewriteDataFilesActionResult result = Actions.forTable(env, table)
                .rewriteDataFiles()
                .targetSizeInBytes(targetFileSize)
                .execute();
        LOG.info("  rewritten: deleted={} file(s) -> added={} file(s)",
                result.deletedDataFiles().size(),
                result.addedDataFiles().size());
    }

    // ── Expire Snapshots ─────────────────────────────────────────────────

    private static void expireSnapshots(Table table, int maxAgeHours, int retainLast) {
        long olderThanMillis = Instant.now()
                .minus(maxAgeHours, ChronoUnit.HOURS)
                .toEpochMilli();
        LOG.info("  olderThanMillis={}, retainLast={}", olderThanMillis, retainLast);
        table.expireSnapshots()
                .expireOlderThan(olderThanMillis)
                .retainLast(retainLast)
                .commit();
    }

    // ── Rewrite Manifests ────────────────────────────────────────────────

    private static void rewriteManifests(Table table) {
        table.rewriteManifests().commit();
    }
}