-- 1) 어제 날짜 시간별 row count (flat 테이블 = 파라미터 단위라 가장 큼)
SELECT date_trunc('hour', <시간컬럼>) AS hr,
       COUNT(*) AS row_cnt
FROM polaris.a_sum_flat
WHERE <시간컬럼> >= current_date - interval 1 day
  AND <시간컬럼> <  current_date
GROUP BY 1 ORDER BY 1;

-- 2) 현재 파일 분포 (스몰 파일 심각도 = compaction 필요성의 핵심)
--    각 테이블 별로 실행
SELECT
  COUNT(*)                                    AS total_files,
  SUM(file_size_in_bytes)                     AS total_bytes,
  AVG(file_size_in_bytes)                     AS avg_bytes,
  percentile(file_size_in_bytes, 0.5)         AS p50_bytes,
  percentile(file_size_in_bytes, 0.95)        AS p95_bytes,
  SUM(CASE WHEN file_size_in_bytes < 134217728 THEN 1 ELSE 0 END) AS small_file_cnt  -- 128MB 미만
FROM d_sum_flat.files;

SELECT COUNT(*) AS total_files, AVG(file_size_in_bytes) AS avg_bytes,
       SUM(CASE WHEN file_size_in_bytes < 134217728 THEN 1 ELSE 0 END) AS small_file_cnt
FROM d_sum_flat.files;

-- 3) 스냅샷 누적량 (expire_snapshots 부하 가늠)
SELECT COUNT(*) AS snap_cnt,
       MIN(committed_at) AS oldest,
       MAX(committed_at) AS newest
FROM d_sum_flat.snapshots;

-- 4) 매니페스트 수 (rewrite_manifests 필요성)
SELECT COUNT(*) AS manifest_cnt
FROM d_sum_flat.manifests;