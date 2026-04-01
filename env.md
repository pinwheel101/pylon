# ============================================================
# 환경 변수 — 아래 값들을 채워넣는다
# ============================================================

# -- CNPG (PostgreSQL) --
CNPG_NAMESPACE=                  # CNPG 클러스터가 있는 namespace
CNPG_HOST=                       # CNPG 서비스 호스트 (예: cnpg-cluster-rw.cnpg.svc)
CNPG_PORT=5432                   # PostgreSQL 포트
CNPG_SUPERUSER=                  # CNPG superuser (보통 postgres)
CNPG_SUPERUSER_SECRET=           # superuser 비밀번호가 들어있는 K8s Secret 이름

# -- Polaris --
POLARIS_NAMESPACE=polaris        # Polaris를 설치할 namespace
POLARIS_DB_NAME=polaris          # Polaris용 데이터베이스 이름
POLARIS_DB_USER=polaris          # Polaris용 데이터베이스 사용자
POLARIS_DB_PASSWORD=             # Polaris DB 사용자 비밀번호 (직접 설정)
POLARIS_REALM=POLARIS            # Polaris realm 이름

# -- MinIO (Iceberg 스토리지) --
MINIO_ENDPOINT=                  # MinIO API 엔드포인트 (예: http://minio.storage.svc:9000)
MINIO_ACCESS_KEY=                # MinIO access key
MINIO_SECRET_KEY=                # MinIO secret key
MINIO_BUCKET=warehouse           # Iceberg 데이터를 저장할 버킷 이름

# -- Flink --
FLINK_NAMESPACE=                 # Flink Operator가 관리하는 namespace
FLINK_IMAGE=                     # Flink Job JAR이 포함된 Docker 이미