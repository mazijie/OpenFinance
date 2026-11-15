#!/bin/sh
set -eu
: "${STARROCKS_S3_SECRET_KEY:?Set STARROCKS_S3_SECRET_KEY in .env}"
case "$STARROCKS_WAREHOUSE_BUCKET" in *[!a-z0-9.-]*|'') echo 'Invalid warehouse bucket' >&2; exit 1;; esac
mc alias set srinit http://minio:9000 "$STARROCKS_MINIO_ADMIN_USER" "$STARROCKS_MINIO_ADMIN_PASSWORD" >/dev/null
cat > /tmp/starrocks-readonly-policy.json <<EOF
{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["s3:GetBucketLocation","s3:ListBucket"],"Resource":["arn:aws:s3:::$STARROCKS_WAREHOUSE_BUCKET"]},{"Effect":"Allow","Action":["s3:GetObject","s3:GetObjectVersion"],"Resource":["arn:aws:s3:::$STARROCKS_WAREHOUSE_BUCKET/*"]}]}
EOF
mc admin policy create srinit openfinance_starrocks_readonly /tmp/starrocks-readonly-policy.json >/dev/null
mc admin user add srinit openfinance_sr_ro "$STARROCKS_S3_SECRET_KEY" >/dev/null
mc admin policy attach srinit openfinance_starrocks_readonly --user openfinance_sr_ro >/dev/null
echo 'StarRocks MinIO read-only account configured.'
