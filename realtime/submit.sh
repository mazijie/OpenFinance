#!/bin/bash
set -eu
lateness=${QUOTE_LATENESS_SECONDS:-120}
case "$lateness" in ''|*[!0-9]*) echo 'QUOTE_LATENESS_SECONDS must be a non-negative integer' >&2; exit 2;; esac
if [ "$lateness" -gt 1800 ]; then echo 'QUOTE_LATENESS_SECONDS must not exceed 1800 with the configured state retention' >&2; exit 2; fi
sed "s/__LATENESS_SECONDS__/$lateness/g" /opt/flink/job.sql > /tmp/quotes-job.sql
until curl -fsS http://flink-jobmanager:8081/overview >/dev/null; do sleep 3; done
exec /opt/flink/bin/sql-client.sh -f /tmp/quotes-job.sql
