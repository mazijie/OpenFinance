#!/bin/bash
set -eu
lateness=${QUOTE_LATENESS_SECONDS:-120}
case "$lateness" in ''|*[!0-9]*) echo 'QUOTE_LATENESS_SECONDS must be a non-negative integer' >&2; exit 2;; esac
if [ "$lateness" -gt 1800 ]; then echo 'QUOTE_LATENESS_SECONDS must not exceed 1800 with the configured state retention' >&2; exit 2; fi
marker=/opt/flink/checkpoints/quotes-v2-submitted
restore=${QUOTE_RESTORE_PATH:-}
if [ -e "$marker" ] && [ -z "$restore" ]; then
    echo 'A quotes-v2 job was already submitted. Restore its checkpoint explicitly; refusing an untracked replay.' >&2
    exit 1
fi
if [ -n "$restore" ]; then
    case "$restore" in file:///opt/flink/checkpoints/*) ;; *) echo 'Invalid restore path' >&2; exit 2;; esac
    case "$restore" in *[!a-zA-Z0-9_./:-]*) echo 'Invalid restore path' >&2; exit 2;; esac
    if [ -f "$marker" ] && grep -Fxq "$restore" "$marker"; then
        echo 'This checkpoint was already used for a submission; refusing another replay.' >&2
        exit 1
    fi
    printf "SET 'execution.state-recovery.path' = '%s';\n" "$restore" > /tmp/quotes-job.sql
else
    : > /tmp/quotes-job.sql
fi
sed "s/__LATENESS_SECONDS__/$lateness/g" /opt/flink/job.sql >> /tmp/quotes-job.sql
until curl -fsS http://flink-jobmanager:8081/overview >/dev/null; do sleep 3; done
if [ -n "$restore" ]; then printf '%s\n' "$restore" >> "$marker"; else printf '%s\n' initial >> "$marker"; fi
exec /opt/flink/bin/sql-client.sh -f /tmp/quotes-job.sql
