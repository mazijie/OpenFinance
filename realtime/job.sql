SET 'execution.target' = 'remote';
SET 'rest.address' = 'flink-jobmanager';
SET 'execution.checkpointing.interval' = '30 s';
SET 'table.local-time-zone' = 'UTC';
SET 'execution.checkpointing.storage' = 'filesystem';
SET 'execution.checkpointing.dir' = 'file:///opt/flink/checkpoints';
SET 'execution.checkpointing.externalized-checkpoint-retention' = 'RETAIN_ON_CANCELLATION';
SET 'table.exec.state.ttl' = '25 h';
SET 'table.exec.source.idle-timeout' = '1 min';
CREATE TABLE quotes_raw (
  event_id STRING, symbol STRING, name STRING, price DOUBLE, source STRING, source_time STRING,
  captured_at STRING, previous_close DOUBLE, `status` STRING, round_id STRING,
  expected INT, failed_batches INT, proc_time AS PROCTIME(),
  event_time AS TO_TIMESTAMP(REPLACE(SUBSTRING(captured_at, 1, 19), 'T', ' '), 'yyyy-MM-dd HH:mm:ss'),
  WATERMARK FOR event_time AS event_time - INTERVAL '1' SECOND * __LATENESS_SECONDS__
) WITH (
  'connector' = 'kafka', 'topic' = 'quotes.raw',
  'properties.bootstrap.servers' = 'kafka:9092', 'properties.group.id' = 'flink-quotes-v2',
  'scan.startup.mode' = 'latest-offset', 'format' = 'json'
);
CREATE TABLE quotes_latest (
  symbol STRING, name STRING, price DOUBLE, source STRING, source_time STRING, captured_at STRING,
  previous_close DOUBLE, `status` STRING, round_id STRING
) WITH (
  'connector' = 'kafka', 'topic' = 'quotes.latest',
  'properties.bootstrap.servers' = 'kafka:9092', 'format' = 'json',
  'sink.delivery-guarantee' = 'exactly-once', 'sink.transactional-id-prefix' = 'openfinance-quotes-latest-v2',
  'properties.transaction.timeout.ms' = '600000'
);
CREATE TABLE quotes_windows (
  symbol STRING, window_start TIMESTAMP(3), window_end TIMESTAMP(3),
  sample_min DOUBLE, sample_max DOUBLE, sample_avg DOUBLE, sample_count BIGINT, window_basis STRING
) WITH (
  'connector' = 'kafka', 'topic' = 'quotes.windows',
  'properties.bootstrap.servers' = 'kafka:9092', 'format' = 'json',
  'sink.delivery-guarantee' = 'exactly-once', 'sink.transactional-id-prefix' = 'openfinance-quotes-windows-v2',
  'properties.transaction.timeout.ms' = '600000'
);
CREATE TABLE quotes_market (
  round_id STRING, expected BIGINT, received BIGINT, valid BIGINT,
  rising BIGINT, falling BIGINT, flat BIGINT, failed_batches INT,
  PRIMARY KEY (round_id) NOT ENFORCED
) WITH (
  'connector' = 'upsert-kafka', 'topic' = 'quotes.market',
  'properties.bootstrap.servers' = 'kafka:9092',
  'key.format' = 'json', 'value.format' = 'json',
  'sink.delivery-guarantee' = 'exactly-once', 'sink.transactional-id-prefix' = 'openfinance-quotes-market-v2',
  'properties.transaction.timeout.ms' = '600000'
);
CREATE TABLE quotes_late (
  symbol STRING, round_id STRING, captured_at STRING, price DOUBLE, `status` STRING,
  watermark_time TIMESTAMP(3)
) WITH (
  'connector' = 'kafka', 'topic' = 'quotes.late',
  'properties.bootstrap.servers' = 'kafka:9092', 'format' = 'json',
  'sink.delivery-guarantee' = 'exactly-once', 'sink.transactional-id-prefix' = 'openfinance-quotes-late-v2',
  'properties.transaction.timeout.ms' = '600000'
);
CREATE TABLE quotes_expired (
  event_id STRING, captured_at STRING
) WITH (
  'connector' = 'kafka', 'topic' = 'quotes.expired',
  'properties.bootstrap.servers' = 'kafka:9092', 'format' = 'json',
  'sink.delivery-guarantee' = 'exactly-once', 'sink.transactional-id-prefix' = 'openfinance-quotes-expired-v2',
  'properties.transaction.timeout.ms' = '600000'
);
CREATE VIEW unique_quotes AS
SELECT symbol, name, price, source, source_time, captured_at, previous_close, `status`, round_id, expected, failed_batches, proc_time, event_time
FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY proc_time ASC) AS rownum
      FROM quotes_raw WHERE event_id IS NOT NULL
        AND event_time > LOCALTIMESTAMP - INTERVAL '24' HOUR) WHERE rownum = 1;
CREATE VIEW ontime_quotes AS
SELECT * FROM unique_quotes
WHERE CURRENT_WATERMARK(event_time) IS NULL OR event_time > CURRENT_WATERMARK(event_time);
EXECUTE STATEMENT SET
BEGIN
  INSERT INTO quotes_expired
    SELECT event_id, captured_at FROM quotes_raw WHERE event_id IS NOT NULL
      AND event_time <= LOCALTIMESTAMP - INTERVAL '24' HOUR;
  INSERT INTO quotes_latest SELECT symbol, name, price, source, source_time, captured_at, previous_close, `status`, round_id
    FROM unique_quotes;
  INSERT INTO quotes_windows
    SELECT symbol, window_start, window_end, MIN(price), MAX(price), AVG(price), COUNT(*), 'capture_time'
    FROM TABLE(TUMBLE(TABLE ontime_quotes, DESCRIPTOR(event_time), INTERVAL '1' MINUTE)) WHERE `status` = 'valid'
    GROUP BY symbol, window_start, window_end;
  INSERT INTO quotes_late
    SELECT symbol, round_id, captured_at, price, `status`, CURRENT_WATERMARK(event_time)
    FROM unique_quotes WHERE `status` = 'valid'
      AND event_time <= CURRENT_WATERMARK(event_time);
  INSERT INTO quotes_market
    SELECT round_id, CAST(MAX(expected) AS BIGINT), COUNT(*),
      SUM(CASE WHEN `status` = 'valid' THEN CAST(1 AS BIGINT) ELSE CAST(0 AS BIGINT) END),
      SUM(CASE WHEN `status` = 'valid' AND price > previous_close THEN CAST(1 AS BIGINT) ELSE CAST(0 AS BIGINT) END),
      SUM(CASE WHEN `status` = 'valid' AND price < previous_close THEN CAST(1 AS BIGINT) ELSE CAST(0 AS BIGINT) END),
      SUM(CASE WHEN `status` = 'valid' AND price = previous_close THEN CAST(1 AS BIGINT) ELSE CAST(0 AS BIGINT) END),
      MAX(failed_batches)
    FROM unique_quotes GROUP BY round_id;
END;
