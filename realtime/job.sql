SET 'execution.target' = 'remote';
SET 'rest.address' = 'flink-jobmanager';
SET 'execution.checkpointing.interval' = '30 s';
SET 'table.local-time-zone' = 'UTC';
SET 'execution.checkpointing.storage' = 'filesystem';
SET 'execution.checkpointing.dir' = 'file:///opt/flink/checkpoints';
SET 'table.exec.state.ttl' = '1 h';
SET 'table.exec.source.idle-timeout' = '1 min';
CREATE TABLE quotes_raw (
  symbol STRING, name STRING, price DOUBLE, source STRING, source_time STRING,
  captured_at STRING, previous_close DOUBLE, `status` STRING, round_id STRING,
  expected INT, failed_batches INT, proc_time AS PROCTIME(),
  event_time AS TO_TIMESTAMP(REPLACE(SUBSTRING(captured_at, 1, 19), 'T', ' '), 'yyyy-MM-dd HH:mm:ss'),
  WATERMARK FOR event_time AS event_time - INTERVAL '1' SECOND * __LATENESS_SECONDS__
) WITH (
  'connector' = 'kafka', 'topic' = 'quotes.raw',
  'properties.bootstrap.servers' = 'kafka:9092', 'properties.group.id' = 'flink-quotes-v1',
  'scan.startup.mode' = 'earliest-offset', 'format' = 'json'
);
CREATE TABLE quotes_latest (
  symbol STRING, name STRING, price DOUBLE, source STRING, source_time STRING, captured_at STRING,
  previous_close DOUBLE, `status` STRING, round_id STRING
) WITH (
  'connector' = 'kafka', 'topic' = 'quotes.latest',
  'properties.bootstrap.servers' = 'kafka:9092', 'format' = 'json'
);
CREATE TABLE quotes_windows (
  symbol STRING, window_start TIMESTAMP(3), window_end TIMESTAMP(3),
  sample_min DOUBLE, sample_max DOUBLE, sample_avg DOUBLE, sample_count BIGINT, window_basis STRING
) WITH (
  'connector' = 'kafka', 'topic' = 'quotes.windows',
  'properties.bootstrap.servers' = 'kafka:9092', 'format' = 'json'
);
CREATE TABLE quotes_market (
  round_id STRING, expected BIGINT, received BIGINT, valid BIGINT,
  rising BIGINT, falling BIGINT, flat BIGINT, failed_batches INT,
  PRIMARY KEY (round_id) NOT ENFORCED
) WITH (
  'connector' = 'upsert-kafka', 'topic' = 'quotes.market',
  'properties.bootstrap.servers' = 'kafka:9092',
  'key.format' = 'json', 'value.format' = 'json'
);
CREATE TABLE quotes_late (
  symbol STRING, round_id STRING, captured_at STRING, price DOUBLE, `status` STRING,
  watermark_time TIMESTAMP(3)
) WITH (
  'connector' = 'kafka', 'topic' = 'quotes.late',
  'properties.bootstrap.servers' = 'kafka:9092', 'format' = 'json'
);
CREATE VIEW unique_quotes AS
SELECT symbol, name, price, source, source_time, captured_at, previous_close, `status`, round_id, expected, failed_batches, proc_time, event_time
FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY round_id, symbol ORDER BY proc_time ASC) AS rownum
      FROM quotes_raw WHERE round_id IS NOT NULL) WHERE rownum = 1;
CREATE VIEW ontime_quotes AS
SELECT * FROM unique_quotes
WHERE CURRENT_WATERMARK(event_time) IS NULL OR event_time > CURRENT_WATERMARK(event_time);
EXECUTE STATEMENT SET
BEGIN
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
