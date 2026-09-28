-- Real-time fraud detection over the bank transaction stream.
--
--   docker compose -f flink/docker-compose.yml exec -T jobmanager \
--     ./bin/sql-client.sh -f /opt/sql/fraud_detection.sql
--
-- Three rules run as one job via a STATEMENT SET, all sharing the single
-- read of bank.transactions and all writing to bank.alerts.

SET 'execution.runtime-mode' = 'streaming';
SET 'parallelism.default' = '2';
SET 'execution.checkpointing.interval' = '30s';
-- A partition with no traffic must not hold the watermark back.
SET 'table.exec.source.idle-timeout' = '10s';

CREATE TABLE transactions (
  txn_id       STRING,
  account_id   STRING,
  customer_id  STRING,
  amount       DECIMAL(12, 2),
  currency     STRING,
  merchant     STRING,
  category     STRING,
  country      STRING,
  city         STRING,
  txn_type     STRING,
  event_time   TIMESTAMP(3),
  WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'bank.transactions',
  'properties.bootstrap.servers' = 'kafka.confluent.svc.cluster.local:9092',
  'properties.group.id' = 'flink-fraud-detection',
  'scan.startup.mode' = 'latest-offset',
  'format' = 'json',
  'json.timestamp-format.standard' = 'ISO-8601',
  'json.fail-on-missing-field' = 'false',
  'json.ignore-parse-errors' = 'true'
);

CREATE TABLE alerts (
  alert_id     STRING,
  rule_name    STRING,
  severity     STRING,
  account_id   STRING,
  customer_id  STRING,
  detail       STRING,
  amount       DECIMAL(12, 2),
  detected_at  TIMESTAMP(3)
) WITH (
  'connector' = 'kafka',
  'topic' = 'bank.alerts',
  'properties.bootstrap.servers' = 'kafka.confluent.svc.cluster.local:9092',
  'format' = 'json',
  'json.timestamp-format.standard' = 'ISO-8601'
);

EXECUTE STATEMENT SET
BEGIN

  -- Rule 1: a single transaction above the reporting threshold.
  INSERT INTO alerts
  SELECT
    MD5(txn_id),
    'LARGE_TRANSACTION',
    CASE WHEN amount > 50000 THEN 'CRITICAL' ELSE 'HIGH' END,
    account_id,
    customer_id,
    CONCAT(txn_type, ' of ', CAST(amount AS STRING), ' ', currency, ' at ', merchant),
    amount,
    event_time
  FROM transactions
  WHERE amount > 10000;

  -- Rule 2: more than 5 transactions on one account inside a minute.
  INSERT INTO alerts
  SELECT
    MD5(CONCAT(account_id, CAST(window_start AS STRING))),
    'VELOCITY',
    'MEDIUM',
    account_id,
    MAX(customer_id),
    CONCAT('burst of ', CAST(COUNT(*) AS STRING),
           ' transactions in 60s totalling ', CAST(SUM(amount) AS STRING)),
    CAST(SUM(amount) AS DECIMAL(12, 2)),
    window_end
  FROM TABLE(TUMBLE(TABLE transactions, DESCRIPTOR(event_time), INTERVAL '1' MINUTE))
  GROUP BY account_id, window_start, window_end
  HAVING COUNT(*) > 5;

  -- Rule 3: the same account used in two countries within five minutes.
  -- MATCH_RECOGNIZE compares each transaction with the one that immediately
  -- follows it on that account. A plain interval join would instead pair every
  -- foreign charge with every later home charge and emit a flood of duplicates.
  INSERT INTO alerts
  SELECT alert_id, rule_name, severity, account_id, customer_id,
         detail, amount, detected_at
  FROM transactions
  MATCH_RECOGNIZE (
    PARTITION BY account_id
    ORDER BY event_time
    MEASURES
      MD5(CONCAT(HOME.txn_id, AWAY.txn_id))       AS alert_id,
      'IMPOSSIBLE_TRAVEL'                         AS rule_name,
      'CRITICAL'                                  AS severity,
      AWAY.customer_id                            AS customer_id,
      CONCAT('spend in ', HOME.country, ' (', HOME.city, ') then ',
             AWAY.country, ' (', AWAY.city, ') minutes apart') AS detail,
      AWAY.amount                                 AS amount,
      AWAY.event_time                             AS detected_at
    ONE ROW PER MATCH
    AFTER MATCH SKIP PAST LAST ROW
    PATTERN (HOME AWAY) WITHIN INTERVAL '5' MINUTE
    DEFINE
      AWAY AS AWAY.country <> HOME.country
  );

END;
