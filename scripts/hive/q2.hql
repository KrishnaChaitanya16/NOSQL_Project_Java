-- scripts/hive/q2.hql

-- Force MR execution (avoids local-mode edge cases with 0 mappers)
SET hive.exec.mode.local.auto=false;

DROP TABLE IF EXISTS q2_raw_input;

CREATE EXTERNAL TABLE q2_raw_input (
    host STRING,
    log_time STRING,
    method STRING,
    path STRING,
    status STRING,
    bytes STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.RegexSerDe'
WITH SERDEPROPERTIES (
    "input.regex" = "^([^ ]+) [^ ]+ [^ ]+ \\[(.*)\\] \"([^ ]+) ([^ ]+)(?: +[^ ]+)?\" ([0-9]{3}) ([^ ]+)$"
)
STORED AS TEXTFILE
LOCATION '${INPUT}';

SELECT concat('MALFORMED_RECORDS=', count(*))
FROM q2_raw_input
WHERE host IS NULL OR status IS NULL;

SELECT concat('TOTAL_RECORDS=', count(*))
FROM q2_raw_input;

INSERT OVERWRITE DIRECTORY '${OUTPUT}'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
SELECT 
    path,
    host,
    count(*) as host_req_count,
    sum(if(bytes = '-' OR bytes = '', 0, cast(bytes AS BIGINT))) as host_total_bytes
FROM q2_raw_input
WHERE host IS NOT NULL AND status IS NOT NULL
  AND method IN ('GET', 'POST', 'HEAD')
  AND path LIKE '/%'
GROUP BY path, host;
