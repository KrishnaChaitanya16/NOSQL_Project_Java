-- scripts/hive/q1.hql

-- Force MR execution (avoids local-mode edge cases with 0 mappers)
SET hive.exec.mode.local.auto=false;

DROP TABLE IF EXISTS q1_raw_input;

CREATE EXTERNAL TABLE q1_raw_input (
    host STRING,
    log_time STRING,
    status STRING,
    bytes STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.RegexSerDe'
WITH SERDEPROPERTIES (
    "input.regex" = "^([^ ]+) [^ ]+ [^ ]+ \\[(.*)\\] \".*\" ([0-9]{3}) ([^ ]+)$"
)
STORED AS TEXTFILE
LOCATION '${INPUT}';

SELECT concat('MALFORMED_RECORDS=', count(*))
FROM q1_raw_input
WHERE host IS NULL OR status IS NULL;

SELECT concat('TOTAL_RECORDS=', count(*))
FROM q1_raw_input;

INSERT OVERWRITE DIRECTORY '${OUTPUT}'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
SELECT 
    concat(substr(log_time, 1, 11), '_', status) as date_status,
    concat(
        cast(count(*) AS STRING), '_', 
        cast(sum(if(bytes = '-' OR bytes = '', 0, cast(bytes AS BIGINT))) AS STRING)
    ) as summary
FROM q1_raw_input
WHERE host IS NOT NULL AND status IS NOT NULL
GROUP BY substr(log_time, 1, 11), status;
