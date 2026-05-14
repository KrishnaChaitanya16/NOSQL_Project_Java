-- scripts/hive/q3.hql

-- Force MR execution (avoids local-mode edge cases with 0 mappers)
SET hive.exec.mode.local.auto=false;

DROP TABLE IF EXISTS q3_raw_input;

CREATE EXTERNAL TABLE q3_raw_input (
    host STRING,
    log_date STRING,
    log_hour STRING,
    status STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.RegexSerDe'
WITH SERDEPROPERTIES (
    "input.regex" = "^([^ ]+) [^ ]+ [^ ]+ \\[([^:]+):([0-9]{2}):[0-9]{2}:[0-9]{2} [^ ]+\\] \".*\" ([0-9]{3}) [^ ]+$"
)
STORED AS TEXTFILE
LOCATION '${INPUT}';

SELECT concat('MALFORMED_RECORDS=', count(*))
FROM q3_raw_input
WHERE host IS NULL OR status IS NULL;

SELECT concat('TOTAL_RECORDS=', count(*))
FROM q3_raw_input;

INSERT OVERWRITE DIRECTORY '${OUTPUT}'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
SELECT 
    concat(log_date, '_', log_hour) as date_hour,
    concat(
        cast(count(*) AS STRING), '_',
        cast(sum(if(cast(status AS INT) >= 400, 1, 0)) AS STRING), '_',
        cast(cast(sum(if(cast(status AS INT) >= 400, 1, 0)) AS DOUBLE) / count(*) AS STRING), '_',
        cast(count(DISTINCT if(cast(status AS INT) >= 400, host, NULL)) AS STRING)
    ) as summary
FROM q3_raw_input
WHERE host IS NOT NULL AND status IS NOT NULL
GROUP BY log_date, log_hour;
