-- scripts/hive/q2_merge.hql

SET hive.exec.mode.local.auto=false;
SET mapreduce.input.fileinputformat.input.dir.recursive=true;
SET hive.mapred.supports.subdirectories=true;

DROP TABLE IF EXISTS q2_stage1_output;

CREATE EXTERNAL TABLE q2_stage1_output (
    path STRING,
    host STRING,
    host_req_count BIGINT,
    host_total_bytes BIGINT
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '${INPUT}';

INSERT OVERWRITE DIRECTORY '${OUTPUT}'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
SELECT 
    path,
    concat(
        cast(sum(host_req_count) AS STRING), '_',
        cast(sum(host_total_bytes) AS STRING), '_',
        cast(count(DISTINCT host) AS STRING)
    ) as summary
FROM q2_stage1_output
WHERE path IS NOT NULL 
  AND host_req_count IS NOT NULL
GROUP BY path
HAVING sum(host_req_count) > 0
ORDER BY sum(host_req_count) DESC
LIMIT 20;
