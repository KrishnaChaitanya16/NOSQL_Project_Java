-- ============================================================
-- Q2 Stage-2: Global merge of all batch Stage-1 outputs
-- Produces the true global Top-20 with exact distinct hosts
-- ============================================================

-- INPUT = /etl/output/pig/q2/stage1  (directory containing all batch_* subdirs)
stage1 = LOAD '$INPUT' USING PigStorage('\t') AS (
    path:chararray,
    summary:chararray    -- count_bytes_host1,host2,...
);

filtered = FILTER stage1 BY path IS NOT NULL AND summary IS NOT NULL;

-- Split summary into fields
split_data = FOREACH filtered GENERATE
    path,
    (long)   REGEX_EXTRACT(summary, '^(\\d+)_',         1) AS request_count,
    (long)   REGEX_EXTRACT(summary, '^\\d+_(\\d+)_',    1) AS total_bytes,
             REGEX_EXTRACT(summary, '^\\d+_\\d+_(.*)',   1) AS hosts_csv:chararray;

-- Group by path to merge across batches
grouped = GROUP split_data BY path;

-- Compute sums and flatten the CSV strings for each path
flattened_csvs = FOREACH grouped GENERATE
    group AS path,
    SUM(split_data.request_count) AS global_request_count,
    SUM(split_data.total_bytes)   AS global_total_bytes,
    FLATTEN(split_data.hosts_csv) AS csv:chararray;

-- Tokenize each CSV into individual hosts
tokenized = FOREACH flattened_csvs GENERATE
    path,
    global_request_count,
    global_total_bytes,
    FLATTEN(TOKENIZE(csv, ',')) AS host:chararray;

-- Get unique hosts per path
unique_hosts = DISTINCT tokenized;

-- Group back to count the distinct hosts
grouped_unique = GROUP unique_hosts BY (path, global_request_count, global_total_bytes);

with_distinct = FOREACH grouped_unique GENERATE
    group.path AS path,
    group.global_request_count AS request_count,
    group.global_total_bytes AS total_bytes,
    COUNT(unique_hosts) AS distinct_hosts:long;

-- Pick global Top-20
grouped_all = GROUP with_distinct ALL;
top20 = FOREACH grouped_all {
    result = TOP(20, 1, with_distinct);
    GENERATE FLATTEN(result) AS (
        path:chararray,
        request_count:long,
        total_bytes:long,
        distinct_hosts:long
    );
};

-- Format for ResultLoader: path TAB count_bytes_distinctHosts
final_out = FOREACH top20 GENERATE
    path,
    CONCAT(
        (chararray)request_count,
        CONCAT('_',
        CONCAT((chararray)total_bytes,
        CONCAT('_', (chararray)distinct_hosts)))
    ) AS summary:chararray;

STORE final_out INTO '$OUTPUT' USING PigStorage('\t');
