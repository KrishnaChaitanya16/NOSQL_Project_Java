-- ============================================================
-- Q2 Stage-1: Aggregate per batch (no Top-20 limit)
-- Emits ALL resources with comma-separated host list
-- ============================================================

raw = LOAD '$INPUT' USING TextLoader() AS (line:chararray);

parsed = FOREACH raw GENERATE
    FLATTEN(
        REGEX_EXTRACT_ALL(
            line,
            '^(\\S+) \\S+ \\S+ \\[.*?\\] \\"(?:GET|POST|HEAD) (\\S+) \\S+\\" \\d{3} (\\S+)'
        )
    ) AS (
        host:chararray,
        path:chararray,
        bytes:chararray
    );

SPLIT parsed INTO
    filtered IF (host != '') AND (path != '') AND (path != '/') AND STARTSWITH(path, '/') AND (path != '-'),
    bad_records OTHERWISE;
STORE bad_records INTO '$OUTPUT_MALFORMED' USING PigStorage('\t');

with_bytes = FOREACH filtered GENERATE
    path,
    host,
    (bytes == '-' ? 0L : (long)bytes) AS byte_val;

-- Group by path+host to collect per-host bytes
grouped_ph = GROUP with_bytes BY (path, host);

per_host = FOREACH grouped_ph GENERATE
    group.path AS path,
    group.host AS host,
    COUNT(with_bytes)         AS host_req_count,
    SUM(with_bytes.byte_val)  AS host_total_bytes;

-- Group by path to aggregate across all hosts
grouped = GROUP per_host BY path;

-- Emit: path \t count_bytes_host1,host2,...
aggregated = FOREACH grouped GENERATE
    group AS path,
    SUM(per_host.host_req_count)   AS request_count,
    SUM(per_host.host_total_bytes) AS total_bytes,
    BagToString(per_host.host, ',') AS hosts:chararray;

-- Format output: path TAB count_bytes_hosts
formatted = FOREACH aggregated GENERATE
    path,
    CONCAT(
        (chararray)request_count,
        CONCAT('_',
        CONCAT((chararray)total_bytes,
        CONCAT('_', hosts)))
    ) AS summary:chararray;

STORE formatted INTO '$OUTPUT' USING PigStorage('\t');