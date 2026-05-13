-- ============================================================
-- Q2 Stage-1: Aggregate per batch (no Top-20 limit)
-- Emits ALL resources with comma-separated host list
-- ============================================================

raw = LOAD '$INPUT' USING TextLoader() AS (line:chararray);

-- Parse using lenient outer CLF regex — protocol inside request is optional
parsed_raw = FOREACH raw GENERATE
    FLATTEN(
        REGEX_EXTRACT_ALL(
            line,
            '^(\\S+)\\s+\\S+\\s+\\S+\\s+\\[([^\\]]+)\\]\\s+\\"(.*)\\"\\s+(\\d{3})\\s+(\\S+)$'
        )
    ) AS (
        host:chararray,
        log_time:chararray,
        request:chararray,
        status:chararray,
        bytes:chararray
    );

parsed = FOREACH parsed_raw GENERATE
    host,
    log_time,
    FLATTEN(
        REGEX_EXTRACT_ALL(
            request,
            '^(\\S+)\\s+(\\S+)(?:\\s+\\S+)?$'
        )
    ) AS (
        method:chararray,
        path:chararray
    ),
    status,
    bytes;

-- Malformed = line could not be parsed by the CLF regex at all
SPLIT parsed INTO
    filtered     IF (host IS NOT NULL) AND (host != '') AND (status IS NOT NULL) AND (status != ''),
    bad_records  OTHERWISE;
STORE bad_records INTO '$OUTPUT_MALFORMED' USING PigStorage('\t');

-- Business-logic filter: standard HTTP methods and valid paths only (not malformed)
cleaned = FILTER filtered BY
    (method == 'GET' OR method == 'POST' OR method == 'HEAD')
    AND STARTSWITH(path, '/');

-- Normalize path: trim and remove non-printable characters (matching MapReduce Java logic)
normalized = FOREACH cleaned GENERATE
    TRIM(REPLACE(path, '[^\\x20-\\x7E]', '')) AS path,
    host,
    bytes;

with_bytes = FOREACH normalized GENERATE
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
    BagToString(per_host.host, '#') AS hosts:chararray;

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