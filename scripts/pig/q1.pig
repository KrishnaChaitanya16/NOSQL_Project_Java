-- ============================================================
-- Q1: Daily request count and total bytes per status code
-- ============================================================

-- -------- Load --------
raw = LOAD '$INPUT' USING PigStorage('\n') AS (line:chararray);

-- -------- Parse --------
parsed = FOREACH raw GENERATE
    FLATTEN(
        REGEX_EXTRACT_ALL(
            line,
            '^(\\S+) \\S+ \\S+ \\[([^\\]]+)\\] \\".*\\" (\\d{3}) (\\S+)$'
        )
    ) AS (
        host:chararray,
        log_time:chararray,
        status:chararray,
        bytes:chararray
    );

-- -------- Extract fields (keep host for null-check in SPLIT) --------
extracted = FOREACH parsed GENERATE
    host,
    log_time,
    status,
    bytes;

-- -------- Split: malformed = line could not be parsed by the regex --------
SPLIT extracted INTO
    filtered     IF (host IS NOT NULL) AND (host != '') AND (status IS NOT NULL) AND (status != ''),
    bad_records  OTHERWISE;
STORE bad_records INTO '$OUTPUT_MALFORMED' USING PigStorage('\t');

-- -------- Extract date --------
with_date = FOREACH filtered GENERATE
    SUBSTRING(log_time, 0, 11) AS log_date,
    status AS status,
    bytes AS bytes;

-- -------- Separate good bytes from bad/missing --------
valid_bytes   = FILTER with_date BY (bytes != '') AND (bytes != '-');
null_bytes    = FILTER with_date BY (bytes == '');
dash_bytes    = FILTER with_date BY (bytes != '') AND (bytes == '-');

-- -------- Key each group --------
keyed_valid = FOREACH valid_bytes GENERATE
    CONCAT(CONCAT(log_date, '_'), status) AS date_status:chararray,
    (long)bytes AS byte_val:long;

keyed_null = FOREACH null_bytes GENERATE
    CONCAT(CONCAT(log_date, '_'), status) AS date_status:chararray,
    0L AS byte_val:long;

keyed_dash = FOREACH dash_bytes GENERATE
    CONCAT(CONCAT(log_date, '_'), status) AS date_status:chararray,
    0L AS byte_val:long;

-- -------- Union all three --------
keyed = UNION keyed_valid, keyed_null, keyed_dash;

-- -------- Group --------
grouped = GROUP keyed BY date_status;

-- -------- Aggregate --------
aggregated = FOREACH grouped GENERATE
    group               AS date_status:chararray,
    COUNT(keyed)        AS request_count:long,
    SUM(keyed.byte_val) AS total_bytes:long;

-- -------- Cast before CONCAT --------
casted = FOREACH aggregated GENERATE
    date_status,
    (chararray)request_count AS rc_str,
    (chararray)total_bytes   AS tb_str;

-- -------- Format output --------
final_out = FOREACH casted GENERATE
    date_status,
    CONCAT(CONCAT(rc_str, '_'), tb_str) AS summary:chararray;

-- -------- Store --------
STORE final_out INTO '$OUTPUT' USING PigStorage('\t');