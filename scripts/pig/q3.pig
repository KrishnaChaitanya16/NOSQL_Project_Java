-- ============================================================
-- Q3: Error rate per hour
-- ============================================================

-- -------- Load --------
raw = LOAD '$INPUT' USING TextLoader() AS (line:chararray);

-- -------- Parse --------
parsed = FOREACH raw GENERATE
    FLATTEN(
        REGEX_EXTRACT_ALL(
            line,
            '^(\\S+) \\S+ \\S+ \\[(\\S+):(\\d{2}):\\d{2}:\\d{2} \\S+\\] \\".*?\\" (\\d{3}) \\S+'
        )
    ) AS (
        host:chararray,
        log_date:chararray,
        log_hour:chararray,
        status:chararray
    );

-- -------- Filter --------
SPLIT parsed INTO
    filtered IF (log_date != '') AND (status != ''),
    bad_records OTHERWISE;
STORE bad_records INTO '$OUTPUT_MALFORMED' USING PigStorage('\t');

-- -------- Tag --------
tagged = FOREACH filtered GENERATE
    CONCAT(CONCAT(log_date, '_'), log_hour) AS date_hour,
    host,
    1L AS total,
    ((int)status >= 400 ? 1L : 0L) AS is_error;

-- -------- Group --------
grouped = GROUP tagged BY date_hour;

-- -------- Aggregate --------
aggregated = FOREACH grouped {
    errors_only = FILTER tagged BY is_error == 1L;
    error_hosts = FOREACH errors_only GENERATE host;
    uniq_hosts = DISTINCT error_hosts;
    GENERATE
        group AS date_hour,
        SUM(tagged.total) AS total_requests,
        SUM(tagged.is_error) AS error_requests,
        COUNT(uniq_hosts) AS distinct_error_hosts;
}

-- -------- Compute Rate and Format --------
formatted = FOREACH aggregated GENERATE
    date_hour,
    CONCAT(
        (chararray)total_requests,
        CONCAT('_',
        CONCAT((chararray)error_requests,
        CONCAT('_',
        CONCAT((chararray)((double)error_requests / (double)total_requests),
        CONCAT('_', (chararray)distinct_error_hosts)))))
    ) AS summary:chararray;

-- -------- Store --------
STORE formatted INTO '$OUTPUT' USING PigStorage('\t');