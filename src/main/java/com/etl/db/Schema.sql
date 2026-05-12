-- ============================================================

-- Drop tables in reverse dependency order before recreating
-- ============================================================

DROP TABLE IF EXISTS malformed_record_summary;
DROP TABLE IF EXISTS query_results;
DROP TABLE IF EXISTS batch_metadata;
DROP TABLE IF EXISTS run_metadata;

-- ============================================================
-- 1. run_metadata
--    One row per pipeline execution triggered by the user.
--    Captures: run_id, pipeline_name, execution_timestamp
-- ============================================================
CREATE TABLE run_metadata (
    run_id              SERIAL PRIMARY KEY,
    pipeline_name       VARCHAR(20) NOT NULL,   -- mapreduce / pig / mongo / hive
    execution_timestamp TIMESTAMP   NOT NULL DEFAULT NOW()
);

-- ============================================================
-- 2. batch_metadata
--    One row per batch within a run.
--    Captures: batch_id, run_id, batch_size, average_batch_size,
--              records_processed, runtime_ms
-- ============================================================
CREATE TABLE batch_metadata (
    id                  SERIAL PRIMARY KEY,
    run_id              INT         NOT NULL REFERENCES run_metadata(run_id),
    batch_id            INT         NOT NULL,          -- 1 = July, 2 = August
    batch_label         VARCHAR(20) NOT NULL,          -- 'July' / 'August'
    batch_size          BIGINT      NOT NULL,          -- total raw lines in this batch file
    average_batch_size  NUMERIC(12,2) NOT NULL,        -- avg lines across all batches in this run
    records_processed   BIGINT      NOT NULL,          -- lines actually processed (excl. malformed)
    runtime_ms          BIGINT      NOT NULL
);

-- ============================================================
-- 3. malformed_record_summary
--    One row per (run, batch, query).
--    Captures: malformed_record_count per query per batch
-- ============================================================
CREATE TABLE malformed_record_summary (
    id                    SERIAL PRIMARY KEY,
    run_id                INT         NOT NULL REFERENCES run_metadata(run_id),
    batch_id              INT         NOT NULL,
    query_name            VARCHAR(10) NOT NULL,   -- Q1 / Q2 / Q3
    malformed_record_count BIGINT     NOT NULL DEFAULT 0
);

-- ============================================================
-- 4. query_results
--    One row per output record from any query across all pipelines.
--    Uses a unified table with nullable columns per query type.
--    query_name acts as a discriminator.
-- ============================================================
CREATE TABLE query_results (
    id              SERIAL PRIMARY KEY,
    run_id          INT         NOT NULL REFERENCES run_metadata(run_id),
    batch_id        INT         NOT NULL,
    query_name      VARCHAR(10) NOT NULL,   -- Q1 / Q2 / Q3

    -- Q1 & Q3 shared fields
    log_date              VARCHAR(20),
    log_hour              INT,

    -- Q1 specific
    status_code           INT,

    -- Q2 specific
    resource_path         TEXT,

    -- Q1 metrics
    request_count         BIGINT,       -- total requests (Q1: per date+status, Q2: per resource)
    total_bytes           BIGINT,       -- total bytes transferred (Q1 & Q2)
    distinct_hosts        INT,          -- distinct hosts requesting resource (Q2)

    -- Q3 specific metrics
    error_request_count   BIGINT,       -- number of requests with status 400-599
    total_requests        BIGINT,       -- total requests in that hour
    error_rate            DOUBLE PRECISION,  -- error_request_count / total_requests
    distinct_error_hosts  INT           -- distinct hosts generating error requests
);