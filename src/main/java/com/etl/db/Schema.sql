CREATE TABLE results (
    id SERIAL PRIMARY KEY,

    -- Metadata (MANDATORY as per requirement)
    pipeline_name VARCHAR(20) NOT NULL,   -- mapreduce / pig / hive / mongo
    query_name VARCHAR(10) NOT NULL,      -- Q1 / Q2 / Q3
    run_id INT NOT NULL,
    batch_id INT NOT NULL,

    -- Query fields (nullable depending on query)
    log_date VARCHAR(20),
    log_hour INT,
    status_code INT,
    resource_path TEXT,

    -- Metrics
    request_count INT,
    total_requests INT,          --  added for Q3
    total_bytes BIGINT,
    distinct_hosts INT,
    error_rate DOUBLE PRECISION,

    -- Execution metadata
    runtime_ms BIGINT NOT NULL
);