CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE TABLE IF NOT EXISTS telemetry (
                                         time         timestamptz      NOT NULL,
                                         device_id    integer          NOT NULL,
                                         temperature  real,
                                         humidity     real,
                                         acc_x        real,
                                         acc_y        real,
                                         acc_z        real,
                                         gyro_x       real,
                                         gyro_y       real,
                                         gyro_z       real,
                                         lat          double precision,
                                         lon          double precision,
                                         altitude     real,
                                         speed        real
);

-- No default time index: every extra index costs write throughput.
SELECT create_hypertable(
               'telemetry', 'time',
               chunk_time_interval => INTERVAL '1 day',
               create_default_indexes => FALSE,
               if_not_exists => TRUE
       );

CREATE INDEX IF NOT EXISTS telemetry_device_time_idx
    ON telemetry (device_id, time DESC);

-- Compression: segment by device so one device's history compresses together
ALTER TABLE telemetry SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'device_id',
    timescaledb.compress_orderby   = 'time DESC'
    );
SELECT add_compression_policy('telemetry', INTERVAL '3 days', if_not_exists => TRUE);
SELECT add_retention_policy('telemetry', INTERVAL '90 days', if_not_exists => TRUE);

-- Dashboards read this rollup, not raw rows
CREATE MATERIALIZED VIEW IF NOT EXISTS telemetry_1m
WITH (timescaledb.continuous) AS
SELECT time_bucket('1 minute', time) AS bucket,
       device_id,
       avg(temperature) AS temp_avg,
       avg(humidity)    AS hum_avg,
       max(speed)       AS speed_max,
       count(*)         AS samples
FROM telemetry
GROUP BY bucket, device_id
    WITH NO DATA;

SELECT add_continuous_aggregate_policy(
               'telemetry_1m',
               start_offset      => INTERVAL '1 hour',
               end_offset        => INTERVAL '1 minute',
               schedule_interval => INTERVAL '1 minute',
               if_not_exists     => TRUE
       );

ALTER ROLE ingestor SET synchronous_commit = off;