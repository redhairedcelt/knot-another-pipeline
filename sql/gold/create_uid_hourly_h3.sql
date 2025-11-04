-- ------------------------------------------------------------------------------
-- Athena CTAS template for the gold.uid_hourly_h3 table
-- ------------------------------------------------------------------------------
-- 1. Replace the placeholders below (dates and bucket if needed)
--    or adapt the WHERE clause to the partitions you want to process.
-- 2. Ensure the H3 Lambda UDF (lat_lng_to_cell_address) is deployed in the target
--    region and accessible to Athena.
-- 3. Drop or archive any existing table/output before running this CTAS, or write
--    to a staging location and promote after validation.
-- ------------------------------------------------------------------------------

DROP TABLE IF EXISTS knap_ais.uid_hourly_h3;

CREATE TABLE knap_ais.uid_hourly_h3
WITH (
    format              = 'PARQUET',
    parquet_compression = 'SNAPPY',
    external_location   = 's3://knap-ais/gold/uid_hourly_h3/',
    partitioned_by      = ARRAY['dt', 'hour'],
    bucketed_by         = ARRAY['mmsi'],
    bucket_count        = 64
)
AS
USING EXTERNAL FUNCTION lat_lng_to_cell_address(lat DOUBLE, lon DOUBLE, res INTEGER)
RETURNS VARCHAR
LAMBDA 'arn:aws:lambda:us-east-1:058264100453:function:H3UDF'
WITH cleaned AS (
    SELECT
        CAST(mmsi AS VARCHAR)                                               AS mmsi,
        COALESCE(
            TRY_CAST(base_date_time AS TIMESTAMP),
            TRY_CAST(
                from_iso8601_timestamp(
                    CASE
                        WHEN base_date_time IS NULL THEN NULL
                        WHEN regexp_like(base_date_time, '.*[Tt].*') THEN base_date_time
                        WHEN regexp_like(base_date_time, '.*[+-][0-9]{2}:?[0-9]{2}$')
                             OR regexp_like(base_date_time, '.*[Zz]$') THEN replace(base_date_time, ' ', 'T')
                        ELSE replace(base_date_time, ' ', 'T') || 'Z'
                    END
                ) AS TIMESTAMP
            )
        )                                                                   AS event_ts,
        TRY_CAST(latitude AS DOUBLE)                                        AS latitude,
        TRY_CAST(longitude AS DOUBLE)                                       AS longitude,
        TRY_CAST(sog AS DOUBLE)                                             AS sog
    FROM knap_ais.silver_ais
    WHERE mmsi IS NOT NULL
      AND base_date_time IS NOT NULL
      AND TRY_CAST(year AS INTEGER) = year(DATE '2025-01-01')
      AND TRY_CAST(month AS INTEGER) = 1
      AND TRY_CAST(day AS INTEGER) BETWEEN 1 AND 31
      AND TRY_CAST(latitude AS DOUBLE) BETWEEN -90 AND 90
      AND TRY_CAST(longitude AS DOUBLE) BETWEEN -180 AND 180
) 
SELECT
    mmsi,
    date_trunc('hour', event_ts)                                            AS hour_ts,
    AVG(latitude)                                                           AS avg_lat,
    AVG(longitude)                                                          AS avg_lon,
    lat_lng_to_cell_address(AVG(latitude), AVG(longitude), 7)               AS h3_index,
    COUNT(*)                                                                AS message_count,
    AVG(sog)                                                                AS avg_sog,
    CAST(current_timestamp AS TIMESTAMP)                                    AS ingested_at,
    CAST(date_trunc('day', date_trunc('hour', event_ts)) AS DATE)           AS dt,
    EXTRACT(hour FROM date_trunc('hour', event_ts))                         AS hour
FROM cleaned
WHERE event_ts >= TIMESTAMP '2025-01-01 00:00:00' -- adjust lower bound
  AND event_ts <  TIMESTAMP '2025-02-01 00:00:00' -- adjust upper bound (exclusive)
GROUP BY
    mmsi,
    date_trunc('hour', event_ts);
