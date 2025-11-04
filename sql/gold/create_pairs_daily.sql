--create pairs_daily table using uid_hourly_h3 so each pair of uids observed together
--over time and space are discoverable for analysis.

CREATE TABLE knap_ais.pairs_daily
WITH (
  format = 'PARQUET',
  parquet_compression = 'SNAPPY',
  external_location = 's3://knap-ais/gold/pairs_daily/',
  partitioned_by = ARRAY['year','month','day'],
  bucketed_by = ARRAY['uid_a','uid_b'],
  bucket_count = 32
) AS
SELECT
  p.uid_a,
  p.uid_b,
  p.dt AS day_date,
  ua.win_cnt AS hA,
  ub.win_cnt AS hB,
  ua.geo_cnt AS gA,
  ub.geo_cnt AS gB,
  p.hT,
  p.gT,
  CAST(p.hT AS DOUBLE) / NULLIF(ua.win_cnt + ub.win_cnt - p.hT, 0) AS temporal_j,
  CAST(p.gT AS DOUBLE) / NULLIF(ua.geo_cnt + ub.geo_cnt - p.gT, 0) AS spatial_j,
  0.5 * (
    CAST(p.hT AS DOUBLE) / NULLIF(ua.win_cnt + ub.win_cnt - p.hT, 0) +
    CAST(p.gT AS DOUBLE) / NULLIF(ua.geo_cnt + ub.geo_cnt - p.gT, 0)
  ) AS gtj,
  -- partition columns LAST and matching partitioned_by:
  year(p.dt)  AS year,
  month(p.dt) AS month,
  day(p.dt)   AS day
FROM (
  SELECT
    w.uid_a,
    w.uid_b,
    w.dt,
    COUNT(DISTINCT w.hour)     AS hT,
    COUNT(DISTINCT w.h3_hash)  AS gT
  FROM (
    SELECT
      a.mmsi      AS uid_a,
      b.mmsi      AS uid_b,
      a.dt        AS dt,
      a.hour      AS hour,
      a.h3_index  AS h3_hash
    FROM knap_ais.uid_hourly_h3 a
    JOIN knap_ais.uid_hourly_h3 b
      ON a.dt       = b.dt
     AND a.hour     = b.hour
     AND a.h3_index = b.h3_index
     AND a.mmsi     < b.mmsi
    WHERE a.dt BETWEEN DATE '2025-01-01' AND DATE '2025-01-31'
  ) w
  GROUP BY w.uid_a, w.uid_b, w.dt
) p
JOIN (
  SELECT
    mmsi,
    dt,
    COUNT(*) AS win_cnt,
    COUNT(DISTINCT h3_index) AS geo_cnt
  FROM knap_ais.uid_hourly_h3
  WHERE dt BETWEEN DATE '2025-01-01' AND DATE '2025-01-31'
  GROUP BY mmsi, dt
) ua
  ON p.uid_a = ua.mmsi AND p.dt = ua.dt
JOIN (
  SELECT
    mmsi,
    dt,
    COUNT(*) AS win_cnt,
    COUNT(DISTINCT h3_index) AS geo_cnt
  FROM knap_ais.uid_hourly_h3
  WHERE dt BETWEEN DATE '2025-01-01' AND DATE '2025-01-31'
  GROUP BY mmsi, dt
) ub
  ON p.uid_b = ub.mmsi AND p.dt = ub.dt
WHERE p.gT > 1;
