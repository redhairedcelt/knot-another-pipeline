-- ------------------------------------------------------------------------------
-- CTAS to build the gold.pairs_total summary table from knap_ais.pairs_daily.
-- ------------------------------------------------------------------------------

CREATE TABLE knap_ais.pairs_total
WITH (
    format = 'PARQUET',
    external_location = 's3://knap-ais/gold/pairs_total/'
)
AS
SELECT
    uid_a,
    uid_b,
    COUNT(DISTINCT day_date)                                     AS total_days_observed,
    SUM(CASE WHEN gtj >= 0.4 THEN 1 ELSE 0 END)                  AS days_with_gtj_above_0_4,
    MAX(gtj)                                                     AS max_gtj,
    MIN(gtj)                                                     AS min_gtj,
    AVG(gtj)                                                     AS avg_gtj,
    approx_percentile(gtj, 0.5)                                  AS median_gtj
FROM knap_ais.pairs_daily
GROUP BY uid_a, uid_b;
