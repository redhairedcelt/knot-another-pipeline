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
    SUM(CASE WHEN gto >= 0.4 THEN 1 ELSE 0 END)                  AS days_with_gto_above_0_4,
    MAX(gto)                                                     AS max_gto,
    MIN(gto)                                                     AS min_gto,
    AVG(gto)                                                     AS avg_gto,
    approx_percentile(gto, 0.5)                                  AS median_gto
FROM knap_ais.pairs_daily
GROUP BY uid_a, uid_b;
