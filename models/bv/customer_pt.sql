-- Point in time view for all customer satellites

WITH time_line AS (
  SELECT customer_key, effective_ts, 'customer_main_s' AS rec_src FROM `dv.customer_main_s`
  UNION DISTINCT
  SELECT customer_key, effective_ts, 'customer_address_s' AS rec_src FROM `dv.customer_address_s`
  UNION DISTINCT
  SELECT customer_key, effective_ts, 'customer_class_sb' AS rec_src FROM `dv.customer_class_sb`
), cms AS (
  SELECT
    customer_key
    ,load_dts
    ,effective_ts
    ,COALESCE(LEAD(effective_ts) OVER(PARTITION BY customer_key ORDER BY effective_ts), TIMESTAMP('9999-12-31')) AS effective_ts_end
  FROM
    `dv.customer_main_s`
), cas AS (
  SELECT
    customer_key
    ,load_dts
    ,effective_ts
    ,COALESCE(LEAD(effective_ts) OVER(PARTITION BY customer_key ORDER BY effective_ts), TIMESTAMP('9999-12-31')) AS effective_ts_end
  FROM
    `dv.customer_address_s`
)
SELECT
  tl.customer_key
  ,tl.effective_ts
  ,cms.load_dts AS load_dts_customer_main_s
  ,cas.load_dts AS load_dts_customer_address_s
  ,ccs.load_dts AS load_dts_customer_class_sb
  ,GREATEST(COALESCE(cms.load_dts, TIMESTAMP_MICROS(0)), COALESCE(cas.load_dts, TIMESTAMP_MICROS(0)), COALESCE(ccs.load_dts, TIMESTAMP_MICROS(0))) AS load_dts
  ,tl.rec_src
FROM
  time_line tl
  LEFT JOIN cms ON cms.customer_key = tl.customer_key AND cms.effective_ts <= tl.effective_ts AND tl.effective_ts < cms.effective_ts_end
  LEFT JOIN cas ON cas.customer_key = tl.customer_key AND cas.effective_ts <= tl.effective_ts AND tl.effective_ts < cas.effective_ts_end
  LEFT JOIN `dv.customer_class_sb` ccs ON ccs.customer_key = tl.customer_key AND ccs.effective_ts <= tl.effective_ts AND tl.effective_ts < ccs.effective_ts_end
