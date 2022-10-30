SELECT
  customer_customer_class_l_key
  ,max(load_dts) AS load_dts
  ,effective_ts
  ,rec_src
FROM (
  
  SELECT
    CAST(customer_id|| '|' ||customer_class_id AS string) AS customer_customer_class_l_key
    ,ingestion_time AS load_dts
    ,date AS effective_ts
    ,'datalake.customer_segmentations' AS rec_src
  FROM
    datalake.customer_segmentations
)
GROUP BY
  customer_customer_class_l_key
  ,effective_ts
  ,rec_src