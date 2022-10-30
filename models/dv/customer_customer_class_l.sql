SELECT
  customer_customer_class_l_key
  ,customer_key
  ,customer_class_key
  ,load_dts
  ,rec_src
FROM (
  SELECT
    customer_customer_class_l_key
    ,customer_key
    ,customer_class_key
    ,load_dts
    ,rec_src
    ,row_number() over(PARTITION BY customer_customer_class_l_key ORDER BY load_dts asc) rn
  FROM (
    
    SELECT
      customer_id || '|' || customer_class_id AS customer_customer_class_l_key
      ,CAST(customer_id AS string) AS customer_key
      ,CAST(customer_class_id AS string) AS customer_class_key
      ,ingestion_time AS load_dts
      ,'datalake.customer_segmentations' AS rec_src
    FROM {{ source('datalake', 'customer_segmentations') }}
  )
)
WHERE rn = 1