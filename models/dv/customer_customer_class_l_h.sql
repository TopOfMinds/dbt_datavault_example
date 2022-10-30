SELECT
  customer_customer_class_l_key
  ,customer_id
  ,customer_class_code
  ,load_dts
  ,rec_src
FROM (
  SELECT
    customer_customer_class_l_key
    ,customer_id
    ,customer_class_code
    ,load_dts
    ,rec_src
    ,row_number() over(PARTITION BY customer_customer_class_l_key ORDER BY load_dts asc) rn
  FROM (
    
    SELECT
      CAST(customer_id|| '|' ||customer_class_id AS string) AS customer_customer_class_l_key
      ,customer_id AS customer_id
      ,customer_class_id AS customer_class_code
      ,ingestion_time AS load_dts
      ,'datalake.customer_segmentations' AS rec_src
    FROM
      {{ source('datalake', 'customer_segmentations') }}
  )
)
WHERE rn = 1