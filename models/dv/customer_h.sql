SELECT
  customer_key
  ,customer_id
  ,load_dts
  ,rec_src
FROM (
  SELECT
    customer_key
    ,customer_id
    ,load_dts
    ,rec_src
    ,row_number() over(PARTITION BY customer_key ORDER BY load_dts asc) rn
  FROM (
    
    SELECT
      CAST(customer_id AS string) AS customer_key
      ,customer_id AS customer_id
      ,ingestion_time AS load_dts
      ,'datalake.sales' AS rec_src
    FROM
      datalake.sales
    UNION ALL
    SELECT
      CAST(customer_id AS string) AS customer_key
      ,customer_id AS customer_id
      ,ingestion_time AS load_dts
      ,'datalake.customer' AS rec_src
    FROM
      datalake.customer
    UNION ALL
    SELECT
      CAST(customer_id AS string) AS customer_key
      ,customer_id AS customer_id
      ,ingestion_time AS load_dts
      ,'datalake.custommer_address' AS rec_src
    FROM
      datalake.customer_address
  )
)
WHERE rn = 1