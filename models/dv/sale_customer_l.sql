SELECT
  sale_customer_l_key
  ,sale_key
  ,customer_key
  ,load_dts
  ,rec_src
FROM (
  SELECT
    sale_customer_l_key
    ,sale_key
    ,customer_key
    ,load_dts
    ,rec_src
    ,row_number() over(PARTITION BY sale_customer_l_key ORDER BY load_dts asc) rn
  FROM (
    
    SELECT
      transaction_id || '|' || customer_id AS sale_customer_l_key
      ,CAST(transaction_id AS string) AS sale_key
      ,CAST(customer_id AS string) AS customer_key
      ,ingestion_time AS load_dts
      ,'datalake.sales' AS rec_src
    FROM datalake.sales
  )
)
WHERE rn = 1