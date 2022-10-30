SELECT
  sale_key
  ,sale_id
  ,load_dts
  ,rec_src
FROM (
  SELECT
    sale_key
    ,sale_id
    ,load_dts
    ,rec_src
    ,row_number() over(PARTITION BY sale_key ORDER BY load_dts asc) rn
  FROM (
    
    SELECT
      CAST(transaction_id AS string) AS sale_key
      ,transaction_id AS sale_id
      ,ingestion_time AS load_dts
      ,'datalake.sales' AS rec_src
    FROM
      {{ source('datalake', 'sales') }}
  )
)
WHERE rn = 1