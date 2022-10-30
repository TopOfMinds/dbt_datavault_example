SELECT
  sale_line_key
  ,max(load_dts) AS load_dts
  ,effective_ts
  ,item_cost
  ,item_quantity
  ,rec_src
FROM (
  
  SELECT
    CAST(sales_line_id AS string) AS sale_line_key
    ,ingestion_time AS load_dts
    ,date AS effective_ts
    ,price AS item_cost
    ,quantity AS item_quantity
    ,'datalake.sales' AS rec_src
  FROM
     {{ ref('sales_line_stg') }}
)
GROUP BY
  sale_line_key
  ,effective_ts
  ,item_cost
  ,item_quantity
  ,rec_src