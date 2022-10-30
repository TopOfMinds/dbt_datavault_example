SELECT
  transaction_id || '|' || offset AS sales_line_id
  ,transaction_id
  ,offset AS line_offset
  ,sl.product_id
  ,sl.price
  ,sl.quantity
  ,sl.total
  ,date
  ,ingestion_time
  ,dt
FROM
  {{ source('datalake', 'sales') }} AS s
  ,UNNEST(s.sales_lines) AS sl WITH OFFSET
