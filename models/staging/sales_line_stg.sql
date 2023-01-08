{% if target.type == 'snowflake' %}
SELECT
  transaction_id || '|' || sl.index AS sales_line_id
  ,transaction_id
  ,sl.index AS line_offset
  ,sl.value:product_id AS product_id
  ,sl.value:price AS price
  ,sl.value:quantity AS quantity
  ,sl.value:total AS total
  ,date
  ,ingestion_time
  ,dt
FROM
  {{ source('datalake', 'sales') }} AS s
  ,LATERAL flatten(input => s.sales_lines) AS sl
{% else %}
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
{% endif %}