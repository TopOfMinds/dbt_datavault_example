{% call deduplicate(['sale_line_l_key']) %}
SELECT
  sales_line_id || '|' || transaction_id || '|' || product_id AS sale_line_l_key
  ,CAST(sales_line_id AS string) AS sale_line_key
  ,CAST(transaction_id AS string) AS sale_key
  ,CAST(product_id AS string) AS product_key
  ,ingestion_time AS load_dts
  ,'datalake.sales' AS rec_src
FROM
  {{ ref('sales_line_stg') }}
{% endcall %}
