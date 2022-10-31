{% call deduplicate(['sale_line_l_key']) %}
SELECT
  {{ make_key(['sales_line_id', 'transaction_id', 'product_id']) }} AS sale_line_l_key
  ,{{ make_key(['sales_line_id']) }} AS sale_line_key
  ,{{ make_key(['transaction_id']) }} AS sale_key
  ,{{ make_key(['product_id']) }} AS product_key
  ,ingestion_time AS load_dts
  ,'datalake.sales' AS rec_src
FROM
  {{ ref('sales_line_stg') }}
{% endcall %}
