{% call deduplicate(['sale_line_key', 'effective_ts', 'item_cost', 'item_quantity']) %}
SELECT
  {{ make_key(['sales_line_id']) }} AS sale_line_key
  ,ingestion_time AS load_dts
  ,date AS effective_ts
  ,price AS item_cost
  ,quantity AS item_quantity
  ,'datalake.sales' AS rec_src
FROM
  {{ ref('sales_line_stg') }}
{% endcall %}
