{% call deduplicate(['sale_line_key']) %}
SELECT
  CAST(sales_line_id AS string) AS sale_line_key
  ,sales_line_id AS sale_line_id
  ,ingestion_time AS load_dts
  ,'datalake.sales' AS rec_src
FROM
  {{ ref('sales_line_stg') }}
{% endcall %}
