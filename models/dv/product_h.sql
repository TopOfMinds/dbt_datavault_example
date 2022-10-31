{% call deduplicate(['product_key']) %}
SELECT
  CAST(product_id AS string) AS product_key
  ,product_id AS ean
  ,ingestion_time AS load_dts
  ,'datalake.sales' AS rec_src
FROM
  {{ ref('sales_line_stg') }}
UNION ALL
SELECT
  CAST(ean AS string) AS product_key
  ,ean AS ean
  ,created AS load_dts
  ,'datalake.product' AS rec_src
FROM
  {{ source('datalake', 'product') }}
UNION ALL
SELECT
  CAST(ean AS string) AS product_key
  ,ean AS ean
  ,created AS load_dts
  ,'datalake.product_sizes' AS rec_src
FROM
  {{ ref('product_sizes_stg') }}
{% endcall %}
