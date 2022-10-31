{% call deduplicate(['product_key', 'effective_ts', 'product_name', 'product_color']) %}
SELECT
  {{ make_key(['ean']) }} AS product_key
  ,created AS load_dts
  ,created AS effective_ts
  ,product_name AS product_name
  ,color AS product_color
  ,'datalake.product' AS rec_src
FROM
  {{ source('datalake', 'product') }}
{% endcall %}
