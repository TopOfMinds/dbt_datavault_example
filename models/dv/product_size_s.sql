{% call deduplicate(['product_key', 'effective_ts', 'length', 'width', 'height', 'weight']) %}
SELECT
  CAST(ean AS string) AS product_key
  ,created AS load_dts
  ,created AS effective_ts
  ,length AS length
  ,width AS width
  ,height AS height
  ,weight AS weight
  ,'datalake.product_sizes' AS rec_src
FROM
  {{ ref('product_sizes_stg') }}
{% endcall %}
