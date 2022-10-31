{% call deduplicate(['customer_class_key', 'effective_ts', 'class_name', 'class_description']) %}
SELECT
  {{ make_key(['id']) }} AS customer_class_key
  ,created AS load_dts
  ,created AS effective_ts
  ,name AS class_name
  ,description AS class_description
  ,'datalake.customer_classes' AS rec_src
FROM
  {{ source('datalake', 'customer_classes') }}
{% endcall %}
