{% call deduplicate(['customer_class_key']) %}
SELECT
  {{ make_key(['id']) }} AS customer_class_key
  ,id AS customer_class_code
  ,created AS load_dts
  ,'datalake.customer_classes' AS rec_src
FROM
  {{ source('datalake', 'customer_classes') }}
{% endcall %}