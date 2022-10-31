{% call deduplicate(['customer_customer_class_l_key']) %}
SELECT
  {{ make_key(['customer_id', 'customer_class_id']) }} AS customer_customer_class_l_key
  ,customer_id AS customer_id
  ,customer_class_id AS customer_class_code
  ,ingestion_time AS load_dts
  ,'datalake.customer_segmentations' AS rec_src
FROM
  {{ source('datalake', 'customer_segmentations') }}
{% endcall %}
