{% call deduplicate(['customer_customer_class_l_key']) %}
SELECT
  CAST(customer_id|| '|' ||customer_class_id AS string) AS customer_customer_class_l_key
  ,customer_id AS customer_id
  ,customer_class_id AS customer_class_code
  ,ingestion_time AS load_dts
  ,'datalake.customer_segmentations' AS rec_src
FROM
  {{ source('datalake', 'customer_segmentations') }}
{% endcall %}
