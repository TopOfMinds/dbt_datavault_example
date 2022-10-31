{% call deduplicate(['customer_customer_class_l_key', 'effective_ts']) %}
SELECT
  CAST(customer_id|| '|' ||customer_class_id AS string) AS customer_customer_class_l_key
  ,ingestion_time AS load_dts
  ,date AS effective_ts
  ,'datalake.customer_segmentations' AS rec_src
FROM
  {{ source('datalake', 'customer_segmentations') }}
{% endcall %}
