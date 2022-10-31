{% call deduplicate(['sale_customer_l_key']) %}
SELECT
  transaction_id || '|' || customer_id AS sale_customer_l_key
  ,CAST(transaction_id AS string) AS sale_key
  ,CAST(customer_id AS string) AS customer_key
  ,ingestion_time AS load_dts
  ,'datalake.sales' AS rec_src
FROM
  {{ source('datalake', 'sales') }}
{% endcall %}
