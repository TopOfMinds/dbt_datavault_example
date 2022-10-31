{% call deduplicate(['sale_customer_l_key']) %}
SELECT
  {{ make_key(['transaction_id', 'customer_id']) }} AS sale_customer_l_key
  ,{{ make_key(['transaction_id']) }} AS sale_key
  ,{{ make_key(['customer_id']) }} AS customer_key
  ,ingestion_time AS load_dts
  ,'datalake.sales' AS rec_src
FROM
  {{ source('datalake', 'sales') }}
{% endcall %}
