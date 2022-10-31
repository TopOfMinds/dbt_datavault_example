{% call deduplicate(['sale_key']) %}
SELECT
  {{ make_key(['transaction_id']) }} AS sale_key
  ,transaction_id AS sale_id
  ,ingestion_time AS load_dts
  ,'datalake.sales' AS rec_src
FROM
  {{ source('datalake', 'sales') }}
{% endcall %}
