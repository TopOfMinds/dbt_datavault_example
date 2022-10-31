{% call deduplicate(['sale_key', 'effective_ts', 'amount', 'tax']) %}
SELECT
  {{ make_key(['transaction_id']) }} AS sale_key
  ,ingestion_time AS load_dts
  ,date AS effective_ts
  ,CAST(total AS numeric) AS amount
  ,CAST(tax AS numeric) AS tax
  ,'datalake.sales' AS rec_src
FROM
  {{ source('datalake', 'sales') }}
{% endcall %}
