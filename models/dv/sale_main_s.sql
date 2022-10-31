{% call deduplicate(['sale_key', 'effective_ts', 'amount', 'tax']) %}
SELECT
  CAST(transaction_id AS string) AS sale_key
  ,ingestion_time AS load_dts
  ,date AS effective_ts
  ,CAST(total AS numeric) AS amount
  ,CAST(tax AS numeric) AS tax
  ,'datalake.sales' AS rec_src
FROM
  {{ source('datalake', 'sales') }}
{% endcall %}
