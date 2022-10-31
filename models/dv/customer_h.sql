{% call deduplicate(['customer_key']) %}
SELECT
  {{ make_key(['customer_id']) }} AS customer_key
  ,customer_id AS customer_id
  ,ingestion_time AS load_dts
  ,'datalake.sales' AS rec_src
FROM
  {{ source('datalake', 'sales') }}
UNION ALL
SELECT
  {{ make_key(['customer_id']) }} AS customer_key
  ,customer_id AS customer_id
  ,ingestion_time AS load_dts
  ,'datalake.customer' AS rec_src
FROM
  {{ source('datalake', 'customer') }}
UNION ALL
SELECT
  {{ make_key(['customer_id']) }} AS customer_key
  ,customer_id AS customer_id
  ,ingestion_time AS load_dts
  ,'datalake.custommer_address' AS rec_src
FROM
  {{ source('datalake', 'customer_address') }}
{% endcall %}
