{% call deduplicate(['customer_key', 'effective_ts', 'first_name', 'last_name', 'email_address', 'home_phone']) %}
SELECT
  CAST(customer_id AS string) AS customer_key
  ,ingestion_time AS load_dts
  ,date AS effective_ts
  ,first_name AS first_name
  ,last_name AS last_name
  ,email AS email_address
  ,cell_number AS home_phone
  ,'datalake.customer' AS rec_src
FROM
  {{ source('datalake', 'customer') }}
{% endcall %}
