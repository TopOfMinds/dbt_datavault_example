{{
    config(
        materialized='incremental'
    )
}}

{%- call deduplicate(['customer_key', 'effective_ts', 'first_name', 'last_name', 'email_address', 'home_phone']) %}
SELECT
  {{ make_key(['customer_id']) }} AS customer_key
  ,ingestion_time AS load_dts
  ,date AS effective_ts
  ,first_name AS first_name
  ,last_name AS last_name
  ,email AS email_address
  ,cell_number AS home_phone
  ,'datalake.customer' AS rec_src
FROM
  {{ source('datalake', 'customer') }}
{% if is_incremental() -%}
WHERE '{{ var('start_ts') }}' <= ingestion_time AND ingestion_time < '{{ var('end_ts') }}'
{%- endif -%}
{%- endcall -%}