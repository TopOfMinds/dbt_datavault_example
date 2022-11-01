{{
    config(
        materialized='incremental'
    )
}}

{%- call deduplicate(['customer_key']) %}
SELECT
  {{ make_key(['customer_id']) }} AS customer_key
  ,customer_id AS customer_id
  ,ingestion_time AS load_dts
  ,'datalake.sales' AS rec_src
FROM
  {{ source('datalake', 'sales') }}
{% if is_incremental() -%}
WHERE '{{ var('start_ts') }}' <= ingestion_time AND ingestion_time < '{{ var('end_ts') }}'
{%- endif %}
UNION ALL
SELECT
  {{ make_key(['customer_id']) }} AS customer_key
  ,customer_id AS customer_id
  ,ingestion_time AS load_dts
  ,'datalake.customer' AS rec_src
FROM
  {{ source('datalake', 'customer') }}
{% if is_incremental() -%}
WHERE '{{ var('start_ts') }}' <= ingestion_time AND ingestion_time < '{{ var('end_ts') }}'
{%- endif %}
UNION ALL
SELECT
  {{ make_key(['customer_id']) }} AS customer_key
  ,customer_id AS customer_id
  ,ingestion_time AS load_dts
  ,'datalake.custommer_address' AS rec_src
FROM
  {{ source('datalake', 'customer_address') }}
{% if is_incremental() -%}
WHERE '{{ var('start_ts') }}' <= ingestion_time AND ingestion_time < '{{ var('end_ts') }}'
{%- endif -%}
{% endcall %}
