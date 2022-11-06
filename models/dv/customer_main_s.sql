{{ config(materialized='incremental') -}}
{% set metadata_yaml -%}
target: 
  key: customer_key
  attributes: ['effective_ts', 'first_name', 'last_name', 'email_address', 'home_phone']
source:
  name: datalake
  table: customer
  key: customer_id
  attributes: ['date', 'first_name', 'last_name', 'email', 'cell_number']
  load_dts: ingestion_time
  rec_src: datalake.customer
{%- endset %}

{{- satellite(fromyaml(metadata_yaml)) }}
