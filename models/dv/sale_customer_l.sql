{{ config(materialized='incremental') -}}
{% set metadata_yaml -%}
target: 
  key: sale_customer_l_key
  hub_keys: ['sale_key', 'customer_key']
source:
  name: datalake
  table: sales
  hubs_business_keys: [['transaction_id'], ['customer_id']]
  load_dts: ingestion_time
  rec_src: datalake.sales
{%- endset %}

{{- link(fromyaml(metadata_yaml)) }}
