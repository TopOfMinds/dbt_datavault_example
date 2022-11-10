{{ config(materialized='incremental') -}}
{% set metadata_yaml -%}
target: 
  link_key: sale_customer_l_key
  hub_keys: ['sale_key', 'customer_key']
sources:
  - name: datalake
    table: sales
    hub_natural_keys: [['transaction_id'], ['customer_id']]
    load_dts: ingestion_time
    rec_src: datalake.sales
{%- endset %}

{{- dbt_datavault.link(metadata_yaml) }}
