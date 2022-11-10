{{ config(materialized='incremental') -}}
{% set metadata_yaml -%}
target: 
  hub_key: customer_key
  natural_keys: ['customer_id']
sources:
  - name: datalake
    table: sales
    natural_keys: ['customer_id']
    load_dts: ingestion_time
    rec_src: datalake.sales
  - name: datalake
    table: customer
    natural_keys: ['customer_id']
    load_dts: ingestion_time
    rec_src: datalake.customer
  - name: datalake
    table: customer_address
    natural_keys: ['customer_id']
    load_dts: ingestion_time
    rec_src: datalake.custommer_address
{%- endset %}

{{- dbt_datavault.hub(metadata_yaml) }}
