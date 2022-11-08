{{ config(materialized='incremental') -}}
{% set metadata_yaml -%}
target: 
  key: customer_key
  business_keys: ['customer_id']
sources:
  - name: datalake
    table: sales
    business_keys: ['customer_id']
    load_dts: ingestion_time
    rec_src: datalake.sales
  - name: datalake
    table: customer
    business_keys: ['customer_id']
    load_dts: ingestion_time
    rec_src: datalake.customer
  - name: datalake
    table: customer_address
    business_keys: ['customer_id']
    load_dts: ingestion_time
    rec_src: datalake.custommer_address
{%- endset %}

{{- hub(metadata_yaml) }}
