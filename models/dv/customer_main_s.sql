{{ config(materialized='incremental') -}}
{% set metadata_yaml -%}
target: 
  hub_key: customer_key
  attributes: ['effective_ts', 'first_name', 'last_name', 'email_address', 'home_phone']
sources:
  - name: datalake
    table: customer
    natural_keys: [customer_id]
    attributes: ['date', 'first_name', 'last_name', 'email', 'cell_number']
    load_dts: ingestion_time
    rec_src: datalake.customer
{%- endset %}

{{- dbt_datavault.satellite(metadata_yaml) }}
