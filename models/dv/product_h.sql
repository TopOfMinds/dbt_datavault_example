{{ config(materialized='incremental') -}}
{% set metadata_yaml -%}
target: 
  hub_key: product_key
  natural_keys: ['ean']
sources:
  - table: 'sales_line_stg'
    filter: 1=1
    natural_keys: ['product_id']
    load_dts: 'ingestion_time'
    rec_src: 'datalake.sales'
  - name: datalake
    table: product
    natural_keys: ['ean']
    load_dts: created
    rec_src: 'datalake.product'
  - table: product_sizes_stg
    natural_keys: ['ean']
    load_dts: created
    rec_src: 'datalake.product_sizes'
{%- endset %}

{{- dbt_datavault.hub(metadata_yaml) }}
