{{ config(materialized='incremental') -}}
{% set metadata_yaml -%}
target: 
  key: product_key
  business_keys: ['ean']
sources:
  - table: 'sales_line_stg'
    filter: 1=1
    business_keys: ['product_id']
    load_dts: 'ingestion_time'
    rec_src: 'datalake.sales'
  - name: datalake
    table: product
    business_keys: ['ean']
    load_dts: created
    rec_src: 'datalake.product'
  - table: product_sizes_stg
    business_keys: ['ean']
    load_dts: created
    rec_src: 'datalake.product_sizes'
{%- endset %}

{{- hub(metadata_yaml) }}
