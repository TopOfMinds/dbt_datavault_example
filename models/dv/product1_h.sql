{% set metadata_yaml -%}
target: 
  key: product_key
  business_keys: ['ean']
sources:
  - type: ref
    table: 'sales_line_stg'
    business_keys: ['product_id']
    load_dts: 'ingestion_time'
    rec_src: 'datalake.sales'
  - type: source
    name: datalake
    table: product
    business_keys: ['ean']
    load_dts: created
    rec_src: 'datalake.product'
  - type: ref
    table: product_sizes_stg
    business_keys: ['ean']
    load_dts: created
    rec_src: 'datalake.product_sizes'
{%- endset %}

{{- hub(metadata_yaml) }}
