{% set metadata_yaml -%}
target: 
  key: sale_line_key
  business_keys: ['sale_line_id']
source:
  table: sales_line_stg
  business_keys: ['sales_line_id']
  load_dts: ingestion_time
  rec_src: datalake.sales
{%- endset %}

{{- hub(fromyaml(metadata_yaml)) }}
