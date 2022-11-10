{% set metadata_yaml -%}
target: 
  hub_key: sale_line_key
  natural_keys: ['sale_line_id']
sources:
  - table: sales_line_stg
    natural_keys: ['sales_line_id']
    load_dts: ingestion_time
    rec_src: datalake.sales
{%- endset %}

{{- dbt_datavault.hub(metadata_yaml) }}
