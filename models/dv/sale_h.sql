{% set metadata_yaml -%}
target: 
  hub_key: sale_key
  natural_keys: ['sale_id']
sources:
  - name: datalake
    table: sales
    natural_keys: ['transaction_id']
    load_dts: ingestion_time
    rec_src: datalake.sales
{%- endset %}

{{- dbt_datavault.hub(metadata_yaml) }}
