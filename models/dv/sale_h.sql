{% set metadata_yaml -%}
target: 
  key: sale_key
  business_keys: ['sale_id']
source:
  name: datalake
  table: sales
  business_keys: ['transaction_id']
  load_dts: ingestion_time
  rec_src: datalake.sales
{%- endset %}

{{- dbt_datavault.hub(metadata_yaml) }}
