{% set metadata_yaml -%}
target: 
  key: customer_key
  attributes: ['effective_ts', 'address', 'city', 'county', 'postal_code', 'country']
source:
  name: datalake
  table: customer_address
  key: customer_id
  attributes: ['date', 'address', 'city', 'county', 'postal_code', 'country']
  load_dts: ingestion_time
  rec_src: datalake.customer_address
{%- endset %}

{{- dbt_datavault.satellite(metadata_yaml) }}
