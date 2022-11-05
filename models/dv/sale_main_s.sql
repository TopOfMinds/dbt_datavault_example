{% set metadata_yaml -%}
target: 
  key: sale_key
  attributes: ['effective_ts', 'amount', 'tax']
source:
  name: datalake
  table: sales
  key: transaction_id
  attributes: ['date', 'CAST(total AS numeric)', 'CAST(tax AS numeric)']
  load_dts: ingestion_time
  rec_src: datalake.sales
{%- endset %}

{{- satellite(fromyaml(metadata_yaml)) }}
