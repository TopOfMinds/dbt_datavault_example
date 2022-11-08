{% set metadata_yaml -%}
target: 
  key: sale_line_key
  attributes: ['effective_ts', 'item_cost', 'item_quantity']
source:
  table: sales_line_stg
  key: sales_line_id
  attributes: ['date', 'price', 'quantity']
  load_dts: ingestion_time
  rec_src: datalake.sales
{%- endset %}

{{- satellite(metadata_yaml) }}
