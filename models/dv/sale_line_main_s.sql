{% set metadata_yaml -%}
target: 
  hub_key: sale_line_key
  attributes: ['effective_ts', 'item_cost', 'item_quantity']
sources:
  - table: sales_line_stg
    natural_keys: [sales_line_id]
    attributes: ['date', 'price', 'quantity']
    load_dts: ingestion_time
    rec_src: datalake.sales
{%- endset %}

{{- dbt_datavault.satellite(metadata_yaml) }}
