{% set metadata_yaml -%}
target: 
  link_key: sale_line_l_key
  hub_keys: ['sale_line_key', 'sale_key', 'product_key']
sources:
  - table: sales_line_stg
    hub_natural_keys: [['sales_line_id'], ['transaction_id'], ['product_id']]
    load_dts: ingestion_time
    rec_src: datalake.sales
{%- endset %}

{{- dbt_datavault.link(metadata_yaml) }}