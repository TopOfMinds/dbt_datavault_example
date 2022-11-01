{% set metadata_yaml -%}
target: 
  key: sale_line_l_key
  hub_keys: ['sale_line_key', 'sale_key', 'product_key']
sources:
  - type: ref
    table: sales_line_stg
    hubs_business_keys: [['sales_line_id'], ['transaction_id'], ['product_id']]
    load_dts: ingestion_time
    rec_src: datalake.sales
{%- endset %}

{{ link(metadata_yaml) }}