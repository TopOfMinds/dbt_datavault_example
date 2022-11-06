{% set metadata_yaml -%}
target: 
  key: product_key
  attributes: ['effective_ts', 'product_name', 'product_color']
source:
  name: datalake
  table: product
  key: ean
  attributes: ['created', 'product_name', 'color']
  load_dts: created
  rec_src: datalake.product
{%- endset %}

{{- satellite(fromyaml(metadata_yaml)) }}
