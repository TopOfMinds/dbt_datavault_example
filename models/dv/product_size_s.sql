{% set metadata_yaml -%}
target: 
  key: product_key
  attributes: ['effective_ts', 'length', 'width', 'height', 'weight']
source:
  table: product_sizes_stg
  key: ean
  attributes: ['created', 'length', 'width', 'height', 'weight']
  load_dts: created
  rec_src: datalake.product_sizes
{%- endset %}

{{- satellite(fromyaml(metadata_yaml)) }}
