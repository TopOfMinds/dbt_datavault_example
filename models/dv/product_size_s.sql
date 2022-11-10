{% set metadata_yaml -%}
target: 
  hub_key: product_key
  attributes: ['effective_ts', 'length', 'width', 'height', 'weight']
sources:
  - table: product_sizes_stg
    natural_keys: [ean]
    attributes: ['created', 'length', 'width', 'height', 'weight']
    load_dts: created
    rec_src: datalake.product_sizes
{%- endset %}

{{- dbt_datavault.satellite(metadata_yaml) }}
