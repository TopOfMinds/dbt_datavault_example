{% set metadata_yaml -%}
target: 
  hub_key: product_key
  attributes: ['effective_ts', 'product_name', 'product_color']
sources:
  - name: datalake
    table: product
    natural_keys: [ean]
    attributes: ['created', 'product_name', 'color']
    load_dts: created
    rec_src: datalake.product
{%- endset %}

{{- dbt_datavault.satellite(metadata_yaml) }}
