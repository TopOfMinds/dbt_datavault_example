{% set metadata_yaml -%}
target: 
  key: 'customer_key'
  load_dts: 'load_dts'
sources:
  - type: ref
    table: 'customer_main_s'
    key: 'customer_key'
    load_dts: 'load_dts'
  - type: ref
    table: 'customer_address_s'
    key: 'customer_key'
    load_dts: 'load_dts'
  - type: ref
    table: 'customer_class_sb'
    key: 'customer_key'
    load_dts: 'load_dts'
{%- endset %}

{{- pit(metadata_yaml) }}