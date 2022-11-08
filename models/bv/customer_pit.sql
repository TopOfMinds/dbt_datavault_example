{% set metadata_yaml -%}
target: 
  key: 'customer_key'
  effective_ts: 'effective_ts'
sources:
  - type: ref
    table: 'customer_main_s'
    key: 'customer_key'
    load_dts: 'load_dts'
    effective_ts: 'effective_ts'
  - type: ref
    table: 'customer_address_s'
    key: 'customer_key'
    load_dts: 'load_dts'
    effective_ts: 'effective_ts'
  - type: ref
    table: 'customer_class_sb'
    key: 'customer_key'
    load_dts: 'load_dts'
    effective_ts: 'effective_ts'
{%- endset %}

{{- dbt_datavault.pit(metadata_yaml) }}