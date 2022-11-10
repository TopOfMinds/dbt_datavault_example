{% set metadata_yaml -%}
target: 
  hub_key: 'customer_key'
  effective_ts: 'effective_ts'
sources:
  - table: 'customer_main_s'
    hub_key: 'customer_key'
    load_dts: 'load_dts'
    effective_ts: 'effective_ts'
  - table: 'customer_address_s'
    hub_key: 'customer_key'
    load_dts: 'load_dts'
    effective_ts: 'effective_ts'
  - table: 'customer_class_sb'
    hub_key: 'customer_key'
    load_dts: 'load_dts'
    effective_ts: 'effective_ts'
{%- endset %}

{{- dbt_datavault.pit(metadata_yaml) }}