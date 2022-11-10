{% set metadata_yaml -%}
target: 
  hub_key: customer_class_key
  natural_keys: ['customer_class_code']
sources:
  - name: datalake
    table: customer_classes
    natural_keys: ['id']
    load_dts: created
    rec_src: datalake.customer_classes
{%- endset %}

{{- dbt_datavault.hub(metadata_yaml) }}
