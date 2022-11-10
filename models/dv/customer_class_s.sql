{% set metadata_yaml -%}
target: 
  hub_key: customer_class_key
  attributes: ['effective_ts', 'class_name', 'class_description']
sources:
  - name: datalake
    table: customer_classes
    natural_keys: [id]
    attributes: ['created', 'name', 'description']
    load_dts: created
    rec_src: datalake.customer_classes
{%- endset %}

{{- dbt_datavault.satellite(metadata_yaml) }}
