{% set metadata_yaml -%}
target: 
  key: customer_class_key
  attributes: ['effective_ts', 'class_name', 'class_description']
source:
  name: datalake
  table: customer_classes
  key: id
  attributes: ['created', 'name', 'description']
  load_dts: created
  rec_src: datalake.customer_classes
{%- endset %}

{{- dbt_datavault.satellite(metadata_yaml) }}
