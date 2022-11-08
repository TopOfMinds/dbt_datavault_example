{% set metadata_yaml -%}
target: 
  key: customer_class_key
  business_keys: ['customer_class_code']
source:
  name: datalake
  table: customer_classes
  business_keys: ['id']
  load_dts: created
  rec_src: datalake.customer_classes
{%- endset %}

{{- hub(metadata_yaml) }}
