{% set metadata_yaml -%}
target: 
  key: 'customer_customer_class_l_key'
  business_keys: ['customer_id', 'customer_class_code']
source:
  name: 'datalake'
  table: 'customer_segmentations'
  filter: '1=1'
  business_keys: ['customer_id', 'customer_class_id']
  load_dts: 'ingestion_time'
  rec_src: 'datalake.customer_segmentations'
{%- endset %}

{{- hub(fromyaml(metadata_yaml)) }}