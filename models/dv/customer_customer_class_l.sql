{% set metadata_yaml -%}
target: 
  key: customer_customer_class_l_key
  hub_keys: ['customer_key', 'customer_class_key']
source:
  name: datalake
  table: customer_segmentations
  hubs_business_keys: [['customer_id'], ['customer_class_id']]
  load_dts: ingestion_time
  rec_src: datalake.customer_segmentations
{%- endset %}

{{- link(fromyaml(metadata_yaml)) }}
