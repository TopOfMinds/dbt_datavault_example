{% set metadata_yaml -%}
target: 
  key: customer_customer_class_l_key
  attributes: ['effective_ts']
source:
  name: datalake
  table: customer_segmentations
  key: customer_id
  keys_: ['customer_id', 'customer_class_id']
  attributes: ['date']
  load_dts: ingestion_time
  rec_src: datalake.customer_segmentations
{%- endset %}

{{- dbt_datavault.satellite(metadata_yaml) }}
