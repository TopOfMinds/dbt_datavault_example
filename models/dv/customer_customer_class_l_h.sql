{% set metadata_yaml -%}
target: 
  hub_key: 'customer_customer_class_l_key'
  natural_keys: ['customer_id', 'customer_class_code']
sources:
  - name: 'datalake'
    table: 'customer_segmentations'
    filter: '1=1'
    natural_keys: ['customer_id', 'customer_class_id']
    load_dts: 'ingestion_time'
    rec_src: 'datalake.customer_segmentations'
{%- endset %}

{{- dbt_datavault.hub(metadata_yaml) }}