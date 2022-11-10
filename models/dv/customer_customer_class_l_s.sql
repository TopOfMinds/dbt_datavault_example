{% set metadata_yaml -%}
target: 
  hub_key: customer_customer_class_l_key
  attributes: ['effective_ts']
sources:
  - name: datalake
    table: customer_segmentations
    natural_keys: ['customer_id', 'customer_class_id']
    attributes: ['date']
    load_dts: ingestion_time
    rec_src: datalake.customer_segmentations
{%- endset %}

{{- dbt_datavault.satellite(metadata_yaml) }}
