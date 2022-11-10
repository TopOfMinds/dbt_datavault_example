{% set metadata_yaml -%}
target: 
  link_key: customer_customer_class_l_key
  hub_keys: ['customer_key', 'customer_class_key']
sources:
  - name: datalake
    table: customer_segmentations
    hub_natural_keys: [['customer_id'], ['customer_class_id']]
    load_dts: ingestion_time
    rec_src: datalake.customer_segmentations
{%- endset %}

{{- dbt_datavault.link(metadata_yaml) }}
