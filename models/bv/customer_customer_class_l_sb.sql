{% set metadata_yaml -%}
target: 
  link_key: customer_customer_class_l_key
  effective_ts: effective_ts
link_source:
    table: customer_customer_class_l
    link_key: customer_customer_class_l_key
    driving_key: customer_key
    other_key: customer_class_key
sat_source:
    table: customer_customer_class_l_s
    link_key: customer_customer_class_l_key
    effective_ts: effective_ts
{%- endset %}

{{- dbt_datavault.driving_key_satellite(metadata_yaml) }}