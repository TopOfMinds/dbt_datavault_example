{% set metadata_yaml -%}
target: 
  hub_key: sale_key
  attributes: ['effective_ts', 'amount', 'tax']
sources:
  - name: datalake
    table: sales
    natural_keys: [transaction_id]
    attributes: ['date', 'CAST(total AS numeric)', 'CAST(tax AS numeric)']
    load_dts: ingestion_time
    rec_src: datalake.sales
{%- endset %}

{{- dbt_datavault.satellite(metadata_yaml) }}
