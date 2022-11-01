{% macro hub(metadata_yaml) -%}
{% set metadata = fromyaml(metadata_yaml) -%}
{% set tgt = metadata.target -%}

{%- call deduplicate([tgt.key]) -%}
{% for src in metadata.sources %}
{% if not loop.first %}UNION ALL{% endif -%}
{% if src.type == 'source' %}
{% set src_table = source(src.name, src.table) -%}
{% elif src.type == 'ref' %}
{% set src_table = ref(src.table) -%}
{% endif -%}
SELECT
  {{ make_key(src.business_keys) }} AS {{ tgt.key }}
  {% for src_business_key, tgt_business_key in zip(src.business_keys, tgt.business_keys) -%}
  ,{{ src_business_key }} AS {{ tgt_business_key }}
  {% endfor -%}
  ,{{ src.load_dts }} AS load_dts
  ,'{{ src.rec_src }}' AS rec_src
FROM
  {{ src_table }}
{% if src.filter or is_incremental() -%}
WHERE
  {% set and_ = joiner("AND ") -%}
  {% if src.filter -%}
  {{ and_() }}{{ src.filter }}
  {% endif -%}
  {% if is_incremental() -%}
  {{ and_() }}'{{ var('start_ts') }}' <= {{ src.load_dts }} AND {{ src.load_dts }} < '{{ var('end_ts') }}'
  {% endif -%}
{% endif -%}
{% endfor -%}
{% endcall -%}
{% endmacro %}
