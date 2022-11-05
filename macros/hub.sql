{% macro hub(metadata) -%}
{% set tgt = metadata.target -%}
{% set sources = metadata.sources if metadata.sources else [metadata.source] -%}
{% set all_fields = [tgt.key] + tgt.business_keys + ['load_dts', 'rec_src'] -%}

{%- call deduplicate([tgt.key], all_fields) -%}
{% for src in sources %}
{% if not loop.first %}UNION ALL{% endif %}
{% set src_table = source(src.name, src.table) if src.name else ref(src.table) -%}
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