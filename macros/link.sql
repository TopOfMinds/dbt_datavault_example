{% macro link(metadata_yaml) -%}
{% set metadata = fromyaml(metadata_yaml) -%}
{% set tgt = metadata.target -%}

{% call deduplicate(['sale_line_l_key']) %}
{% for src in metadata.sources %}
{% if not loop.first %}UNION ALL{% endif -%}
{% if src.type == 'source' %}
{% set src_table = source(src.name, src.table) -%}
{% elif src.type == 'ref' %}
{% set src_table = ref(src.table) -%}
{% endif -%}
{% set src_flat_bks = src.hubs_business_keys | sum(start=[]) -%}
SELECT
  {{ make_key(src_flat_bks) }} AS sale_line_l_key
  {% for hub_business_keys, tgt_hub_key in zip(src.hubs_business_keys, tgt.hub_keys) -%}
  ,{{ make_key(hub_business_keys) }} AS {{ tgt_hub_key }}
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
{% endcall %}
{% endmacro %}
