{% macro satellite(metadata_yaml) -%}
{%- set metadata = fromyaml(metadata_yaml) -%}
{%- set tgt = metadata.target -%}
{%- set src = metadata.source -%}
{%- if src.type == 'source' -%}
  {%- set src_table = source(src.name, src.table) -%}
{%- elif src.type == 'ref' -%}
  {%- set src_table = ref(src.table) -%}
{%- endif -%}

{%- call deduplicate([tgt.key] + tgt.attributes) %}
SELECT
  {{ make_key([src.key]) }} AS {{ tgt.key }}
  ,{{ src.load_dts }} AS load_dts
  {% for src_attr, tgt_attr in zip(src.attributes, tgt.attributes) -%}
  ,{{ src_attr }} AS {{ tgt_attr }}
  {% endfor -%}
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
{% endcall -%}
{% endmacro %}