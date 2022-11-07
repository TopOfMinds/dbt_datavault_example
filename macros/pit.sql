{% macro pit(metadata_yaml) -%}
{% set metadata = fromyaml(metadata_yaml) -%}
{% set tgt = metadata.target -%}

WITH time_line AS (
{%- for src in metadata.sources %}
{%- if not loop.first %}UNION DISTINCT {% endif %}
{%- if src.type == 'source' %}
{%- set src_table = source(src.name, src.table) -%}
{%- elif src.type == 'ref' %}
{%- set src_table = ref(src.table) -%}
{%- endif %}
    SELECT
        {{ src.key }} AS {{ tgt.key }}
        ,{{ src.effective_ts }} AS {{ tgt.effective_ts }}
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
){%- for src in metadata.sources %}
 {%- if src.type == 'source' %}
 {%- set src_table = source(src.name, src.table) -%}
 {%- elif src.type == 'ref' %}
 {%- set src_table = ref(src.table) -%}
, {{ src.table }} AS (
    SELECT
        {{ src.key}}
        ,{{ src.load_dts}}
        ,{{ src.effective_ts}}
        ,COALESCE(LEAD({{ src.effective_ts}}) OVER(PARTITION BY  {{ src.key}} ORDER BY {{ src.effective_ts}} ASC), CAST('9999-12-31' AS TIMESTAMP)) AS effective_end_ts
    FROM 
        {{ src_table }}
 {% endif -%}
){% endfor %}
SELECT  
  tl.{{ tgt.key }}
  ,tl.{{ tgt.effective_ts }}
  ,COALESCE(LEAD(tl.{{ tgt.effective_ts }}) OVER(PARTITION BY tl.{{ tgt.key }} ORDER BY tl.{{ tgt.effective_ts }} ASC), CAST('9999-12-31' AS TIMESTAMP)) AS effective_end_ts
  {%- for src in metadata.sources %}
  ,{{ src.table }}.{{ src.load_dts }} AS {{ src.table }}_{{ src.load_dts }}
  {%- endfor %}
FROM 
  time_line tl
{%- for src in metadata.sources %}
{%- if src.type == 'source' %}
{%- set src_table = source(src.name, src.table) -%}
{%- elif src.type == 'ref' %}
{%- set src_table = ref(src.table) -%}
{%- endif %}
LEFT JOIN {{ src.table }} ON tl.{{ tgt.key }} = {{ src.table }}.{{ src.key }} AND {{ src.table }}.effective_ts <=  tl.{{ tgt.effective_ts }} AND tl.{{ tgt.effective_ts }} < {{ src.table }}.effective_end_ts
{%- endfor -%} 
{% endmacro %}