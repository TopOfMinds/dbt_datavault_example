{% macro pit(metadata_yaml) -%}
{% set metadata = fromyaml(metadata_yaml) -%}
{% set tgt = metadata.target -%}

WITH pit AS (
    SELECT
        {{ tgt.key }}
        ,{{ tgt.load_dts }}
    FROM (
            {%- for src in metadata.sources %}
            {%- if not loop.first %}UNION ALL {% endif %}
            {%- if src.type == 'source' %}
            {%- set src_table = source(src.name, src.table) -%}
            {%- elif src.type == 'ref' %}
            {%- set src_table = ref(src.table) -%}
            {%- endif %}
            SELECT
            {{ src.key }} AS {{ tgt.key }}
            ,{{ src.load_dts }} AS load_dts
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
    )
    GROUP BY
         {{ tgt.key }}
        ,{{ tgt.load_dts }} 
)
SELECT  
  pit.{{ tgt.key }}
  ,pit.{{ tgt.load_dts }}
  ,COALESCE(LEAD(pit.{{ tgt.load_dts }}) OVER(PARTITION BY pit.{{ tgt.key }} ORDER BY pit.{{ tgt.load_dts }} ASC), CAST('9999-12-31' AS TIMESTAMP)) AS load_end_dts
  {%- for src in metadata.sources %}
  ,MAX({{ src.table }}.{{ src.load_dts }}) OVER(PARTITION BY pit.{{ tgt.key }} ORDER BY pit.{{ tgt.load_dts }} ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS {{ src.table }}_{{ src.load_dts }}
  {%- endfor %}
FROM 
  pit pit
{%- for src in metadata.sources %}
{%- if src.type == 'source' %}
{%- set src_table = source(src.name, src.table) -%}
{%- elif src.type == 'ref' %}
{%- set src_table = ref(src.table) -%}
{%- endif %}
LEFT JOIN {{ src_table }} {{ src.table }} ON pit.{{ tgt.key }} = {{ src.table }}.{{ src.key }} AND pit.{{ tgt.load_dts }} =  {{ src.table }}.{{ src.load_dts }}
{%- endfor -%} 
{% endmacro %}