{% macro deduplicate(dedup_fields, order_field='load_dts') -%}
SELECT
  * EXCEPT(rn)
FROM (
  SELECT
    *
    ,ROW_NUMBER() OVER(PARTITION BY {{ dedup_fields | join(', ') }} ORDER BY {{ order_field }} ASC) rn
  FROM (
    {{- caller() | indent(4) }}
  ) AS q
  {%- if is_incremental() %}
  WHERE NOT EXISTS (
    SELECT 
      1
    FROM
      {{ this }} AS t
    WHERE 
      {%- for dedup_field in dedup_fields %}
      COALESCE(CAST(t.{{ dedup_field }} AS STRING), '#') = COALESCE(CAST(q.{{ dedup_field }} AS STRING), '#'){% if not loop.last %} AND{% endif %}
      {%- endfor %} 
  )
  {%- endif %}  
)
WHERE rn = 1
{% endmacro %}