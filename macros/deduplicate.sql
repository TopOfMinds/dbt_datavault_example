{% macro deduplicate(dedup_fields, order_field='load_dts') -%}
SELECT
  * EXCEPT(rn)
FROM (
  SELECT
    *
    ,row_number() over(PARTITION BY {{ dedup_fields | join(', ') }} ORDER BY {{ order_field }} asc) rn
  FROM (
    {{ caller() | indent(4) }}
  )
)
WHERE rn = 1
{% endmacro %}