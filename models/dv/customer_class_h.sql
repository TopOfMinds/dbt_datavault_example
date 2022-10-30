SELECT
  customer_class_key
  ,customer_class_code
  ,load_dts
  ,rec_src
FROM (
  SELECT
    customer_class_key
    ,customer_class_code
    ,load_dts
    ,rec_src
    ,row_number() over(PARTITION BY customer_class_key ORDER BY load_dts asc) rn
  FROM (
    
    SELECT
      CAST(id AS string) AS customer_class_key
      ,id AS customer_class_code
      ,created AS load_dts
      ,'datalake.customer_classes' AS rec_src
    FROM
      {{ source('datalake', 'customer_classes') }}
  )
)
WHERE rn = 1