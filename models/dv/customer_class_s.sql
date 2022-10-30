SELECT
  customer_class_key
  ,max(load_dts) AS load_dts
  ,effective_ts
  ,class_name
  ,class_description
  ,rec_src
FROM (
  
  SELECT
    CAST(id AS string) AS customer_class_key
    ,created AS load_dts
    ,created AS effective_ts
    ,name AS class_name
    ,description AS class_description
    ,'datalake.customer_classes' AS rec_src
  FROM
    datalake.customer_classes
)
GROUP BY
  customer_class_key
  ,effective_ts
  ,class_name
  ,class_description
  ,rec_src