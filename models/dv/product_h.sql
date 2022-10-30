SELECT
  product_key
  ,ean
  ,load_dts
  ,rec_src
FROM (
  SELECT
    product_key
    ,ean
    ,load_dts
    ,rec_src
    ,row_number() over(PARTITION BY product_key ORDER BY load_dts asc) rn
  FROM (
    
    SELECT
      CAST(product_id AS string) AS product_key
      ,product_id AS ean
      ,ingestion_time AS load_dts
      ,'datalake.sales' AS rec_src
    FROM
      dv.sales_line_stg_v
    UNION ALL
    SELECT
      CAST(ean AS string) AS product_key
      ,ean AS ean
      ,created AS load_dts
      ,'datalake.product' AS rec_src
    FROM
      datalake.product
    UNION ALL
    SELECT
      CAST(ean AS string) AS product_key
      ,ean AS ean
      ,created AS load_dts
      ,'datalake.product_sizes' AS rec_src
    FROM
      dv.product_sizes_stg_v
  )
)
WHERE rn = 1