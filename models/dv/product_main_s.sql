SELECT
  product_key
  ,max(load_dts) AS load_dts
  ,effective_ts
  ,product_name
  ,product_color
  ,rec_src
FROM (
  
  SELECT
    CAST(ean AS string) AS product_key
    ,created AS load_dts
    ,created AS effective_ts
    ,product_name AS product_name
    ,color AS product_color
    ,'datalake.product' AS rec_src
  FROM
    datalake.product
)
GROUP BY
  product_key
  ,effective_ts
  ,product_name
  ,product_color
  ,rec_src