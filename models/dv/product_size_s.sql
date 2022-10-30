SELECT
  product_key
  ,max(load_dts) AS load_dts
  ,effective_ts
  ,length
  ,width
  ,height
  ,weight
  ,rec_src
FROM (
  
  SELECT
    CAST(ean AS string) AS product_key
    ,created AS load_dts
    ,created AS effective_ts
    ,length AS length
    ,width AS width
    ,height AS height
    ,weight AS weight
    ,'datalake.product_sizes' AS rec_src
  FROM
     {{ ref('product_sizes_stg') }}
)
GROUP BY
  product_key
  ,effective_ts
  ,length
  ,width
  ,height
  ,weight
  ,rec_src