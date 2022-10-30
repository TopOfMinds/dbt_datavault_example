WITH id_ean AS (
  SELECT DISTINCT
    product_id
    ,ean
  FROM
    `datalake.product`
)
SELECT
  ps.product_id
  ,ean
  ,length
  ,width
  ,height
  ,weight
  ,created
  ,dt
FROM
  `datalake.product_sizes` ps
  LEFT JOIN id_ean ie ON ie.product_id = ps.product_id
