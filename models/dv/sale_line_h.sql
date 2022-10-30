SELECT
  sale_line_key
  ,sale_line_id
  ,load_dts
  ,rec_src
FROM (
  SELECT
    sale_line_key
    ,sale_line_id
    ,load_dts
    ,rec_src
    ,row_number() over(PARTITION BY sale_line_key ORDER BY load_dts asc) rn
  FROM (
    
    SELECT
      CAST(sales_line_id AS string) AS sale_line_key
      ,sales_line_id AS sale_line_id
      ,ingestion_time AS load_dts
      ,'datalake.sales' AS rec_src
    FROM
      dv.sales_line_stg_v
  )
)
WHERE rn = 1