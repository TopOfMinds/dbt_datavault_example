SELECT
  sale_key
  ,max(load_dts) AS load_dts
  ,effective_ts
  ,amount
  ,tax
  ,rec_src
FROM (
  
  SELECT
    CAST(transaction_id AS string) AS sale_key
    ,ingestion_time AS load_dts
    ,date AS effective_ts
    ,total AS amount
    ,tax AS tax
    ,'datalake.sales' AS rec_src
  FROM
    datalake.sales
)
GROUP BY
  sale_key
  ,effective_ts
  ,amount
  ,tax
  ,rec_src