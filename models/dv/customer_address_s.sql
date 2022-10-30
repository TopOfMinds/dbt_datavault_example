SELECT
  customer_key
  ,max(load_dts) AS load_dts
  ,effective_ts
  ,address
  ,city
  ,county
  ,postal_code
  ,country
  ,rec_src
FROM (
  
  SELECT
    CAST(customer_id AS string) AS customer_key
    ,ingestion_time AS load_dts
    ,date AS effective_ts
    ,address AS address
    ,city AS city
    ,county AS county
    ,postal_code AS postal_code
    ,country AS country
    ,'datalake.customer_address' AS rec_src
  FROM
    {{ source('datalake', 'customer_address') }}
)
GROUP BY
  customer_key
  ,effective_ts
  ,address
  ,city
  ,county
  ,postal_code
  ,country
  ,rec_src