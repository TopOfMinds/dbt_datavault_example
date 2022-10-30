SELECT
  customer_key
  ,load_dts
  ,effective_ts
  ,first_name
  ,last_name
  ,first_name || ' ' || last_name AS full_name
  ,lower(email_address) AS email_address
  ,home_phone
  ,rec_src
FROM dv.customer_main_s
