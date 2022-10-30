SELECT
  l.customer_customer_class_l_key
  ,GREATEST(l.load_dts, s.load_dts) AS load_dts
  ,s.effective_ts
  ,COALESCE(LEAD(s.effective_ts) OVER(PARTITION BY l.customer_key ORDER BY s.effective_ts), TIMESTAMP('9999-12-31')) AS effective_ts_end
  ,s.rec_src
FROM `dv.customer_customer_class_l` l
LEFT JOIN `dv.customer_customer_class_l_s` s on s.customer_customer_class_l_key = l.customer_customer_class_l_key

