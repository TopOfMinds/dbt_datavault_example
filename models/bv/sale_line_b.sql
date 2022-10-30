SELECT
  slms.sale_line_key
  ,sll.sale_key
  ,sll.product_key
  ,scl.customer_key
  ,slms.effective_ts
  ,slms.load_dts
  ,slms.rec_src
FROM
  `dv.sale_line_main_s` slms
  JOIN `dv.sale_line_l` sll ON sll.sale_line_key = slms.sale_line_key
  JOIN `dv.sale_customer_l` scl ON scl.sale_key = sll.sale_key

