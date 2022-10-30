-- Business sat that connects the customer class directly to the customer ensamble, following the context close to key pattern
-- It also makes it easier to use it in a customer dimension

SELECT
  ch.customer_key
  ,cccls.load_dts
  ,cccls.effective_ts
  ,cccls.effective_ts_end
  ,ccs.customer_class_key
  ,ccs.class_name
  -- ,ccs.class_description
  ,cccls.rec_src
FROM
   {{ ref('customer_h') }} ch
  JOIN  {{ ref('customer_customer_class_l') }} cccl ON cccl.customer_key = ch.customer_key
  JOIN  {{ ref('customer_customer_class_l_sb') }} cccls ON cccls.customer_customer_class_l_key = cccl.customer_customer_class_l_key
  JOIN  {{ ref('customer_class_s') }} ccs ON ccs.customer_class_key = cccl.customer_class_key

