WITH calendar AS (
  SELECT day
  FROM UNNEST(GENERATE_DATE_ARRAY('2025-01-01', '2026-09-30', INTERVAL 1 DAY)) AS day
)

SELECT 
  c.day as master_date,
  o.offer_id,
  o.offer_number,
  o.product,
  o.valid_from,
  o.valid_to,
  o.max_daily_fulfillment_quantity,
  o.unit_price,
  case when day = current_date() then available_quantity end as available_quantity,
  product_color,
  stem_length,
  vendor_region,
  product_type_name,
  product_category,
  Farm,
  currency,
  Vendor,
  mainimage_url,
  vendor_status,
  variation_status,
  farm_status,
  offer_status,
  number_of_days,
  is_new_offer,
  is_expiring_soon,
  order_status

FROM calendar c
LEFT JOIN `dbt_prod_dwh.fct_vp_offers` o
  ON c.day BETWEEN DATE(o.valid_from) AND DATE(o.valid_to)