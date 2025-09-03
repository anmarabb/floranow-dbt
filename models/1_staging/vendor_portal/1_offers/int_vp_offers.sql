select *,
       DATE_DIFF(DATE(valid_to), DATE(valid_from), DAY) AS number_of_days,
       max_daily_fulfillment_quantity * DATE_DIFF(DATE(valid_to), DATE(valid_from), DAY) AS total_offered_quantity,

       round(unit_price * max_daily_fulfillment_quantity * DATE_DIFF(DATE(valid_to), DATE(valid_from), DAY),2) as total_offer_price,
       CASE WHEN offer_status = 'active' AND DATE_DIFF(CURRENT_DATE(), DATE(valid_from), DAY) BETWEEN 0 AND 7 THEN 1 ELSE 0 END AS is_new_offer,
       CASE WHEN offer_status = 'active' AND DATE_DIFF(DATE(valid_to), CURRENT_DATE(), DAY) BETWEEN 0 AND 7 THEN 1 ELSE 0 END AS is_expiring_soon,

from {{ref("stg_vp_offers")}}