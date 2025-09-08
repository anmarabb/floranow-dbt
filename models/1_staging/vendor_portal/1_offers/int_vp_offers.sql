 with order_items as(
    select offer_number,
           case when count(order_id) > 0 then 'Has Orders' end as order_status,

    from {{ref("fct_vp_order_items")}}
    group by 1
)


select o.*,

       DATE_DIFF(DATE(valid_to), DATE(valid_from), DAY) AS number_of_days,
       max_daily_fulfillment_quantity * DATE_DIFF(DATE(valid_to), DATE(valid_from), DAY) AS total_offered_quantity,

       round(unit_price * max_daily_fulfillment_quantity * DATE_DIFF(DATE(valid_to), DATE(valid_from), DAY),2) as total_offer_price,
       CASE WHEN offer_status = 'active' AND DATE_DIFF(CURRENT_DATE(), DATE(valid_from), DAY) BETWEEN 0 AND 7 THEN 1 ELSE 0 END AS is_new_offer,
       CASE WHEN offer_status = 'active' AND DATE_DIFF(DATE(valid_to), CURRENT_DATE(), DAY) BETWEEN 0 AND 7 THEN 1 ELSE 0 END AS is_expiring_soon,

       oi.order_status,

       CASE
            WHEN EXTRACT(YEAR FROM valid_from) = EXTRACT(YEAR FROM CURRENT_DATE()) AND EXTRACT(MONTH FROM valid_from) = EXTRACT(MONTH FROM CURRENT_DATE()) THEN 'New'
            WHEN EXTRACT(YEAR FROM valid_from) = EXTRACT(YEAR FROM DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH)) AND EXTRACT(MONTH FROM valid_from) = EXTRACT(MONTH FROM DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH)) THEN 'Medium'
            ELSE 'Old'
       END AS freshness_category,



from {{ref("stg_vp_offers")}} o
left join order_items oi on o.offer_number = oi.offer_number