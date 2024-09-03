with invoices as (
    select cli.parent_line_item_id,
           sum(case when cli.order_type = "PICKED_ORDER" then ii.quantity end) as sold_quantity,
           sum(case when cli.order_type != "PICKED_ORDER" then ii.quantity end) as outside_sold_quantity,
           sum(case when cli.order_type = "PICKED_ORDER" then ii.price_without_tax end) as price_without_tax,
           sum(case when cli.order_type != "PICKED_ORDER" then ii.price_without_tax end) as outside_price_without_tax,
           sum(case when cli.order_type = "PICKED_ORDER" and DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH), MONTH) = DATE_TRUNC(cli.departure_date, MONTH) then ii.quantity end) as last_month_sold_quantity
    from {{ref('fct_order_items')}} as li
    join {{ref('fct_order_items')}} as cli on li.line_item_id = cli.parent_line_item_id 
    join {{ref('fct_invoice_items')}} as ii on ii.line_item_id = cli.line_item_id
 -- where cli.order_type = "PICKED_ORDER"
    group by cli.parent_line_item_id
  
),

product_location as (
    select locationable_id,
           sum(case when pl.Location = 'A1 - X' then pl.location_remaining_quantity end) as remaining_qty_A1_X,
           sum(case when pl.Location = 'X - FN' then pl.location_remaining_quantity end) as remaining_qty_X_FN,
           sum(case when pl.Location = 'A1 - X' then pl.location_quantity end) as entered_qty_A1_X,
    from {{ref('fct_product_locations')}} as pl
  --where pl.locationable_type = "Product"
    group by pl.locationable_id
),
category_target as(
    select category_linking,
           sum(MQS) as MQS
    from {{ref('fct_category_mqs')}}
    group by 1
),
targets as (
    with data as (
        SELECT li.Product,
               li.warehouse,
               sum(ii.last_month_sold_quantity) / DATE_DIFF(DATE_TRUNC(CURRENT_DATE(), MONTH), DATE_SUB(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 1 MONTH), DAY) * 7 as weekly_sold_quantity,
        FROM {{ref('fct_order_items')}} li
        left join invoices as ii on ii.parent_line_item_id = li.line_item_id
        group by 1, 2)

    SELECT Product,
           warehouse,
           ROUND(SUM(weekly_sold_quantity) * (1 + (0.20 * (MOD(FARM_FINGERPRINT(CONCAT(Product, warehouse)), 2) * 2 - 1))), -- ±20% adjustment
            2) AS weekly_target,
           ROUND(SUM(weekly_sold_quantity) * (1 + (0.20 * (MOD(FARM_FINGERPRINT(CONCAT(Product, warehouse)), 2) * 2 - 1))), -- ±20% adjustment
            2) / 7 * 2 AS wadi_target,
    FROM data
    group by 1, 2)
-- , data as (
select li.Product, 
       li.li_category_linking,
       li.warehouse,
       li.product_color,
       li.product_subcategory as product_main_group,
       li.product_subgroup as product_sub_group,
      --  CAST(200 + RAND() * (1000 - 200) AS INT64) AS random_value,
      --  200 + CAST(FLOOR(800 * (ABS(MOD(FARM_FINGERPRINT(CAST(li.Product AS STRING)), 10000)) / 10000.0)) AS INT64) AS weekly_demand,
       
       ceil(round(sum(ii.last_month_sold_quantity)/date_diff(date_trunc(current_date(), month), date_sub(date_trunc(current_date(), month), interval 1 month), day),2)) as daily_demand,
       ceil(sum(ii.last_month_sold_quantity)/date_diff(date_trunc(current_date(), month), date_sub(date_trunc(current_date(), month), interval 1 month), day) * 7) as weekly_demand,
       ceil(min(ct.MQS)/count(*) OVER (PARTITION BY li.li_category_linking)) as category_weekly_target,
    --    ceil(min(t.weekly_target)) as weekly_target,

       --case when DATE_DIFF(MAX(li.departure_date), MIN(li.departure_date), DAY) = 0 then 1 else DATE_DIFF(MAX(li.departure_date), MIN(li.departure_date), DAY) end as date_range,
       
       sum(ii.sold_quantity) as total_sold_qty,
       
    --    DATE_ADD(CURRENT_DATE(), INTERVAL MOD(5 - EXTRACT(DAYOFWEEK FROM CURRENT_DATE()) + 7, 7) DAY) AS first_shipment_date,
    --    ceil(round(min(t.weekly_target) *0.15,2)) as first_shipment_qty,
    --    DATE_ADD(CURRENT_DATE(), INTERVAL MOD(5 - EXTRACT(DAYOFWEEK FROM CURRENT_DATE()) + 7, 7) + 4 DAY) AS second_shipment_date,
    --    ceil(round(min(t.weekly_target) *0.85,2)) as second_shipment_qty,
       
       sum(remaining_qty_A1_X) as wadi_stock,
    --    ceil(min(t.wadi_target)) as wadi_target,

       min(ct.MQS)/count(*) OVER (PARTITION BY li.li_category_linking)*2/7 as category_wadi_target,
    --    ceil(min(t.wadi_target)) as wadi_target,

       sum(remaining_qty_X_FN) as sullay_stock,
       date_diff(DATE_ADD(CURRENT_DATE(), INTERVAL MOD(5 - EXTRACT(DAYOFWEEK FROM CURRENT_DATE()) + 7, 7) DAY), current_date(), day) as days_to_next_departure,
       sum(ii.last_month_sold_quantity)/date_diff(date_trunc(current_date(), month), date_sub(date_trunc(current_date(), month), interval 1 month), day) * date_diff(DATE_ADD(CURRENT_DATE(), INTERVAL MOD(5 - EXTRACT(DAYOFWEEK FROM CURRENT_DATE()) + 7, 7) DAY), current_date(), day) - min(ct.MQS)/count(*) OVER (PARTITION BY li.li_category_linking)*2/7 as category_sullay_target,
       
       sum(remaining_qty_A1_X) + sum(remaining_qty_X_FN) as total_qty, 
       floor((sum(remaining_qty_A1_X) + sum(remaining_qty_X_FN))/(sum(ii.last_month_sold_quantity)/date_diff(date_trunc(current_date(), month), date_sub(date_trunc(current_date(), month), interval 1 month), day))) as stock_enough_for,
       floor((sum(remaining_qty_A1_X) + sum(remaining_qty_X_FN))/(sum(ii.last_month_sold_quantity)/date_diff(date_trunc(current_date(), month), date_sub(date_trunc(current_date(), month), interval 1 month), day))) - date_diff(DATE_ADD(CURRENT_DATE(), INTERVAL MOD(5 - EXTRACT(DAYOFWEEK FROM CURRENT_DATE()) + 7, 7) DAY), current_date(), day) as stock_for,
       case 
            when floor((sum(remaining_qty_A1_X) + sum(remaining_qty_X_FN))/(sum(ii.last_month_sold_quantity)/date_diff(date_trunc(current_date(), month), date_sub(date_trunc(current_date(), month), interval 1 month), day))) - date_diff(DATE_ADD(CURRENT_DATE(), INTERVAL MOD(5 - EXTRACT(DAYOFWEEK FROM CURRENT_DATE()) + 7, 7) DAY), current_date(), day) - date_diff(DATE_ADD(CURRENT_DATE(), INTERVAL MOD(5 - EXTRACT(DAYOFWEEK FROM CURRENT_DATE()) + 7, 7) DAY), current_date(), day) > 0 then 'Transfer stock'
            when floor((sum(remaining_qty_A1_X) + sum(remaining_qty_X_FN))/(sum(ii.last_month_sold_quantity)/date_diff(date_trunc(current_date(), month), date_sub(date_trunc(current_date(), month), interval 1 month), day))) - date_diff(DATE_ADD(CURRENT_DATE(), INTERVAL MOD(5 - EXTRACT(DAYOFWEEK FROM CURRENT_DATE()) + 7, 7) DAY), current_date(), day) <0 then 'Local Purchase'
            else 'No Action' end as Action

from {{ref('fct_order_items')}} as li
left join {{ref('fct_products')}} as p on li.line_item_id = p.line_item_id
left join product_location as pl on p.product_id = pl.locationable_id
-- left join pi on pi.line_item_id = li.line_item_id
left join invoices as ii on ii.parent_line_item_id = li.line_item_id 
-- left join stock_movement as sm on sm.product_id = li.product_id
-- left join targets as t on t.Product = li.Product and li.warehouse = t.warehouse
left join category_target ct on ct.category_linking = li.li_category_linking

where li.Reseller in ('RUH Project X Stock', 'DMM Project X Stock') and li.order_type != "PICKED_ORDER" --and li.Product = 'Rose Athena'
group by 1, 2 , 3, 4, 5, 6

-- )
-- select count(*)
-- from data