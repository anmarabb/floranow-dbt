with monthly_demand as (

                with anmar as (
                    select
                        Product,
                        warehouse,
                        Supplier,
                    -- Supplier,
                        year_month_departure_date,
                        count(distinct year_month_departure_date) over (partition by Product, warehouse)  as months_count,
                        sum(sold_quantity) as total_demand_for_month_supplier, -- Total demand for each month and origin
                        sum(i_sold_quantity) as i_total_demand_for_month_supplier,
                        sum(case when extract(year from year_month_departure_date) = extract(year from current_date) -1  then i_sold_quantity end) as i_total_demand_for_month_last_year_supplier,
                       -- COALESCE(sum(sold_quantity),0) as monthly_demand,
                        avg(lead_time)/30.5 as month_lead_time,
                        avg(lead_time) as lead_time,
                        avg(lead_time_2023)/30.5 as month_lead_time_last_year,

                    from {{ref('fct_products')}} as p 
                    where  stock_model in ('Reselling', 'Commission Based', 'Internal - Project X')
                  --and p.Product = 'Rose Ever Red'
                 -- and p.warehouse='Dubai Warehouse'
                 --and year_month_departure_date = '2023-10-01'


                    group by 1, 2,3,4
                ),

                aggregated as (
                    select
                        Product,
                        warehouse,
                        Supplier,
                        sum(total_demand_for_month_supplier) as total_demand_by_supplier, -- Total demand per origin
                        sum(i_total_demand_for_month_supplier) as i_total_demand_by_supplier,
                        sum(i_total_demand_for_month_last_year_supplier) as i_total_demand_by_supplier_last_year,
                        months_count
                    from anmar
                    group by Product, warehouse, Supplier, months_count
                              )


                        select 
                        Product,  
                        warehouse,
                        Supplier,
                        --Supplier,
                       SAFE_DIVIDE(total_demand_by_supplier,months_count) as avg_monthly_demand, -- Calculate average monthly demand per origin
                       SAFE_DIVIDE(i_total_demand_by_supplier,months_count) as i_avg_monthly_demand,
                       SAFE_DIVIDE(i_total_demand_by_supplier_last_year,12) as i_avg_monthly_demand_last_year


                        --stddev_pop (md.monthly_demand) as std_dev_monthly_demand,
/*
                        CASE
                        WHEN AVG(month_lead_time) < 0 THEN SQRT(1)
                            ELSE SQRT(AVG(month_lead_time))
                        END AS sqrt_avg_lead_time_per_month,

*/

                        from aggregated as md

          ), 
last_year_demand as (
    SELECT 
    Product,
    warehouse,
    Supplier,
    SUM(CASE WHEN year_month_departure_date = DATE_SUB(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 1 YEAR) THEN sold_quantity ELSE 0 END) AS sold_quantity_last_year_month,
    SUM(CASE WHEN departure_date = DATE_SUB(CURRENT_DATE(), INTERVAL 1 YEAR) THEN sold_quantity ELSE 0 END) AS sold_quantity_last_year_day
FROM {{ref('fct_products')}}
where  stock_model in ('Reselling', 'Commission Based', 'Internal - Project X')
--where year_month_departure_date = "2023-08-01" and warehouse = "Jouf WareHouse" and Product = 'Spray Rose Vanessa'
GROUP BY 1, 2, 3
),

invoices_data as (
select product,
       warehouse,
       supplier,
       origin,
       stock_model,
       SUM(CASE WHEN DATE_DIFF(DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY), date(invoice_header_printed_at), DAY) < 29 AND DATE_DIFF(DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY), date(invoice_header_printed_at), DAY) >= -1 THEN quantity ELSE 0 END) as i_last_30d_sold_quantity,
       SUM(CASE WHEN DATE_DIFF(DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY), date(invoice_header_printed_at), DAY) < 6 AND DATE_DIFF(DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY), date(invoice_header_printed_at), DAY) >= -1 THEN quantity ELSE 0 END) as i_last_7d_sold_quantity,
       SAFE_DIVIDE(SUM(CASE WHEN DATE_DIFF(DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY), date(invoice_header_printed_at), DAY) < 20 AND DATE_DIFF(DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY), date(invoice_header_printed_at), DAY) >= -1 THEN quantity ELSE 0 END), 3) as i_last_3_weeks_avg_sold_quantity, 
       SUM(CASE WHEN DATE_DIFF(DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY), date(invoice_header_printed_at), DAY) < 2 AND DATE_DIFF(DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY), date(invoice_header_printed_at), DAY) >= -1 THEN quantity ELSE 0 END) as i_last_3d_sold_quantity, 
       SUM(CASE WHEN (LOWER(feed_source_name) LIKE '%flash%' OR LOWER(feed_source_name) LIKE '%promo%') AND DATE_DIFF(DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY), DATE(invoice_header_printed_at), DAY) < 6 AND DATE_DIFF(DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY), DATE(invoice_header_printed_at), DAY) >= -1 THEN quantity ELSE 0 END) AS i_last_7d_sold_quantity_promo,
       SUM(CASE WHEN ((feed_source_name IS NULL) OR (LOWER(feed_source_name) NOT LIKE '%flash%' AND LOWER(feed_source_name) NOT LIKE '%promo%')) AND DATE_DIFF(DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY), DATE(invoice_header_printed_at), DAY) < 6 AND DATE_DIFF(DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY), DATE(invoice_header_printed_at), DAY) >= -1 THEN quantity ELSE 0 END) 
       AS i_last_7d_sold_quantity_normal

from {{ref('fct_invoice_items')}}
where record_type = 'Invoice - AUTO' and inv_items_reprot_filter = 'Floranow Sales'
group by 1,2,3,4,5
),

-- CTE: Next coming stock date per Product + Warehouse + Supplier (AFTER current_departure_date)
next_coming_stock as (
    SELECT 
        fp.Product,
        fp.warehouse,
        fp.Supplier,
        MAX(ow3.current_departure_date) as order_arrival_date,
        MIN(CASE 
            WHEN fp.departure_date > COALESCE(ow3.current_departure_date, CURRENT_DATE())
            THEN fp.departure_date 
            ELSE NULL 
        END) as next_coming_date
    FROM {{ref('fct_products')}} fp
    LEFT JOIN {{ref('fct_spree_offering_windows')}} ow3 
        ON ow3.warehouse = fp.warehouse 
        AND fp.Origin = ow3.Origin 
        AND fp.Supplier = ow3.Supplier
    WHERE fp.coming_quantity > 0
      AND fp.stock_model in ('Reselling', 'Commission Based', 'Internal - Project X')
    GROUP BY fp.Product, fp.warehouse, fp.Supplier
),

-- CTE: Coming stock available before current_departure_date
coming_by_departure as (
    SELECT 
        fp.Product,
        fp.warehouse,
        fp.Supplier,
        MAX(ow2.current_departure_date) as dept_date,
        SUM(CASE 
            WHEN fp.departure_date <= ow2.current_departure_date 
            THEN fp.coming_quantity 
            ELSE 0 
        END) as coming_before_departure
    FROM {{ref('fct_products')}} fp
    LEFT JOIN {{ref('fct_spree_offering_windows')}} ow2 
        ON ow2.warehouse = fp.warehouse 
        AND fp.Origin = ow2.Origin 
        AND fp.Supplier = ow2.Supplier
    WHERE fp.coming_quantity > 0
      AND fp.stock_model in ('Reselling', 'Commission Based', 'Internal - Project X')
    GROUP BY fp.Product, fp.warehouse, fp.Supplier
)

select
COALESCE(p.product, id.product)     AS Product,
COALESCE(p.warehouse, id.warehouse) AS warehouse,
COALESCE(p.supplier, id.supplier)   AS Supplier,
max(COALESCE(p.origin,   id.origin)) as Origin,

avg(md.avg_monthly_demand) as avg_monthly_demand,
avg(md.i_avg_monthly_demand) as i_avg_monthly_demand,
avg(md.i_avg_monthly_demand_last_year) as i_avg_monthly_demand_last_year,
--max(md.std_dev_monthly_demand) as std_dev_monthly_demand,

--max(sqrt_avg_lead_time_per_month) as sqrt_avg_lead_time_per_month,


--1.28*max(md.std_dev_monthly_demand)*max(sqrt_avg_lead_time_per_month) as monthly_safety_stock,

max(first_departure_date) as first_departure_date,
max(second_departure_date) as second_departure_date,

count(distinct p.year_month_departure_date) as months_count,

count(*) as purchase_orders_count,
count(distinct p.Product) as SKUs_count,

sum(customer_ordered) as customer_ordered,
sum(requested_quantity) as requested_quantity,
sum(location_quantity) as location_quantity,


sum(fulfilled_quantity) as fulfilled_quantity,


sum(incident_quantity_inventory_dmaged) as incident_quantity_inventory_dmaged,

sum(incidents_quantity) as incidents_quantity,
sum(inventory_extra_quantity) as inventory_extra_quantity,




sum(ordered_quantity) as ordered_quantity,
sum(past_ordered_quantity) as past_ordered_quantity,


sum(transit_quantity) as transit_quantity,
sum(transit_quantity_awais) as transit_quantity_awais,
sum(in_stock_quantity) as in_stock_quantity,
sum(active_in_stock_quantity) as active_in_stock_quantity,


sum(expired_stock_quantity) as expired_stock_quantity,
sum(aging_stock_quantity) as aging_stock_quantity,



sum(coming_quantity) as coming_quantity,
sum(sold_quantity) as sold_quantity,
sum(last_30d_sold_quantity) as last_30d_sold_quantity,
sum(last_7d_sold_quantity) as last_7d_sold_quantity,
sum(last_year_30d_sold_quantity) as last_year_30d_sold_quantity,
sum(last_year_7d_sold_quantity) as last_year_7d_sold_quantity,
sum(last_year_next_7d_sold_quantity) as last_year_next_7d_sold_quantity,
sum(i_sold_quantity) as i_sold_quantity,

sum(i_last_year_30d_sold_quantity) as i_last_year_30d_sold_quantity,
sum(i_last_year_7d_sold_quantity) as i_last_year_7d_sold_quantity,
sum(i_last_year_next_7d_sold_quantity) as i_last_year_next_7d_sold_quantity,
sum(last_30d_incident_quantity_inventory_dmaged) as last_30d_incident_quantity_inventory_dmaged,
sum(last_7d_incident_quantity_inventory_dmaged) as last_7d_incident_quantity_inventory_dmaged,


sum(item_sold) as item_sold,

case 
when sum(sold_quantity) <100 then 'D - Less Than 100'
when sum(sold_quantity) >10000 then 'A-10K'
when sum(sold_quantity) >1000 then 'B-1K'
else 'C - 100 To 1K' end as product_classification,

avg(lead_time) as lead_time,
-- current_departure_date,
sum(second_departure_coming_quantity) as second_departure_coming_quantity,
sum(first_departure_coming_quantity) as first_departure_coming_quantity,



DATE_DIFF(max(date(first_departure_date)),  CURRENT_DATE(), DAY) AS days_to_first_departure,
DATE_DIFF(max(date(second_departure_date)),  CURRENT_DATE(), DAY) AS days_to_second_departure,


sum(sold_quantity_2022) as sold_quantity_2022,
sum(sold_quantity_2023) as sold_quantity_2023,

count (distinct p.master_shipment_id) as total_shipments,

SAFE_DIVIDE(count(distinct p.master_shipment_id), count(distinct p.year_month_departure_date)) AS shipment_frequency_per_month,


max(shelf_life_days) as shelf_life_days,

max(ow.current_departure_date) as current_departure_date,

DATE_DIFF(max(date(ow.current_departure_date)), CURRENT_DATE(), DAY) AS dynamic_lead_time, --days_to_next_shipment_departure

avg(sold_quantity_last_year_month) as sold_quantity_this_month_last_year_month,

avg(sold_quantity_last_year_day) as sold_quantity_today_last_year,

max(first_request_departure_date) as first_request_departure_date,

sum(first_departure_requested_quantity) as first_departure_requested_quantity,

sum(incident_quantity_receiving_stage) as incident_quantity_receiving_stage,



max(id.i_last_30d_sold_quantity) as i_last_30d_sold_quantity,
max(id.i_last_7d_sold_quantity) as i_last_7d_sold_quantity,
max(id.i_last_3_weeks_avg_sold_quantity) as i_last_3_weeks_avg_sold_quantity,
max(id.i_last_3d_sold_quantity) as i_last_3d_sold_quantity,

max(i_last_7d_sold_quantity_promo) as i_last_7d_sold_quantity_promo,
max(i_last_7d_sold_quantity_normal) as i_last_7d_sold_quantity_normal,

-- ORDER RECOMMENDATION CALCULATIONS --

-- Next coming date
MAX(ncs.next_coming_date) as next_coming_date,

-- 1. Daily Demand
SAFE_DIVIDE(max(id.i_last_7d_sold_quantity), 7) as daily_demand,

-- 2. Trend Factor (capped 0.5 to 2.0)
LEAST(2.0, GREATEST(0.5, 
    COALESCE(
        SAFE_DIVIDE(
            SAFE_DIVIDE(max(id.i_last_7d_sold_quantity), 7),
            SAFE_DIVIDE(max(id.i_last_30d_sold_quantity), 30)
        ),
        1.0
    )
)) as trend_factor,

-- 3. Seasonality (capped 0.5 to 3.0)
LEAST(3.0, GREATEST(0.5,
    COALESCE(
        SAFE_DIVIDE(
            sum(i_last_year_next_7d_sold_quantity),
            NULLIF(sum(i_last_year_7d_sold_quantity), 0)
        ), 
        1.0
    )
)) as seasonality,

-- 4. Adjusted Demand
SAFE_DIVIDE(max(id.i_last_7d_sold_quantity), 7) 
* LEAST(2.0, GREATEST(0.5, 
    COALESCE(
        SAFE_DIVIDE(
            SAFE_DIVIDE(max(id.i_last_7d_sold_quantity), 7),
            SAFE_DIVIDE(max(id.i_last_30d_sold_quantity), 30)
        ),
        1.0
    )
))
* LEAST(3.0, GREATEST(0.5,
    COALESCE(
        SAFE_DIVIDE(
            sum(i_last_year_next_7d_sold_quantity),
            NULLIF(sum(i_last_year_7d_sold_quantity), 0)
        ), 
        1.0
    )
)) as adjusted_demand,

-- 5. Coverage Days (from order arrival to next coming stock)
LEAST(
    COALESCE(max(shelf_life_days), 7),
    COALESCE(
        DATE_DIFF(MAX(ncs.next_coming_date), max(ow.current_departure_date), DAY),
        COALESCE(max(shelf_life_days), 7)
    )
) as coverage_days,

-- 6. Safety Stock
1.28 
* (SAFE_DIVIDE(max(id.i_last_7d_sold_quantity), 7) * 0.3)
* SQRT(
    LEAST(
        COALESCE(max(shelf_life_days), 7),
        COALESCE(
            DATE_DIFF(MAX(ncs.next_coming_date), max(ow.current_departure_date), DAY),
            COALESCE(max(shelf_life_days), 7)
        )
    )
) as safety_stock,

-- 7. Total Need
(
    SAFE_DIVIDE(max(id.i_last_7d_sold_quantity), 7) 
    * LEAST(2.0, GREATEST(0.5, 
        COALESCE(
            SAFE_DIVIDE(
                SAFE_DIVIDE(max(id.i_last_7d_sold_quantity), 7),
                SAFE_DIVIDE(max(id.i_last_30d_sold_quantity), 30)
            ),
            1.0
        )
    ))
    * LEAST(3.0, GREATEST(0.5,
        COALESCE(
            SAFE_DIVIDE(
                sum(i_last_year_next_7d_sold_quantity),
                NULLIF(sum(i_last_year_7d_sold_quantity), 0)
            ), 
            1.0
        )
    ))
    * LEAST(
        COALESCE(max(shelf_life_days), 7),
        COALESCE(
            DATE_DIFF(MAX(ncs.next_coming_date), max(ow.current_departure_date), DAY),
            COALESCE(max(shelf_life_days), 7)
        )
    )
)
+ (
    1.28 
    * (SAFE_DIVIDE(max(id.i_last_7d_sold_quantity), 7) * 0.3)
    * SQRT(
        LEAST(
            COALESCE(max(shelf_life_days), 7),
            COALESCE(
                DATE_DIFF(MAX(ncs.next_coming_date), max(ow.current_departure_date), DAY),
                COALESCE(max(shelf_life_days), 7)
            )
        )
    )
) as total_need,

-- 8. Available Stock
sum(in_stock_quantity) + COALESCE(MAX(cbd.coming_before_departure), 0) as available_stock,

-- 9. Order Quantity
GREATEST(0,
    (
        SAFE_DIVIDE(max(id.i_last_7d_sold_quantity), 7) 
        * LEAST(2.0, GREATEST(0.5, 
            COALESCE(
                SAFE_DIVIDE(
                    SAFE_DIVIDE(max(id.i_last_7d_sold_quantity), 7),
                    SAFE_DIVIDE(max(id.i_last_30d_sold_quantity), 30)
                ),
                1.0
            )
        ))
        * LEAST(3.0, GREATEST(0.5,
            COALESCE(
                SAFE_DIVIDE(
                    sum(i_last_year_next_7d_sold_quantity),
                    NULLIF(sum(i_last_year_7d_sold_quantity), 0)
                ), 
                1.0
            )
        ))
        * LEAST(
            COALESCE(max(shelf_life_days), 7),
            COALESCE(
                DATE_DIFF(MAX(ncs.next_coming_date), max(ow.current_departure_date), DAY),
                COALESCE(max(shelf_life_days), 7)
            )
        )
    )
    + (
        1.28 
        * (SAFE_DIVIDE(max(id.i_last_7d_sold_quantity), 7) * 0.3)
        * SQRT(
            LEAST(
                COALESCE(max(shelf_life_days), 7),
                COALESCE(
                    DATE_DIFF(MAX(ncs.next_coming_date), max(ow.current_departure_date), DAY),
                    COALESCE(max(shelf_life_days), 7)
                )
            )
        )
    )
    - (sum(in_stock_quantity) + COALESCE(MAX(cbd.coming_before_departure), 0))
) as order_quantity,

-- 10. Recommendation
CASE 
    WHEN max(ow.current_departure_date) IS NULL THEN 'NO WINDOW'
    WHEN (
        (
            SAFE_DIVIDE(max(id.i_last_7d_sold_quantity), 7) 
            * LEAST(2.0, GREATEST(0.5, 
                COALESCE(
                    SAFE_DIVIDE(
                        SAFE_DIVIDE(max(id.i_last_7d_sold_quantity), 7),
                        SAFE_DIVIDE(max(id.i_last_30d_sold_quantity), 30)
                    ),
                    1.0
                )
            ))
            * LEAST(3.0, GREATEST(0.5,
                COALESCE(
                    SAFE_DIVIDE(
                        sum(i_last_year_next_7d_sold_quantity),
                        NULLIF(sum(i_last_year_7d_sold_quantity), 0)
                    ), 
                    1.0
                )
            ))
            * LEAST(
                COALESCE(max(shelf_life_days), 7),
                COALESCE(
                    DATE_DIFF(MAX(ncs.next_coming_date), max(ow.current_departure_date), DAY),
                    COALESCE(max(shelf_life_days), 7)
                )
            )
        )
        + (
            1.28 
            * (SAFE_DIVIDE(max(id.i_last_7d_sold_quantity), 7) * 0.3)
            * SQRT(
                LEAST(
                    COALESCE(max(shelf_life_days), 7),
                    COALESCE(
                        DATE_DIFF(MAX(ncs.next_coming_date), max(ow.current_departure_date), DAY),
                        COALESCE(max(shelf_life_days), 7)
                    )
                )
            )
        )
        - (sum(in_stock_quantity) + COALESCE(MAX(cbd.coming_before_departure), 0))
    ) > 0 THEN 'ORDER'
    ELSE 'SKIP'
END as recommendation,

-- 11. Days of Supply
SAFE_DIVIDE(
    sum(in_stock_quantity) + COALESCE(MAX(cbd.coming_before_departure), 0),
    SAFE_DIVIDE(max(id.i_last_7d_sold_quantity), 7) 
    * LEAST(2.0, GREATEST(0.5, 
        COALESCE(
            SAFE_DIVIDE(
                SAFE_DIVIDE(max(id.i_last_7d_sold_quantity), 7),
                SAFE_DIVIDE(max(id.i_last_30d_sold_quantity), 30)
            ),
            1.0
        )
    ))
) as days_of_supply

from {{ref('fct_products')}} as p 
left join monthly_demand md on md.Product = p.Product and md.warehouse = p.warehouse and p.Supplier = md.Supplier
left join  {{ref('fct_spree_offering_windows')}} as ow on ow.warehouse = p.warehouse  and p.Origin = ow.Origin and  p.Supplier = ow.Supplier
left join last_year_demand lyd on lyd.Product = p.Product and lyd.warehouse = p.warehouse and lyd.Supplier = p.Supplier
full outer join invoices_data id on id.Product = p.Product and id.warehouse = p.warehouse and id.Supplier = p.Supplier and p.Origin = id.Origin and p.stock_model = id.stock_model
left join next_coming_stock ncs on ncs.Product = COALESCE(p.Product, id.product) and ncs.warehouse = COALESCE(p.warehouse, id.warehouse) and ncs.Supplier = COALESCE(p.Supplier, id.supplier)
left join coming_by_departure cbd on cbd.Product = COALESCE(p.Product, id.product) and cbd.warehouse = COALESCE(p.warehouse, id.warehouse) and cbd.Supplier = COALESCE(p.Supplier, id.supplier)

where COALESCE(p.stock_model, id.stock_model) in ('Reselling', 'Commission Based', 'Internal - Project X') 

--and p.Product = 'Rose Ever Red'
--and p.warehouse='Dubai Warehouse'
group by 1,2,3
