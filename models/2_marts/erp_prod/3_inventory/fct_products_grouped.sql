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
       SUM(CASE WHEN DATE_DIFF(DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY), date(invoice_header_printed_at), DAY) < 30 AND DATE_DIFF(DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY), date(invoice_header_printed_at), DAY) >= 0 THEN quantity ELSE 0 END) as i_last_30d_sold_quantity,
       SUM(CASE WHEN DATE_DIFF(DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY), date(invoice_header_printed_at), DAY) < 7 AND DATE_DIFF(DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY), date(invoice_header_printed_at), DAY) >= 0 THEN quantity ELSE 0 END) as i_last_7d_sold_quantity,
       SAFE_DIVIDE(SUM(CASE WHEN DATE_DIFF(DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY), date(invoice_header_printed_at), DAY) < 21 AND DATE_DIFF(DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY), date(invoice_header_printed_at), DAY) >= 0 THEN quantity ELSE 0 END), 3) as i_last_3_weeks_avg_sold_quantity, 
       SUM(CASE WHEN DATE_DIFF(DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY), date(invoice_header_printed_at), DAY) < 3 AND DATE_DIFF(DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY), date(invoice_header_printed_at), DAY) >= 0 THEN quantity ELSE 0 END) as i_last_3d_sold_quantity, 
       SUM(CASE WHEN (LOWER(feed_source_name) LIKE '%flash%' OR LOWER(feed_source_name) LIKE '%promo%') AND DATE_DIFF(DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY), DATE(invoice_header_printed_at), DAY) < 7 AND DATE_DIFF(DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY), DATE(invoice_header_printed_at), DAY) >= 0 THEN quantity ELSE 0 END) AS i_last_7d_sold_quantity_promo,
       SUM(CASE WHEN ((feed_source_name IS NULL) OR (LOWER(feed_source_name) NOT LIKE '%flash%' AND LOWER(feed_source_name) NOT LIKE '%promo%')) AND DATE_DIFF(DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY), DATE(invoice_header_printed_at), DAY) < 7 AND DATE_DIFF(DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY), DATE(invoice_header_printed_at), DAY) >= 0 THEN quantity ELSE 0 END) 
       AS i_last_7d_sold_quantity_normal

from {{ref('fct_invoice_items')}}
where record_type = 'Invoice - AUTO' and inv_items_reprot_filter = 'Floranow Sales'
group by 1,2,3,4,5
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

from {{ref('fct_products')}} as p 
left join monthly_demand md on md.Product = p.Product and md.warehouse = p.warehouse and p.Supplier = md.Supplier
left join  {{ref('fct_spree_offering_windows')}} as ow on ow.warehouse = p.warehouse  and p.Origin = ow.Origin and  p.Supplier = ow.Supplier
left join last_year_demand lyd on lyd.Product = p.Product and lyd.warehouse = p.warehouse and lyd.Supplier = p.Supplier
full outer join invoices_data id on id.Product = p.Product and id.warehouse = p.warehouse and id.Supplier = p.Supplier and p.Origin = id.Origin and p.stock_model = id.stock_model

where COALESCE(p.stock_model, id.stock_model) in ('Reselling', 'Commission Based', 'Internal - Project X') 

--and p.Product = 'Rose Ever Red'
--and p.warehouse='Dubai Warehouse'
group by 1,2,3
