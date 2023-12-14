with monthly_demand as (

with anmar as (
    select
        Product,
        warehouse,
       -- Supplier,
        year_month_departure_date,
        count(distinct year_month_departure_date) as months_count,
       -- count(distinct Supplier) as supplier_count,
        COALESCE(sum(sold_quantity),0) as monthly_demand,
        avg(lead_time)/30.5 as month_lead_time,
        avg(lead_time) as lead_time,

    from {{ref('fct_products')}} as p 
    where  stock_model = 'Reselling'
 --   and p.Product = 'Rose Ever Red'
 --  and p.warehouse='Dubai Warehouse'
  -- and year_month_departure_date = '2022-10-01'


    group by 1, 2,3
)

select 
Product,  
warehouse,
--Supplier,
avg(md.monthly_demand) as avg_monthly_demand,


stddev_pop (md.monthly_demand) as std_dev_monthly_demand,

CASE
  WHEN AVG(month_lead_time) < 0 THEN SQRT(1)
     ELSE SQRT(AVG(month_lead_time))
   END AS sqrt_avg_lead_time_per_month,



from anmar as md
group by 1, 2

)

select
p.Product,
p.warehouse,
--p.origin
--p.Supplier,

avg(md.avg_monthly_demand) as avg_monthly_demand,
max(md.std_dev_monthly_demand) as std_dev_monthly_demand,

max(sqrt_avg_lead_time_per_month) as sqrt_avg_lead_time_per_month,


1.28*max(md.std_dev_monthly_demand)*max(sqrt_avg_lead_time_per_month) as monthly_safety_stock,

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
sum(in_stock_quantity) as in_stock_quantity,
sum(coming_quantity) as coming_quantity,
sum(sold_quantity) as sold_quantity,
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
from {{ref('fct_products')}} as p 
left join monthly_demand md on md.Product = p.Product and md.warehouse = p.warehouse 

where  stock_model = 'Reselling'

--and p.Product = 'Rose Ever Red'
--and p.warehouse='Dubai Warehouse'
group by 1,2
