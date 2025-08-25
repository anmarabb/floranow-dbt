 select Product,
       date(departure_date) as date, 
       sum(quantity) as requested_quantity,
       0 as coming_quantity,
       0 as remaining_quantity,
       0 as actual_quantity,
       0 as forecast_quantity,

from {{ref("fct_order_requests")}}
where status = 'REQUESTED'
and product_name in ('Rose Ever Red', 'Rose Athena', 'Chrysanthemum Spray Pina Colada', 'Gypsophila Xlence', 'Rose Madam Red')
-- and departure_date >= current_date() 
and warehouse = 'Dubai Warehouse'
group by 1, 2

UNION ALL

select Product,
       date(departure_date) as date, 
       0 as requested_quantity,
       sum(coming_quantity) as coming_quantity,
       0 as remaining_quantity,
       0 as actual_quantity,
       0 as forecast_quantity,

from {{ref("fct_products")}} p
where Product in ('Rose Ever Red', 'Rose Athena', 'Chrysanthemum Spray Pina Colada', 'Gypsophila Xlence', 'Rose Madam Red')
and p.reseller_label = 'Express' and warehouse = 'Dubai Warehouse'
group by 1, 2

UNION ALL

select Product,
       current_date() as date, 
       0 as requested_quantity,
       0 as coming_quantity,
       sum(case 
                when p.Stock = 'Inventory Stock' 
                and live_stock = 'Live Stock' 
                and p.modified_stock_model in ('Reselling', 'SCaaS', 'TBF', 'Internal') 
                and flag_1 in ('scaned_flag', 'scaned_good') then remaining_quantity else 0 
            end) as remaining_quantity,
       0 as actual_quantity,
       0 as forecast_quantity,

from {{ref("fct_products")}} p
where Product in ('Rose Ever Red', 'Rose Athena', 'Chrysanthemum Spray Pina Colada', 'Gypsophila Xlence', 'Rose Madam Red')
and p.reseller_label = 'Express' and warehouse = 'Dubai Warehouse'
group by 1, 2

UNION ALL

select Product,
       date(date) as date, 
       0 as requested_quantity,
       0 as coming_quantity,
       0 as remaining_quantity,
       sum(actual) as actual_quantity,
       sum(forecast) as forecast_quantity,


from {{ref("stg_demand_forecast")}}
group by 1, 2