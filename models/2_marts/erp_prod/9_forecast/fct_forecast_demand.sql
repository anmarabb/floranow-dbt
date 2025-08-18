 select product_name as Product,
       date(departure_date) as date, 
       sum(quantity) as requested_quantity,
       0 as coming_quantity,
       0 as remaining_quantity,
       0 as actual_quantity,
       0 as forecast_quantity,

from {{ref("fct_order_requests")}}
where status = 'REQUESTED'
and product_name in ('Rose Ever Red', 'Rose Athena', 'Chrysanthemum Spray Pina Colada', 'Gypsophila Xlence', 'Rose Madam Red')
and departure_date >= current_date()
group by 1, 2

UNION ALL

select Product,
       date(departure_date) as date, 
       0 as requested_quantity,
       sum(coming_quantity) as coming_quantity,
       sum(express_remaining_quantity) as remaining_quantity,
       0 as actual_quantity,
       0 as forecast_quantity,

from {{ref("fct_products")}}
where Product in ('Rose Ever Red', 'Rose Athena', 'Chrysanthemum Spray Pina Colada', 'Gypsophila Xlence', 'Rose Madam Red')
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