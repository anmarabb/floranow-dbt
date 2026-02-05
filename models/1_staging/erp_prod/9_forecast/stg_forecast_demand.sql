select * except(product, warehouse), 
       product as Product,
       case when date >= date_trunc(current_date(), month) then date_trunc(current_date(), month) else null end as period_start

from {{ source(var('erp_source'), 'forecast_demand') }}