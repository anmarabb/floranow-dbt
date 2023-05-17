with

source as ( 


 
select 
li.order_number,
sum(incidents_count) as total_order_incidents_count,



from {{ref('fct_order_items')}} as li 

group by li.order_number
)

select * from source

--test