with

source as ( 

 
select 
*,

current_timestamp() as insertion_timestamp, 


from {{ref('fct_order_items')}} as li 
group by li.order_number
)

select * from source