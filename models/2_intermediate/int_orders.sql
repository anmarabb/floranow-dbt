
--this is the orders table, generated from line_items table with group by line_item_jd
--it is beter to group by order_number not order id, because order_id has null values regarding to the data from florisfot.


With source as (
 select 
 
 li.order_number,

 count(li.line_item_id) as number_of_items, --number of items in a single order
 
 from {{ ref('stg_line_items') }} as li
 group by li.order_number
)
select 

*,

current_timestamp() as ingestion_timestamp,
 




from source as pod