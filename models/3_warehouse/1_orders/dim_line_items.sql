with

source as ( 

 
select 

li.id as line_item_id,
li.line_item_type,
li.order_number,

li.order_type, --online,offline,standing

li.customer,
li.user,



--supplier,
--order R123432, ordared for UAE market on 5/3/2023 for delvery date  7/3/2023 and delvered on  7/3/2023 so it was on time delvery with out any incedint 
--the order was form Experess stock same day delivery. and the orginal supplier is fontana,

case when li.line_item_type in ('Reselling Purchase Orders', 'EXTRA') and li.location = 'loc' and pi.incidents_count is  null then 1 else 0 end as Received_not_scanned,

pi.incidents_count,

current_timestamp() as insertion_timestamp, 



from {{ref('int_line_items')}} as li 
left join {{ ref('int_purchase_line_item') }} as parent_purchase_line_item on parent_purchase_line_item.id = li.parent_line_item_id
left join {{ ref('fct_product_incidents_groupby_order_line') }} as pi on pi.line_item_id = li.id
)

select * from source