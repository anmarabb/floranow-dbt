
select

order_id,
order_number,
li_record_type,
Reseller,
customer,
warehouse,
count(distinct line_item_id) as items,



from {{ref('fct_order_items')}} as li 
group by 1,2,3,4,5,6
