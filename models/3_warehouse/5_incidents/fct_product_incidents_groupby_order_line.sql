
select 
pi.line_item_id,
concat( "https://erp.floranow.com/line_items/", pi.line_item_id) as line_item_link,

count(*) as incidents_count,


from {{ ref('fct_product_incidents') }} as pi 



group by pi.line_item_id
 