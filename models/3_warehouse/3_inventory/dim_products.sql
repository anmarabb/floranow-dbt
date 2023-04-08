with

source as ( 
        
select     

p.id as inventory_item_id,
concat( "https://erp.floranow.com/products/", p.id) as inventory_item_link,

p.product_name as product,
p.stem_length,
p.color,


    current_timestamp() as insertion_timestamp, 

from {{ ref('stg_products')}} as p
left join {{ ref('base_stocks')}} as st on p.stock_id = st.stock_id and p.reseller_id = st.reseller_id

left join {{ ref('fct_order_items')}} as li on p.line_item_id = li.line_item_id

left join {{ ref('stg_feed_sources')}} as fs on p.origin_feed_source_id = fs.feed_source_id 



    )

select * from source