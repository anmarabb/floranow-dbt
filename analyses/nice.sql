with

source as ( 
        
select     

count(*)
from {{ ref('stg_product_incidents')}} as pi
left join {{ ref('stg_line_items')}} as li on pi.line_item_id = li.line_item_id

    )

select * from source