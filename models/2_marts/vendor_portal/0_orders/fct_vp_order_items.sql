select *,

       case when order_item_status = 'PENDING' then  quantity else 0 end as ordered_quantity,
       case when order_item_status = 'REJECTED' then  quantity else 0 end as rejected_quantity,
       case when order_item_status = 'CONFIRMED' then  quantity else 0 end as confirmed_quantity,

from {{ref ("int_vp_order_items")}}