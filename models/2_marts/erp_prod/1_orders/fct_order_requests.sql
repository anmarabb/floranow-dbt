select *,
       supplier_region as Origin,
       quantity - confirmed_quantity as cancelled_quantity,
from {{ref("int_order_requests")}}