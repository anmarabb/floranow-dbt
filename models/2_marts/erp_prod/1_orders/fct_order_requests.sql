select *,
       supplier_region as Origin,
       quantity - ordered_quantity as cancelled_quantity,
from {{ref("int_order_requests")}}