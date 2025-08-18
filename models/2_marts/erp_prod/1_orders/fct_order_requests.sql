select *,
       supplier_region as Origin,
       quantity - ordered_quantity as cancelled_quantity,
       product_name as Product
from {{ref("int_order_requests")}}