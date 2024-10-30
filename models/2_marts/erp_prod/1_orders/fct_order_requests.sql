select *,
       supplier_region as Origin,
from {{ref("int_order_requests")}}