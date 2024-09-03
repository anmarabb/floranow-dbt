select Product,
       product_color,
       MQS
from {{ source(var('erp_source'), 'product_mqs') }} 