select *
from {{ source(var('erp_source'), 'vp_purchase_order') }}