select *
from {{ref("stg_vp_order_items")}}

union all

select *
from {{ref("stg_vp_confirmed_order_items")}}

union all

select *
from {{ref("stg_vp_rejected_order_items")}}