with order_items as (
    select *
    from {{ref("stg_vp_order_items")}}

    union all

    select *
    from {{ref("stg_vp_confirmed_order_items")}}

    union all

    select *
    from {{ref("stg_vp_rejected_order_items")}}

    union all

    select *
    from {{ref("stg_vp_canceled_order_items")}}
)

select 
    oi.*,
    po.purchase_order_status
from order_items as oi
left join {{ ref('stg_purchase_order') }} as po on oi.purchase_order_id = po.purchase_order_id