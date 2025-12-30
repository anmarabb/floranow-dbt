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
    po.purchase_order_status,
    w.warehouse_name as warehouse,
    u.user_category
from order_items as oi
left join {{ ref('stg_purchase_order') }} as po on oi.purchase_order_id = po.purchase_order_id
left join {{ ref('base_warehouses') }} as w on oi.warehouse_id = w.warehouse_id
left join {{ ref('base_users') }} as u on oi.debtor_number = u.debtor_number