
with

    prep_order as 
    (
        
        select 
            
            sum(fmso.fulfiled_quantity) as fulfiled_quantity, 
            fmso.fm_order_id  
            
         from  {{ ref('stg_fm_outbound_stock_items') }} as fmso 
         group by fm_order_id
         
         
    )

select

--fm_orders
    o.customer_debtor_number,
    o.customer_name,
    o.warehouse_name,
    o.quantity,


fmso.fulfiled_quantity,



--fm_products
    fmp.product_name,
    fmp.number,
    fmp.categorization,
    fmp.fob_price,


--line_items
    li.order_number,
    li.unit_price as Unit_price_without_tax,
    li.total_price_without_tax,
    li.total_price_include_tax,

cast(o.created_at as date) as Created_date,

time_add(cast(o.created_at as time), INTERVAL 3 hour) as Created_time,

o.departure_date,

o.delivery_date,

sh.fm_shipment_id,

from   {{ ref('stg_fm_orders') }} as o
left join {{ ref('stg_fm_products') }} as fmp on o.fm_product_id = fmp.fm_product_id
left join prep_order as fmso on fmso.fm_order_id = o.fm_order_id
left join {{ ref('stg_line_items') }} as li on o.buyer_order_number = li.number
left join {{ ref('stg_fm_shipments') }} as sh on sh.fm_shipment_id = o.fm_shipment_id


