
with

    prep_order as 
    (
        
        select 
            
            sum(fmso.fulfiled_quantity) as fulfiled_quantity, 
            fmso.fm_order_id  
            
         from  {{ ref('stg_fm_outbound_stock_items') }} as fmso 
         group by fm_order_id
         
         
    ),

    prep_box_items as 
    (

        select 
            
            sum(bi.packed_quantity) as packed_quantity,
            sum(bi.unpacked_quantity) as unpacked_quantity,
            max(order_status) as order_status,
            --(bi.fm_box_id) as fm_box_id,
            bi.fm_order_id,
              
            
         from   {{ ref('stg_fm_box_items') }} as bi 
         group by fm_order_id

    ),





    prep_outbound_stock_items as (

        select 
            
            osi.fm_order_id,
            array_agg(osi.production_date ignore nulls) as production_date_array,
            
         from   {{ ref('stg_fm_outbound_stock_items') }} as osi 
         group by fm_order_id

    ),

prep_fm_product_incidents as (

select 
            
         pi.fm_order_id,
         sum(case when pi.incident_type = 'SHORTAGE' then  pi.incident_quantity else 0 end) as incident_shortge_qunatity,
            
         from   {{ ref('int_fm_product_incidents') }} as pi 
         group by fm_order_id


    )



select

--fm_orders
    o.buyer_order_number,
    o.customer_debtor_number,
    o.customer_name,
    o.warehouse_name,
    case 
        when o.warehouse_name in ("Riyadh Warehouse", "Hail Warehouse","Qassim Warehouse") then "Riyadh Warehouse" 
        when o.warehouse_name in ("Hafar WareHouse", "Dammam Warehouse") then "Dammam Warehouse"
        else o.warehouse_name end as hub_warehouse,

    null as anmar, 
    
    o.quantity,
    o.fm_order_id,
    concat( "https://erp.floranow.com/fm/orders/", o.fm_order_id) as fm_order_link,


fmso.fulfiled_quantity,



--fm_products
    p.Product,
    p.astra_barcode,
    --p.categorization,
    p.fob_price,
    p.color,
    p.stem_length,
    p.sub_group,


--line_items
    li.order_number,
    li.unit_price as Unit_price_without_tax,
    li.total_price_without_tax,
    li.total_price_include_tax,

cast(o.created_at as date) as Created_date,

cast(o.created_at as time) as Created_time,

o.departure_date,

o.delivery_date,

--sh.fm_shipment_id,

customer.customer_type,




osi.production_date_array,

COALESCE (bi.packed_quantity,0) as packed_quantity,
COALESCE (bi.unpacked_quantity,0) as unpacked_quantity,

COALESCE (pi.incident_shortge_qunatity,0) as incident_shortge_qunatity,
bi.order_status,

from   {{ ref('stg_fm_orders') }} as o
left join {{ ref('fct_fm_products') }} as p on o.fm_product_id = p.fm_product_id
left join prep_order as fmso on fmso.fm_order_id = o.fm_order_id
left join {{ ref('stg_line_items') }} as li on o.buyer_order_number = li.number


--left join {{ ref('stg_fm_shipments') }} as sh on sh.fm_shipment_id = o.fm_shipment_id

left join {{ref('base_users')}} as customer on customer.debtor_number = o.customer_debtor_number

left join prep_box_items as bi on bi.fm_order_id = o.fm_order_id

left join prep_outbound_stock_items as osi on osi.fm_order_id = o.fm_order_id

left join prep_fm_product_incidents as pi on pi.fm_order_id = o.fm_order_id