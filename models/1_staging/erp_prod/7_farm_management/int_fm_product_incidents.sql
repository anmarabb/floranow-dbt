
select

--product_incidents
    pi.fm_product_incident_id,
    pi.fm_product_id,
    pi.incident_type,
    pi.stage,
    pi.quantity as incident_quantity,
    cast(pi.created_at as date) as incident_at,
    time_add(cast(pi.created_at as time), INTERVAL 3 hour) as Creation_time,

    concat( "https://erp.floranow.com/fm/product_incidents/", pi.fm_product_incident_id) as fm_product_incident_link,


u.name as reported_by,
u.account_manager as account_manager,

p.Product,
p.astra_barcode,
p.fob_price,
p.sub_group,
p.main_group,
p.color,
p.stem_length,

stt.production_date,


--orders
    o.customer_name,
    o.customer_debtor_number,
    o.fm_order_id,

--stt.sourceable_type,
--stt.fm_stock_transaction_id,

reported_by_id,

from   {{ ref('stg_fm_product_incidents') }} as pi
left join {{ ref('fct_fm_products') }} as p on pi.fm_product_id = p.fm_product_id
left join  {{ ref('base_users') }} as u on pi.reported_by_id = u.id

left join {{ ref('stg_fm_stock_transactions') }} as stt on stt.sourceable_id = pi.fm_product_incident_id and stt.sourceable_type = 'Fm::ProductIncident'

left join {{ ref('stg_fm_orders') }} as o on o.fm_order_id = pi.incidentable_id and pi.incidentable_type = 'Fm::Order'


--incidentable_id

--incidentable_type


--where stt.sourceable_type = 'Fm::ProductIncident' and stt.production_date is null
--order by incident_at desc
--where pi.incident_type <> 'DAMAGED'
--where pi.fm_product_incident_id = 7161

--where pi.fm_product_incident_id = 7086

