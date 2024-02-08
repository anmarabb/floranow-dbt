select



--boxes
    b.fm_boxe_id,
    b.number as box_number,
    b.sequence,


--products
    p.number as Astra_id,
    p.product_name,
    p.color,

--box_items
    bi.produced_quantity,

--shipments
    sh.fm_shipment_id,
    cast(sh.created_at as date) as shipmet_creation_date, 
    time_add(cast(sh.created_at as time), INTERVAL 3 hour) as shipment_creation_time,


case when sh.shipment_type = 'INBOUND' and sh.status <> 'PENDING' and sh.created_at > '2023-11-11' then 'Production'
else 'Normal' end as shipment_type,

from   {{ref('stg_fm_box_items')}} as bi
left join {{ref('stg_fm_boxes')}} as b on bi.fm_box_id = b.fm_boxe_id

left join {{ref('stg_fm_shipments')}} as sh on b.fm_shipment_id = sh.fm_shipment_id


left join {{ref('stg_fm_products')}} as p on bi.fm_product_id = p.fm_product_id

