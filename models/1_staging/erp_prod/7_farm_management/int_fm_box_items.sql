select



--boxes
    b.fm_box_id,
    b.number as box_number,
    b.sequence,


--products
    p.astra_barcode,
    p.product_name,
    p.color,
    
    p.main_group,
    p.sub_group,
    p.stem_length,
    p.bud_height,
    p.bud_count,
    p.fob_price,
    


--box_items
    bi.produced_quantity,
    bi.packed_quantity,
    bi.unpacked_quantity,

--shipments
    sh.fm_shipment_id,
    cast(sh.created_at as date) as shipmet_creation_date, 
    time_add(cast(sh.created_at as time), INTERVAL 3 hour) as shipment_creation_time,


case when sh.shipment_type = 'INBOUND' and sh.status <> 'PENDING' and sh.created_at > '2023-11-11' then 'Production'
else 'Normal' end as shipment_type,

bhi.production_date,

from   {{ref('stg_fm_box_items')}} as bi
left join {{ref('stg_fm_boxes')}} as b on bi.fm_box_id = b.fm_box_id
left join {{ref('stg_fm_shipments')}} as sh on b.fm_shipment_id = sh.fm_shipment_id
left join {{ref('int_fm_products')}} as p on bi.fm_product_id = p.fm_product_id


left join {{ref('stg_fm_batch_items')}} as bhi on bhi.fm_batch_item_id = bi.fm_box_item_id
