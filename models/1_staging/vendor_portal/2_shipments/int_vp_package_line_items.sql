select
    pli.*,
    
    -- Package Information
    p.package_number,
    p.box_label,
    p.is_full,
    p.used_space,
    p.shipment_id as package_shipment_id,
    
    -- Line Item Information
    li.product_name,
    li.quantity as line_item_quantity,
    li.original_quantity as line_item_original_quantity

from {{ ref('stg_vp_package_line_items') }} as pli
left join {{ ref('stg_vp_packages') }} as p on pli.package_id = p.package_id
left join {{ ref('stg_vp_line_items') }} as li on pli.line_item_id = li.line_item_id

