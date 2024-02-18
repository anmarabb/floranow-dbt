select

    p.fm_product_id,

    p.number,

    p.product_name,

    p.color,
    p.raw_color,

    p.quantity,

    p.available_quantity,

    p.fob_price,
    p.main_group,
    p.sub_group,
    p.stem_length,
    p.bud_height,
    p.bud_count,


from   {{ ref('int_fm_products') }} as p