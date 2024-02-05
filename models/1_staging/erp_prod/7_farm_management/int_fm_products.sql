
select

    p.fm_product_id,

    p.number,

    p.product_name,

    p.color,

    p.quantity,

    p.available_quantity,

    p.fob_price,


from   {{ ref('stg_fm_products') }} as p