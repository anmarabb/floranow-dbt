select


production_date,
fm_product_id as product_id,
--product
    sub_group,
    color,
    contract_status,
    bud_count,
    stem_length,
    Product,

    quantity,



from   {{ ref('int_fm_stock_transactions') }} as p