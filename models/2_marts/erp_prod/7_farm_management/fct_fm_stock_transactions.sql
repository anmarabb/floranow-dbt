select


production_date,

--product
    sub_group,
    color,
    contract_status,
    bud_count,
    stem_length,
    product_name,

    quantity,



from   {{ ref('int_fm_stock_transactions') }} as p