select


stock_transaction_at,
production_date,
expired_at,

--product
    sub_group,
    color,
    available_quantity,
    contract_status,
    bud_count,
    stem_length,
    product_name,
    transaction_type,
from   {{ ref('int_fm_stock_transactions') }} as p