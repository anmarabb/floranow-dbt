select 

    -- PK
    _id as order_item_id,


    -- Data 
    orderitemnumber as order_item_number,
    quantity,
    unitfobprice as unit_fob_price,
    cancelstatus as cancel_status,
    createdat as created_at,




from {{ source(var('erp_source'), 'vp_canceled_order_items') }}