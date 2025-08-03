select 

    -- PK
    _id as order_item_id,


    -- FK
    orderid as order_id,
    farmid as farm_id,
    floranowfarmid as floranow_farm_id,
    vendorid as vendor_id,
    floranowvendorid as floranow_vendor_id,


    -- Data 
    ordernumber as order_number,
    destination,
    customerdebtornumber as debtor_number,
    customername as Customer,
    productname as Product,
    currency,
    departuredate as departure_date,
    price as unit_price,
    unitfobprice as unit_fob_price,
    quantity,
    status as order_item_status,
    ordertype as order_type,
    farmname as Farm,
    vendorname as Vendor,
    offernumber as offer_number,

from {{ source(var('erp_source'), 'vp_order_items') }}