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
    SAFE_CAST(NULLIF(REGEXP_REPLACE(TRIM(CAST(price AS STRING)),r'[^0-9.\-]',''),'') AS FLOAT64) AS unit_price,
    SAFE_CAST(NULLIF(REGEXP_REPLACE(TRIM(CAST(unitfobprice AS STRING)),r'[^0-9.\-]',''),'') AS FLOAT64) AS unit_fob_price,
    -- unitfobprice as unit_fob_price,
    CAST(quantity AS INT64) as quantity,
    'REJECTED' as order_item_status,
    ordertype as order_type,
    farmname as Farm,
    vendorname as Vendor,
    offernumber as offer_number,

    cast(null as date) as ordered_at,
    cast(null as date) as confirmed_at,
    date(rejectedat) as rejected_at,
    cast(null as date) as cancelled_at,
    reason,
    
    case when reason = 'cutoff time has passed' then 'AUTO' else 'MANUAL' END AS rejection_type,
    cast(null as STRING) as confirmation_type,

    consigneeName as consignee_name,
    consigneeCutOffStage as consignee_cut_off_stage,
    POE as point_of_entry,
    consigneeAddressLine1 as consignee_address_line_1,
    consigneeAddressLine2 as consignee_address_line_2,
    consigneeCountry as consignee_country,
    consigneeRegion as consignee_region,
    consigneePhoneNumber as consignee_phone_number,

from {{ source(var('erp_source'), 'vp_rejected_order_items') }}