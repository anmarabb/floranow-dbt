select

    -- PK
    _id as order_request_id,

    -- FK
    rfqid as rfq_id,
    farmrfqid as farm_rfq_id,

    -- Farm & Vendor
    farmid as farm_id,
    farmname as Farm,
    floranownumber as floranow_number,
    vendorid as vendor_id,
    vendorfloranownumber as vendor_floranow_number,

    -- Status
    status as order_request_status,
    failurereason as failure_reason,

    -- Quantities & Pricing
    cast(generationversion as int64) as generation_version,
    safe_cast(nullif(regexp_replace(trim(cast(agreedunitprice as string)), r'[^0-9.\-]', ''), '') as float64) as agreed_unit_price,
    cast(agreedquantity as int64) as agreed_quantity,
    agreedcurrency as agreed_currency,

    -- Order
    ordernumber as order_number,

    -- Dates
    date(departuredate) as departure_date,
    date(deliverydate) as delivery_date,
    timestamp(submittedat) as submitted_at,
    timestamp(createdat) as created_at,
    timestamp(updatedat) as updated_at,

    -- Nested / JSON fields
    customerinfo as customer_info,
    specificoffer as specific_offer,
    resolvedoffer as resolved_offer,

from {{ source(var('erp_source'), 'vp_order_requests') }}
