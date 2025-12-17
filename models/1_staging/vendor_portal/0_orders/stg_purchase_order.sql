select 
    -- PK
    _id as purchase_order_id,

    -- FK
    farmid as farm_id,
    vendorid as vendor_id,
    freightforwarderid as freight_forwarder_id,

    -- Data
    name as purchase_order_name,
    status as purchase_order_status,
    origin,
    departuredate as departure_date,
    poe as point_of_entry,
    deadline,

    -- Dates
    date(createdat) as created_at,
    cutofftime as cutoff_time,

    -- Consignee Information
    consigneename as consignee_name,
    consigneecuoffstage as consignee_cut_off_stage,
    consigneeaddressline1 as consignee_address_line_1,
    consigneeaddressline2 as consignee_address_line_2,
    consigneecountry as consignee_country,
    consigneeregion as consignee_region,
    consigneephonenumber as consignee_phone_number,

    -- Freight Forwarder Information
    freightforwardername as freight_forwarder_name,

    -- Company Information
    companystreetaddress as company_street_address,
    companycity as company_city,
    companyname as company_name,
    companycountry as company_country,
    companyphonenumber as company_phone_number

from {{ source(var('erp_source'), 'vp_purchase_order') }}