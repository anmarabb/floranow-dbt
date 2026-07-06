select

    -- PK
    _id as farm_rfq_id,

    -- FK
    rfqid as rfq_id,
    rfqbuilderid as rfq_builder_id,
    farmid as farm_id,
    vendorid as vendor_id,
    offerid as offer_id,

    -- Farm & Vendor
    farmname as Farm,
    floranownumber as floranow_number,
    farmemail as farm_email,
    farmcurrency as farm_currency,
    farmorigin as farm_origin,
    farmgroup as farm_group,
    vendorfloranownumber as vendor_floranow_number,

    -- Status
    status as farm_rfq_status,
    cast(isexpired as bool) as is_expired,
    cast(isordered as bool) as is_ordered,
    cast(ordersubmitted as bool) as order_submitted,
    channel,

    -- Quantities & Pricing
    cast(requestedquantity as int64) as requested_quantity,
    safe_cast(nullif(regexp_replace(trim(cast(requestedfobprice as string)), r'[^0-9.\-]', ''), '') as float64) as requested_fob_price,
    safe_cast(nullif(regexp_replace(trim(cast(offerprice as string)), r'[^0-9.\-]', ''), '') as float64) as offer_price,
    safe_cast(nullif(regexp_replace(trim(cast(agreedunitprice as string)), r'[^0-9.\-]', ''), '') as float64) as agreed_unit_price,
    cast(agreedquantity as int64) as agreed_quantity,
    agreedcurrency as agreed_currency,
    safe_cast(nullif(regexp_replace(trim(cast(lastfobprice as string)), r'[^0-9.\-]', ''), '') as float64) as last_fob_price,
    safe_cast(nullif(regexp_replace(trim(cast(lastofferedunitprice as string)), r'[^0-9.\-]', ''), '') as float64) as last_offered_unit_price,
    safe_cast(nullif(regexp_replace(trim(cast(lastofferedtotalprice as string)), r'[^0-9.\-]', ''), '') as float64) as last_offered_total_price,
    cast(lastofferedquantity as int64) as last_offered_quantity,
    lastcurrency as last_currency,
    lastpricingresult as last_pricing_result,

    -- Cutoff & Flags
    timestamp(cutofftime) as cut_off_time,
    pricingtype as pricing_type,
    cast(skipcutoffvalidation as bool) as skip_cutoff_validation,
    cast(isevent as bool) as is_event,
    flag,

    -- Actor
    createdby as created_by,
    createdbyname as created_by_name,

    -- Order
    ordernumber as order_number,
    rejectionreason as rejection_reason,

    -- Negotiation / Moves
    cast(movecount as int64) as move_count,
    lastactortype as last_actor_type,
    lastmovetype as last_move_type,
    lastfarmmovetype as last_farm_move_type,
    latestclientmovetype as latest_client_move_type,
    timestamp(lastmoveat) as last_move_at,
    timestamp(latestclientmoveat) as latest_client_move_at,
    lastnotes as last_notes,
    latestfarmmovenotes as latest_farm_move_notes,
    latestclientmovenotes as latest_client_move_notes,

    -- Nested / JSON fields
    customerinfo as customer_info,
    deliverywindow as delivery_window,
    specificoffer as specific_offer,
    offerfarmsnapshot as offer_farm_snapshot,
    offervendorsnapshot as offer_vendor_snapshot,
    specificproduct as specific_product,
    alternativeproduct as alternative_product,
    selectedoffer as selected_offer,
    resolvedoffer as resolved_offer,
    genericoffer as generic_offer,
    onlineproduct as online_product,
    marginapprovalsnapshot as margin_approval_snapshot,
    creationmarginsnapshot as creation_margin_snapshot,

    -- CNF
    cast(cnfboxqty as int64) as cnf_box_qty,
    cnffreightlogistics as cnf_freight_logistics,
    cnfvolumetricweight as cnf_volumetric_weight,

    -- Dates
    timestamp(createdat) as created_at,
    timestamp(updatedat) as updated_at,

from {{ source(var('erp_source'), 'vp_farm_rfqs') }}
