select

    -- PK
    _id as rfq_id,

    -- FK
    rfqbuilderid as rfq_builder_id,

    -- Status
    status as rfq_status,
    cast(isexpired as bool) as is_expired,
    cast(isordered as bool) as is_ordered,

    -- Quantities & Pricing
    cast(requestedquantity as int64) as requested_quantity,
    cast(awardedquantity as int64) as awarded_quantity,
    pricingtype as pricing_type,
    safe_cast(nullif(regexp_replace(trim(cast(requestedfobprice as string)), r'[^0-9.\-]', ''), '') as float64) as requested_fob_price,
    safe_cast(nullif(regexp_replace(trim(cast(sellingprice as string)), r'[^0-9.\-]', ''), '') as float64) as selling_price,

    -- Cutoff & Flags
    cast(cutoffhours as int64) as cutoff_hours,
    timestamp(cutofftime) as cut_off_time,
    cast(skipcutoffvalidation as bool) as skip_cutoff_validation,
    cast(isevent as bool) as is_event,
    flag,
    channel,

    -- Actor
    createdby as created_by,
    createdbyname as created_by_name,

    -- Nested / JSON fields
    customerinfo as customer_info,
    deliverywindow as delivery_window,
    farmids as farm_ids,
    specificoffer as specific_offer,
    custominfo as custom_info,
    genericoffer as generic_offer,

    -- Generation
    cast(generationversion as int64) as generation_version,
    cast(version as int64) as version,
    timestamp(lastgenerationtime) as last_generation_time,
    timestamp(lastgeneratedupto) as last_generated_up_to,

    -- Dates
    timestamp(createdat) as created_at,
    timestamp(updatedat) as updated_at,

from {{ source(var('erp_source'), 'vp_rfqs') }}
