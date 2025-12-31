select
    -- Primary Key
    id as line_item_id,

    -- Foreign Keys
    caused_by_adjustment_id,

    -- Order Information
    order_number,
    order_id,
    order_type,
    status,

    -- Farm & Vendor Information
    farm_id,
    farm_name,
    floranow_farm_id,
    vendor_id,
    vendor_name,
    floranow_vendor_id,

    -- Product Information
    variation_id,
    variation_number,
    offer_number,
    product_name,
    color,

    -- Customer Information
    destination,
    customer_debtor_number,
    customer_name,

    -- Pricing
    currency,
    price,
    unit_fob_price,

    -- Quantities
    original_quantity,
    quantity,

    -- Product Specifications
    bundle_weight,
    diameter,
    head_diameter,
    head_size,
    height,
    weight,
    quality,
    length,
    pot_size,
    no_of_buds,
    no_of_stems_per_bunch,
    maturity_cut_stage,
    minimum_length_of_flower_stem,
    minimum_bud_height,
    country_of_origin,

    -- Configuration
    box_configurations,

    -- Dates
    date(departure_date) as departure_date,
    timestamp(created_at) as created_at,
    timestamp(updated_at) as updated_at

from {{ source(var('erp_source'), 'sh_line_items') }}
