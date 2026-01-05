select
    -- Primary Key
    line_item_id,
    
    -- Foreign Keys
    caused_by_adjustment_id,
    order_id,
    farm_id,
    vendor_id,
    floranow_farm_id,
    floranow_vendor_id,
    variation_id,
    
    -- Order Information
    order_number,
    order_type,
    status,
    
    -- Farm & Vendor Information
    farm_name,
    vendor_name,
    
    -- Product Information
    variation_number,
    offer_number,
    product_name,
    color,
    country_of_origin,
    origin_country_name,
    
    -- Customer Information
    destination,
    destination_country_name,
    customer_debtor_number,
    customer_name,
    
    -- Pricing
    currency,
    price,
    unit_fob_price,
    
    -- Fact Measures (Quantities)
    original_quantity,
    quantity,
    shortage_quantity,
    package_count,
    total_packed_quantity,
    
    -- Calculated Revenue Fields
    quantity * unit_fob_price as total_fob_value,
    original_quantity * unit_fob_price as original_total_fob_value,
    shortage_quantity * unit_fob_price as shortage_value,
    
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
    
    -- Configuration
    box_configurations,
    
    -- Dates
    departure_date,
    created_at,
    updated_at,
    
    -- Calculated Fields 
    case 
        when shortage_quantity > 0 then 'Has Shortage'
        when quantity = original_quantity then 'No Shortage'
        else 'Unknown'
    end as shortage_status,
    
    case 
        when package_count > 0 then 'Packed'
        when package_count = 0 and quantity > 0 then 'Not Packed'
        else 'No Quantity'
    end as packing_status,


from {{ ref('int_vp_line_items') }}

