select
    -- Primary Key
    shipment_id,
    
    -- Foreign Keys
    master_shipment_id,
    vendor_id,
    farm_id,
    warehouse_id,
    
    -- Identifiers
    shipment_name,
    split_number,
    concat("https://erp.floranow.com/vendor_portal/shipments/", shipment_id) as shipment_link,
    
    -- Status & State
    state as shipment_status,
    master_shipment_state as master_shipment_status,
    
    -- Location Information
    origin,
    origin_country_name,
    destination,
    destination_country_name,
    point_of_entry,
    consignee,
    consignee_name,
    consignee_country,
    consignee_region,
    consignee_address_line1,
    consignee_address_line2,
    consignee_cut_off_stage,
    
    -- Master Shipment Information
    master_shipment_name,
    master_shipment_origin_code,
    master_shipment_destination_code,
    master_shipment_poe,
    master_shipment_total_weight,
    
    -- Freight Information
    freight_forwarder_name,
    freight_forwarder_id,
    delivery_term,
    customer_debtor_number,
    
    -- Warehouse Information
    warehouse_name,
    financial_administration,
    
    -- Customer Information
    user_category,
    customer_type,
    account_type,
    user_warehouse_id,
    
    -- Capacity & Timing
    max_capacity,
    cut_off_time,
    
    -- Dates
    departure_date,
    created_at,
    updated_at,
    
    -- State Transition Dates & Users
    created_by,
    reviewed_products_by,
    reviewed_products_at,
    packed_and_labeled_by,
    packed_and_labeled_at,
    attached_documents_by,
    attached_documents_at,
    sent_by,
    sent_at,
    
    -- Fact Measures (Quantities)
    total_stems_count,
    number_of_boxes,
    available_quantity,
    
    -- Metadata
    vendor_name,
    farm_name,


from {{ ref('int_vp_shipments') }}

