select
    -- Primary Key
    id as shipment_id,

    -- Foreign Keys
    master_shipment_id,
    vendor_id,
    farm_id,
    warehouse_id,

    -- Identifiers
    name as shipment_name,
    split_number,

    -- Status & State
    state,

    -- Location Information
    origin,
    destination,
    poe as point_of_entry,
    consignee,
    consignee_name,
    consignee_country,
    consignee_region,
    consignee_address_line1,
    consignee_address_line2,
    consignee_cut_off_stage,

    -- Freight Information
    freight_forwarder_name,
    freight_forwarder_id,
    delivery_term,
    customer_debtor_number,

    -- Capacity & Timing
    max_capacity,
    timestamp(cut_off_time) as cut_off_time,

    -- Dates
    date(departure_date) as departure_date,
    timestamp(created_at) as created_at,
    timestamp(updated_at) as updated_at,

    -- State Transition Dates & Users
    created_by,
    reviewed_products_by,
    timestamp(reviewed_products_at) as reviewed_products_at,
    packed_and_labeled_by,
    timestamp(packed_and_labeled_at) as packed_and_labeled_at,
    attached_documents_by,
    timestamp(attached_documents_at) as attached_documents_at,
    sent_by,
    timestamp(sent_at) as sent_at,

    -- Quantities
    total_stems_count,
    number_of_boxes,
    available_quantity,

    -- Metadata
    vendor_name,
    farm_name,

from {{ source(var('erp_source'), 'sh_shipments') }}

