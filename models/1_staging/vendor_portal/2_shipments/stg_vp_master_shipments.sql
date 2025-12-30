select
    -- Primary Key
    id as master_shipment_id,

    -- Identifiers
    name as master_shipment_name,

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
    freight_forwarder,
    freight_forwarder_name,
    freight_forwarder_id,
    CAST(delivery_term AS INT64) as delivery_term,
    customer_debtor_number,

    -- Dates
    date(departure_date) as departure_date,
    timestamp(created_at) as created_at,
    timestamp(updated_at) as updated_at,

    -- Aggregated Counts
    total_weight,
    total_shipments_count,
    total_attachments_count,

from {{ source(var('erp_source'), 'sh_master_shipments') }}

