select
    -- Primary Key
    id as movement_request_id,

    -- Foreign Keys
    source_shipment_id,
    target_shipment_id,

    -- Movement Information
    movement_type,
    CAST(status AS INT64) as status,
    number_of_boxes,

    -- Status Information
    error_message,

    -- Additional Metadata
    moved_by,

    -- Dates
    timestamp(created_at) as created_at

from {{ source(var('erp_source'), 'sh_movement_requests') }}

