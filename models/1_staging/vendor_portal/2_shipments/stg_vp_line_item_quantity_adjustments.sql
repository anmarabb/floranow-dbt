select
    -- Primary Key
    id as adjustment_id,

    -- Foreign Keys
    line_item_id,
    shipment_id,
    variation_id,

    -- Adjustment Information
    adjustment_type,
    old_quantity,
    new_quantity,
    reason,
    note,

    -- Additional Metadata
    created_by,

    -- Dates
    timestamp(created_at) as created_at

from {{ source(var('erp_source'), 'sh_line_item_quantity_adjustments') }}

