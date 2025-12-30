select
    -- Primary Key
    id as package_id,

    -- Foreign Keys
    shipment_id,
    warehouse_id,

    -- Identifiers
    box_label,
    number as package_number,
    package_type,
    package_dimensions,
    sequence_number,

    -- Status Flags
    is_packed,
    is_full,
    has_additions,

    -- Quantities & Metrics
    item_count,
    total_items_count,
    total_items_count_1,
    total_stems_count,
    package_weight,
    used_space,

    -- Additional Fields
    assigned_additional_debtor_number,
    preview_line_items,
    sticker_preview_line_items,
    packed_by,

    -- Dates
    timestamp(created_at) as created_at,
    timestamp(packed_at) as packed_at,

from {{ source(var('erp_source'), 'sh_packages') }}


