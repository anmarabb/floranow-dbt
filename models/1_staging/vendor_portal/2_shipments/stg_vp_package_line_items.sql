select
    -- Primary Key
    id as package_line_item_id,

    -- Foreign Keys
    package_id,
    line_item_id,

    -- Quantities
    quantity,

    -- Dates
    timestamp(created_at) as created_at,

from {{ source(var('erp_source'), 'sh_package_line_items') }}

