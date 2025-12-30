select
    -- Primary Key
    id as movement_request_item_id,

    -- Foreign Keys
    movement_request_id,
    package_id,
    line_item_id,

    -- Movement Information
    quantity,
    date(old_departure_date) as old_departure_date,
    date(new_departure_date) as new_departure_date,

from {{ source(var('erp_source'), 'sh_movement_request_items') }}


