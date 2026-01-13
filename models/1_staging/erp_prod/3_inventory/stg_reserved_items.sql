WITH source AS (
    SELECT * 
    FROM {{ source(var('erp_source'), 'reserved_items') }} AS ri
    -- WHERE ri.deleted_at IS NULL
    --     AND ri.__hevo__marked_deleted IS NOT TRUE
)
SELECT 
    -- Primary Key
    ri.id AS reserved_item_id,
    
    -- Foreign Keys
    ri.product_id,
    
    -- Dimensions
    ri.status,  -- PENDING, PARTIALLY_RELEASED, PROCESSING, RELEASED, CANCELLED
    
    -- Dates
    ri.created_at AS reserved_at,
    ri.updated_at,
    -- ri.deleted_at,
    
    -- Facts
    ri.quantity,
    ri.released_quantity,
    -- Net reserved quantity (quantity - released_quantity)
    COALESCE(ri.quantity, 0) - COALESCE(ri.released_quantity, 0) AS net_reserved_quantity,
    
    -- Metadata
    current_timestamp() AS ingestion_timestamp

FROM source AS ri

