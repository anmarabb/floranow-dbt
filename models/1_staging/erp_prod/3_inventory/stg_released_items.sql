WITH source AS (
    SELECT * 
    FROM {{ source(var('erp_source'), 'released_items') }} AS rel
    WHERE rel.deleted_at IS NULL
        AND rel.__hevo__marked_deleted IS NOT TRUE
)
SELECT 
    -- Primary Key
    rel.id AS released_item_id,
    
    -- Foreign Keys
    rel.product_id,
    
    -- Dimensions
    rel.status,  -- PENDING, COMPLETED, CANCELLED
    
    -- Dates
    rel.created_at AS released_at,
    rel.updated_at,
    rel.deleted_at,
    
    -- Facts
    rel.quantity,
    
    -- Metadata
    current_timestamp() AS ingestion_timestamp

FROM source AS rel

