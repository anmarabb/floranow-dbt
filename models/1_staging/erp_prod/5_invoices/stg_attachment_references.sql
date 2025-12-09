select *
FROM {{ source(var('erp_source'), 'attachment_references') }} as b