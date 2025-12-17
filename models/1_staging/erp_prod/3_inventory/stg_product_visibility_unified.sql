select 
    *
from {{ source(var('erp_source'), 'product_visibility_unified') }}

