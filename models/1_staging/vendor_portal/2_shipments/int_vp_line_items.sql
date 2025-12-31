with prep_countryas as (
    select distinct 
        country_iso_code as code, 
        country_name 
    from {{ source(var('erp_source'), 'country') }}
),

package_aggregates as (
    select
        line_item_id,
        count(distinct package_id) as package_count,
        sum(quantity) as total_packed_quantity
    from {{ ref('stg_vp_package_line_items') }}
    group by line_item_id
)

select
    li.*,
    
    -- Origin Country Name (expanded from code)
    origin_country.country_name as origin_country_name,
    
    -- Destination Country Name (expanded from code)
    dest_country.country_name as destination_country_name,
    
    -- Shortage Calculation
    case 
        when li.original_quantity > li.quantity then li.original_quantity - li.quantity
        else 0
    end as shortage_quantity,
    
    -- Package Information (aggregated)
    pkg.package_count,
    pkg.total_packed_quantity

from {{ ref('stg_vp_line_items') }} as li
left join prep_countryas as origin_country on li.country_of_origin = origin_country.code
left join prep_countryas as dest_country on li.destination = dest_country.code
left join package_aggregates as pkg on li.line_item_id = pkg.line_item_id

