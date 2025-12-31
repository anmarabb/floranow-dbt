with prep_countryas as (
    select distinct 
        country_iso_code as code, 
        country_name 
    from {{ source(var('erp_source'), 'country') }}
),

child_shipments as (
    select
        cast(master_shipment_id as int64) as master_shipment_id,
        count(distinct shipment_id) as actual_shipments_count
    from {{ ref('stg_vp_shipments') }}
    where master_shipment_id is not null
    group by master_shipment_id
)

select
    ms.*,
    
    -- Origin Country Name (expanded from code like "KE")
    origin_country.country_name as origin_country_name,
    
    -- Destination Country Name (expanded from code)
    dest_country.country_name as destination_country_name,
    
    -- Validation Fields
    coalesce(cs.actual_shipments_count, 0) as actual_shipments_count,

from {{ ref('stg_vp_master_shipments') }} as ms
left join prep_countryas as origin_country on ms.origin = origin_country.code
left join prep_countryas as dest_country on ms.destination = dest_country.code
left join child_shipments as cs on ms.master_shipment_id = cs.master_shipment_id

