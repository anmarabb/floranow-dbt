with prep_countryas as (
    select distinct 
        country_iso_code as code, 
        country_name 
    from {{ source(var('erp_source'), 'country') }}
)

select
    s.*,
    
    -- Master Shipment Information
    ms.master_shipment_name,
    ms.state as master_shipment_state,
    ms.origin as master_shipment_origin_code,
    ms.destination as master_shipment_destination_code,
    ms.point_of_entry as master_shipment_poe,
    ms.total_weight as master_shipment_total_weight,
    
    -- Origin Country Name (expanded from code like "KE")
    origin_country.country_name as origin_country_name,
    
    -- Destination Country Name (expanded from code)
    dest_country.country_name as destination_country_name,
    
    -- Warehouse Information (try multiple sources)
    coalesce(
        w.warehouse_name,
        w_from_user.warehouse_name
    ) as warehouse_name,
    coalesce(
        w.financial_administration,
        w_from_user.financial_administration
    ) as financial_administration,
    
    -- Customer Information (from debtor_number)
    u.user_category,
    u.customer_type,
    u.account_type,
    u.warehouse_id as user_warehouse_id

from {{ ref('stg_vp_shipments') }} as s
left join {{ ref('stg_vp_master_shipments') }} as ms on CAST(s.master_shipment_id AS INT64) = ms.master_shipment_id
left join prep_countryas as origin_country on s.origin = origin_country.code
left join prep_countryas as dest_country on s.destination = dest_country.code
left join {{ ref('base_warehouses') }} as w on CAST(s.warehouse_id AS INT64) = w.warehouse_id
left join {{ ref('base_users') }} as u on s.customer_debtor_number = u.debtor_number
left join {{ ref('base_warehouses') }} as w_from_user on u.warehouse_id = w_from_user.warehouse_id

