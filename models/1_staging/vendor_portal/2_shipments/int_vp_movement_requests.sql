with movement_items_dates as (
    select
        movement_request_id,
        any_value(old_departure_date) as old_departure_date,
        any_value(new_departure_date) as new_departure_date,
        avg(date_diff(new_departure_date, old_departure_date, day)) as avg_days_rescheduled
    from {{ ref('stg_vp_movement_request_items') }}
    where old_departure_date is not null and new_departure_date is not null
    group by movement_request_id
)

select
    mr.*,
    
    -- Source Shipment Information
    source_s.shipment_name as source_shipment_name,
    source_s.state as source_shipment_state,
    source_s.departure_date as source_shipment_departure_date,
    
    -- Target Shipment Information
    target_s.shipment_name as target_shipment_name,
    target_s.state as target_shipment_state,
    target_s.departure_date as target_shipment_departure_date,
    
    -- Reschedule Information (from movement_request_items)
    mid.old_departure_date,
    mid.new_departure_date,
    mid.avg_days_rescheduled as days_rescheduled,
    
    -- Reschedule Direction
    case 
        when mid.new_departure_date is not null
            and mid.old_departure_date is not null
            and mid.new_departure_date > mid.old_departure_date then 'Delayed'
        when mid.new_departure_date is not null
            and mid.old_departure_date is not null
            and mid.new_departure_date < mid.old_departure_date then 'Advanced'
        when mid.new_departure_date is not null
            and mid.old_departure_date is not null
            and mid.new_departure_date = mid.old_departure_date then 'Same Day'
        else null
    end as reschedule_direction

from {{ ref('stg_vp_movement_requests') }} as mr
left join {{ ref('stg_vp_shipments') }} as source_s on mr.source_shipment_id = source_s.shipment_id
left join {{ ref('stg_vp_shipments') }} as target_s on mr.target_shipment_id = target_s.shipment_id
left join movement_items_dates as mid on mr.movement_request_id = mid.movement_request_id

