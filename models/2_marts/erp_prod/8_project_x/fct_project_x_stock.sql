with pm as (
        with pi as (

            select incidentable_id,
                   sum(incident_quantity) as incident_quantity

            from {{ref('fct_product_incidents')}}
            where incidentable_type = 'ProductLocationMovement'
            group by 1
            )
  
        select item_movement_request_id,
               sum(moved_out_quantity) as confirmed_quantity,
               sum(moved_in_quantity) as warehoused_quantity,
               sum(incident_quantity) as incident_quantity
        from {{ref ('dim_product_location_movements')}} pm
        left join pi on pi.incidentable_id = pm.id  
        group by 1
        )


select mr.created_at,
       mr.product_name,
       mr.color,
       --p.Supplier as farm_name,
       w.warehouse_name as warehouse,
       mr.requested_quantity,
       pm.confirmed_quantity,
       pm.warehoused_quantity,
       pm.incident_quantity

from {{ref ('stg_item_movement_requests')}} mr
left join pm on mr.id = pm.item_movement_request_id
left join {{ref ('base_warehouses')}} w on mr.destination_warehouse_id = w.warehouse_id