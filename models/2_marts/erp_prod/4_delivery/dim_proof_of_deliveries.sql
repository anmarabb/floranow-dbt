with

source as ( 
        
select     
customer_id,
route_name,

proof_of_delivery_id,
order_date,
delivery_date,
source_type,
ids_count,
pod_status,

Customer,
warehouse,
country,
financial_administration,
account_manager,


dispatched_by,
skipped_by,
moved_by,
split_by,

case 
    when pod_status = 'DISPATCHED' then concat('dispatched_by',': ',dispatched_by)
    when pod_status = 'SKIPPED' then concat('skipped_by',': ',skipped_by) 
    else null end as action_by,



item_count,



    case 
    when date_diff(date(delivery_date)  ,current_date(), month) > 1 then 'Wrong date' 
    when delivery_date > current_date() then "Future" 
    when delivery_date = current_date() then "Today" 
    when delivery_date < current_date()-1 then "Past" 

    else "Past" end as select_delivery_date,


    current_timestamp() as insertion_timestamp, 

from {{ ref('int_proof_of_deliveries')}} as pod



    )

select * from source