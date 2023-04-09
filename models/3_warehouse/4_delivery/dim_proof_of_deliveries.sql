with

source as ( 
        
select     
dim_date,

proof_of_delivery_id,
order_date,
delivery_date,
source_type,
ids_count,
pod_status,

customer,
warehouse,
country,
financial_administration,


dispatched_by,
item_count,





    case 
    when date_diff(date(delivery_date)  ,current_date(), month) > 1 then 'Wrong date' 
    when delivery_date > current_date() then "Future" 
    when delivery_date = current_date()-1 then "Yesterday" 
    when delivery_date = current_date() then "Today" 
    when date_diff(cast(current_date() as date ),cast(delivery_date as date), MONTH) = 0 then 'Month To Date'
    when date_diff(cast(current_date() as date ),cast(delivery_date as date), MONTH) = 1 then 'Last Month'
    when date_diff(cast(current_date() as date ),cast(delivery_date as date), YEAR) = 0 then 'Year To Date'

    else "Past" end as select_delivery_date,


    current_timestamp() as insertion_timestamp, 

from {{ ref('int_proof_of_deliveries')}} as pod



    )

select * from source