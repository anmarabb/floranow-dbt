 
 with line_items as (
select 
proof_of_delivery_id,
count (*) as item_count ,
min(created_at) as order_date,
sum(fulfilled_quantity) as fulfilled_quantity,
sum(fulfilled_value) as fulfilled_value,
from {{ ref('stg_line_items') }}
group by proof_of_delivery_id
 ),


 invoices as
(
select 
proof_of_delivery_id,
max(financial_administration) as financial_administration,
count (invoice_header_id) as invoice_count,
from {{ ref('stg_invoices') }}
--where proof_of_delivery_id is not null
group by proof_of_delivery_id
 )
 
 select 
 
 pod.customer_id,
pod.proof_of_delivery_id,
--date(li.order_date) as order_date,
pod.delivery_date,
pod.created_at as order_date,
pod.source_type,
pod.ids_count,
pod.pod_status,
pod.delivered_at,
pod.window_starts_at,


customer.name as Customer,
customer.Warehouse,
customer.Country,
customer.account_manager,




dispatched_by.name as dispatched_by,
moved_by.name as moved_by,
split_by.name as split_by,
skipped_by.name as skipped_by,

li.item_count, 

i.invoice_count,
i.financial_administration,

rou.route_name,

date.dim_date,

customer.debtor_number,

delivery_at,

fulfilled_quantity,
fulfilled_value,
pod_ready_datetime,

current_timestamp() as insertion_timestamp, 

 from {{ ref('stg_proof_of_deliveries') }} as pod
 
left join {{ref('base_users')}} as customer on customer.id = pod.customer_id
left join {{ref('base_users')}} as dispatched_by on dispatched_by.id = pod.dispatched_by_id
left join {{ref('base_users')}} as moved_by on moved_by.id = pod.moved_by_id
left join {{ref('base_users')}} as split_by on split_by.id = pod.split_by_id
left join {{ref('base_users')}} as skipped_by on skipped_by.id = pod.skipped_by_id

left join {{ref('stg_routes')}}  as rou on rou.route_id = pod.route_id
left join line_items as li on li.proof_of_delivery_id = pod.proof_of_delivery_id
left join invoices as i on i.proof_of_delivery_id = pod.proof_of_delivery_id

left join {{ref('dim_date')}}  as date on date.dim_date = date(pod.created_at)


