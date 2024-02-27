With source as (
 select * from {{ source(var('erp_source'), 'fm_stock_transactions') }}
)
select 
 
 
 --PK
   id as fm_stock_transaction_id,

 --FK
fm_cycle_count_id,
fm_location_id,
fm_product_id,
sourceable_id,
user_id,

--dim

production_date,
created_at as stock_transaction_at,
updated_at,
expired_at,


transaction_type, --INBOUND, OUTBOUND
sourceable_type,

reason,
status,

--fct

quantity,


current_timestamp() as ingestion_timestamp,
 




from source as p

