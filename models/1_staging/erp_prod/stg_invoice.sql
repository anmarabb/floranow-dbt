With source as (
 select * from {{ source('erp_prod', 'invoices') }}
)
select 

*





from source as i