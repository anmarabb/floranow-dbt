
With source as (
 select * from {{ source('erp_prod', 'line_items') }}
)
select 

*
 




from source as li

