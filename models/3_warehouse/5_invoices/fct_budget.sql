with

source as ( 

select
*,
current_timestamp() as insertion_timestamp, 


from {{ref('stg_budget')}} as bud 
)

select * from source

