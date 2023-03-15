
With source as (
 select * from {{ source('erp_prod', 'line_items') }}
)
select 

li.id as line_item_id,

li.categorization,

  JSON_EXTRACT_SCALAR(categorization, '$.age') as age,
 -- JSON_EXTRACT_SCALAR(categorization, '$.name') as name,


from source as li

