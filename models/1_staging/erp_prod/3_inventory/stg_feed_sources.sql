With source as (
 select * from {{ source(var('erp_source'), 'feed_sources') }}
)
select 

fs.id as feed_source_id,
fs.name as feed_source_name,
s.supplier_name,
fs.supplier_id,
fs.currency,
fs.supplying_country_code,
fs.reselling,
fs.floranow_feed_id,

fs.availability_type,

case 
    when fs.feed_type = 0 then 'direct_link'
    when fs.feed_type = 1 then 'reselling'
    when fs.feed_type = 2 then 'grower_portal_feed'
    when fs.feed_type = 3 then 'imported'
    else 'cheak'
    end as feed_type,



 


current_timestamp() as ingestion_timestamp,



from source as fs
left join {{ ref('base_suppliers') }} as s on s.supplier_id = fs.supplier_id
