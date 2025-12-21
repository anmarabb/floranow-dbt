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
     WHEN fs.feed_type = 0 THEN 'direct_link'
     WHEN fs.feed_type = 1 THEN 'reselling'
     WHEN fs.feed_type = 2 THEN 'grower_portal_feed'
     WHEN fs.feed_type = 3 THEN 'imported'
     WHEN fs.feed_type = 4 THEN 'farm'
     WHEN fs.feed_type = 5 THEN 'import_sheet_feed'
     WHEN fs.feed_type = 6 THEN 'vendor_portal_feed'
end as feed_type,

CASE
     WHEN fs.service_provider = 0 THEN 'florisoft'
     WHEN fs.service_provider = 1 THEN 'axerrio'
     WHEN fs.service_provider = 2 THEN 'grower_portal'
     WHEN fs.service_provider = 3 THEN 'floranow_erp'
     WHEN fs.service_provider = 4 THEN 'farm_management'
     WHEN fs.service_provider = 5 THEN 'import_sheet'
     WHEN fs.service_provider = 6 THEN 'vendor_portal'
END AS service_provider_name,

 


current_timestamp() as ingestion_timestamp,



from source as fs
left join {{ ref('base_suppliers') }} as s on s.supplier_id = fs.supplier_id
