

With
prep_countryas as (select distinct country_iso_code  as code, country_name from {{ source(var('erp_source'), 'country') }} )

select 

   msh.* EXCEPT (origin),

 c.country_name as origin, 

current_timestamp() as ingestion_timestamp, 




from{{ ref('stg_master_shipments') }}  as msh
left join prep_countryas as c on msh.origin = c.code
