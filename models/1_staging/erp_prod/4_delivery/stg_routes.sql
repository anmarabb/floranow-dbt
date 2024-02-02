With source as (
 select * from {{ source(var('erp_source'), 'routes') }}
)
select 

            --PK
                id as route_id,
            --FK
            warehouse_id,
  

            --dim
            name as route_name,
            description,
            pod_seq_prefix,


                --date
                created_at,
                updated_at,
                deleted_at,
   
            
            --fct









current_timestamp() as ingestion_timestamp,
 




from source as pod
