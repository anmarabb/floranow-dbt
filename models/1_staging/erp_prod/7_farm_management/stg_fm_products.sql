With source as (
 select * from {{ source(var('erp_source'), 'fm_products') }}
)
select 
 
 
 --PK
   id as fm_product_id,

 --FK
    supplier_id,
    feed_source_id,
    fm_catalog_item_id,


--dim
    --main_group,
    --sub_group,
    properties,
    categorization,
    product_name,
    color as raw_color,
    status,
    moq,
    images,

    failure_reason,

    created_at,
    updated_at,



--fct
    quantity,
    published_quantity,
    produced_quantity,
    available_quantity,


    fob_price,
    number as astra_barcode,



    packing_rate,
    pn,


validated,



case 
    when main_group.hevo_20007300 is not null then main_group.hevo_20007300
    when main_group.hevo_30001100 is not null then main_group.hevo_30001100
    when main_group.hevo_10000100 is not null then main_group.hevo_10000100
    when main_group.hevo_20008900 is not null then main_group.hevo_20008900
    when main_group.hevo_30002400 is not null then main_group.hevo_30002400
    when main_group.hevo_10000200 is not null then main_group.hevo_10000200
    when main_group.hevo_20013300 is not null then main_group.hevo_20013300
    when main_group.hevo_20007900 is not null then main_group.hevo_20007900
    when main_group.hevo_20013800 is not null then main_group.hevo_20013800
    when main_group.hevo_20011700 is not null then main_group.hevo_20011700
    when main_group.hevo_20017300 is not null then main_group.hevo_20017300
    when main_group.hevo_20012800 is not null then main_group.hevo_20012800
    when main_group.hevo_20011000 is not null then main_group.hevo_20011000
    when main_group.hevo_20003400 is not null then main_group.hevo_20003400
    when main_group.hevo_30000400 is not null then main_group.hevo_30000400
    when main_group.hevo_20010000 is not null then main_group.hevo_20010000
    when main_group.hevo_20000100 is not null then main_group.hevo_20000100
    when main_group.hevo_20003200 is not null then main_group.hevo_20003200
    when main_group.hevo_20007300 is not null then main_group.hevo_20007300
    when main_group.hevo_20007600 is not null then main_group.hevo_20007600
    when main_group.hevo_20018700 is not null then main_group.hevo_20018700
    when main_group.hevo_20013000 is not null then main_group.hevo_20013000

end as main_group,


case
    when sub_group.hevo_20000102 is not null then sub_group.hevo_20000102
    when sub_group.hevo_20003401 is not null then sub_group.hevo_20003401
    when sub_group.hevo_20003402 is not null then sub_group.hevo_20003402
    when sub_group.hevo_20013001 is not null then sub_group.hevo_20013001  
    when sub_group.hevo_20007601 is not null then sub_group.hevo_20007601
    when sub_group.hevo_20003204 is not null then sub_group.hevo_20003204
    when sub_group.hevo_20003403 is not null then sub_group.hevo_20003403
    when sub_group.hevo_20003205 is not null then sub_group.hevo_20003205
    when sub_group.hevo_20003203 is not null then sub_group.hevo_20003203
    when sub_group.hevo_20018701 is not null then sub_group.hevo_20018701
    when sub_group.hevo_20003207 is not null then sub_group.hevo_20003207
    when sub_group.hevo_20007602 is not null then sub_group.hevo_20007602

end as sub_group,


case when pn.p1 in ('S20','S29') then s1_value else null end as stem_length,
case when pn.p2 in ('S19') then s2_value else null end as bud_height,
case when pn.p2 in ('S22') then s2_value else null end as bud_count,


current_timestamp() as ingestion_timestamp,
 




from source as p

where p.__hevo__marked_deleted is false