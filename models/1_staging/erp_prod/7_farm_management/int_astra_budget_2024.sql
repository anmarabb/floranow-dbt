

with produced_quantity as 

  (

    select
    sub_group,
    color,
    week_number,
    sum(produced_quantity) as produced_quantity,

    from   {{ ref('fct_fm_box_items') }} as boxitems
    where shipment_type = 'Production' and fm_shipment_id not in (3555,3506,3511)
    group by 1,2,3
  ),

  available_quantity as 
  
    (
    select    
    sub_group,
    color,
    --week_number,
    sum(available_quantity) as available_quantity,

    from   {{ ref('int_fm_products') }} as p
    group by 1,2

    ),


  inbound_quantity as 
  
    (
    select    
    sub_group,
    color,
    week_number,
    sum(inbound_quantity) as inbound_quantity,

    from   {{ ref('fct_fm_inbound_stock_items') }} as inbound
    where fm_shipment_id not in (3555,3506,3511)
    group by 1,2,3

    )


SELECT

db.*,

COALESCE(boxitems.produced_quantity, 0) AS produced_quantity,
COALESCE(p.available_quantity, 0) AS available_quantity,
COALESCE(inbound.inbound_quantity, 0) AS inbound_quantity,

FROM  {{ref('stg_astra_budget_2024')}} as db
left join produced_quantity as boxitems on boxitems.sub_group = db.sub_group and boxitems.color = db.color and  boxitems.week_number = db.week_number
left join available_quantity as p on p.sub_group = db.sub_group and p.color = db.color
left join inbound_quantity as inbound on inbound.sub_group = db.sub_group and inbound.color = db.color and  inbound.week_number = db.week_number

