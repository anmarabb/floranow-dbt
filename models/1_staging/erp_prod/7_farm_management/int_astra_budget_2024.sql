

with produced_quantity as 

  (

    select
    sub_group,
    color,
    week_number,
    sum(produced_quantity) as produced_quantity,

    from   {{ ref('fct_fm_box_items') }} as boxitems
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

    )


SELECT

db.*,
boxitems.produced_quantity,
p.available_quantity,
FROM  {{ref('stg_astra_budget_2024')}} as db
left join produced_quantity as boxitems on boxitems.sub_group = db.sub_group and boxitems.color = db.color and  boxitems.week_number = db.week_number
left join available_quantity as p on p.sub_group = db.sub_group and p.color = db.color
