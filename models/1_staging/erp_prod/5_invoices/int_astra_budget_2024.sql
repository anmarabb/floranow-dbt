

with producation as 

  (

    select
    sub_group,
    color,
    week_number,
   -- produced_quantity,
   sum(produced_quantity) as produced_quantity,
    from   {{ ref('fct_fm_box_items') }} as p
    where sub_group = 'Alstroemeria' and color = 'white' and  week_number = '2024 - week 7'
    group by 1,2,3


  )


SELECT

db.*,
p.produced_quantity,

FROM  {{ref('stg_astra_budget_2024')}} as db
left join producation as p on p.sub_group = db.sub_group and p.color = db.color and  p.week_number = db.week_number
