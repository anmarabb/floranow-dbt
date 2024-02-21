

with astra_budget as 

    (
        select
        db.sub_group,
        db.color,

        FROM  {{ref('fct_astra_budget_2024')}} as db
        group by 1,2

    ),

 box_items as 
    (

        select
        bi.sub_group,
        bi.color,
        from   {{ref('fct_fm_box_items')}} as bi
        where sub_group is not null
        group by 1,2

    )

select
bi.sub_group as box_items_sub_group,
bi.color as box_items_color,
from box_items as bi
left join astra_budget as db on db.sub_group = bi.sub_group and db.color = bi.color
where db.sub_group is null