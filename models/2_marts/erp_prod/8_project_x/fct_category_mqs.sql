with remaining as (
    select product_subcategory,
           product_subgroup,
           product_color,
           sum(remaining_qty_A1_X) as A1_X_remaining,
           sum(remaining_qty_X_FN) as X_FN_remaining

    from {{ref('fct_project_x')}}
    group by 1, 2, 3
)

select cm.*,
       CONCAT(coalesce(trim(cm.sub_category),''), coalesce(trim(cm.sub_group),''), coalesce(lower(trim(cm.product_color)),'')) as category_linking,
       A1_X_remaining,
       X_FN_remaining


from {{ref ('stg_category_mqs')}} as cm
left join remaining as r on r.product_subcategory = cm.sub_category and r.product_subgroup = cm.sub_group and r.product_color = cm.product_color 