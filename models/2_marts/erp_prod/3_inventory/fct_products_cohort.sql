-- fct_products_cohort: Product + Warehouse grain from fct_products, i_* selling + order calculations (uncapped trend/seasonality)

with

invoices_by_pw as (
    select
        product,
        warehouse,
        sum(case when date_diff(date_sub(current_date(), interval 1 day), date(invoice_header_printed_at), day) < 29 and date_diff(date_sub(current_date(), interval 1 day), date(invoice_header_printed_at), day) >= -1 then quantity else 0 end) as i_last_30d_sold_quantity,
        sum(case when date_diff(date_sub(current_date(), interval 1 day), date(invoice_header_printed_at), day) < 6 and date_diff(date_sub(current_date(), interval 1 day), date(invoice_header_printed_at), day) >= -1 then quantity else 0 end) as i_last_7d_sold_quantity,
        safe_divide(sum(case when date_diff(date_sub(current_date(), interval 1 day), date(invoice_header_printed_at), day) < 20 and date_diff(date_sub(current_date(), interval 1 day), date(invoice_header_printed_at), day) >= -1 then quantity else 0 end), 3) as i_last_3_weeks_avg_sold_quantity,
        sum(case when date_diff(date_sub(current_date(), interval 1 day), date(invoice_header_printed_at), day) < 2 and date_diff(date_sub(current_date(), interval 1 day), date(invoice_header_printed_at), day) >= -1 then quantity else 0 end) as i_last_3d_sold_quantity,
        sum(case when (lower(feed_source_name) like '%flash%' or lower(feed_source_name) like '%promo%') and date_diff(date_sub(current_date(), interval 1 day), date(invoice_header_printed_at), day) < 6 and date_diff(date_sub(current_date(), interval 1 day), date(invoice_header_printed_at), day) >= -1 then quantity else 0 end) as i_last_7d_sold_quantity_promo,
        sum(case when ((feed_source_name is null) or (lower(feed_source_name) not like '%flash%' and lower(feed_source_name) not like '%promo%')) and date_diff(date_sub(current_date(), interval 1 day), date(invoice_header_printed_at), day) < 6 and date_diff(date_sub(current_date(), interval 1 day), date(invoice_header_printed_at), day) >= -1 then quantity else 0 end) as i_last_7d_sold_quantity_normal
    from {{ ref('fct_invoice_items') }}
    where record_type = 'Invoice - AUTO'
      and inv_items_reprot_filter = 'Floranow Sales'
      and stock_model in ('Reselling', 'Commission Based', 'Internal - Project X')
    group by 1, 2
),

ow_by_pw as (
    select
        fp.Product,
        fp.warehouse,
        min(ow.current_departure_date) as current_departure_date
    from {{ ref('fct_products') }} fp
    left join {{ ref('fct_spree_offering_windows') }} ow
        on ow.warehouse = fp.warehouse and fp.Origin = ow.Origin and fp.Supplier = ow.Supplier
    where fp.stock_model in ('Reselling', 'Commission Based', 'Internal - Project X')
    group by fp.Product, fp.warehouse
),

next_coming_stock as (
    select
        fp.Product,
        fp.warehouse,
        min(case when fp.departure_date > coalesce(ow_by_pw.current_departure_date, current_date()) then fp.departure_date else null end) as next_coming_date
    from {{ ref('fct_products') }} fp
    left join ow_by_pw on ow_by_pw.Product = fp.Product and ow_by_pw.warehouse = fp.warehouse
    where fp.coming_quantity > 0
      and fp.stock_model in ('Reselling', 'Commission Based', 'Internal - Project X')
    group by fp.Product, fp.warehouse
),

coming_by_departure as (
    select
        fp.Product,
        fp.warehouse,
        sum(case when fp.departure_date <= ow_by_pw.current_departure_date then fp.coming_quantity else 0 end) as coming_before_departure
    from {{ ref('fct_products') }} fp
    left join ow_by_pw on ow_by_pw.Product = fp.Product and ow_by_pw.warehouse = fp.warehouse
    where fp.coming_quantity > 0
      and fp.stock_model in ('Reselling', 'Commission Based', 'Internal - Project X')
    group by fp.Product, fp.warehouse
)

select
    p.Product,
    p.warehouse,

    max(inv.i_last_30d_sold_quantity) as i_last_30d_sold_quantity,
    max(inv.i_last_7d_sold_quantity) as i_last_7d_sold_quantity,
    max(inv.i_last_3_weeks_avg_sold_quantity) as i_last_3_weeks_avg_sold_quantity,
    max(inv.i_last_3d_sold_quantity) as i_last_3d_sold_quantity,
    max(inv.i_last_7d_sold_quantity_promo) as i_last_7d_sold_quantity_promo,
    max(inv.i_last_7d_sold_quantity_normal) as i_last_7d_sold_quantity_normal,

    sum(p.in_stock_quantity) as in_stock_quantity,
    sum(p.coming_quantity) as coming_quantity,
    max(p.shelf_life_days) as shelf_life_days,
    max(ow.current_departure_date) as current_departure_date,
    max(ncs.next_coming_date) as next_coming_date,
    max(cbd.coming_before_departure) as coming_before_departure,

    sum(p.i_last_year_7d_sold_quantity) as i_last_year_7d_sold_quantity,
    sum(p.i_last_year_next_7d_sold_quantity) as i_last_year_next_7d_sold_quantity,

    safe_divide(max(inv.i_last_7d_sold_quantity), 7) as daily_demand,

    coalesce(
        safe_divide(safe_divide(max(inv.i_last_7d_sold_quantity), 7), safe_divide(max(inv.i_last_30d_sold_quantity), 30)),
        1.0
    ) as trend_factor,

    coalesce(
        safe_divide(sum(p.i_last_year_next_7d_sold_quantity), nullif(sum(p.i_last_year_7d_sold_quantity), 0)),
        1.0
    ) as seasonality,

    safe_divide(max(inv.i_last_7d_sold_quantity), 7)
    * coalesce(safe_divide(safe_divide(max(inv.i_last_7d_sold_quantity), 7), safe_divide(max(inv.i_last_30d_sold_quantity), 30)), 1.0)
    * coalesce(safe_divide(sum(p.i_last_year_next_7d_sold_quantity), nullif(sum(p.i_last_year_7d_sold_quantity), 0)), 1.0)
    as adjusted_demand,

    least(
        coalesce(max(p.shelf_life_days), 7),
        coalesce(
            date_diff(max(ncs.next_coming_date), max(ow.current_departure_date), day),
            coalesce(max(p.shelf_life_days), 7)
        )
    ) as coverage_days,

    1.28
    * (safe_divide(max(inv.i_last_7d_sold_quantity), 7) * 0.3)
    * sqrt(
        least(
            coalesce(max(p.shelf_life_days), 7),
            coalesce(date_diff(max(ncs.next_coming_date), max(ow.current_departure_date), day), coalesce(max(p.shelf_life_days), 7))
        )
    ) as safety_stock,

    (
        safe_divide(max(inv.i_last_7d_sold_quantity), 7)
        * coalesce(safe_divide(safe_divide(max(inv.i_last_7d_sold_quantity), 7), safe_divide(max(inv.i_last_30d_sold_quantity), 30)), 1.0)
        * coalesce(safe_divide(sum(p.i_last_year_next_7d_sold_quantity), nullif(sum(p.i_last_year_7d_sold_quantity), 0)), 1.0)
        * least(coalesce(max(p.shelf_life_days), 7), coalesce(date_diff(max(ncs.next_coming_date), max(ow.current_departure_date), day), coalesce(max(p.shelf_life_days), 7)))
    )
    + (
        1.28 * (safe_divide(max(inv.i_last_7d_sold_quantity), 7) * 0.3)
        * sqrt(least(coalesce(max(p.shelf_life_days), 7), coalesce(date_diff(max(ncs.next_coming_date), max(ow.current_departure_date), day), coalesce(max(p.shelf_life_days), 7))))
    )
    as total_need,

    sum(p.in_stock_quantity) + coalesce(max(cbd.coming_before_departure), 0) as available_stock,

    greatest(0,
        (
            safe_divide(max(inv.i_last_7d_sold_quantity), 7)
            * coalesce(safe_divide(safe_divide(max(inv.i_last_7d_sold_quantity), 7), safe_divide(max(inv.i_last_30d_sold_quantity), 30)), 1.0)
            * coalesce(safe_divide(sum(p.i_last_year_next_7d_sold_quantity), nullif(sum(p.i_last_year_7d_sold_quantity), 0)), 1.0)
            * least(coalesce(max(p.shelf_life_days), 7), coalesce(date_diff(max(ncs.next_coming_date), max(ow.current_departure_date), day), coalesce(max(p.shelf_life_days), 7)))
        )
        + (
            1.28 * (safe_divide(max(inv.i_last_7d_sold_quantity), 7) * 0.3)
            * sqrt(least(coalesce(max(p.shelf_life_days), 7), coalesce(date_diff(max(ncs.next_coming_date), max(ow.current_departure_date), day), coalesce(max(p.shelf_life_days), 7))))
        )
        - (sum(p.in_stock_quantity) + coalesce(max(cbd.coming_before_departure), 0))
    ) as order_quantity,

    case
        when max(ow.current_departure_date) is null then 'NO WINDOW'
        when (
            (safe_divide(max(inv.i_last_7d_sold_quantity), 7) * coalesce(safe_divide(safe_divide(max(inv.i_last_7d_sold_quantity), 7), safe_divide(max(inv.i_last_30d_sold_quantity), 30)), 1.0) * coalesce(safe_divide(sum(p.i_last_year_next_7d_sold_quantity), nullif(sum(p.i_last_year_7d_sold_quantity), 0)), 1.0) * least(coalesce(max(p.shelf_life_days), 7), coalesce(date_diff(max(ncs.next_coming_date), max(ow.current_departure_date), day), coalesce(max(p.shelf_life_days), 7))))
            + (1.28 * (safe_divide(max(inv.i_last_7d_sold_quantity), 7) * 0.3) * sqrt(least(coalesce(max(p.shelf_life_days), 7), coalesce(date_diff(max(ncs.next_coming_date), max(ow.current_departure_date), day), coalesce(max(p.shelf_life_days), 7)))))
            - (sum(p.in_stock_quantity) + coalesce(max(cbd.coming_before_departure), 0))
        ) > 0 then 'ORDER'
        else 'SKIP'
    end as recommendation,

    safe_divide(
        sum(p.in_stock_quantity) + coalesce(max(cbd.coming_before_departure), 0),
        safe_divide(max(inv.i_last_7d_sold_quantity), 7)
        * coalesce(safe_divide(safe_divide(max(inv.i_last_7d_sold_quantity), 7), safe_divide(max(inv.i_last_30d_sold_quantity), 30)), 1.0)
    ) as days_of_supply

from {{ ref('fct_products') }} p
left join invoices_by_pw inv on inv.product = p.Product and inv.warehouse = p.warehouse
left join ow_by_pw ow on ow.Product = p.Product and ow.warehouse = p.warehouse
left join next_coming_stock ncs on ncs.Product = p.Product and ncs.warehouse = p.warehouse
left join coming_by_departure cbd on cbd.Product = p.Product and cbd.warehouse = p.warehouse
where p.stock_model in ('Reselling', 'Commission Based', 'Internal - Project X')
group by p.Product, p.warehouse
