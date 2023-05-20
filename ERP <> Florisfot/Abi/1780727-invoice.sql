select
    pkey as id,
    SUBDEBNR as sub_debtor_number,
    HORDER.DEBNR as debtor_number,
    ORDNR as order_number,
    FACTNR as invoice_number,
    HORDER.ARTTXT as product_name,
    LEVTOTAAL as quantity,
    ARTLEN as stem_length,
    color.colortxt as color,
    PART1PRIJS as landed_cost,
    VERKBEDRAG as unit_price,
    INCLREGKK as price_without_tax,
    DEVISIE as stock_code,
    HORDER.LEVCOD as supplier_code,
    cast(HORDER.FCTDAT AS SQL_VARCHAR) as invoice_date,
    HORDER.VE as sales_unit,
    CAST(HORDER.CREATIE AS SQL_VARCHAR) as created_at,
    HORDER.PARTIJNR as parcel_number,
    cast(HORDER.PARTIJDAT AS SQL_VARCHAR) as parcel_date,
    cast(HORDER.orddat AS SQL_VARCHAR) as order_date,
    HORDER.ARTNR as article_id,
    HORDER.ARTGRPCOD as article_group_id,
    HORDER.Celcod as product_group_id
from
    HORDER
    left join ARTIKEL on HORDER.ARTNR = ARTIKEL.ARTNR
    left join color on color.COLORNR = ARTIKEL.colornr
    left join LEVER on LEVER.levcod = HORDER.LEVCOD
where
    HORDER.ARTTXT <> 'Trolley'
    and HORDER.FACTNR in (
        select
            distinct temp.FACTNR
        from
            (
                select
                    CAST(HORDERKP.CREATIE AS SQL_VARCHAR) as invoice_created_at,
                    max(BETAAL.creatie) as last_payment,
                    HORDERKP.FACTNR,
                    HORDERKP.FACTTOTAAL,
                    isnull(sum(BETAAL.BEDRAG), 0) as PaidAmount
                from
                    HORDERKP
                    left join BETAAL on HORDERKP.FACTNR = BETAAL.FACTNR
                where
                    (
                        HORDERKP.BTWOK = true
                        or (
                            HORDERKP.BTWOK = false
                            and HORDERKP.intern = false
                        )
                    )
                    and FCTDAT is not null
                group by
                    HORDERKP.FACTTOTAAL,
                    HORDERKP.FACTNR,
                    invoice_created_at
            ) temp
        where
            (
                (
                    abs(temp.FACTTOTAAL-(isnull(temp.PaidAmount, 0))) > 0.001
                )
                and temp.invoice_created_at >= '2021-06-27 00:00:00'
            )
            or (
                temp.invoice_created_at >= '2022-01-01 00:00:00'
                or temp.last_payment >= '2022-01-01 00:00:00'
            )
    )
    and HORDER.pkey >: sql_last_value
order by
    HORDER.pkey asc