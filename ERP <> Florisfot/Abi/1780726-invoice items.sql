Select
    id,
    debtor_number,
    printed_by,
    is_internal,
    order_number,
    paid_amount,
    client_name,
    invoice_number,
    currency,
    total_amount_without_vat,
    total_tax,
    total_amount,
    order_date,
    delivery_date,
    invoice_date,
    printed_at,
    created_at,
    country_code,
    remaining_amount,
    invoice_client_name,
    count(distinct barcode) as items_count
from
    (
        select
            distinct i.pkey as id,
            i.DEBNR as debtor_number,
            i.PRINTBY as printed_by,
            i.intern as is_internal,
            i.ORDNR as order_number,
            round(isnull(sum(p.BEDRAG), 0), 2) as paid_amount,
            i.DEBNAAM as client_name,
            i.FACTNR as invoice_number,
            i.VALCOD as currency,
            i.PRTOTVERK as total_amount_without_vat,
            (
                case
                    when i.BTWLAAG <> 0 then i.BTWLAAG
                    else i.BTWHOOG
                end
            ) as total_tax,
            i.FACTTOTAAL as total_amount,
            CAST(i.ORDDAT as SQL_VARCHAR) as order_date,
            CAST(i.VERTREKDAG as SQL_VARCHAR) as delivery_date,
            CAST(i.FCTDAT as SQL_VARCHAR) as invoice_date,
            CAST(i.PRINTTIJD as SQL_VARCHAR) as printed_at,
            CAST(i.CREATIE as SQL_VARCHAR) as created_at,
            i.LANDCOD as country_code,
            round((i.FACTTOTAAL-(isnull(sum(p.BEDRAG), 0))), 2) as remaining_amount,
            i.LVNAAMDB as invoice_client_name
        from
            HORDERKP i
            left join BETAAL p on i.FACTNR = p.FACTNR
        where
            (
                (
                    i.BTWOK = false
                    and i.intern = false
                )
                or i.BTWOK = true
            )
            and i.pkey >: sql_last_value
        group by
            i.FACTNR,
            i.DEBNR,
            i.ORDNR,
            i.DEBNAAM,
            i.LANDCOD,
            i.FACTNR,
            i.FCTDAT,
            i.VALCOD,
            i.FACTTOTAAL,
            i.ORDDAT,
            i.ORDTFNR,
            i.pkey,
            i.intern,
            i.PRTOTVERK,
            i.BTWLAAG,
            i.BTWHOOG,
            i.LVNAAMDB,
            i.BTWNRDEB,
            i.PRINTBY,
            i.PRINTTIJD,
            i.VERTREKDAG,
            i.CREATIE
        having
            (
                abs(i.FACTTOTAAL-(isnull(sum(p.BEDRAG), 0))) > 0.001
                and i.creatie >= '2021-06-27 00:00:00'
            )
            or (
                i.creatie >= '2022-01-01 00:00:00'
                OR max(p.creatie) >= '2022-01-01 00:00:00'
            )
    ) TempT
    left join HORDER on TempT.invoice_number = HORDER.FACTNR
where
    HORDER.FCTDAT is not null
    and HORDER.ARTTXT <> 'Trolley'
group by
    HORDER.FACTNR,
    id,
    debtor_number,
    printed_by,
    is_internal,
    order_number,
    paid_amount,
    client_name,
    invoice_number,
    currency,
    total_amount_without_vat,
    total_tax,
    total_amount,
    order_date,
    delivery_date,
    invoice_date,
    printed_at,
    created_at,
    country_code,
    remaining_amount,
    invoice_client_name
order by
    id asc