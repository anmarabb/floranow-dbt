Select
    horder.ordnr as OrderNumber,
    '20' + left (HORDER.ordnr, 2) + '-' + substring(HORDER.ordnr, 3, 2) + '-' + substring(HORDER.ordnr, 5, 2) as "Shipment Date",
    horder.debnr as DebtorNumber,
    Max(debiteur.debnaam) as DebtorName,
    horder.LEVCOD as SupCode,
    Max(lever.levnaam) as SupName,
    horder.factnr as InvoiceNumber,
    SUM (levtotaal * VERKBEDRAG) as InvoiceTotal,
    MAX (HeaderTotal) as HeaderTotal,
    verkoper.VERKOOPTXT AS SalesExec,
    case
        debiteur.DEBCAT
        when 10001 then 'Retail Shops'
        when 10002 then 'Alissar'
        when 10003 then 'Hotels'
        when 10004 then 'Weddings & Events'
        when 10005 then 'Super Market'
        else 'No Category'
    end as Category
from
    horder
    left outer join (
        Select
            factnr,
            sum (facttotaal) as HeaderTotal
        from
            horderkp
        Where
            FCTDAT >=: DateFrom
            AND FCTDAT <=: DateTo
        group by
            factnr
    ) S on horder.factnr = s.factnr
    Left outer join lever on horder.levcod = lever.levcod
    Left outer join debiteur on horder.debnr = debiteur.debnr
    left outer join verkoper on debiteur.acctmngr = verkoper.verkoopnr
Where
    HORDER.fctdat >=: DateFrom
    AND HORDER.fctdat <=: DateTo
    and HORDER.DEBNR not like '9%'
    and HORDER.DEBNR not like '8%'
    and HORDER.DEBNR not in ('WASTE')
    and HORDER.DEBNR not like '%SLD'
group by
    horder.ordnr,
    horder.debnr,
    horder.factnr,
    horder.LEVCOD,
    Category,
    '20' + left (HORDER.ordnr, 2) + '-' + substring(HORDER.ordnr, 3, 2) + '-' + substring(HORDER.ordnr, 5, 2),
    SalesExec
Order by
    horder.FACTNR