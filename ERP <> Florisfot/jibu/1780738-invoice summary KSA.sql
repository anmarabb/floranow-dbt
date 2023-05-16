select
    asdasd.*,
    (
        select
            top 1 SUBSTRING(
                subquerytable.ARTTXT,
                CASE
                    POSITION('*INV:' IN ARTTXT)
                    WHEN 0 THEN 1000
                    ELSE POSITION('*INV:' IN ARTTXT) + 5
                END,
                10000
            )
        from
            horder subquerytable
        where
            subquerytable.factnr = asdasd.InvoiceNumber
    ) originalInvoice
from
    (
        Select
            distinct horder.debnr as DebtorNumber,
            Max(debiteur.debnaam) as DebtorName,
            max(VERKOPER.VERKOOPTXT) as SalesManager,
            isnull(DEBCAT.OMSCHRIJVING, 'No Category') as Category,
            horder.ordnr as OrderNumber,
            horder.FCTDAt as InvoiceDate,
            horder.factnr as InvoiceNumber,
            MAX (HeaderTotal) * 100 / 115 as InvAmt,
            (MAX (HeaderTotal) -(MAX (HeaderTotal) * 100 / 115)) as VAT,
            MAX (HeaderTotal) as InvoiceTotal,
            debiteur.DEBPLAATS as City
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
            left outer join DEBCAT on DEBITEUR.DEBCAT = DEBCAT.CATEGORY
        Where
            HORDER.fctdat >=: DateFrom
            AND HORDER.fctdat <=: DateTo
            and HORDER.DEBNR not like '9%'
            and HORDER.DEBNR not like '8%'
            and HORDER.DEBNR not in ('WASTE')
            and debiteur.FINADMIN in ('9', '10', '11', '12', '13')
        group by
            horder.ordnr,
            horder.debnr,
            horder.factnr,
            horder.LEVCOD,
            Category,
            InvoiceDate,
            debiteur.DEBPLAATS
    ) asdasd --Order by horder.FACTNR