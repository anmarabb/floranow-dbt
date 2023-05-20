Select
    horder.ordnr as OrderNumber,
    '20' + left (HORDER.ordnr, 2) + '-' + substring(HORDER.ordnr, 3, 2) + '-' + substring(HORDER.ordnr, 5, 2) as "Shipment Date",
    horder.factnr as InvoiceNumber,
    horder.debnr as DebtorNumber,
    Max(debiteur.debnaam) as DebtorName,
    horder.ARTNR as ArticleNumber,
    ARTTXT as ProductName,
    Max(lever.levnaam) as SupName,
    sum (levtotaal) as TotalStems,
    SUM (PART1PRIJS) as "Cost (Landed)",
    sum (verkbedrag) as SoldPrice,
    SUM (levtotaal * VERKBEDRAG) as InvoiceTotal,
    verkoper.VERKOOPTXT AS SalesExec,
    isnull(DEBCAT.OMSCHRIJVING, 'No Category') as Category,
    horder.USERID as CreatedUser
from
    horder
    Left outer join lever on horder.levcod = lever.levcod
    Left outer join debiteur on horder.debnr = debiteur.debnr
    left outer join verkoper on debiteur.acctmngr = verkoper.verkoopnr
    left outer join DEBCAT on DEBITEUR.DEBCAT = DEBCAT.CATEGORY
Where
    HORDER.fctdat >=: DateFrom
    AND HORDER.fctdat <=: DateTo
    and HORDER.DEBNR not like '9%'
    and HORDER.DEBNR not like '8%'
group by
    horder.ordnr,
    Category,
    horder.debnr,
    horder.factnr,
    SalesExec,
    horder.LEVCOD,
    ARTTXT,
    '20' + left (HORDER.ordnr, 2) + '-' + substring(HORDER.ordnr, 3, 2) + '-' + substring(HORDER.ordnr, 5, 2),
    ArticleNumber,
    horder.USERID
Order by
    horder.FACTNR