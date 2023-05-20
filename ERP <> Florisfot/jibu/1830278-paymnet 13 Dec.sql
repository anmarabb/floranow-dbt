select
    BETAAL.DEBNR,
    DEBITEUR.DEBNAAM as Customername,
    isnull(DEBCAT.OMSCHRIJVING, 'No Category') as Category,
    BETAAL.FACTNR as InvoiceNo,
    BETAAL.FACTDAT as Invoicedate,
    BETAAL.BETDAT as PaymentDate,
    BEDRAG as PaidAmount,
    BETAAL.TEXT as PaymentDetail,
    BETWIJZE.BETWIJZE as PaymentMethod,
    VERKOPER.VERKOOPTXT as SalesManager
from
    BETAAL
    left outer join DEBITEUR on BETAAL.DEBNR = DEBITEUR.DEBNR
    left outer join BETWIJZE on BETAAL.BETCODE = BETWIJZE.BETCODE
    left outer join VERKOPER on DEBITEUR.ACCTMNGR = VERKOPER.VERKOOPNR
    left outer join DEBCAT on DEBITEUR.DEBCAT = DEBCAT.CATEGORY
where
    BETAAL.BETDAT >=: DateFrom
    AND BETAAL.BETDAT <=: DateTo