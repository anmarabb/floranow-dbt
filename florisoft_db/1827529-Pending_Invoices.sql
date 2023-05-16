-- Invoices for clients existing in Florisoft and not in ERP
select
    *
from
    `floranow.florisoft_db.HORDERKP` as fs_invoicemaster
where
    fs_invoicemaster.DEBNR in (
        select
            distinct user_florisoft.DEBNR
        from
            `floranow.florisoft_db.DEBITEUR` as user_florisoft
            left outer join `floranow.erp_prod.users` as user_erp on user_florisoft.DEBNR = user_erp.debtor_number
        where
            user_erp.debtor_number is null
    )