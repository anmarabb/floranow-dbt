select

pt.id,
pt.user_id,
pt.trx_reference,
pt.approved,

pt.total_amount,
pt.paid_amount,
pt.credit_note_amount,
pt.currency,

--date
pt.created_at,		
pt.updated_at,		
pt.collected_at,		
pt.payment_received_at,		
pt.canceled_at,		
pt.deleted_at,		

pt.status, --SUCCESS, DRAFT, FAILED, PROCESSING, CANCELED
pt.payment_method, --BANK_TRANSFER, VISA_CARD, PAYMENT_BY_CREDIT, CASH, CHEQUE, WRITE_OFF, CREDIT, OVER_PAYED, null
pt.transaction_type, --EXTERNAL, MANUAL, ONLINE
pt.collected,
pt.financial_administration_id,

pt.invoice_numbers,

pt.payment_gateway,


pt.number,
pt.sequence,

pt.response,
    invoice_number,		
    payment_method,		
    invoice_date,		
    note,		
    debtor_number,		
    paid_amount,	
    online,
    created_at,		
    currency,		
    user_id,		
    unique_id,
    method,		
    trace,		
    order,
        ref,	
        cartid,	
        test,	
        amount,	
        currency,	
        description,	
        paymethod,	
        status,
            code,		
            text,
        transaction,
            ref,		
            date,		
            type,		
            class,		
            status,		
            code,		
            message
        card,
            type,		
            last4,		
            country,		
            first6
            expiry
                month
                year
        customer,
            email,
            name,
                forenames,
                surname,
                title,
            address,
                line1,
                city,
                country,
                state,
                areacode,
                line2,

            


pt.added_by,
pt.approved_by,
pt.created_by,
pt.updated_by,
pt.collected_by,
pt.payment_received_by,
pt.canceled_by,

pt.bank_account_id,
pt.cancellation_reason,
pt.meta_data,
    editable,	
    payment_transaction_job_id,

from `floranow.erp_prod.payment_transactions` as pt