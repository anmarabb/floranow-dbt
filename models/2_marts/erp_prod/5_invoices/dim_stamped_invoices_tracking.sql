select 
       i.invoice_header_id,
       i.pod_status,
       i.invoice_number , 
       i.is_stamped,
       i.date_invoice_header_created_at ,
       i.invoice_header_printed_at,
       i.invoice_header_status,
       i.proof_of_delivery_id,
       i.printed_by,
       i.dispatched_by,
    --    attached_by.name as attached_by,

from {{ref('fct_invoices')}} as i
-- left join attachment_references ar on CAST(i.id AS VARCHAR)= ar.record_id
-- left join users attached_by on ar.user_id = attached_by.id
