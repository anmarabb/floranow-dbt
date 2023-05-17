select count(*) as row_count from floranow_erp_db.public.invoices as i;


select count(*) as row_count 
from floranow_erp_db.public.invoice_items as ii
left join floranow_erp_db.public.users as u on u.id = ii.customer_id
left join floranow_erp_db.public.invoices as i on ii.invoice_id = i.id

where 
  ii.deleted_at is null 
  and
  ii.status = 'APPROVED'
;


select sum(invoices.total_amount) from invoices
join users as u on u.id = invoices.customer_id
join warehouses w on u.warehouse_id = w.id
where w.id = 10
;




select 
count(*) as record_count,
count (distinct invoice_id)
from invoice_items
join users as u on u.id = invoice_items.customer_id
join warehouses w on u.warehouse_id = w.id
where w.id = 10 
;


invoice = Invoice.find_or_initialize_by(number: "BH-#{data[:number]}")
      if invoice.id.blank?
        invoice.total_amount = data[:invoice_type] == 'out_refund' ? -data[:amount_total].to_d.round(2) : data[:amount_total].to_d.round(2)
        invoice.remaining_amount = data[:invoice_type] == 'out_refund' ? -data[:amount_total].to_d.round(2) : data[:amount_total].to_d.round(2)
        invoice.paid_amount = 0
        invoice.currency = data[:currency]
        invoice.status = :signed
        invoice.invoice_type = data[:invoice_type] == 'out_refund' ? :credit_note : :normal
        invoice.language = 'ar'
        invoice.items_collection_method = :delivery_date
        invoice.tax_rate = (data[:amount_tax].to_d / data[:amount_untaxed].to_d).round(2)
        invoice.customer_id = user.id
        invoice.financial_administration_id = user.financial_administration_id
        invoice.items_collection_date = data[:date_invoice].to_date
        invoice.printed_at = data[:date_invoice].to_datetime
        invoice.total_tax = data[:invoice_type] == 'out_refund' ? -data[:amount_tax].to_d.round(2) : data[:amount_tax].to_d.round(2)
        invoice.generation_type = :manual
        invoice.source_type = :external
        invoice.due_date = data[:date_due]
        invoice.meta_data = data.as_json
white_check_mark
eyes
raised_hands