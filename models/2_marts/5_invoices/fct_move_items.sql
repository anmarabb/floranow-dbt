with

source as ( 

select

*,



case 
when company_id = 3 then 'Bloomax Flowers LTD'
when company_id = 2 then 'Global Floral Arabia tr'
when company_id = 1 then 'Flora Express Flower Trading LLC'
else  'cheack'
end as reporting_company_name,


gross_revenue_with_tax - invoice_total_tax as gross_revenue,
credit_nots_with_tax -credit_note_total_tax as credit_note,

---payment_transactions


current_timestamp() as insertion_timestamp 


from {{ref('int_move_items')}} as mi
)

select * from source

