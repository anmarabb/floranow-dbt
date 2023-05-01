With source as (
 select * from {{ source('erp_prod', 'statement_of_accounts') }}
)
select 
            --PK
                id as statement_of_account_id,
            --FK
            printed_by_id,
            sent_by_id,
            user_id,
  

            --dim
                --date
                printed_at,
                sent_at,
                created_at,
                updated_at,
                from_date,
                to_date,


            

                --dim
                currency,
                invoices_ids,
                summary_file,
                transaction_ids,
                move_items_ids,

               
                --fct
                total_amount,
                total_credit_amount,
                total_debit_amount,

            


current_timestamp() as ingestion_timestamp,

from source
