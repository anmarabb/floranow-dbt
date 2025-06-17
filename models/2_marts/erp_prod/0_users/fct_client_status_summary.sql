select account_manager,
       Customer,
       debtor_number,
       snapshot_date as master_date,
       total_active,
       total_inactive,
       total_blocked,
       total_churned,
       
from {{ source(var('erp_source'), 'client_status_summary') }} 