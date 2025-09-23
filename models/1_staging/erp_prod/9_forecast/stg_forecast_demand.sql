select *
from {{ source(var('erp_source'), 'forecast_demand') }}