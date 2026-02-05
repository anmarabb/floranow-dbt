select *
from {{ ref("stg_forecast_demand") }}
