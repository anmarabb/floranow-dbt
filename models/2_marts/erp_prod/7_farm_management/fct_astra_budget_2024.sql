SELECT

*,
current_timestamp() as insertion_timestamp,

FROM  {{ref('int_astra_budget_2024')}} as db