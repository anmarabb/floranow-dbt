with
    source as (
        select
            *,
            current_timestamp() as insertion_timestamp,
        from
{{ref('stg_line_items')}}
    ),
    unique_source as (
        select
            *,
            row_number() over(partition by customer_id) as row_number,
        from
            source
    )
select
    *
except
(row_number),
from
    unique_source
Where
    row_number = 1