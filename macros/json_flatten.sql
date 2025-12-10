{% set json_column_query %}
select distinct
json.key as column_name
from {{ source('erp_prod' , 'line_items')}},

lateral flatten(input => categorization) json
{% endset %}


{% set results = run_query(json_column_query) %}




{% if execute %}
{% set results_list = results.columns [e].values ( ) %}
{% else %}
{% set results_list = [] %}
{% endif %}

select
artist_data,
{% for column_name in results_list %}
{{ column_name }} as {{ column_name }},
{% endfor %}

from {{source('erp_prod' , 'line_items')}} 