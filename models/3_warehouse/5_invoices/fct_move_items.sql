with

source as ( 

select

*,



case 
when reporting_company_id = 3 then 'Bloomax Flowers LTD'
when reporting_company_id = 2 then 'Global Floral Arabia tr'
when reporting_company_id = 1 then 'Flora Express Flower Trading LLC'
else  'cheack'
end as reporting_company_name,



current_timestamp() as insertion_timestamp 


from {{ref('int_move_items')}} as mi
)

select * from source

