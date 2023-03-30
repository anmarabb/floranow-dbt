select
*
--id as purchase_line_item_id,
--user_id,
--customer_id,


from {{ ref('stg_line_items') }} as li
where line_item_type = 'Reselling Purchase Orders' 