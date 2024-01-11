WITH date_series AS (
    SELECT d AS date
    FROM UNNEST(GENERATE_DATE_ARRAY('2024-01-01', '2024-02-01', INTERVAL 1 DAY)) AS d
),
warehouse_list AS (
    SELECT warehouse
    FROM UNNEST([
        'Riyadh Warehouse',
        'Dubai Warehouse',
        'Dammam Warehouse',
        'Jouf WareHouse',
        'Medina Warehouse',
        'Jeddah Warehouse',
        'Hafar WareHouse',
        'Qassim Warehouse',
        'Hail Warehouse',
        'Tabuk Warehouse'


    ]) AS warehouse
),

financial_administration_list AS (
    SELECT financial_administration
    FROM UNNEST([
        'KSA',
        'UAE',
        'Internal',
        'Kuwait',
        'Bulk',
        'Qatar'
    
    ]) AS financial_administration
)
SELECT 
    date_series.date,
    financial_administration_list.financial_administration,
    warehouse_list.warehouse,
FROM 
    date_series
CROSS JOIN 
    warehouse_list
CROSS JOIN 
    financial_administration_list






