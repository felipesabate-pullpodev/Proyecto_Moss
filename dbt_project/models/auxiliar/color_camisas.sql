{{ config(
    materialized="table",
    schema="aux_table"
) }}


SELECT 
string_field_0 AS color,
string_field_1 AS sku
FROM `moss-448416.dataset.aux_color_camisas`