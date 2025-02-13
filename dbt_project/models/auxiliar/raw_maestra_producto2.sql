{{
  config(
    materialized = 'ephemeral',
    )
}}


With raw AS (
SELECT * 
FROM `moss-448416.dataset.maestra_producto_2` )

SELECT 
int64_field_0 AS variante_code_desc,
string_field_1 AS grupo_producto,
string_field_2 AS categoria
FROM raw