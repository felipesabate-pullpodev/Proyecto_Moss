{{
  config(
    materialized = 'ephemeral',
    )
}}

-- Maestra de Productos que nos comparte el cliente. 


SELECT *
FROM `moss-448416.dataset.maestra_productos`