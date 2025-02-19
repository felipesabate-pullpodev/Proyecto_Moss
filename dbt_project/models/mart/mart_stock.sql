{{
  config(
    materialized = 'table',
    schema = 'mart'
    )
}}



-- Estan todos los cte`s con filtro de variante_id. 
WITH stock AS (
  SELECT 
   variant_id,
   variant_code,
   variant_description,
   office_name AS sucursal,
   SUM(quantity) AS stock_actual
   FROM {{ ref('stg_actual_stock') }}
   group by all
)
,

raw_maestra_producto2 AS (
    SELECT 
        CAST(int64_field_0 AS STRING) AS variante_code_desc,  -- Convertimos a STRING si no lo es
        string_field_1 AS grupo_producto,
        string_field_2 AS categoria
     FROM `moss-448416.dataset.maestra_producto_2`
)



SELECT *,
CASE
  WHEN s.variant_description = 'S' THEN '1'
  WHEN s.variant_description = 'M' THEN '2'
  WHEN s.variant_description = 'L' THEN '3'
  WHEN s.variant_description = 'XL' THEN '4'
  WHEN s.variant_description = 'XXL' THEN '5'
  WHEN s.variant_description = '3XL' THEN '6'
  WHEN s.variant_description = '4XL' THEN '7'
  ELSE variant_description  -- Se mantiene como STRING
 END AS orden_tallas,

    CASE
  WHEN s.variant_code LIKE '%AREN%' THEN 'Arena'
  WHEN s.variant_code LIKE '%AZUL%' THEN 'Azul'
  WHEN s.variant_code LIKE '%AZMA%' THEN 'Azul Marino'
  WHEN s.variant_code LIKE '%BLAN%' THEN 'Blanco'
  WHEN s.variant_code LIKE '%CALI%' THEN 'Calipso'
  WHEN s.variant_code LIKE '%CELE%' THEN 'Celeste'
  WHEN s.variant_code LIKE '%DAMA%' THEN 'Damasco'
  WHEN s.variant_code LIKE '%ROYA%' THEN 'Royal'
  WHEN s.variant_code LIKE '%SAND%' THEN 'Sandia'
  WHEN s.variant_code LIKE '%TURQ%' THEN 'Turquesa'
  WHEN s.variant_code LIKE '%CAQU%' THEN 'Caqui'
  WHEN s.variant_code LIKE '%INDG%' THEN 'Indigo'
  WHEN s.variant_code LIKE '%CEMA%' THEN 'Celeste Mar'
  WHEN s.variant_code LIKE '%CORA%' THEN 'Coral'
  WHEN s.variant_code LIKE '%CRUD%' THEN 'Crudo'
  WHEN s.variant_code LIKE '%CAPA%' THEN 'Calipso Pastel'
  WHEN s.variant_code LIKE '%AQUA%' THEN 'Aqua'
  WHEN s.variant_code LIKE '%GRPI%' THEN 'Gris Piedra'
  WHEN s.variant_code LIKE '%BEIG%' THEN 'Beige'
  WHEN s.variant_code LIKE '%VERD%' THEN 'Verde'
  WHEN s.variant_code LIKE '%ROSA%' THEN 'Rosa'
  WHEN s.variant_code LIKE '%CAFE%' THEN 'Cafe'
  WHEN s.variant_code LIKE '%NEGR%' THEN 'Negro'
  WHEN s.variant_code LIKE '%BLD%' THEN 'Blanco Diseño'
  WHEN s.variant_code LIKE '%MARE%' THEN 'Marengo'
  WHEN s.variant_code LIKE '%BLOY%' THEN 'Blanco/Royal'
  WHEN s.variant_code LIKE '%BLAM%' THEN 'Blanco/Azul Marino'
  WHEN s.variant_code LIKE '%MOST%' THEN 'Mostaza'
  WHEN s.variant_code LIKE '%VECL%' THEN 'Verde Claro'
  WHEN s.variant_code LIKE '%TERR%' THEN 'Terracota'
  WHEN s.variant_code LIKE '%MOVI%' THEN 'Mostaza Vintage'
  WHEN s.variant_code LIKE '%GRIS%' THEN 'Gris'
  WHEN s.variant_code LIKE '%LILA%' THEN 'Lila'
  WHEN s.variant_code LIKE '%CEDE%' THEN 'Celeste Denin'
  WHEN s.variant_code LIKE '%AZDE%' THEN 'Azul Denin'
  WHEN s.variant_code LIKE '%BEVE%' THEN 'Beige/Verde'
  WHEN s.variant_code LIKE '%OLIV%' THEN 'Oliva'
  WHEN s.variant_code LIKE '%ROJO%' THEN 'Rojo'
  WHEN s.variant_code LIKE '%BURD%' THEN 'Burdeo'
  WHEN s.variant_code LIKE '%MENT%' THEN 'Menta'
  WHEN s.variant_code LIKE '%PETR%' THEN 'Petroleo'
  WHEN s.variant_code LIKE '%ZULI%' THEN 'Azulino'
  WHEN s.variant_code LIKE '%MELO%' THEN 'Melon'
  WHEN s.variant_code LIKE '%LADR%' THEN 'Ladrillo'
  WHEN s.variant_code LIKE '%AMAR%' THEN 'Amarillo'
  WHEN s.variant_code LIKE '%CEOS%' THEN 'Celeste Oscuro'
  WHEN s.variant_code LIKE '%MORA%' THEN 'Morado'
  WHEN s.variant_code LIKE '%ROVI%' THEN 'Rojo Vintage'
  WHEN s.variant_code LIKE '%CAVE%' THEN 'Cafe Verdoso'
  WHEN s.variant_code LIKE '%AZAC%' THEN 'Azul Acero'
  WHEN s.variant_code LIKE '%VEBO%' THEN 'Verde Botella'
  WHEN s.variant_code LIKE '%AZPE%' THEN 'Azul Petrol'
  WHEN s.variant_code LIKE '%AZLA%' THEN 'Azul Lavanda'
  WHEN s.variant_code LIKE '%AZNA%' THEN 'Azul Navy'
  WHEN s.variant_code LIKE '%MOOS%' THEN 'Morado Oscuro'
  WHEN s.variant_code LIKE '%AROS%' THEN 'Arena Oscuro'
  WHEN s.variant_code like '%AZGR%' THEN 'Azul/Gris'
  WHEN s.variant_code LIKE '%AZSA%' THEN 'Azul/Sandia'
  WHEN s.variant_code LIKE '%AZVE%' THEN 'Azul/Verde'
  WHEN s.variant_code LIKE '%DEAZ%' THEN 'Denim/Azul'
  WHEN s.variant_code LIKE '%SADE%' THEN 'Sandia/Denim'
  WHEN s.variant_code LIKE '%CETU%' THEN 'Celeste/Turquesa'
  WHEN s.variant_code LIKE '%TOST%' THEN 'Tostado'
  WHEN s.variant_code LIKE '%RGME%' THEN 'Gris Medio'
  WHEN s.variant_code LIKE '%BETA%' THEN 'Beige Tan'
  WHEN s.variant_code LIKE '%VECA%' THEN 'Verde Caqui'
  WHEN s.variant_code LIKE '%TAUP%' THEN 'Taupe'
  WHEN s.variant_code LIKE '%MASI%' THEN 'Masilla'
  WHEN s.variant_code LIKE '%DENI%' THEN 'Denim'
  WHEN s.variant_code LIKE '%BLAZ%' THEN 'Blanco/Azul'
  ELSE 'Otro'
  END color 
FROM stock s
LEFT JOIN raw_maestra_producto2 rm 
    ON LEFT(CAST(s.variant_code AS STRING), 4) = rm.variante_code_desc
