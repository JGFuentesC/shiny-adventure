-- Arquitecto de BI: Script para Cubo MOLAP Agregado en BigQuery
-- Objetivo: Generar una tabla agregada para análisis "slice & dice" en herramientas de BI.
-- Metodología:
-- 1. ENRIQUECIMIENTO: Crear dimensiones categóricas (buckets) en una tabla base.
-- 2. AGREGACIÓN: Agrupar por todas las dimensiones y calcular métricas con SUM y COUNT.
-- 3. MATERIALIZACIÓN: Crear la tabla final agregada.

-- =========================================================================================
-- PASO 1: Construir la tabla materializada y agregada `cubo_molap_agregado`.
-- Este script ahora omite el clustering y se enfoca en una agregación robusta.
-- =========================================================================================
CREATE OR REPLACE TABLE `{{ PROJECT_ID }}.{{ DATASET }}.cubo_molap` AS
WITH
-- Subquery para preparar todas las dimensiones categóricas antes de agregar.
base_dimensiones AS (
  SELECT
    -- Llave para conteo
    id,

    -- Dimensiones directas
    horaApertura_cat AS dim_hora_apertura,
    horaCierre_cat AS dim_hora_cierre,
    giroNegocio_cat AS dim_tipo_negocio,
    usaCredito AS dim_tipo_credito,
    promocionNegocio AS dim_promocion_publicidad,
    altaSAT AS dim_alta_sat,
    registroContabilidad AS dim_tipo_registro,
    sexoEmprendedor AS dim_sexo_emprendedor,
    escolaridadEmprendedor AS dim_escolaridad_emprendedor,
    estadoCivil AS dim_estado_civil,

    -- Dimensiones calculadas (Buckets/Rangos)
    CASE
      WHEN edadEmprendedor BETWEEN 18 AND 24 THEN '18-24'
      WHEN edadEmprendedor BETWEEN 25 AND 34 THEN '25-34'
      WHEN edadEmprendedor BETWEEN 35 AND 44 THEN '35-44'
      WHEN edadEmprendedor BETWEEN 45 AND 54 THEN '45-54'
      WHEN edadEmprendedor BETWEEN 55 AND 64 THEN '55-64'
      WHEN edadEmprendedor >= 65 THEN '65+'
      ELSE 'No especificado'
    END AS dim_rango_edad_emprendedor,
    CASE
        WHEN antiguedadNegocio < 1 THEN '< 1 año'
        WHEN antiguedadNegocio BETWEEN 1 AND 2 THEN '1-2 años'
        WHEN antiguedadNegocio BETWEEN 3 AND 5 THEN '3-5 años'
        WHEN antiguedadNegocio BETWEEN 6 AND 10 THEN '6-10 años'
        WHEN antiguedadNegocio > 10 THEN '11+ años'
        ELSE 'No especificado'
    END AS dim_vida_promedio_negocio,
    CASE
      WHEN familiaAyuda = 'Sí' THEN 'Familiar'
      ELSE 'No Familiar'
    END AS dim_tipo_negocio_familiar,
    CASE
      WHEN numEmpleados = 0 THEN '0 (Autoempleo)'
      WHEN numEmpleados BETWEEN 1 AND 2 THEN '1-2'
      WHEN numEmpleados BETWEEN 3 AND 5 THEN '3-5'
      WHEN numEmpleados BETWEEN 6 AND 10 THEN '6-10'
      WHEN numEmpleados BETWEEN 11 AND 20 THEN '11-20'
      WHEN numEmpleados BETWEEN 21 AND 50 THEN '21-50'
      WHEN numEmpleados > 50 THEN '51+'
      ELSE 'No especificado'
    END AS dim_rango_empleados,

    -- Campos para métricas
    ventasPromedioDiarias,
    numEmpleados,
    dependientesEconomicos,
    CASE
      WHEN NTILE(10) OVER (ORDER BY ventasPromedioDiarias DESC) <= 1 THEN 1
      ELSE 0
    END AS es_exitoso_flag

  FROM
    `{{ PROJECT_ID }}.{{ DATASET }}.sme_data`
)
-- =========================================================================================
-- QUERY FINAL DE AGREGACIÓN: Agrupamos por todas las dimensiones y calculamos las métricas.
-- =========================================================================================
SELECT
  -- Dimensiones
  dim_hora_apertura,
  dim_hora_cierre,
  dim_rango_edad_emprendedor,
  dim_tipo_negocio,
  dim_vida_promedio_negocio,
  dim_tipo_credito,
  dim_promocion_publicidad,
  dim_alta_sat,
  dim_tipo_negocio_familiar,
  dim_tipo_registro,
  dim_rango_empleados,
  dim_sexo_emprendedor,
  dim_escolaridad_emprendedor,
  dim_estado_civil,

  -- Métricas Agregadas
  SUM(ventasPromedioDiarias) AS met_total_ventas_diarias,
  COUNT(id) AS met_conteo_negocios,
  SUM(es_exitoso_flag) AS met_conteo_negocios_exitosos,
  SUM(numEmpleados) AS met_total_empleados,
  SUM(dependientesEconomicos) AS met_total_dependientes_economicos

FROM
  base_dimensiones
GROUP BY
  1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14;

-- Fin del script.
