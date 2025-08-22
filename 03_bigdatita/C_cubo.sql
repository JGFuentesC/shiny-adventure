#1. ¿Cuál es la probabilidad de que un viaje tenga propina?
#2. ¿Cuáles son las rutas (A-B) más demandadas?
#3. ¿Cuáles son los umbrales de monto de viaje para separar viajes de Cash/CC?
#4. ¿La tarifa se ve impactada por el tipo de taxi (verde/amarillo)?
#5. ¿Qué temporada del año se gasta más?
#6. ¿Que variables encarecen el monto del viaje?

SELECT
  CASE
    WHEN prob_clase_1>=0 AND prob_clase_1<.2 THEN '00. [0,0.2)'
    WHEN prob_clase_1>=0.2
  AND prob_clase_1<.4 THEN '01. [0.2,0.4)'
    WHEN prob_clase_1>=0.4 AND prob_clase_1<.6 THEN '02. [0.4,0.6)'
    WHEN prob_clase_1>=0.6
  AND prob_clase_1<.8 THEN '03. [0.6,0.8)'
    WHEN prob_clase_1>=0.8 THEN '04. [0.8,1]'
END
  AS r_proba,
  tgt_propina_sup_20,
  d_forma_pago,
d_punto_ab,
  COUNT(*) AS casos
FROM (
  SELECT
    (
    SELECT
      p.prob
    FROM
      UNNEST(predicted_tgt_propina_sup_20_probs) AS p
    WHERE
      SAFE_CAST(p.label AS INT64) = 1
      OR SAFE_CAST(p.label AS BOOL) = TRUE
    LIMIT
      1 ) AS prob_clase_1,
    tgt_propina_sup_20, d_forma_pago,d_punto_ab
  FROM
    ML.PREDICT( MODEL `bi-jul-sep-2025.taxis_nyc.modelo_proba_propina`,
      (
      SELECT
        *
      FROM
        `bi-jul-sep-2025.taxis_nyc.tad_propina`) ) )
GROUP BY
  all