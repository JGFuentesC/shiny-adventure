-- agrega un filtro para que solo considere negocios que abren en la mañana y cierran en la tarde
SELECT
  sme_data.giroNegocio_cat,
  AVG(sme_data.ventasPromedioDiarias) AS promedio_ventas
FROM
  `[PROJECT_ID]`.`[DATASET]`.`[TABLE]` AS sme_data
WHERE
  sme_data.horaApertura_cat = '01. Mañana'
  AND sme_data.horaCierre_cat = '02. Tarde'
GROUP BY
  sme_data.giroNegocio_cat
ORDER BY
  sme_data.giroNegocio_cat;