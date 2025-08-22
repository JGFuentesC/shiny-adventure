CREATE OR REPLACE TABLE
  `bi-jul-sep-2025.taxis_nyc.tad_propina` AS -- TAD = TABLA ANALÍTICA DE DATOS (X,y)
SELECT
  EXTRACT(hour
  FROM
    pickup_datetime) AS d_hora,
  EXTRACT(month
  FROM
    pickup_datetime) AS d_mes,
  CONCAT(pickup_location_id,'-',dropoff_location_id) AS d_punto_ab,
  TIMESTAMP_DIFF(dropoff_datetime, pickup_datetime,minute) AS c_duracion_viaje,
  trip_type_recoded AS d_tipo_viaje,
  payment_type_recode AS d_forma_pago,
  fare_amount AS c_monto_tarifa,
  passenger_count AS c_num_pasajeros,
  taxi_brand AS d_tipo_taxi,
  CASE
    WHEN tip_amount/fare_amount>0.2 THEN 1
    ELSE 0
END
  AS tgt_propina_sup_20
FROM
  `bi-jul-sep-2025.taxis_nyc.taxi_trips`
WHERE
  dt_month BETWEEN '2020-04-01'
  AND '2022-11-01'
  AND dropoff_datetime>pickup_datetime;

CREATE OR REPLACE MODEL
  taxis_nyc.modelo_proba_propina OPTIONS( MODEL_TYPE = 'LOGISTIC_REG',
    CATEGORY_ENCODING_METHOD = 'ONE_HOT_ENCODING',
    ENABLE_GLOBAL_EXPLAIN = TRUE,
    INPUT_LABEL_COLS = ['tgt_propina_sup_20'],
    DATA_SPLIT_METHOD = 'RANDOM',
    DATA_SPLIT_EVAL_FRACTION = 0.3) AS
SELECT
  *
FROM
  `bi-jul-sep-2025.taxis_nyc.tad_propina`;

