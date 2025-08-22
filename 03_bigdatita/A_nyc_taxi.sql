CREATE OR REPLACE TABLE
  bi-jul-sep-2025.taxis_nyc.taxi_trips
PARTITION BY
  dt_month AS
WITH
  greenTaxi AS ( -- CTE = Common Table Expression
  SELECT
    pickup_datetime,
    dropoff_datetime,
    pickup_location_id,
    dropoff_location_id,
    fare_amount,
    CASE
      WHEN trip_type='1.0' THEN 'Streer-hail'
      WHEN trip_type='2.0' THEN 'Dispatch'
      ELSE 'Other'
  END
    AS trip_type_recoded,
    CASE
      WHEN payment_type='1.0' THEN 'Credit Card'
      WHEN payment_type='2.0' THEN 'Cash'
  END
    AS payment_type_recode,
    total_amount,
    tip_amount,
    passenger_count
  FROM
    `bigquery-public-data.new_york_taxi_trips.tlc_green_trips_2022`
  WHERE
    fare_amount>0
    AND trip_distance>0
    AND passenger_count>0
    AND trip_type IS NOT NULL
    AND payment_type IN ('1.0',
      '2.0')
  UNION ALL
  SELECT
    pickup_datetime,
    dropoff_datetime,
    pickup_location_id,
    dropoff_location_id,
    fare_amount,
    CASE
      WHEN trip_type='1.0' THEN 'Streer-hail'
      WHEN trip_type='2.0' THEN 'Dispatch'
      ELSE 'Other'
  END
    AS trip_type_recoded,
    CASE
      WHEN payment_type='1.0' THEN 'Credit Card'
      WHEN payment_type='2.0' THEN 'Cash'
  END
    AS payment_type_recode,
    total_amount,
    tip_amount,
    passenger_count
  FROM
    `bigquery-public-data.new_york_taxi_trips.tlc_green_trips_2021`
  WHERE
    fare_amount>0
    AND trip_distance>0
    AND passenger_count>0
    AND trip_type IS NOT NULL
    AND payment_type IN ('1.0',
      '2.0')
  UNION ALL
  SELECT
    pickup_datetime,
    dropoff_datetime,
    pickup_location_id,
    dropoff_location_id,
    fare_amount,
    CASE
      WHEN trip_type='1.0' THEN 'Streer-hail'
      WHEN trip_type='2.0' THEN 'Dispatch'
      ELSE 'Other'
  END
    AS trip_type_recoded,
    CASE
      WHEN payment_type='1.0' THEN 'Credit Card'
      WHEN payment_type='2.0' THEN 'Cash'
  END
    AS payment_type_recode,
    total_amount,
    tip_amount,
    passenger_count
  FROM
    `bigquery-public-data.new_york_taxi_trips.tlc_green_trips_2020`
  WHERE
    fare_amount>0
    AND trip_distance>0
    AND passenger_count>0
    AND trip_type IS NOT NULL
    AND payment_type IN ('1.0',
      '2.0')
  UNION ALL
  SELECT
    pickup_datetime,
    dropoff_datetime,
    pickup_location_id,
    dropoff_location_id,
    fare_amount,
    CASE
      WHEN trip_type='1.0' THEN 'Streer-hail'
      WHEN trip_type='2.0' THEN 'Dispatch'
      ELSE 'Other'
  END
    AS trip_type_recoded,
    CASE
      WHEN payment_type='1.0' THEN 'Credit Card'
      WHEN payment_type='2.0' THEN 'Cash'
  END
    AS payment_type_recode,
    total_amount,
    tip_amount,
    passenger_count
  FROM
    `bigquery-public-data.new_york_taxi_trips.tlc_green_trips_2019`
  WHERE
    fare_amount>0
    AND trip_distance>0
    AND passenger_count>0
    AND trip_type IS NOT NULL
    AND payment_type IN ('1.0',
      '2.0')
  UNION ALL
  SELECT
    pickup_datetime,
    dropoff_datetime,
    pickup_location_id,
    dropoff_location_id,
    fare_amount,
    CASE
      WHEN trip_type='1.0' THEN 'Streer-hail'
      WHEN trip_type='2.0' THEN 'Dispatch'
      ELSE 'Other'
  END
    AS trip_type_recoded,
    CASE
      WHEN payment_type='1.0' THEN 'Credit Card'
      WHEN payment_type='2.0' THEN 'Cash'
  END
    AS payment_type_recode,
    total_amount,
    tip_amount,
    passenger_count
  FROM
    `bigquery-public-data.new_york_taxi_trips.tlc_green_trips_2018`
  WHERE
    fare_amount>0
    AND trip_distance>0
    AND passenger_count>0
    AND trip_type IS NOT NULL
    AND payment_type IN ('1.0',
      '2.0')),
  yellowTaxi AS (
  SELECT
    pickup_datetime,
    dropoff_datetime,
    pickup_location_id,
    dropoff_location_id,
    fare_amount,
    'Unknown' AS trip_type_recoded,
    CASE
      WHEN payment_type='1.0' THEN 'Credit Card'
      WHEN payment_type='2.0' THEN 'Cash'
  END
    AS payment_type_recode,
    total_amount,
    tip_amount,
    passenger_count
  FROM
    `bigquery-public-data.new_york_taxi_trips.tlc_yellow_trips_2022`
  WHERE
    fare_amount>0
    AND trip_distance>0
    AND passenger_count>0
    AND payment_type IN ('1.0',
      '2.0')
  UNION ALL
  SELECT
    pickup_datetime,
    dropoff_datetime,
    pickup_location_id,
    dropoff_location_id,
    fare_amount,
    'Unknown' AS trip_type_recoded,
    CASE
      WHEN payment_type='1.0' THEN 'Credit Card'
      WHEN payment_type='2.0' THEN 'Cash'
  END
    AS payment_type_recode,
    total_amount,
    tip_amount,
    passenger_count
  FROM
    `bigquery-public-data.new_york_taxi_trips.tlc_yellow_trips_2021`
  WHERE
    fare_amount>0
    AND trip_distance>0
    AND passenger_count>0
    AND payment_type IN ('1.0',
      '2.0')
  UNION ALL
  SELECT
    pickup_datetime,
    dropoff_datetime,
    pickup_location_id,
    dropoff_location_id,
    fare_amount,
    'Unknown' AS trip_type_recoded,
    CASE
      WHEN payment_type='1.0' THEN 'Credit Card'
      WHEN payment_type='2.0' THEN 'Cash'
  END
    AS payment_type_recode,
    total_amount,
    tip_amount,
    passenger_count
  FROM
    `bigquery-public-data.new_york_taxi_trips.tlc_yellow_trips_2020`
  WHERE
    fare_amount>0
    AND trip_distance>0
    AND passenger_count>0
    AND payment_type IN ('1.0',
      '2.0')
  UNION ALL
  SELECT
    pickup_datetime,
    dropoff_datetime,
    pickup_location_id,
    dropoff_location_id,
    fare_amount,
    'Unknown' AS trip_type_recoded,
    CASE
      WHEN payment_type='1.0' THEN 'Credit Card'
      WHEN payment_type='2.0' THEN 'Cash'
  END
    AS payment_type_recode,
    total_amount,
    tip_amount,
    passenger_count
  FROM
    `bigquery-public-data.new_york_taxi_trips.tlc_yellow_trips_2019`
  WHERE
    fare_amount>0
    AND trip_distance>0
    AND passenger_count>0
    AND payment_type IN ('1.0',
      '2.0')
  UNION ALL
  SELECT
    pickup_datetime,
    dropoff_datetime,
    pickup_location_id,
    dropoff_location_id,
    fare_amount,
    'Unknown' AS trip_type_recoded,
    CASE
      WHEN payment_type='1.0' THEN 'Credit Card'
      WHEN payment_type='2.0' THEN 'Cash'
  END
    AS payment_type_recode,
    total_amount,
    tip_amount,
    passenger_count
  FROM
    `bigquery-public-data.new_york_taxi_trips.tlc_yellow_trips_2018`
  WHERE
    fare_amount>0
    AND trip_distance>0
    AND passenger_count>0
    AND payment_type IN ('1.0',
      '2.0'))
SELECT
  *,
  DATE_TRUNC(CAST(pickup_datetime AS date),month) AS dt_month,
  'Green' AS taxi_brand
FROM
  greenTaxi
UNION ALL
SELECT
  *,
  DATE_TRUNC(CAST(pickup_datetime AS date),month) AS dt_month,
  'Yellow' AS taxi_brand
FROM
  yellowTaxi