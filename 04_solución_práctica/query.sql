SELECT
  start_time,
  B.location AS dim_A,
  C.location AS dim_B,
  subscriber_type AS dim_susc_type,
  bike_type AS dim_bike_type,
  SUM(duration_minutes) AS mt_duration,
  COUNT(*) AS mt_trips
FROM
  bigquery-public-data.austin_bikeshare.bikeshare_trips A
INNER JOIN
  bigquery-public-data.austin_bikeshare.bikeshare_stations B
ON
  A.start_station_id = B.station_id
INNER JOIN
  bigquery-public-data.austin_bikeshare.bikeshare_stations C
ON
  SAFE_CAST(A.end_station_id AS int64)= C.station_id
WHERE A.start_station_id is not null and A.end_station_id is not null 
GROUP BY
  ALL