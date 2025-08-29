SELECT
  types AS dim_types,
  `cluster` dim_cluster,
  CASE
    WHEN stat_attack>120 AND `stat_special-attack`>116 THEN TRUE
    ELSE FALSE
END
  AS dim_fuerte,
  case 
   when stat_defense<=55 then true else false 
  end dim_gana_pikachu,
  name as dim_name,
  SUM(stat_speed) AS mt_speed,
  SUM(`stat_special-attack`) mt_sp_attack,
  SUM(stat_attack) mt_attack,
  SUM(weight) mt_weight,
  COUNT(*) AS mt_num
FROM
  [DATASET].stats
INNER JOIN
  [DATASET].clusters
USING
  (id)
GROUP BY
  1,
  2,
  3,
  4,
  5