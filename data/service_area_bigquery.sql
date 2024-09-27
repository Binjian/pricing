SELECT `elife-data-warehouse-prod.ods.ride_ride_1`.id          AS ride_id,
       `elife-data-warehouse-prod.ods.ride_ride_1`.service_area_id,
       `elife-data-warehouse-prod.ods.ride_ride_1`.from_time_str,
       `elife-data-warehouse-prod.dim.dim_service_area_1`.name AS service_area
FROM `elife-data-warehouse-prod.ods.ride_ride` AS `elife-data-warehouse-prod.ods.ride_ride_1` JOIN `elife-data-warehouse-prod.dim.dim_service_area` AS `elife-data-warehouse-prod.dim.dim_service_area_1`
ON `elife-data-warehouse-prod.ods.ride_ride_1`.service_area_id = `elife-data-warehouse-prod.dim.dim_service_area_1`.id
WHERE from_time_str > '2024-01-01'
LIMIT 200