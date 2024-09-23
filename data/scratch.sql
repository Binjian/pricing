SELECT id AS ride_id, trip_count
FROM `elife-data-warehouse-prod.ods.ride_ride`
WHERE `elife-data-warehouse-prod.ods.ride_ride`.trip_count > 1
LIMIT 10