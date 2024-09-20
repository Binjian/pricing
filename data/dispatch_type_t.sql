SELECT
    t1.id AS ride_id,
    t1.to_fleet_id AS dispatch_fleet_id,
    t2.auction_id AS auction_id,
    t3.fleet_id AS auction_fleet_id,
    CASE
        WHEN (t1.to_fleet_id = t3.fleet_id) THEN 'auction'
        ELSE 'dispatch' END AS dispatch_type
FROM "elife-data-warehouse-prod.ods.ride_dispatch" AS "t1"
LEFT OUTER JOIN "elife-data-warehouse-prod.ods.ride_auction_ride" AS "t2"
            ON t1.ride_id = t2.ride_id
LEFT OUTER JOIN "elife-data-warehouse-prod.ods.ride_auction_fleet" AS "t3"
            ON t2.auction_id = t3.auction_id