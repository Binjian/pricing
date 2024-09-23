WITH
    PricingRaw AS (SELECT `elife-data-warehouse-prod.ods.ride_ride_1`.id           AS ride_id,
                          `elife-data-warehouse-prod.ods.ride_ride_1`.trip_count,
                          `elife-data-warehouse-prod.ods.ride_ride_1`.from_utc,
                          `elife-data-warehouse-prod.ods.ride_ride_1`.from_time_str,
                          `elife-data-warehouse-prod.ods.ride_ride_1`.from_timezone_str,
                          `elife-data-warehouse-prod.ods.ride_ride_1`.to_time_str,
                          `elife-data-warehouse-prod.ods.ride_ride_1`.to_timezone_str,
                          `elife-data-warehouse-prod.ods.ride_ride_1`.passenger_count,
                          `elife-data-warehouse-prod.ods.ride_ride_1`.luggage_count,
                          `elife-data-warehouse-prod.ods.ride_ride_1`.children_count,
                          `elife-data-warehouse-prod.ods.ride_ride_1`.infant_count,
                          `elife-data-warehouse-prod.ods.ride_ride_1`.distance,
                          `elife-data-warehouse-prod.ods.ride_ride_1`.duration,
                          `elife-data-warehouse-prod.ods.ride_dispatch_1`.id       AS dispatch_id,
                          `elife-data-warehouse-prod.ods.ride_dispatch_1`.trip_no,
                          `elife-data-warehouse-prod.ods.ride_dispatch_1`.amount   AS dispatch_amount,
                          `elife-data-warehouse-prod.ods.ride_dispatch_1`.currency AS dispatch_currency,
                          anon_1.from_date_str,
                          anon_1.from_time_fix_str,
                          anon_1.from_datetime_fix_str,
                          anon_2.trip_type_id,
                          anon_2.trip_type,
                          anon_3.ride_status_id,
                          anon_3.ride_status,
                          anon_4.dispatch_status_id,
                          anon_4.dispatch_status,
                          anon_5.dispatch_type,
                          `elife-data-warehouse-prod.ods.ride_fleet_1`.name        AS fleet,
                          anon_6.partner_id,
                          anon_6.partner,
                          anon_7.start_place_id,
                          anon_7.start_place,
                          anon_7.lng                                               AS start_lng,
                          anon_7.ltt                                               AS start_ltt,
                          anon_8.end_place_id,
                          anon_8.end_place,
                          anon_8.lng                                               AS end_lng,
                          anon_8.ltt                                               AS end_ltt,
                          anon_9.vehicle_class_id,
                          anon_9.vehicle_class,
                          ROW_NUMBER() OVER (PARTITION BY `elife-data-warehouse-prod.ods.ride_dispatch_1`.id) AS row_num
                   FROM `elife-data-warehouse-prod.ods.ride_ride` AS `elife-data-warehouse-prod.ods.ride_ride_1`
                            JOIN `elife-data-warehouse-prod.ods.ride_dispatch` AS `elife-data-warehouse-prod.ods.ride_dispatch_1`
                                 ON `elife-data-warehouse-prod.ods.ride_ride_1`.id =
                                    `elife-data-warehouse-prod.ods.ride_dispatch_1`.ride_id
                            JOIN (SELECT `elife-data-warehouse-prod.ods.ride_ride_1`.id                                                     AS ride_id,
                                         substring(`elife-data-warehouse-prod.ods.ride_ride_1`.from_time_str, 1,
                                                   10)                                                                                      AS from_date_str,
                                         concat(substring(`elife-data-warehouse-prod.ods.ride_ride_1`.from_time_str, 12,
                                                          16),
                                                ' ')                                                                                        AS from_time_fix_str,
                                         concat(substring(`elife-data-warehouse-prod.ods.ride_ride_1`.from_time_str, 1,
                                                          10), ' ',
                                                substring(`elife-data-warehouse-prod.ods.ride_ride_1`.from_time_str, 12,
                                                          16),
                                                ':00')                                                                                      AS from_datetime_fix_str,
                                         EXTRACT(DAYOFWEEK FROM CAST(concat(
                                                 substring(`elife-data-warehouse-prod.ods.ride_ride_1`.from_time_str, 1,
                                                           10), ' ',
                                                 substring(`elife-data-warehouse-prod.ods.ride_ride_1`.from_time_str,
                                                           12, 16),
                                                 ':00') AS DATETIME))                                                                       AS day_of_week_local,
                                         EXTRACT(DAYOFWEEK FROM
                                                 CAST(timestamp_seconds(`elife-data-warehouse-prod.ods.ride_ride_1`.from_utc) AS DATETIME)) AS day_of_week_utc,
                                         datetime(timestamp_seconds(`elife-data-warehouse-prod.ods.ride_ride_1`.from_utc))                  AS from_datetime_utc,
                                         `elife-data-warehouse-prod.ods.ride_ride_1`.from_timezone_str                                      AS from_timezone_str
                                  FROM `elife-data-warehouse-prod.ods.ride_ride` AS `elife-data-warehouse-prod.ods.ride_ride_1`) AS anon_1
                                 ON `elife-data-warehouse-prod.ods.ride_ride_1`.id = anon_1.ride_id
                            LEFT OUTER JOIN (SELECT anon_10.ride_id                                  AS ride_id,
                                                    anon_10.trip_type_id                             AS trip_type_id,
                                                    `elife-data-warehouse-prod.ods.ride_enum_1`.name AS trip_type
                                             FROM (SELECT `elife-data-warehouse-prod.ods.ride_dispatch_1`.ride_id AS ride_id,
                                                          `elife-data-warehouse-prod.ods.ride_trip_1`.trip_type   AS trip_type_id
                                                   FROM `elife-data-warehouse-prod.ods.ride_dispatch` AS `elife-data-warehouse-prod.ods.ride_dispatch_1`
                                                            JOIN `elife-data-warehouse-prod.ods.ride_trip` AS `elife-data-warehouse-prod.ods.ride_trip_1`
                                                                 ON `elife-data-warehouse-prod.ods.ride_dispatch_1`.ride_id =
                                                                    `elife-data-warehouse-prod.ods.ride_trip_1`.ride_id) AS anon_10
                                                      LEFT OUTER JOIN `elife-data-warehouse-prod.ods.ride_enum` AS `elife-data-warehouse-prod.ods.ride_enum_1`
                                                                      ON anon_10.trip_type_id = `elife-data-warehouse-prod.ods.ride_enum_1`.id) AS anon_2
                                            ON `elife-data-warehouse-prod.ods.ride_ride_1`.id = anon_2.ride_id
                            LEFT OUTER JOIN (SELECT `elife-data-warehouse-prod.ods.ride_ride_1`.id   AS ride_id,
                                                    `elife-data-warehouse-prod.ods.ride_ride_1`.stat AS ride_status_id,
                                                    `elife-data-warehouse-prod.ods.ride_enum_1`.name AS ride_status
                                             FROM `elife-data-warehouse-prod.ods.ride_ride` AS `elife-data-warehouse-prod.ods.ride_ride_1`
                                                      LEFT OUTER JOIN `elife-data-warehouse-prod.ods.ride_enum` AS `elife-data-warehouse-prod.ods.ride_enum_1`
                                                                      ON `elife-data-warehouse-prod.ods.ride_ride_1`.stat =
                                                                         `elife-data-warehouse-prod.ods.ride_enum_1`.id) AS anon_3
                                            ON `elife-data-warehouse-prod.ods.ride_ride_1`.id = anon_3.ride_id
                            LEFT OUTER JOIN (SELECT `elife-data-warehouse-prod.ods.ride_dispatch_1`.id   AS ride_id,
                                                    `elife-data-warehouse-prod.ods.ride_dispatch_1`.stat AS dispatch_status_id,
                                                    `elife-data-warehouse-prod.ods.ride_enum_1`.name     AS dispatch_status
                                             FROM `elife-data-warehouse-prod.ods.ride_dispatch` AS `elife-data-warehouse-prod.ods.ride_dispatch_1`
                                                      LEFT OUTER JOIN `elife-data-warehouse-prod.ods.ride_enum` AS `elife-data-warehouse-prod.ods.ride_enum_1`
                                                                      ON `elife-data-warehouse-prod.ods.ride_dispatch_1`.stat =
                                                                         `elife-data-warehouse-prod.ods.ride_enum_1`.id) AS anon_4
                                            ON `elife-data-warehouse-prod.ods.ride_ride_1`.id = anon_4.ride_id
                            LEFT OUTER JOIN (SELECT `elife-data-warehouse-prod.ods.ride_dispatch_1`.id             AS ride_id,
                                                    `elife-data-warehouse-prod.ods.ride_dispatch_1`.to_fleet_id    AS dispatch_fleet_id,
                                                    `elife-data-warehouse-prod.ods.ride_auction_ride_1`.auction_id AS auction_id,
                                                    `elife-data-warehouse-prod.ods.ride_auction_fleet_1`.fleet_id  AS auction_fleet_id,
                                                    CASE
                                                        WHEN (
                                                            `elife-data-warehouse-prod.ods.ride_dispatch_1`.to_fleet_id =
                                                            `elife-data-warehouse-prod.ods.ride_auction_fleet_1`.fleet_id)
                                                            THEN 'auction'
                                                        ELSE 'dispatch' END                                        AS dispatch_type,
                                                    `elife-data-warehouse-prod.ods.ride_fleet_1`.name              AS fleet
                                             FROM `elife-data-warehouse-prod.ods.ride_dispatch` AS `elife-data-warehouse-prod.ods.ride_dispatch_1`
                                                      LEFT OUTER JOIN `elife-data-warehouse-prod.ods.ride_auction_ride` AS `elife-data-warehouse-prod.ods.ride_auction_ride_1`
                                                                      ON `elife-data-warehouse-prod.ods.ride_dispatch_1`.ride_id =
                                                                         `elife-data-warehouse-prod.ods.ride_auction_ride_1`.ride_id
                                                      LEFT OUTER JOIN `elife-data-warehouse-prod.ods.ride_auction_fleet` AS `elife-data-warehouse-prod.ods.ride_auction_fleet_1`
                                                                      ON `elife-data-warehouse-prod.ods.ride_auction_ride_1`.auction_id =
                                                                         `elife-data-warehouse-prod.ods.ride_auction_fleet_1`.auction_id
                                                      LEFT OUTER JOIN `elife-data-warehouse-prod.ods.ride_fleet` AS `elife-data-warehouse-prod.ods.ride_fleet_1`
                                                                      ON `elife-data-warehouse-prod.ods.ride_dispatch_1`.to_fleet_id =
                                                                         `elife-data-warehouse-prod.ods.ride_fleet_1`.id) AS anon_5
                                            ON `elife-data-warehouse-prod.ods.ride_ride_1`.id = anon_5.ride_id
                            LEFT OUTER JOIN (SELECT anon_11.ride_id                                     AS ride_id,
                                                    anon_11.partner_id                                  AS partner_id,
                                                    `elife-data-warehouse-prod.ods.ride_partner_1`.name AS partner
                                             FROM (SELECT `elife-data-warehouse-prod.ods.ride_ride_1`.id                 AS ride_id,
                                                          `elife-data-warehouse-prod.ods.ride_partner_tran_1`.partner_id AS partner_id
                                                   FROM `elife-data-warehouse-prod.ods.ride_ride` AS `elife-data-warehouse-prod.ods.ride_ride_1`
                                                            LEFT OUTER JOIN `elife-data-warehouse-prod.ods.ride_partner_tran` AS `elife-data-warehouse-prod.ods.ride_partner_tran_1`
                                                                            ON `elife-data-warehouse-prod.ods.ride_ride_1`.partner_tran_id =
                                                                               `elife-data-warehouse-prod.ods.ride_partner_tran_1`.id) AS anon_11
                                                      LEFT OUTER JOIN `elife-data-warehouse-prod.ods.ride_partner` AS `elife-data-warehouse-prod.ods.ride_partner_1`
                                                                      ON anon_11.partner_id = `elife-data-warehouse-prod.ods.ride_partner_1`.id) AS anon_6
                                            ON `elife-data-warehouse-prod.ods.ride_ride_1`.id = anon_6.ride_id
                            LEFT OUTER JOIN (SELECT `elife-data-warehouse-prod.ods.ride_ride_1`.id            AS ride_id,
                                                    `elife-data-warehouse-prod.ods.ride_ride_1`.from_place_id AS start_place_id,
                                                    `elife-data-warehouse-prod.dim.dim_place_1`.name          AS start_place,
                                                    `elife-data-warehouse-prod.dim.dim_place_1`.lng           AS lng,
                                                    `elife-data-warehouse-prod.dim.dim_place_1`.lat           AS ltt
                                             FROM `elife-data-warehouse-prod.ods.ride_ride` AS `elife-data-warehouse-prod.ods.ride_ride_1`
                                                      LEFT OUTER JOIN `elife-data-warehouse-prod.dim.dim_place` AS `elife-data-warehouse-prod.dim.dim_place_1`
                                                                      ON `elife-data-warehouse-prod.ods.ride_ride_1`.from_place_id =
                                                                         `elife-data-warehouse-prod.dim.dim_place_1`.id) AS anon_7
                                            ON `elife-data-warehouse-prod.ods.ride_ride_1`.id = anon_7.ride_id
                            LEFT OUTER JOIN (SELECT `elife-data-warehouse-prod.ods.ride_ride_1`.id          AS ride_id,
                                                    `elife-data-warehouse-prod.ods.ride_ride_1`.to_place_id AS end_place_id,
                                                    `elife-data-warehouse-prod.dim.dim_place_1`.name        AS end_place,
                                                    `elife-data-warehouse-prod.dim.dim_place_1`.lng         AS lng,
                                                    `elife-data-warehouse-prod.dim.dim_place_1`.lat         AS ltt
                                             FROM `elife-data-warehouse-prod.ods.ride_ride` AS `elife-data-warehouse-prod.ods.ride_ride_1`
                                                      LEFT OUTER JOIN `elife-data-warehouse-prod.dim.dim_place` AS `elife-data-warehouse-prod.dim.dim_place_1`
                                                                      ON `elife-data-warehouse-prod.ods.ride_ride_1`.to_place_id =
                                                                         `elife-data-warehouse-prod.dim.dim_place_1`.id) AS anon_8
                                            ON `elife-data-warehouse-prod.ods.ride_ride_1`.id = anon_8.ride_id
                            LEFT OUTER JOIN (SELECT `elife-data-warehouse-prod.ods.ride_ride_1`.id               AS ride_id,
                                                    `elife-data-warehouse-prod.ods.ride_ride_1`.vehicle_class_id AS vehicle_class_id,
                                                    `elife-data-warehouse-prod.ods.ride_vehicle_class_1`.name    AS vehicle_class
                                             FROM `elife-data-warehouse-prod.ods.ride_ride` AS `elife-data-warehouse-prod.ods.ride_ride_1`
                                                      LEFT OUTER JOIN `elife-data-warehouse-prod.ods.ride_vehicle_class` AS `elife-data-warehouse-prod.ods.ride_vehicle_class_1`
                                                                      ON `elife-data-warehouse-prod.ods.ride_ride_1`.vehicle_class_id =
                                                                         `elife-data-warehouse-prod.ods.ride_vehicle_class_1`.id) AS anon_9
                                            ON `elife-data-warehouse-prod.ods.ride_ride_1`.id = anon_9.ride_id
                            LEFT OUTER JOIN `elife-data-warehouse-prod.ods.ride_fleet` AS `elife-data-warehouse-prod.ods.ride_fleet_1`
                                            ON `elife-data-warehouse-prod.ods.ride_dispatch_1`.to_fleet_id =
                                               `elife-data-warehouse-prod.ods.ride_fleet_1`.id
                   WHERE `elife-data-warehouse-prod.ods.ride_ride_1`.from_time_str > '2024-01-01'
                     AND `elife-data-warehouse-prod.ods.ride_dispatch_1`.currency = 'USD')
SELECT *,
FROM
    PricingRaw
WHERE row_num=1
LIMIT 10