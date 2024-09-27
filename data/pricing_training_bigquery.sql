WITH TripTypeID AS (SELECT t1.ride_id,
                           CASE
                               WHEN t2.trip_no = t1.trip_no THEN t2.trip_type
                               ELSE NULL
                               END AS trip_type_id,
                    FROM `elife-data-warehouse-prod.ods.ride_dispatch` AS t1
                             JOIN `elife-data-warehouse-prod.ods.ride_trip` AS t2 ON t1.ride_id = t2.ride_id),
     TripType AS (SELECT t1.ride_id,
                         t1.trip_type_id,
                         t2.name AS trip_type,
                  FROM TripTypeID AS t1
                           LEFT JOIN `elife-data-warehouse-prod.ods.ride_enum AS` t2 ON t1.trip_type_id = t2.id),
     RideStatus AS (SELECT t1.id   AS ride_id,
                           t1.stat AS ride_status_id,
                           t2.name AS ride_status,
                    FROM `elife-data-warehouse-prod.ods.ride_ride` AS t1
                             LEFT JOIN `elife-data-warehouse-prod.ods.ride_enum` AS t2 ON t1.stat = t2.id),
     PartnerID AS (SELECT t1.id   AS ride_id,
                          CASE
                              WHEN t2.id = t1.partner_tran_id THEN t2.partner_id
                              ELSE NULL
                              END AS partner_id,
                   FROM `elife-data-warehouse-prod.ods.ride_ride` AS t1
                            LEFT JOIN `elife-data-warehouse-prod.ods.ride_partner_tran` AS t2
                                      ON t1.partner_tran_id = t2.id),
     Partner AS (SELECT t1.ride_id,
                        t1.partner_id,
                        t2.name AS partner,
                 FROM PartnerID AS t1
                          LEFT JOIN `elife-data-warehouse-prod.ods.ride_partner` AS t2 ON t1.partner_id = t2.id),
     FromPlace AS (SELECT t1.id            AS ride_id,
                          t1.from_place_id as start_place_id,
                          t2.name          AS start_place,
                          t2.lng           AS longt,
                          t2.lat           AS latit,
                   FROM `elife-data-warehouse-prod.ods.ride_ride` AS t1
                            LEFT JOIN `elife-data-warehouse-prod.dim.dim_place` AS t2 ON t1.from_place_id = t2.id),
     ToPlace AS (SELECT t1.id          AS ride_id,
                        t1.to_place_id as end_place_id,
                        t2.name        AS end_place,
                        t2.lng         AS longt,
                        t2.lat         AS latit,
                 FROM `elife-data-warehouse-prod.ods.ride_ride` AS t1
                          LEFT JOIN `elife-data-warehouse-prod.dim.dim_place` AS t2 ON t1.to_place_id = t2.id),
     VehicleClass AS (SELECT t1.id               AS ride_id,
                             t1.vehicle_class_id as vehicle_class_id,
                             t2.name             AS vehicle_class,
                      FROM `elife-data-warehouse-prod.ods.ride_ride` AS t1
                               LEFT JOIN `elife-data-warehouse-prod.ods.ride_vehicle_class` AS t2
                                         ON t1.vehicle_class_id = t2.id),
     PricingTraining AS (SELECT t0.id       AS ride_id,
                                t1.id       AS dispatch_id,
                                t2.trip_type_id,
                                t2.trip_type,
                                t1.trip_no,
                                t3.ride_status_id,
                                t3.ride_status,
                                t4.partner_id,
                                t4.partner,
                                t0.from_time_str,
                                t0.from_timezone_str,
                                t0.to_time_str,
                                t0.to_timezone_str,
                                t5.start_place_id,
                                t5.start_place,
                                t5.longt    AS start_longt,
                                t5.latit    AS start_latit,
                                t6.end_place_id,
                                t6.end_place,
                                t6.longt    AS end_longt,
                                t6.latit    AS end_latit,
                                t0.distance,
                                t0.duration,
                                t7.vehicle_class_id,
                                t7.vehicle_class,
                                t0.passenger_count,
                                t0.luggage_count,
                                t0.trip_count,
                                t0.children_count,
                                t0.infant_count,
                                t1.amount   as dispatch_amount,
                                t1.currency as dispatch_currency,

                         FROM `elife-data-warehouse-prod.ods.ride_ride` AS t0
                                  JOIN `elife-data-warehouse-prod.ods.ride_dispatch` AS t1 ON t0.id = t1.ride_id
                                  LEFT JOIN TripType AS t2 ON t0.id = t2.ride_id
                                  LEFT JOIN RideStatus AS t3 ON t0.id = t3.ride_id
                                  LEFT JOIN Partner AS t4 ON t0.id = t4.ride_id
                                  LEFT JOIN FromPlace AS t5 ON t0.id = t5.ride_id
                                  LEFT JOIN ToPlace AS t6 ON t0.id = t6.ride_id
                                  LEFT JOIN VehicleClass AS t7 ON t0.id = t7.ride_id
                         WHERE from_time_str > '2024-01-01')

SELECT t.ride_id,
       t.dispatch_id,
       t.trip_type,
       t.trip_no,
       t.ride_status,
       t.partner,
       t.from_time_str,
       t.from_timezone_str,
       t.to_time_str,
       t.to_timezone_str,
       t.start_place,
       t.start_latit,
       t.start_longt,
       t.end_place,
       t.end_latit,
       t.end_longt,
       t.distance,
       t.duration,
       t.vehicle_class,
       t.passenger_count,
       t.luggage_count,
       t.trip_count,
       t.children_count,
       t.infant_count,
       t.dispatch_amount,
       t.dispatch_currency,

FROM PricingTraining AS t
WHERE from_time_str > '2024-01-01'
LIMIT 200
