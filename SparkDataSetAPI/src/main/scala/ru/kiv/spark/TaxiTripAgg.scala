package ru.kiv.spark

case class TaxiTripAgg (Zone: String
                       ,zone_count: Int
                       ,min_trip_distance: Double
                       ,max_trip_distance: Double
                       ,agg_trip_distance: Double
                       ,rmse_trip_distance: Double
                    )
