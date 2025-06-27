package ru.kiv.spark

case class TaxiTrip (Zone: String
                     ,trip_distance: Double
                    )

object TaxiTrip {
  def applay (x: TaxiZone, y: TaxiRoad): TaxiTrip = {
    TaxiTrip(
      x.Zone,
      y.trip_distance
    )
  }
}