package ru.kiv.spark

case class TaxiData (
                      LocationID: Int,
                      Borough: String,
                      Zone: String,
                      service_zone: String,
                    )
object TaxiData {

  def apply(s: String):TaxiData = {
    val a = s.split(",")
        TaxiData(
          a(0).toInt,
          a(1),
          a(2),
          a(3))
  }

  def getKey(s: String): Int = {
    val a = s.split(",")
    if (a.length == 4)
      a(0).toInt
    else
      0
  }
}