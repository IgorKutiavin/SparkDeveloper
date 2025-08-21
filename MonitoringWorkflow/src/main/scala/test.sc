import scala.util.matching.Regex

val en1 = "cp_[adh1_hive]_[dp_dsb]_[default]_[]__dl_raw__camsnbo_incsdocument_state"
val en2 = "host_adb_mc__adb_tm_db__i$cam__v$soi_vodnal"

val pr:Regex = """.*__([0-9a-zA-Z-_$]+)__([0-9a-zA-Z-_$]+)""".r

en2 match{
  case pr(sh,tbl) => println(s"schema: $sh tbl_nme: $tbl")
  case _ => println("now data found")
}

val datePattern = """(\d{4})-(\d{2})-(\d{2})""".r
val text = "Today's date is 2024-08-19."

text match {
  case datePattern(year, month, day) => println(s"Year: $year, Month: $month, Day: $day")
  case _ => println("No date found.")
}

//import scala.util.matching.Regex

val numberPattern: Regex = "[0-9]".r

numberPattern.findFirstMatchIn("awesomepassword1") match {
  case Some(_) => println("Password OK")
  case None => println("Password must contain a number")
}