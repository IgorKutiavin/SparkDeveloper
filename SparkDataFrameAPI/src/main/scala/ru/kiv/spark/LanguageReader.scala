package ru.kiv.spark

import org.apache.spark.sql.catalyst.encoders.AgnosticEncoders.RowEncoder
import org.apache.spark.sql.{DataFrame, Encoder, Encoders, Row}
import org.apache.spark.sql.execution.columnar.STRUCT
import org.apache.spark.sql.functions.{col, collect_list, expr, map_values, str_to_map, struct}
import ru.kiv.spark.SparkDataFrameAPI.spark
import org.apache.spark.sql.catalyst.encoders.RowEncoder

import scala.collection.mutable.ArrayBuffer

//case class LanguageReader(Country: String, Languages:Array[String])
case class LanguageReader(afr: String, amh: String, ara: String, arc: String, aym: String, aze: String, bar: String, bel: String, ben: String, ber: String, bis: String, bjz: String, bos: String, bul: String, bwg: String, cal: String, cat: String, ces: String, cha: String, ckb: String, cnr: String, crs: String, dan: String, deu: String, div: String, dzo: String, ell: String, eng: String, est: String, fao: String, fas: String, fij: String, fil: String, fin: String, fra: String, gil: String, gle: String, glv: String, grn: String, gsw: String, hat: String, heb: String, her: String, hgm: String, hif: String, hin: String, hmo: String, hrv: String, hun: String, hye: String, ind: String, isl: String, ita: String, jam: String, jpn: String, kal: String, kat: String, kaz: String, kck: String, khi: String, khm: String, kin: String, kir: String, kon: String, kor: String, kwn: String, lao: String, lat: String, lav: String, lin: String, lit: String, loz: String, ltz: String, lua: String, mah: String, mey: String, mfe: String, mkd: String, mlg: String, mlt: String, mon: String, mri: String, msa: String, mya: String, nau: String, nbl: String, ndc: String, nde: String, ndo: String, nep: String, nfr: String, niu: String, nld: String, nno: String, nob: String, nor: String, nrf: String, nso: String, nya: String, nzs: String, pap: String, pau: String, pih: String, pol: String, por: String, pov: String, prs: String, pus: String, que: String, rar: String, roh: String, ron: String, run: String, rus: String, sag: String, sin: String, slk: String, slv: String, smi: String, smo: String, sna: String, som: String, sot: String, spa: String, sqi: String, srp: String, ssw: String, swa: String, swe: String, tam: String, tet: String, tgk: String, tha: String, tir: String, tkl: String, toi: String, ton: String, tpi: String, tsn: String, tso: String, tuk: String, tur: String, tvl: String, ukr: String, urd: String, uzb: String, ven: String, vie: String, xho: String, zdj: String, zho: String, zib: String, zul: String)

object LanguageReader  {
  import spark.implicits._

//  implicit object rowEncoder extends Encoder[String,ArrayBuffer[String]]{
//    override def encoder(a: String, b: ArrayBuffer[String]): Row = Row(a,b)
//  }

    def apply(df: DataFrame): DataFrame = {

    val ndf = df.withColumn("Country", col("name.common"))
      .select("Country", "languages.*") //.as[(String,LanguageReader)]
//      .map( x => (x._1, x._2))

//      )
//      )

    val langDF = ndf.map(x => {
      val l = ArrayBuffer[String]()
      for (i <- 1 until x.length
           if x.get(i) != null)
      yield
        l += x.get(i).toString
      (x.get(0).toString, l)
    }).select(col("_1").as("Country"),col("_2").as("languages"))
      .withColumn("Language", expr("explode(languages)"))
      .drop("Lang")
      .groupBy("Language").agg(collect_list("Country").as("Countries"))
      .withColumn("NumCountries", expr("array_size(Countries)"))
      .select("Language","NumCountries","Countries")

    langDF
  }
}