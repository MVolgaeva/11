

import org.apache.spark.SparkConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object DataLoader {

  var criminalsDF:DataFrame = _
  var cardsDF:DataFrame=_
  var addressesWithGps:DataFrame=_
  var polygonsDF:DataFrame = _

  private var _addressesWithGpsSchema = StructType(Array(
    StructField("id", IntegerType, true),
    StructField("address", StringType, true),
    StructField("translated_address", StringType, true),
    StructField("longitude", StringType, true),
    StructField("latitude", StringType, true)))

  private var _polygonsDFSchema = StructType(Array(
    StructField("minX",StringType, true ),
    StructField("maxX",StringType, true ),
    StructField("minY",StringType, true ),
    StructField("maxY",StringType, true )))

  // unit ничего не возвращает
  def load(sparkSession: SparkSession): Unit ={

    val jdbcDF = sparkSession.read
      .format("jdbc")
      .option("url", "jdbc:postgresql://localhost/GLONASS?user=postgres&password=margo2636085m")
      .option("dbtable", "callcenter.cardcriminals")
      .option("user", "postgres")
      .option("password", "margo2636085m")
      .load()

    //    jdbcDF.show(10)

    cardsDF = sparkSession.read
      .format("jdbc")
      .option("url", "jdbc:postgresql://localhost/GLONASS?user=postgres&password=margo2636085m")
      .option("dbtable",
        //                  "(select t2.id, t2.createddatetime,t2.addresstext,t2.applicantlocation, t1.longitude, t1.latitude from callcenter.address_with_gps as t1 left join callcenter.cards as t2 on t1.id=t2.id)as t1")
        "(select t2.id, t2.addresstext, t2.createddatetime,t2.applicantlocation from callcenter.cards as t2)as t1")
      .option("columnname", "id, createddatetime, addresstext, applicantlocation,latitude,longitude")
      .option("user", "postgres")
      .option("password", "margo2636085m")
      .load()

    addressesWithGps = sparkSession.read
      .format("csv")
      .option("header", "true")
      .option("delimiter", ",")
      .option("nullValue", "")
      .option("treatEmptyValuesAsNulls", "true")
      .schema(_addressesWithGpsSchema)
      .load("D:\\SpatialAutocorrelation\\src\\main\\resources\\addreses_with_gps_coordinates.csv")

    polygonsDF = sparkSession.read
      .format("csv")
      .option("header", "true")
      .option("delimiter", ",")
      .option("nullValue", "")
      .option("treatEmptyValuesAsNulls", "true")
      .schema(_polygonsDFSchema)
      .load("D:\\SpatialAutocorrelation\\src\\main\\resources\\polygons.csv")

    criminalsDF = sparkSession.read
      .format("jdbc")
      .option("url", "jdbc:postgresql://localhost/GLONASS?user=postgres&password=margo2636085m")
      .option("dbtable",
        //"(select id, addresscode, addresstext, date_trunc('day',datetime) as datetime from callcenter.cards where id in (select cardid from callcenter.cardsufferers ) and addresstext like '%Казань%' order by id)AS t")
        "( select cast(count(id) as double PRECISION) as count ,date_trunc('day',datetime) as datetime from callcenter.cards where id in (select cardid from callcenter.cardsufferers ) and addresstext like '%Казань%' group by date_trunc('day',datetime)) as t")
      .option("columnname", "count, datetime")
      .option("columnntype", "double, datetime")
      .option("user", "postgres")
      .option("password", "margo2636085m")
      .load()



  }

}
