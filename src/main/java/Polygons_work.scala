import breeze.linalg.Axis._1
import com.cloudera.sparkts.models.ARIMA
import com.vividsolutions.jts
import com.vividsolutions.jts.geom._
import com.vividsolutions.jts.geom.Geometry
import com.vividsolutions.jts.util.GeometricShapeFactory
import org.apache.parquet.filter2.predicate.Operators.Column
import org.apache.spark.api.java.JavaPairRDD
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.geosparksql.expressions.{ST_Point, ST_PolygonFromEnvelope, ST_PolygonFromText}
import org.apache.spark.sql.{Dataset, Encoders, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.datasyslab.geospark.enums.{FileDataSplitter, GridType, IndexType}
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator

import org.datasyslab.geospark.spatialRDD.{CircleRDD, PointRDD, PolygonRDD, SpatialRDD}
import org.datasyslab.geospark.{enums, spatialPartitioning}
import org.datasyslab.geosparksql.utils.Adapter
import org.geotools.geometry.jts.JTS
import org.wololo.geojson
import org.wololo.jts2geojson.GeoJSONWriter
import shapeless.PolyDefns.->
import org.datasyslab.geosparksql.utils.{Adapter, GeoSparkSQLRegistrator}
import org.datasyslab.geosparksql.UDF
import org.apache.spark.sql.functions._

import scala.math
//import org.datasyslab.geosparksql.utils.Adapter
import org.apache.spark.sql.SparkSession
import org.datasyslab.geospark.spatialOperator.JoinQuery

import org.datasyslab.geospark.spatialRDD.RectangleRDD
import scala.collection.JavaConversions._
import org.apache.spark.sql.RowFactory
import org.datasyslab.geospark.utils.GeoSparkConf
import geotrellis.util.Haversine

import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.mllib.linalg.distributed.{IndexedRow, IndexedRowMatrix, RowMatrix}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}

object Polygons_work {

  def partitioning1(sparkSession: SparkSession) : Unit = {

    GeoSparkSQLRegistrator.registerAll(sparkSession)
    val sc = sparkSession.sparkContext
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val pointWktDF = DataLoader.addressesWithGps.select("id", "longitude", "latitude").join(DataLoader.cardsDF,
      "id")
      .select("longitude", "latitude", "id", "createddatetime", "addresstext", "applicantlocation").limit(500)

    pointWktDF.createOrReplaceTempView("pointtable")
    println(pointWktDF.count())

    //create PointDF
    val pointDF = sparkSession.sql("select ST_Point(cast(longitude as Decimal(24,20))," +
      " cast(latitude as Decimal(24,20)), cast(id as String),cast(createddatetime as String),cast(addresstext as String)," +
      " cast(applicantlocation as String)) as area from pointtable") //,id,createddatetime,addresstext  from pointtable")
    println(pointDF.count())
    pointDF.printSchema()

    //create PointRDD
    val pointRDD = new SpatialRDD[Geometry]
    pointRDD.rawSpatialRDD = Adapter.toRdd(pointDF)
    pointRDD.analyze()


    val polygonWktDF = DataLoader.polygonsDF
  .select("minX", "maxX", "minY", "maxY").rdd
  .map(r => (r.getString(0), r.getString(1), r.getString(2), r.getString(3),
    r.getString(1).toDouble - r.getString(0).toDouble / 2 + r.getString(0).toDouble,
    r.getString(3).toDouble - r.getString(2).toDouble / 2 + r.getString(2).toDouble))
  .toDF("minX", "maxX", "minY", "maxY", "centreX", "centreY")
    val polygonID = polygonWktDF.withColumn("ID", monotonically_increasing_id())
    polygonID.createOrReplaceTempView("polygontable")
    val polygonID=polygonWktDF.withColumn("ID",monotonically_increasing_id())
    polygonID.createOrReplaceTempView("polygontable")

    //create PolygonDF
    val polygonDF = sparkSession.sql("select ST_PolygonFromEnvelope(cast(minX as Decimal(24,20))" +
      ", cast(minY as Decimal(24,20)), cast(maxX as Decimal(24,20)), cast(maxY as Decimal(24,20))," +
      " cast(ID as String), cast(centreX as String), cast(centreY as String) ) from polygontable")

    //create PolygonRDD
    val polygonRDD = new SpatialRDD[Geometry]
    polygonRDD.rawSpatialRDD = Adapter.toRdd(polygonDF)
    polygonRDD.analyze()

    // SPartitioning of PointRDD and PolygonRDD
    pointRDD.spatialPartitioning(GridType.EQUALGRID)
    polygonRDD.spatialPartitioning(pointRDD.getPartitioner)

    //Join
    val joinResultPairRDD = JoinQuery.SpatialJoinQueryFlat(pointRDD,polygonRDD,false,false)
    val joinResultDf= Adapter.toDf(joinResultPairRDD,sparkSession)//.schema("polygon", "point","id","addresstext","createddatetime")
    println(joinResultDf.count())
    joinResultDf.coalesce(1).write.csv("joinresult.csv")
    joinResultDf.schema
    joinResultDf.printSchema()

    //compute the weighted matrix, adjacency matrix
//    val frame = Adapter.toDf(result1, sparkSession).select("_c1", "_c5","_c2","_c3","_c6","_c7")
//      .filter("_c1 != _c5")
//    var arr = Array.ofDim[Double](frame.count().toInt, frame.count().toInt)
//    frame.show(20, false)
//    println(frame.count())
//    frame.collect().foreach(row => {
//      val p1 = row.getString(0).toInt
//      val p2 = row.getString(1).toInt
//      val c1x = row.getString(2).toDouble
//      val c1y = row.getString(3).toDouble
//      val c2x = row.getString(4).toDouble
//      val c2y = row.getString(5).toDouble
//      arr(p1)(p2) = math.sqrt(math.pow(c2x - c1x, 2) + math.pow(c2y - c1y, 2))
//    })
//    var total:Int = 0
//    for {i <- 0 until arr.size
//         j <- 0 until arr.size
//    } if (arr(i)(j) > 0) {
//      total = total + 1
//      println(arr(i)(j))
//    }
//
//    println("total "+total)

  }

}
