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
import java.util.Arrays





object Polygons_work {

  def partitioning1(sparkSession: SparkSession) : Unit = {

    GeoSparkSQLRegistrator.registerAll(sparkSession)
    val sc = sparkSession.sparkContext
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._


/*    val pointWktDF =
      DataLoader.addressesWithGps.select("id", "longitude", "latitude")
      .join(DataLoader.cardsDF, "id")
      .select("longitude", "latitude", "id", "createddatetime", "addresstext", "applicantlocation")*/


/*    val df1 = pointWktDF.toDF("longitude", "latitude", "id", "createddatetime", "addresstext", "applicantlocation")
    val criminalpoint = DataLoader.cardcriminalsDF.select("cardid")
    val df2 = criminalpoint.toDF("cardid")
    val criminalpoints = df1.join(df2, $"id" === $"cardid")
    val crDF = criminalpoint.join(pointWktDF, $"id" === $"cardid")
    crDF.show()*/

    val cardsDF = DataLoader.cardsDF.select("id", "createddatetime", "addresstext", "applicantlocation")


    val accpoint = DataLoader.cardaccidentsDF.select("cardid")
    val acccardDF = cardsDF.join(accpoint,$"id" === $"cardid")

    val pointWktDF = DataLoader.addressesWithGps.select("id", "longitude", "latitude")
        .join(acccardDF,"id")
      .select("longitude", "latitude", "id", "createddatetime", "addresstext", "applicantlocation")


 //   criminalpoints.show()

    pointWktDF.createOrReplaceTempView("pointtable")
    println(pointWktDF.count())
    pointWktDF.show(15)

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


//    val polygonWktDF = DataLoader.polygonsDF.select("minX", "maxX", "minY", "maxY")
val polygonWktDF = DataLoader.polygonsDF
  .select("minX", "maxX", "minY", "maxY").rdd
  .map(r => (r.getString(0), r.getString(1), r.getString(2), r.getString(3),
    r.getString(1).toDouble - r.getString(0).toDouble / 2 + r.getString(0).toDouble,
    r.getString(3).toDouble - r.getString(2).toDouble / 2 + r.getString(2).toDouble))
  .toDF("minX", "maxX", "minY", "maxY", "centreX", "centreY")
    //полуичьт строковой id
    val polygonIDtmp = polygonWktDF.withColumn("IDtmp", monotonically_increasing_id())
       val polygonID =  polygonIDtmp.withColumn("ID", polygonIDtmp.col("IDtmp")).drop("IDtmp")
    polygonID.createOrReplaceTempView("polygontable")

    polygonID.show()

    //create PolygonDF
    val polygonDF = sparkSession.sql("select ST_PolygonFromEnvelope(cast(minX as Decimal(24,20))" +
      ", cast(minY as Decimal(24,20)), cast(maxX as Decimal(24,20)), cast(maxY as Decimal(24,20)) ," +
      " cast(ID as String), cast(centreX as String), cast(centreY as String)) from polygontable")

    println("PolygonDF")
    polygonDF.show(20)

    val polcent = sparkSession.sql("select ID, centreX, centreY from polygontable")

    //create PolygonRDD
    val polygonRDD = new SpatialRDD[Geometry]
    polygonRDD.rawSpatialRDD = Adapter.toRdd(polygonDF)
    polygonRDD.analyze()


    // SPartitioning of PointRDD and PolygonRDD
    pointRDD.spatialPartitioning(GridType.KDBTREE)
    polygonRDD.spatialPartitioning(pointRDD.getPartitioner)

    //Join
    val joinResultPairRDD = JoinQuery.SpatialJoinQueryFlat(pointRDD,polygonRDD,false,false)
    val joinResultDf= Adapter.toDf(joinResultPairRDD,sparkSession)

    val colNames1 = Seq("polygon","polygonID","centerX","centerY","point","cardid","datetime","address","text")
    val polpoints = joinResultDf.toDF(colNames1: _*)
    println("Polygons and their points:")
    polpoints.show(25)
    val a1 =  polpoints.groupBy("polygonID").count()





    val circleRDD1 = new CircleRDD(polygonRDD, 0.01) // Create a CircleRDD using the given distance
    circleRDD1.analyze()

    circleRDD1.spatialPartitioning(GridType.KDBTREE)
    polygonRDD.spatialPartitioning(circleRDD1.getPartitioner)

    val considerBoundaryIntersection1 = true // Only return gemeotries fully covered by each query window in queryWindowRDD
    val usingIndex1 = true

    val result1 = JoinQuery.DistanceJoinQueryFlat(polygonRDD, circleRDD1, usingIndex1, considerBoundaryIntersection1)
    val joinResult1Df= Adapter.toDf(result1,sparkSession).filter("_c1 != _c5")
    val polneigh = joinResult1Df.toDF("polygon","polygonID","centerX","centerY","polygon_neigh","neighID","neighCX","neighCY")

    println("Polygons and neighbours:")
    polneigh.show(5)
    //number of neighbours
    val total = joinResult1Df.count()



    //compute the distance  between polygons
    val distances = polneigh.rdd.map { row =>
      val id = row.getAs[String]("polygonID")
      val cx = row.getAs[String]("centerX")
      val cy = row.getAs[String]("centerY")
      val id_other = row.getAs[String]("neighID")
      val cx_other = row.getAs[String]("neighCX")
      val cy_other = row.getAs[String]("neighCY")

      (id, id_other, math.sqrt((math.pow(cx.toDouble - cx_other.toDouble, 2) + math.pow(cy.toDouble - cy_other.toDouble, 2))))
    }.toDF("id", "id_other", "distance")

    //transform to the kilometers
    val dist_km = distances.withColumn("distance", distances.col("distance")*lit(111.139))
    println("Polygons and distance between them:")
    dist_km.show(5)



    val polygons_dist_events_count = dist_km.select("id", "id_other", "distance")
      .join(a1,$"id" === $"polygonID")
        .join(a1.toDF("polygonID_neigh","events_count"),$"id_other" === $"polygonID_neigh")
        .drop("id","id_other")
      .filter("distance <= 5.00")


    //count number of polygons
    var pol_kol = DataLoader.polygonsDF.count()
    println("Number of polygons: "+pol_kol)

    //calculate N/sumofones in the spatial weighted matrix
    //get the first multiplier
    var mn1 = pol_kol.toDouble/total
    println("N/S0 = "+mn1)
    val mean_numb_points = polygons_dist_events_count.select(mean("count")).first().getDouble(0)
    println("Mean number of events in polygons = " + mean_numb_points)
    val mean_numb_points_col =
      polygons_dist_events_count.withColumn("polygon_mean-events", polygons_dist_events_count.col("count") - lit(mean_numb_points))
          .withColumn("neigh_mean-events", polygons_dist_events_count.col("events_count") - lit(mean_numb_points))

    val tt = mean_numb_points_col.withColumn("product",mean_numb_points_col.col("polygon_mean-events")*mean_numb_points_col.col("neigh_mean-events"))
    val mean_sum = tt.agg(sum("product"))
    val powt_mean = mean_numb_points_col.withColumn("(mean)2",pow(( polygons_dist_events_count.col("count") - lit(mean_numb_points)),2))
    val powt_mean_sum = powt_mean.agg(sum("(mean)2"))
    val div = mean_sum.first().getDouble(0)/powt_mean_sum.first().getDouble(0)
    val pr = mn1 * div
    println("Moran's I = "+pr)
    val expI = -1 / (pol_kol.toDouble-1)
    println("Expected I: "+expI)


/*    val powt_mean4 = mean_numb_points_col.withColumn("(mean)4",pow(( polygons_dist_events_count.col("count") - lit(mean_numb_points)),4))
    val powt_mean_sum4 = powt_mean4.agg(sum("(mean)4"))
    val powt_mean22 = powt_mean.withColumn("(mean)22",pow(powt_mean.col("(mean)2"),2))
    val powt_mean22_sum = powt_mean22.agg(sum("(mean)22"))
    val D = powt_mean_sum4.first().getDouble(0)/powt_mean22_sum.first().getDouble(0)

    val C = (pol_kol.toDouble-1)*(pol_kol.toDouble-2)*(pol_kol.toDouble-3)*math.pow(total,2)*/



/*    val utils = new Utilits(sc, sqlContext)
    val adjacencyDF = utils.adjacencyToDF(arr)
    adjacencyDF.show()*/

    //rename columns
/*    val colNames = Seq("polygon","polygon_neighbour","distance")
    val adjDF = adjacencyDF.toDF(colNames: _*)
    adjDF.show(5)*/


 //   dist_km.show(10)
/*    val mean_weight = adjDF.select(mean("distance")).first().getDouble(0)
//    println(mean_weight)
    //compute di for Moran's Index
    val mean_weight_col = adjDF.withColumn("mean-weight", adjDF.col("distance") - lit(mean_weight))
    val mean_weight_col_sum = mean_weight_col.agg(sum("mean-weight"))
    mean_weight_col_sum.show()
//    mean_weight_col.show(10)
    val powt_mean = adjDF.withColumn("(mean-destination)2", pow((mean_weight_col.col("distance")- lit(mean_weight)) ,2))
    val powt_mean_sum = powt_mean.agg(sum("(mean-destination)2"))
    val div = mean_weight_col_sum.first().getDouble(0)/powt_mean_sum.first().getDouble(0)
    println("Moran's I = " + div)*/
  }

}
