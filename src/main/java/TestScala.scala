import com.cloudera.sparkts.models.ARIMA
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import org.datasyslab.geosparksql.utils.GeoSparkSQLRegistrator



object TestScala {

  def main(args: Array[String]): Unit = {
    //Initialize SparkSession
    val sparkSession = SparkSession
      .builder()
      .config("spark.serializer", classOf[KryoSerializer].getName)
      .config("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName)
      .appName("spark-sql-basic")
      .master("local[*]")
      .getOrCreate()
    DataLoader.load(sparkSession)
    Polygons_work.partitioning1(sparkSession)

  }
}
