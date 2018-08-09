import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.mllib.linalg._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DoubleType, IntegerType, StructField, StructType}

class Utilits(sc: SparkContext, sqlContext: SQLContext) {
  def toRDD(m: Matrix): RDD[Vector] = {
    val columns = m.toArray.grouped(m.numRows)
    val rows = columns.toSeq.transpose
    val vectors = rows.map(row => new DenseVector(row.toArray))
    sc.parallelize(vectors)
  }

  def adjacencyToDF(m: Array[Array[Double]]): DataFrame = {
    val dataIterator = m.zipWithIndex
      .flatMap { case (row, v1) =>
        row.zipWithIndex.collect {
          case (weight, v2) if (weight > 0.0) =>
            Row(v1, v2, weight)
        }
      }
    val rdd = sc.parallelize(dataIterator.toStream)
    sqlContext.createDataFrame(
      rdd,
      StructType(
        Seq(
          StructField("v1", IntegerType, nullable = false),
          StructField("v2", IntegerType, nullable = false),
          StructField("weight", DoubleType, nullable = false)
        )
      )
    )
  }
}