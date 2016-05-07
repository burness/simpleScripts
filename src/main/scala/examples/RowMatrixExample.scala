package examples

import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.mllib.random.RandomRDDs

/**
  * Created by burness on 16/4/29.
  */
object RowMatrixExample {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Test Row Matrix").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val rdd = RandomRDDs.normalVectorRDD(sc, 100, 10, 0, 0)
    val rowMatrix = new RowMatrix(rdd)
    val colNum = rowMatrix.numCols()
    val rowNum = rowMatrix.numRows()
    val matrixCov = rowMatrix.computeCovariance()
    println(s"rowMatrix stats: $colNum" +
      s",$rowNum" +
      s",$matrixCov ")


  }

}
