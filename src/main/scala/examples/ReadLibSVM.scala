package examples

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD

/**
  * Created by burness on 16/4/29.
  */
object ReadLibSVM {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Spark Read LibSVM").setMaster("local[2]")
    val sc = new SparkContext(conf)
    Logger.getRootLogger.setLevel(Level.WARN)

    val examples: RDD[LabeledPoint] = MLUtils.loadLibSVMFile(sc,  getClass.getResource("/data/sample_libsvm_data.txt").toString)
    examples.foreach{
      case s=>
        println(s.toString())
    }
    val exampleSize = examples.count()
    println(exampleSize)

  }




}
