import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by burness on 16/4/14.
  */
object SparkStreamingKafka {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Spark Streaming").setMaster("local[2]")
    val sc = new SparkContext(conf)
    Logger.getRootLogger.setLevel(Level.WARN)
    val ssc = new StreamingContext(sc, Seconds(2))
    val zkQuorum = "localhost:2181"
    val group = "test-group"
    val topics = "test"
    val numThreads = 1
    val topicMap = topics.split(",").map((_,numThreads.toInt)).toMap
    val lineMap = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap)
    val lines = lineMap.map(_._2)
    val words = lines.flatMap(_.split(" "))
    val pair = words.map(x=>(x,1))
    val wordCounts = pair.reduceByKeyAndWindow(_+_,_-_,Minutes(10),Seconds(2),2)
    wordCounts.print
    ssc.checkpoint(getClass.getResource("/checkpointKafka").toString.split(":")(1))
    ssc.start
    ssc.awaitTermination
  }
}
