//import org.apache.commons.configuration.ConfigurationFactory.ConfigurationBuilder
import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.storage.StorageLevel
import StorageLevel._
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.twitter._
import twitter4j.auth._
import twitter4j.conf._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._

/**
  * Created by burness on 16/4/13.
  */
object SparkStreaming {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Spark Streaming").setMaster("local[2]")
    val sc = new SparkContext(conf)
    Logger.getRootLogger.setLevel(Level.WARN)
//    val ssc = new StreamingContext(sc, Seconds(20))// 20 seconds as a batch interval
//    val lines = ssc.socketTextStream("localhost",8585,MEMORY_ONLY)
//    val wordsFlatMap = lines.flatMap(_.split(" "))
//    val wordsMap = wordsFlatMap.map( w => (w,1))
//    val wordCount = wordsMap.reduceByKey( (a,b) => (a+b))
//    wordCount.print
//    ssc.start()
//    ssc.awaitTermination()
    /*
     consumer key :e7KiYQz1koMZOuxNtyxu9pjyK
     consumer secret:6bHUHyQwPdxQlIOiKSVyFHNAEI2cel6qibaat3wQk2RV0ls0FO
     access token:969272604-iw8OzM90fFCDHoHGQrBQuMXXd1q2wISXtZKj5THz
     access token secret:mBkDJe2aq1HWBX1LXRK6Vs0Mz8HvgrOGhccbFItUgUISq
     */
    val ssc = new StreamingContext(sc, Seconds(10))
    val cb = new ConfigurationBuilder
    cb.setDebugEnabled(true)
    .setOAuthConsumerKey("e7KiYQz1koMZOuxNtyxu9pjyK")
    .setOAuthConsumerSecret("6bHUHyQwPdxQlIOiKSVyFHNAEI2cel6qibaat3wQk2RV0ls0FO")
    .setOAuthAccessToken("969272604-iw8OzM90fFCDHoHGQrBQuMXXd1q2wISXtZKj5THz")
    .setOAuthAccessTokenSecret("mBkDJe2aq1HWBX1LXRK6Vs0Mz8HvgrOGhccbFItUgUISq")
    val auth = new OAuthAuthorization(cb.build())
    val tweets = TwitterUtils.createStream(ssc, Some(auth))
    val englishTweets = tweets.filter(_.getLang=="en")
    val status = englishTweets.map(status=>status.getText)
    status.print
    ssc.checkpoint(getClass.getResource("/checkpoint").toString.split(":")(1))
    ssc.start
    ssc.awaitTermination




  }

}