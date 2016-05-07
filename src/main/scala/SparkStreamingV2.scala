import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by burness on 16/4/13.
  */
object SparkStreamingV2 {

  case class Person(key: Int, value: String)

  def main(args: Array[String]) {

    //val filters = args
    val filters = Array("ps3", "ps4", "playstation", "sony", "vita", "psvita")
    //val filers = "ThisIsSparkStreamingFilter_100K_per_Second"

    val delimeter = "|"

    System.setProperty("twitter4j.oauth.consumerKey", "e7KiYQz1koMZOuxNtyxu9pjyK")
    System.setProperty("twitter4j.oauth.consumerSecret", "6bHUHyQwPdxQlIOiKSVyFHNAEI2cel6qibaat3wQk2RV0ls0FO")
    System.setProperty("twitter4j.oauth.accessToken", "969272604-iw8OzM90fFCDHoHGQrBQuMXXd1q2wISXtZKj5THz")
    System.setProperty("twitter4j.oauth.accessTokenSecret", "mBkDJe2aq1HWBX1LXRK6Vs0Mz8HvgrOGhccbFItUgUISq")
    System.setProperty("twitter4j.http.useSSL", "true")

    val conf = new SparkConf().setAppName("TwitterApp").setMaster("local[4]")

    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(5))

    val tweetStream = TwitterUtils.createStream(ssc, None, filters)

    val tweetRecords = tweetStream.map(status => {

      def getValStr(x: Any): String = {
        if (x != null && !x.toString.isEmpty()) x.toString + "|" else "|"
      }


      var tweetRecord =
        getValStr(status.getUser().getId()) +
          getValStr(status.getUser().getScreenName()) +
          getValStr(status.getUser().getFriendsCount()) +
          getValStr(status.getUser().getFavouritesCount()) +
          getValStr(status.getUser().getFollowersCount()) +
          getValStr(status.getUser().getLang()) +
          getValStr(status.getUser().getLocation()) +
          getValStr(status.getUser().getName()) +
          getValStr(status.getId()) +
          getValStr(status.getCreatedAt()) +
          getValStr(status.getGeoLocation()) +
          getValStr(status.getInReplyToUserId()) +
          getValStr(status.getPlace()) +
          getValStr(status.getRetweetCount()) +
          getValStr(status.getRetweetedStatus()) +
          getValStr(status.getSource()) +
          getValStr(status.getInReplyToScreenName()) +
          getValStr(status.getText())

      tweetRecord

    })

    tweetRecords.print

    //    tweetRecords.filter(t => (t.getLength() > 0)).saveAsTextFiles("/user/hive/warehouse/social.db/tweeter_data/tweets", "data")

    ssc.start()
    ssc.awaitTermination()
//    ssc.stop()

  }

}
