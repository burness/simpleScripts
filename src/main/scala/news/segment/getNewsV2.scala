package news.segment

import java.io.{InputStream, BufferedReader}
import java.net.{HttpURLConnection, URL}

import org.apache.http.client.methods.HttpGet
import org.apache.http.impl.client.DefaultHttpClient

import scala.io.Source

/**
  * Created by burness on 16/4/11.
  */
object getNewsV2 {


  /**
    * @param urlAll
 *            :请求接口
    * @param httpArg
 *            :参数
    * @return 返回结果
    */
  def request(httpUrl:String, httpArg:String) {
    val httpUrlFinal = httpUrl + "?" + httpArg
    val httpGet = new HttpGet(httpUrlFinal)
    httpGet.setHeader("apikey", "2525e730e2c27269c818f7e954d33c51")
    val client = new DefaultHttpClient
    val result = client.execute(httpGet)
    return result

    //    println(result)

  }

  def main(args: Array[String]) {
    val httpUrl = "http://apis.baidu.com/songshuxiansheng/news/news"
    val httpArg = "channel_news"
    val jsonResult = request(httpUrl, httpArg)
    println(jsonResult)
  }



}
