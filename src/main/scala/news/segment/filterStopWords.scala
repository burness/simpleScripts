package news.segment

import scala.io.Source

/**
  * Created by burness on 16/4/11.
  */
object filterStopWords {
  def filterStopWords(segmentWords:Array[String]): Array[String] ={
    val stopWordsFile = getClass.getResource("/sentiment/stopwords_zh.txt").toString
    val stopWords = Source.fromFile(stopWordsFile.split(":")(1)).getLines().toList
    val segmentWrodsNoStop = segmentWords.filter(!stopWords.contains(_))
    return segmentWrodsNoStop
  }

  def main(args: Array[String]) {
    val segmentWords = segment.segment("中国科学院计算技术研究所的宗成庆教授正在教授自然语言处理课程")
    val filterResults = filterStopWords(segmentWords)
    segmentWords.foreach(println)
    filterResults.foreach(println)


  }


}
