import java.io.{File, PrintWriter}

import org.jsoup.Jsoup

import scala.io.Source
import scala.reflect.io.Path

/**
  * Created by burness on 16/4/10.
  */
object SentimentAnalysis {
  def parseData(dataFile: String, outFile: String): Unit = {
    val pw = new PrintWriter(new File(outFile))
    val stopWordsFile = getClass.getResource("/sentiment/stopwords.txt").toString
    val stopWords = Source.fromFile(stopWordsFile.split(":")(1)).getLines().toList
    println(stopWords)
    for (line <- Source.fromFile(dataFile).getLines) {
      val content = line.split("\t")
      val id = content(0)
      val sentiment = content(1)
      val review = content(2)
      //        println(review)
      //        println(sentiment.toInt)
      //        删除review的html的标签
      val reviewText = Jsoup.parse(review).text()
      //        删除非字幕字符
      val letterRegex = "[^a-zA-Z]"
      val reviewTextOnlyLetter = reviewText.replaceAll(letterRegex, " ").replaceAll(" +", " ").toLowerCase
      //        删除stop words
      val reviewTextLetterArray = reviewTextOnlyLetter.split(" ")
      val reviewTextLetterNoStopArray = reviewTextLetterArray.filter(!stopWords.contains(_))
      val reviewTextLetterNoStop = reviewTextLetterNoStopArray.mkString(" ")
      val lineProcessed = id + "\t" + sentiment + "\t" + reviewTextLetterNoStop + "\n"
      pw.write(lineProcessed)
    }
    pw.close()
  }

  def main(args: Array[String]) {
    val dataFile = getClass.getResource("/sentiment/labeledTrainData.tsv").toString.split(":")(1)
    val outFile = getClass.getResource("/sentiment/labeledTrainDataProcessed.tsv").toString.split(":")(1)
    parseData(dataFile, outFile)
  }

}
