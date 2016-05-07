package news.segment

import org.apache.spark.SparkContext
import org.apache.spark.mllib.feature.Word2Vec
import org.apache.spark.sql.SQLContext

/**
  * Created by burness on 16/4/11.
  */
object w2v {
  def w2v(sc:SparkContext,sentencesFile: String): Unit ={
    val input = sc.textFile(sentencesFile).map(_.split("|").toSeq)
    val sqlContext = new SQLContext(sc)
//    val inputDataFrame = sqlContext.createDataFrame(input)
    val word2vec = new Word2Vec()
    val model = word2vec.fit(input)

  }


}
