package news.segment

import com.hankcs.hanlp.HanLP
import com.hankcs.hanlp.seg.common.Term
import com.hankcs.hanlp.tokenizer.NLPTokenizer

import scala.collection.mutable.ArrayBuffer

/**
  * Created by burness on 16/4/10.
  */
object segment {
  def segment(sentence:String): Array[String] ={
    val segmentCase = HanLP.segment(sentence)
    val segmentWords = new ArrayBuffer[String]()
    segmentCase.toArray.foreach(segmentWords += _.toString.split("/")(0))
    return segmentWords.toArray
  }


  def main(args: Array[String]): Unit = {

    val segmentWords = segment("中国科学院计算技术研究所的宗成庆教授正在教授自然语言处理课程")
    println(segmentWords.mkString("|"))
  }
}
