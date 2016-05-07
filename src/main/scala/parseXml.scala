import scala.xml.XML

/**
  * Created by burness on 16/4/6.
  */
object parseXml {
  def parseXml(filePath: String): Unit = {
    val xmlData = XML.loadFile(filePath)

    val headerField = (xmlData \\ "jobNode").map {
      case s =>
        val jobName = (s \ "jobName").text
        println(s"JobName: $jobName \n----------------------")
        val paramsName = (s \\ "kvParam" ).map{
          case s=>
            val paramName = s\"@name"
            val paramVal = s.text
            println(s"paramName: $paramName, paramVal: $paramVal")
        }
        println("\n")
    }
  }

  def main(args: Array[String]) {
    val filePath = getClass.getResource("/jobConf.xml")
    parseXml(filePath.getPath)
  }

}
