import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkContext, SparkConf}


/**
  * Created by burness on 16/4/9.
  */
object SparkSql {

  case class Person(first_name: String, last_name: String, age: Int)

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Spark SQL").setMaster("local[1]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    //    val filePath = getClass.getResource("/person/person.json").toString
    //    val p = sc.textFile(filePath)
    //    val personDF = p.map(_.split(",")).map(pp=>Person(pp(0),pp(1),pp(2).toInt)).toDF()
    //    personDF.registerTempTable("person")
    //    val persons = sqlContext.sql("select * from person order by age desc")
    //    persons.collect.foreach(println)
    //    val pmap = p.map(_.split(","))
    //    val personData = pmap.map(p=>Row(p(0),p(1),p(2).toInt))
    //    val schema = StructType(
    //      Array(StructField("first_name",StringType,true),
    //        StructField("last_name",StringType,true),
    //        StructField("age",IntegerType,true)
    //      ))
    //    val personDF = sqlContext.createDataFrame(personData, schema)
    //    personDF.registerTempTable("person")
    //    val persons = sqlContext.sql("select * from person")
    //    persons.collect.foreach(println)
    //    val personRDD = sc.textFile(filePath).map(_.split(",")).map(p=>Person(p(0),p(1),p(2).toInt))
    //    val person = personRDD.toDF()
    //    person.registerTempTable("person")
    //    val sixtyPlus = sqlContext.sql("select * from person where age > 60")
    //    sixtyPlus.collect().foreach(println)
    //    sixtyPlus.saveAsParquetFile("/Users/burness/git_repository/simpleScripts/src/main/resources/sp.parquet")
    //    val parquetDF = sqlContext.load("/Users/burness/git_repository/simpleScripts/src/main/resources/sp.parquet")
    //    parquetDF.registerTempTable("sixtyPlus")
    //    val parquetSixtyPlus = sqlContext.sql("select * from sixtyPlus")
    //    parquetSixtyPlus.collect().foreach(println)
    //    val person = sqlContext.read.json(filePath)
    //    person.registerTempTable("person")
    //    val sixtyPlus = sqlContext.sql("select * from person where age > 60")
    //    sixtyPlus.collect.foreach(println)
    //    val savePath2 = getClass.getResource("/person/").toString.split(":")(1)+"person.Sp"
    //    val savePath = "/Users/burness/git_repository/simpleScripts/src/main/resources/person/personSp"
    //    println(savePath2)
    //    import sqlContext.implicits._
    //    println(savePath)
    //    sixtyPlus.toJSON.saveAsTextFile(savePath)
    //    sixtyPlus.toJSON.saveAsTextFile(savePath2)
    //    sc.stop()

//    val url = "jdbc:mysql://192.168.35.235:3306/mysql"
//    val prop = new java.util.Properties
//    prop.setProperty("user", "root")
//    prop.setProperty("password", "qweasdzxc")
//    val people = sqlContext.read.jdbc(url, "person", prop)
//    people.show
//    val males = sqlContext.read.jdbc(url, "person", Array("gender ='M'"), prop)
//    males.show
//    val first_names = people.select("first_name")
//    first_names.show
//    val below60 = people.filter(people("age") < 60)
//    below60.show
//    //    val grouped = people.groupBy("gender")
//    //    val gender_count = grouped.count
//    people.write.json(getClass.getResource("/person/personDB/").toString.split(":")(1) + "person.json")
//    people.write.parquet(getClass.getResource("/person/personDB/").toString.split(":")(1) + "person.parquet")
//    可以读入hdfs\local text\parquet\json
      val people = sqlContext.read.load(getClass.getResource("/person/personDB/").toString.split(":")(1) + "person.parquet")



  }

}
