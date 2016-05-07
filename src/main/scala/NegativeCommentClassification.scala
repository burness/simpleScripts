//package scala
//
////import com.yhd.segment.api.CommentSegmenter
//import com.huaban.analysis.jieba.JiebaSegmenter
//import org.apache.spark.mllib.linalg.{DenseVector, Vectors}
//
////import com.yhd.segment.nlp.model.Lexeme
//import org.apache.spark.ml.Pipeline
//import org.apache.spark.ml.classification.RandomForestClassifier
//import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorIndexer}
//import org.apache.spark.mllib.feature.Word2VecModel
//import org.apache.spark.sql.hive.HiveContext
//import org.apache.spark.sql.types._
//import org.apache.spark.sql.{DataFrame, Row, SQLContext, SaveMode}
//import org.apache.spark.{SparkConf, SparkContext}
//
//import scala.util.Try
//
///**
//  * Created by root on 9/29/15.
//  * DEV: spark-submit --master yarn-client --executor-memory 4G --driver-memory 4G --num-executors 5 --jars /root/wangyuming/yhd-spark-test/lib/jieba-analysis-1.0.2.jar,/root/wangyuming/yhd-spark-test/lib/search-segment-1.0.0-SNAPSHOT.jar --class org.apache.spark.mllib.linalg.RFTraining /root/wangyuming/Spark-Example/target/SparkExample-0.0.1-SNAPSHOT.jar
//  * PRD: /usr/local/spark2/bin/spark-submit --master yarn-client  --queue tandem --conf spark.network.timeout=1200000 --conf
//  * spark.storage.memoryFraction=0.2 --conf "spark.executor.extraJavaOptions=-XX:MaxPermSize=384m" --executor-memory 2G --driver-memory 2G --num-executors 8  --jars /home/wym/spark/training/jieba-analysis-1_0_2.jar,/home/wym/spark/training/search-segment-1_0_0-SNAPSHOT.jar --class org.apache.spark.mllib.linalg.NegativeCommentClassification /home/wym/spark/training/SparkExample-0.0.1-SNAPSHOT.jar
//  */
//object NegativeCommentClassification {
//
//  def main(args: Array[String]) {
//
//    //DEV
//    //    val word2vecModelPath = "hdfs://heju:8020/user/wym/ml/data/model/google/word2vec.bin"
//    //    val trainingFile = "hdfs://heju:8020/user/wym/ml/data/training.txt"
//
//    //PRD
//    val word2vecModelPath = "/user/wym/ml/word2vec.bin"
//    val trainingFile = "/user/wym/ml/training/training.txt"
//
//    var jdbcUrl: String = ""
//    var jdbcUserName: String = ""
//    var jdbcPassword: String = ""
//    var jdbcTable: String = ""
//    var beginDate: String = ""
//    var endDate: String = ""
//
//    if(args.length != 6){
//      println("WARN: use default file path.")
//      println("usage: [jdbcUrl] [jdbcUserName] [jdbcPassword] [jdbcTable] [beginDate] [endDate]")
//      System.exit(-1)
//    } else {
//      jdbcUrl = args(0)
//      jdbcUserName = args(1)
//      jdbcPassword = args(2)
//      jdbcTable = args(3)
//      beginDate = args(4)
//      endDate = args(5)
//    }
//
//    val sparkConf = new SparkConf().setAppName("Negative comment classification")//.setMaster("local[1]")
//    val sc = new SparkContext(sparkConf)
//
//    val sqlContext = new SQLContext(sc)
//    val hiveContect = new HiveContext(sc)
//
//    val schemaString = ("cmmnt_id good_cntnt " +
//      "label_jiage label_wuliu label_zhiliang label_jiahuo label_fuwu label_ganzhi label_longtong")
//      .split(" ")
//    val schema = StructType(
//      schemaString.map(fieldName =>
//        if (fieldName.equals("cmmnt_id"))
//          StructField(fieldName, LongType)
//        else if(fieldName.equals("good_cntnt"))
//          StructField(fieldName, StringType)
//        else
//          StructField(fieldName, DoubleType)
//      ))
//
//    val rowRDD = sc.textFile(trainingFile).map(_.split("\t")).map(p =>
//      Row(p(0).trim.toLong, p(1).trim,
//        p(2).trim.toDouble, p(3).trim.toDouble, p(4).trim.toDouble,
//        p(5).trim.toDouble, p(6).trim.toDouble, p(7).trim.toDouble, p(8).trim.toDouble))
//
//    val trainingDatas = hiveContect.createDataFrame(rowRDD, schema).cache()
//
//    trainingDatas.sqlContext.sql("select ")
//
//    val model: Word2VecModel = Word2VecModel.load(sc, word2vecModelPath)
//
//    sqlContext.udf.register("clean", (s : String) =>
//      s.replaceAll("[\\pP|~|$|^|<|>|\\||\\+|= ]*", ""))
//
//    sqlContext.udf.register("word2avgVector", (s : String) => {
//
//      val words = new JiebaSegmenter().sentenceProcess(s).toArray().foldLeft("")((sum, i) =>
//        sum + i.asInstanceOf[String] + " ").split(" ")
//
//      val vectors = words.map(w => Try(model.transform(w))).filter(_.isSuccess).map(_.get)
//      if (vectors.length == 0){
//        model.transform("0")
//      } else {
//        Vectors.fromBreeze(vectors.map(_.toBreeze).reduceLeft(_ + _) / vectors.length.toDouble)
//      }
//    })
//
//    sqlContext.udf.register("getRatio", (d : DenseVector) => "%1.3f".format(d.values(1)).toDouble)
//
//    var result: Map[String, DataFrame] = Map()
//
//    val testDatas = hiveContect.table("dw.fct_cmmnt_pm")
//      .where("rate < 3 and good_cntnt is not null " +
//        "and length(trim(good_cntnt)) > 0 and " +
//        "to_date(cmmnt_date) between to_date('" + beginDate + "') and to_date('" + endDate + "')")
//    var testData = testDatas
//      .selectExpr(
//        "cmmnt_id",
//        "good_cntnt",
//        "word2avgVector(clean(good_cntnt)) as features",
//        "cmmnt_date",
//        "end_user_id",
//        "ordr_id",
//        "mrchnt_id",
//        "prod_id",
//        "pm_id",
//        "pltfm_id",
//        "rate").cache()
//
//    schemaString.foreach { label =>
//      if(label.startsWith("label")){
//        val trainingData = trainingDatas
//          .selectExpr("cmmnt_id", "word2avgVector(clean(good_cntnt))", label)
//          .toDF("cmmnt_id","features", "label")
//        val labelIndexer = new StringIndexer()
//          .setInputCol("label")
//          .setOutputCol("indexedLabel")
//          .fit(trainingData)
//        // Automatically identify categorical features, and index them.
//        // Set maxCategories so features with > 4 distinct values are treated as continuous.
//        val featureIndexer = new VectorIndexer()
//          .setInputCol("features")
//          .setOutputCol("indexedFeatures")
//          .setMaxCategories(2)
//          .fit(trainingData)
//
//        // Train a RandomForest model.
//        val rf = new RandomForestClassifier()
//          .setLabelCol("indexedLabel")
//          .setFeaturesCol("indexedFeatures")
//          .setMaxDepth(10)
//          .setNumTrees(40)
//          .setImpurity("entropy")//"variance", "entropy", "gini"
//          .setFeatureSubsetStrategy("onethird")//"auto", "all", "onethird", "sqrt", "log2"
//          .setMaxBins(32)
//        //.setSeed(123)
//
//        // Convert indexed labels back to original labels.
//        val labelConverter = new IndexToString()
//          .setInputCol("prediction")
//          .setOutputCol("predictedLabel")
//          .setLabels(labelIndexer.labels)
//
//        // Chain indexers and forest in a Pipeline
//        val pipeline = new Pipeline()
//          .setStages(Array(labelIndexer, featureIndexer, rf, labelConverter))
//
//        // Train model.  This also runs the indexers.
//        val pipelineModel = pipeline.fit(trainingData)
//        val predictions =  pipelineModel.transform(testData)
//        val probability = predictions
//          .selectExpr("cmmnt_id", "getRatio(probability)")
//          .toDF("cmmnt_id_" + label , label + "_pro")
//          .cache()
//        result += (label -> probability)
//      }
//    }
//
//    schemaString.foreach { name =>
//      if (name.startsWith("label")){
//        val toJoin = result.get(name).get
//        testData = testData.join(toJoin, testData("cmmnt_id") === toJoin("cmmnt_id_" + name))
//      }
//    }
//
//    val savedResult = testData.select(
//      "cmmnt_id",
//      "good_cntnt",
//      "end_user_id",
//      "ordr_id",
//      "mrchnt_id",
//      "prod_id",
//      "pm_id",
//      "pltfm_id",
//      "rate",
//      "label_jiage_pro",
//      "label_wuliu_pro",
//      "label_zhiliang_pro",
//      "label_jiahuo_pro",
//      "label_fuwu_pro",
//      "label_ganzhi_pro",
//      "label_longtong_pro",
//      "cmmnt_date")
//
//    savedResult
//      .write
//      .mode(SaveMode.Overwrite)
//      .saveAsTable("tmp.wym_tmp_comment_result")
//    sc.stop()
//
//  }
//}
