package examples


import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by burness on 16/4/29.
  */
object SampleRDD {


  def main(args: Array[String]) {
    val rootLogger = Logger.getRootLogger
    rootLogger.setLevel(Level.WARN)
    val dataFile = getClass.getResource("/data/sample_binary_classification_data.txt").toString
    println(dataFile)
    run(dataFile)

  }

  def run(params: String) {
//    Logger.getRootLogger.setLevel(Level.WARN)
    val conf = new SparkConf().setAppName(s"SampledRDDs with $params").setMaster("local[4]")
    val sc = new SparkContext(conf)

    val fraction = 0.1 // fraction of data to sample

    val examples = MLUtils.loadLibSVMFile(sc, params)
    val numExamples = examples.count()
    if (numExamples == 0) {
      throw new RuntimeException("Error: Data file had no samples to load.")
    }
    println(s"Loaded data with $numExamples examples from file: ${params}")

    // Example: RDD.sample() and RDD.takeSample()
    val expectedSampleSize = (numExamples * fraction).toInt
    println(s"Sampling RDD using fraction $fraction.  Expected sample size = $expectedSampleSize.")
    val sampledRDD = examples.sample(withReplacement = true, fraction = fraction)
    println(s"  RDD.sample(): sample has ${sampledRDD.count()} examples")
    val sampledArray = examples.takeSample(withReplacement = true, num = expectedSampleSize)
    println(s"  RDD.takeSample(): sample has ${sampledArray.size} examples")

    println()

    // Example: RDD.sampleByKey() and RDD.sampleByKeyExact()
    val keyedRDD = examples.map { lp => (lp.label.toInt, lp.features) }
    println(s"  Keyed data using label (Int) as key ==> Orig")
    //  Count examples per label in original data.
    val keyCounts = keyedRDD.countByKey()

    //  Subsample, and count examples per label in sampled data. (approximate)
    val fractions = keyCounts.keys.map((_, fraction)).toMap
    fractions.foreach(println(_))
    val sampledByKeyRDD = keyedRDD.sampleByKey(withReplacement = true, fractions = fractions)
    val keyCountsB = sampledByKeyRDD.countByKey()
    val sizeB = keyCountsB.values.sum
    println(s"  Sampled $sizeB examples using approximate stratified sampling (by label)." +
      " ==> Approx Sample")

    //  Subsample, and count examples per label in sampled data. (approximate)
    val sampledByKeyRDDExact =
      keyedRDD.sampleByKeyExact(withReplacement = true, fractions = fractions)
    val keyCountsBExact = sampledByKeyRDDExact.countByKey()
    val sizeBExact = keyCountsBExact.values.sum
    println(s"  Sampled $sizeBExact examples using exact stratified sampling (by label)." +
      " ==> Exact Sample")

    //  Compare samples
    println(s"   \tFractions of examples with key")
    println(s"Key\tOrig\tApprox Sample\tExact Sample")
    keyCounts.keys.toSeq.sorted.foreach { key =>
      val origFrac = keyCounts(key) / numExamples.toDouble
      val approxFrac = if (sizeB != 0) {
        keyCountsB.getOrElse(key, 0L) / sizeB.toDouble
      } else {
        0
      }
      val exactFrac = if (sizeBExact != 0) {
        keyCountsBExact.getOrElse(key, 0L) / sizeBExact.toDouble
      } else {
        0
      }
      println(s"$key\t$origFrac\t$approxFrac\t$exactFrac")
    }

    sc.stop()
  }

}
