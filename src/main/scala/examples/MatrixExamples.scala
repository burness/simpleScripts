package examples

import org.apache.spark.mllib.linalg.{Matrix, Matrices}

/**
  * Created by burness on 16/4/29.
  */
object MatrixExamples {
  def matrix1: Unit ={
    val dm: Matrix = Matrices.dense(3, 2, Array(1.0, 3.0, 5.0, 2.0, 4.0, 6.0))
    val sm: Array[Double] = Matrices.sparse(3, 2, Array(0, 1, 3), Array(0, 2, 1), Array(9, 6, 8)).toArray
    println(s"dense matrix: $dm")
//    println(s"sparse matrix: $sm")
    sm.foreach(println(_))
  }

  def main(args: Array[String]) {
    matrix1
  }
}
