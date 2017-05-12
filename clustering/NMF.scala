package edu.gatech.cse8803.clustering

/**
  * @author Hang Su <hangsu@gatech.edu>
  */


import breeze.linalg.{DenseVector => BDV, DenseMatrix => BDM, sum}
import breeze.linalg._
import breeze.numerics._
import org.apache.hadoop.yarn.webapp.hamlet.HamletSpec.B
import org.apache.spark.mllib.linalg.{Vectors, Vector}
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.linalg.Matrices


object NMF {

  /**
   * Run NMF clustering 
   * @param V The original non-negative matrix 
   * @param k The number of clusters to be formed, also the number of cols in W and number of rows in H
   * @param maxIterations The maximum number of iterations to perform
   * @param convergenceTol The maximum change in error at which convergence occurs.
   * @return two matrixes W and H in RowMatrix and DenseMatrix format respectively 
   */
  def run(V: RowMatrix, k: Int, maxIterations: Int, convergenceTol: Double = 1e-4): (RowMatrix, BDM[Double]) = {

    /**
     * TODO 1: Implement your code here
     * Initialize W, H randomly 
     * Calculate the initial error (Euclidean distance between V and W * H)
     */

    var H = BDM.rand[Double](k, V.numCols().toInt)
    var W = new RowMatrix(V.rows.map(_ => BDV.rand[Double](k)).map(fromBreeze).cache)

    val WH = multiply(W,H)
    val startError = error(V,WH)
    println("Initial Error: "+startError)



    /**
     * TODO 2: Implement your code here
     * Iteratively update W, H in a parallel fashion until error falls below the tolerance value 
     * The updating equations are, 
     * H = H.* W^T^V ./ (W^T^W H)
     * W = W.* VH^T^ ./ (W H H^T^)
     */
    var newError = 0.0
    var prevError = 0.0
    var iters = 0
    println("                        ")
    do {
      prevError = newError

      val wsh = computeWTV(W,W)*H
      val wtv = computeWTV(W,V)

      val divis= wtv:/(wsh:+10.0e-9)
      H = H:*divis

      val preW=dotDiv(multiply(V,H.t),multiply(W,H*H.t))
      W=dotProd(W,preW)
      W.rows.cache.count

      val newMatrix = multiply(W,H)
      newError = error(V, newMatrix)
      println("Iterations: "+iters+", "+"current error: "+newError)
      iters += 1
    } while (iters < maxIterations && newError > convergenceTol)
    (W, H)
  }

    /** TODO: Remove the placeholder for return and replace with correct values */


  /**  
  * RECOMMENDED: Implement the helper functions if you needed
  * Below are recommended helper functions for matrix manipulation
  * For the implementation of the first three helper functions (with a null return), 
  * you can refer to dotProd and dotDiv whose implementation are provided
  */
  /**
  * Note:You can find some helper functions to convert vectors and matrices
  * from breeze library to mllib library and vice versa in package.scala
  */
  private def error(a:RowMatrix,b:RowMatrix):Double={
    val diff = new RowMatrix(a.rows.zip(b.rows).map{case (v1: Vector, v2: Vector) => toBreezeVector(v1) :- toBreezeVector(v2)}.map(fromBreeze))
    val s = dotProd(diff,diff).rows
    val err = s.map(x=> x.toArray.sum).reduce(_+_)
    err*0.5
  }

  /** compute the mutiplication of a RowMatrix and a dense matrix */
  def multiply(X: RowMatrix, d: BDM[Double]): RowMatrix = {
    val res = X.multiply(fromBreeze(d))
    res
  }

 /** get the dense matrix representation for a RowMatrix */
  def getDenseMatrix(X: RowMatrix): BDM[Double] = {
    null
  }

  /** matrix multiplication of W.t and V */
  def computeWTV(W: RowMatrix, V: RowMatrix): BDM[Double] = {
    val x= W.rows.zip(V.rows).map{f => val a= new BDM[Double](f._1.size,1,f._1.toArray)
        val b =  new BDM[Double](1,f._2.size,f._2.toArray)
        val answer: BDM[Double] = a * b
        answer
    }
    x.reduce(_+_)
  }

  /** dot product of two RowMatrixes */
  def dotProd(X: RowMatrix, Y: RowMatrix): RowMatrix = {
    val rows = X.rows.zip(Y.rows).map{case (v1: Vector, v2: Vector) =>
      toBreezeVector(v1) :* toBreezeVector(v2)
    }.map(fromBreeze)
    new RowMatrix(rows)
  }

  /** dot division of two RowMatrixes */
  def dotDiv(X: RowMatrix, Y: RowMatrix): RowMatrix = {
    val rows = X.rows.zip(Y.rows).map{case (v1: Vector, v2: Vector) =>
      toBreezeVector(v1) :/ toBreezeVector(v2).mapValues(_ + 10.0e-9)
    }.map(fromBreeze)
    new RowMatrix(rows)}
}