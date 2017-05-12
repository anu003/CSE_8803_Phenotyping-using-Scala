/**
 * @author Hang Su
 */
package edu.gatech.cse8803.features

import edu.gatech.cse8803.model.{LabResult, Medication, Diagnostic}
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._


object FeatureConstruction {

  /**
   * ((patient-id, feature-name), feature-value)
   */
  type FeatureTuple = ((String, String), Double)

  /**
   * Aggregate feature tuples from diagnostic with COUNT aggregation,
   * @param diagnostic RDD of diagnostic
   * @return RDD of feature tuples
   */
  def constructDiagnosticFeatureTuple(diagnostic: RDD[Diagnostic]): RDD[FeatureTuple] = {
    /**
     * TODO implement your own code here and remove existing
     * placeholder code
     */
    val res = diagnostic.map(r => ((r.patientID, r.code), 1.0)).reduceByKey(_ + _)
    res
  }

  /**
   * Aggregate feature tuples from medication with COUNT aggregation,
   * @param medication RDD of medication
   * @return RDD of feature tuples
   */
  def constructMedicationFeatureTuple(medication: RDD[Medication]): RDD[FeatureTuple] = {
    /**
     * TODO implement your own code here and remove existing
     * placeholder code
     */
    val res = medication.map(r => ((r.patientID, r.medicine), 1.0)).reduceByKey(_ + _)
    res
  }

  /**
   * Aggregate feature tuples from lab result, using AVERAGE aggregation
   * @param labResult RDD of lab result
   * @return RDD of feature tuples
   */
  def constructLabFeatureTuple(labResult: RDD[LabResult]): RDD[FeatureTuple] = {
    /**
     * TODO implement your own code here and remove existing
     * placeholder code
     */
    val sum = labResult.map(r => ((r.patientID, r.testName), r.value)).reduceByKey(_ + _)
    val count = labResult.map(r => ((r.patientID, r.testName), 1.0)).reduceByKey(_ + _)
    val res = sum.join(count).map(r => (r._1, r._2._1 / r._2._2))
    res
  }

  /**
   * Aggregate feature tuple from diagnostics with COUNT aggregation, but use code that is
   * available in the given set only and drop all others.
   * @param diagnostic RDD of diagnostics
   * @param candidateCode set of candidate code, filter diagnostics based on this set
   * @return RDD of feature tuples
   */
  def constructDiagnosticFeatureTuple(diagnostic: RDD[Diagnostic], candidateCode: Set[String]): RDD[FeatureTuple] = {
    /**
     * TODO implement your own code here and remove existing
     * placeholder code
     */
    val res = diagnostic.map(r => ((r.patientID, r.code), 1.0)).reduceByKey(_ + _).filter(r => (candidateCode.contains(r._1._2)))
    res
  }

  /**
   * Aggregate feature tuples from medication with COUNT aggregation, use medications from
   * given set only and drop all others.
   * @param medication RDD of diagnostics
   * @param candidateMedication set of candidate medication
   * @return RDD of feature tuples
   */
  def constructMedicationFeatureTuple(medication: RDD[Medication], candidateMedication: Set[String]): RDD[FeatureTuple] = {
    /**
     * TODO implement your own code here and remove existing
     * placeholder code
     */
    val res = medication.map(r => ((r.patientID, r.medicine), 1.0)).reduceByKey(_ + _).filter(r => (candidateMedication.contains(r._1._2)))
    res
  }


  /**
   * Aggregate feature tuples from lab result with AVERAGE aggregation, use lab from
   * given set of lab test names only and drop all others.
   * @param labResult RDD of lab result
   * @param candidateLab set of candidate lab test name
   * @return RDD of feature tuples
   */
  def constructLabFeatureTuple(labResult: RDD[LabResult], candidateLab: Set[String]): RDD[FeatureTuple] = {
    /**
     * TODO implement your own code here and remove existing
     * placeholder code
     */
    val sum = labResult.map(r => ((r.patientID, r.testName), r.value)).reduceByKey(_ + _)
    val count = labResult.map(r => ((r.patientID, r.testName), 1.0)).reduceByKey(_ + _)
    val res = sum.join(count).map(r => (r._1, r._2._1 / r._2._2)).filter(r => (candidateLab.contains(r._1._2)))
    res
  }


  /**
   * Given a feature tuples RDD, construct features in vector
   * format for each patient. feature name should be mapped
   * to some index and convert to sparse feature format.
   * @param sc SparkContext to run
   * @param feature RDD of input feature tuples
   * @return
   */
  def construct(sc: SparkContext, feature: RDD[FeatureTuple]): RDD[(String, Vector)] = {

    /** save for later usage */
    feature.cache()

    /** create a feature name to id map*/

    /** transform input feature */
    val names = feature.map(_._1._2).distinct()
    val count = names.count().toInt
    val index = names.zipWithIndex
    /**
     * Functions maybe helpful:
     *    collect
     *    groupByKey
     */

    /**
     * TODO implement your own code here and remove existing
     * placeholder code
     */
    val result = feature.map(r => (r._1._2, (r._1._1, r._2))).join(index).map(r => (r._2._1._1, (r._2._2.toInt, r._2._1._2)) )
      .groupByKey().map(r => (r._1, Vectors.sparse(count, r._2.map(x => x._1).toArray, r._2.map(x => x._2).toArray)))
    /*val result = sc.parallelize(Seq(("Patient-NO-1", Vectors.dense(1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0)),
                                    ("Patient-NO-2", Vectors.dense(1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0)),
                                    ("Patient-NO-3", Vectors.dense(1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0)),
                                    ("Patient-NO-4", Vectors.dense(1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0)),
                                    ("Patient-NO-5", Vectors.dense(1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0)),
                                    ("Patient-NO-6", Vectors.dense(1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0)),
                                    ("Patient-NO-7", Vectors.dense(1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0)),
                                    ("Patient-NO-8", Vectors.dense(1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0)),
                                    ("Patient-NO-9", Vectors.dense(1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0)),
                                    ("Patient-NO-10", Vectors.dense(1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0))))
    */
    //result.sortByKey(true).take(10).foreach(println)
    result
    /** The feature vectors returned can be sparse or dense. It is advisable to use sparse */
  }
}